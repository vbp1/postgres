/*-------------------------------------------------------------------------
 *
 * dwb_recovery.c
 *	  Startup-time handling of the short-lived double write buffer ring:
 *	  the apply-pass that repairs torn data pages, and the durable
 *	  generation protocol around it.
 *
 * On every double_writes start (clean, unclean or cold) the durable
 * generation in pg_dwb/control is bumped BEFORE the ring opens for new
 * writes, so slots left behind by the previous run can never masquerade as
 * current after a future crash.  Order: read G -> (RING_CLEAN not set)
 * apply-pass over generation G + fsync -> durable control.generation := G+1
 * -> open ring.  Starts in the other modes leave the ring untouched,
 * except that a restored base backup's ring contents are discarded.
 *
 * The apply-pass runs before WAL replay and repairs the data files
 * directly: a candidate slot must carry a valid meta_crc, the current
 * generation and a valid image_crc; candidates are deduplicated per page
 * keeping the highest LSN, and a page is rewritten from its slot copy when
 * the on-disk version fails verification or carries an older LSN.  This is
 * the only repair path — the runtime read path never consults the ring —
 * and it covers pages replay never reads, such as hint-bit-only pages
 * logged as XLOG_FPI_FOR_HINT without an image.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "pgstat.h"
#include "storage/bufpage.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/wait_event.h"

/* dedup table entry: the best candidate slot seen for one page */
typedef struct DWBApplyCandidate
{
	BufferTag	tag;			/* hash key */
	XLogRecPtr	lsn;
	uint64		batch_id;		/* tie-breaker for equal LSNs: equal-LSN
								 * copies of one generation can differ only in
								 * hint bits, so either is a valid redo base
								 * and the id (monotonic in batch-open order,
								 * see DWBBatchHeader.batch_id) just makes the
								 * pick deterministic */
	uint32		batch_idx;
	uint32		slot_idx;
	pg_crc32c	image_crc;		/* revalidates the image on re-read */
} DWBApplyCandidate;

/* one fork the apply-pass has written to and must fsync (HASH_BLOBS key) */
typedef struct DWBAppliedFork
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
} DWBAppliedFork;

StaticAssertDecl(sizeof(DWBAppliedFork) ==
				 sizeof(RelFileLocator) + sizeof(ForkNumber),
				 "DWBAppliedFork has padding; unsafe as a HASH_BLOBS key");

static XLogRecPtr DWBApplyPass(const DWBControlFileData *control);
static bool DWBWipeRing(void);
static bool DWBRingIsQuiescent(void);

/*
 * Read one batch file's page image into an aligned buffer.  The apply-pass
 * variant of DWBReadSlotImage: geometry comes from the on-disk control
 * file, not the GUCs, and failures are plain ERRORs (startup context, no
 * critical section).
 */
static void
DWBApplyReadImage(int fd, const char *path, uint32 batch_pages,
				  uint32 slot_idx, char *dst)
{
	off_t		off = DWBMetaRegionSize(batch_pages) +
		(off_t) slot_idx * BLCKSZ;
	ssize_t		r;

	pgstat_report_wait_start(WAIT_EVENT_DWB_BATCH_READ);
	r = pg_pread(fd, dst, BLCKSZ, off);
	pgstat_report_wait_end();
	if (r != BLCKSZ)
	{
		if (r < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read file \"%s\": read %zd of %d",
						path, r, BLCKSZ)));
	}
}

/*
 * The eager repair pass (3.8 of the design plan).  Scans the whole ring,
 * selects candidate slots of the current generation, dedups them per page
 * and rewrites the data pages that are torn or older than their copy.
 * Returns the highest LSN actually applied, or InvalidXLogRecPtr.
 *
 * Runs in the startup process before WAL replay, strictly read-only with
 * respect to the ring: a crash in the middle leaves control.generation
 * untouched and the next start simply repeats the pass (already-repaired
 * pages then carry disk_lsn >= slot_lsn and are skipped).
 */
static XLogRecPtr
DWBApplyPass(const DWBControlFileData *control)
{
	Size		meta_region = DWBMetaRegionSize(control->batch_pages);
	char	   *meta_buf = palloc(meta_region);
	char	   *image_buf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, 0);
	char	   *disk_buf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, 0);
	HASHCTL		info;
	HTAB	   *candidates;
	HTAB	   *applied_forks;
	HASH_SEQ_STATUS seq;
	DWBApplyCandidate *cand;
	DWBAppliedFork *fork;
	int			n_candidates = 0;
	int			n_applied = 0;
	XLogRecPtr	applied_upto = InvalidXLogRecPtr;

	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(DWBApplyCandidate);
	candidates = hash_create("DWB apply-pass candidates",
							 (long) control->num_batches * control->batch_pages,
							 &info,
							 HASH_ELEM | HASH_BLOBS);

	/* forks written to, for the final fsync sweep */
	info.keysize = sizeof(DWBAppliedFork);
	info.entrysize = sizeof(DWBAppliedFork);
	applied_forks = hash_create("DWB apply-pass forks", 16, &info,
								HASH_ELEM | HASH_BLOBS);

	/*
	 * Scan every batch file with the geometry recorded in control.  A batch
	 * or slot that fails any local validity check is skipped, not an error:
	 * by the write protocol a torn or half-written slot means the
	 * corresponding data-file write never started, so the disk holds an older
	 * durable version that replay can advance from.
	 */
	for (uint32 batch_idx = 0; batch_idx < control->num_batches; batch_idx++)
	{
		char		path[MAXPGPATH];
		int			fd;
		ssize_t		r;
		DWBBatchHeader hdr;
		DWSlotMeta *metas;

		DWBBatchFilePath(path, batch_idx);
		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));

		pgstat_report_wait_start(WAIT_EVENT_DWB_BATCH_READ);
		r = pg_pread(fd, meta_buf, meta_region, 0);
		pgstat_report_wait_end();
		if (r != (ssize_t) meta_region)
		{
			if (r < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %zd of %zu",
							path, r, meta_region)));
		}

		memcpy(&hdr, meta_buf, sizeof(DWBBatchHeader));
		if (hdr.magic != DWB_BATCH_MAGIC ||
			hdr.version != DWB_VERSION ||
			hdr.n_slots > control->batch_pages ||
			!EQ_CRC32C(hdr.crc, DWBBatchHeaderCrc(&hdr)))
		{
			/* never sealed, or torn mid-write: nothing durable depends on it */
			CloseTransientFile(fd);
			continue;
		}

		metas = (DWSlotMeta *) (meta_buf + sizeof(DWBBatchHeader));
		for (uint32 slot_idx = 0; slot_idx < hdr.n_slots; slot_idx++)
		{
			DWSlotMeta *meta = &metas[slot_idx];
			DWBApplyCandidate *entry;
			bool		found;

			if (!EQ_CRC32C(meta->meta_crc, DWBSlotMetaCrc(meta)))
				continue;
			if (meta->generation != control->generation)
				continue;
			if (meta->flags & DWB_SLOT_ABORTED)
				continue;

			DWBApplyReadImage(fd, path, control->batch_pages, slot_idx,
							  image_buf);
			if (!EQ_CRC32C(meta->image_crc, DWBImageCrc(image_buf)))
				continue;

			/*
			 * Keep the highest LSN; on equal LSNs the higher batch_id wins
			 * (equal LSNs with different contents are real: a re-flush after
			 * hint-bit-only changes does not move the LSN — but such copies
			 * differ only in hint bits, so any of them is a valid redo base
			 * and the id merely makes the pick deterministic; with two writer
			 * classes a later flush can even land in an earlier- opened
			 * batch).  Within one batch the later slot wins by plain
			 * overwrite, matching the order the slots were filled in.
			 */
			n_candidates++;
			entry = hash_search(candidates, &meta->tag, HASH_ENTER, &found);
			if (found &&
				(entry->lsn > meta->page_lsn ||
				 (entry->lsn == meta->page_lsn &&
				  entry->batch_id > hdr.batch_id)))
				continue;
			entry->lsn = meta->page_lsn;
			entry->batch_id = hdr.batch_id;
			entry->batch_idx = batch_idx;
			entry->slot_idx = slot_idx;
			entry->image_crc = meta->image_crc;
		}

		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m", path)));
	}

	/*
	 * Repair.  For each winning candidate, rewrite the data page when the
	 * on-disk version fails verification or is older than the copy.  A
	 * dropped relation (no file) or a truncated one (block beyond EOF) is
	 * skipped: there is nothing to repair and replay or a replayed truncate
	 * drives the final state.
	 */
	hash_seq_init(&seq, candidates);
	while ((cand = hash_seq_search(&seq)) != NULL)
	{
		RelFileLocator rlocator = BufTagGetRelFileLocator(&cand->tag);
		ForkNumber	forknum = BufTagGetForkNum(&cand->tag);
		BlockNumber blkno = cand->tag.blockNum;
		SMgrRelation reln;
		char		path[MAXPGPATH];
		int			fd;
		DWBAppliedFork fkey;

		reln = smgropen(rlocator, INVALID_PROC_NUMBER);
		if (!smgrexists(reln, forknum))
			continue;
		if (blkno >= smgrnblocks(reln, forknum))
			continue;

		smgrread(reln, forknum, blkno, disk_buf);

		/*
		 * A "new" page (empty header) is never repaired.  For a candidate
		 * holding an initialized image, an empty on-disk header means the
		 * covered write's first sector never reached disk and the block had
		 * never held an initialized page before — its init record therefore
		 * lies after the last checkpoint, and replay recreates the page
		 * without reading the current contents.  (A staged image can itself
		 * be all-zero — FlushBuffer may flush a still-new page — but such
		 * a copy carries LSN 0: skipping it here changes nothing, and on the
		 * repair branch below it would merely complete an intended zeroing.)
		 * The skip is also required for correctness in the other direction:
		 * after a truncate + re-extend within one generation the ring can
		 * hold a pre-truncate copy of this block, and the re-extended zeroed
		 * page (LSN 0) would lose the LSN comparison below to that stale
		 * image, which nothing would then replay over.
		 */
		if (PageIsNew((Page) disk_buf))
			continue;

		if (PageIsVerified((Page) disk_buf, blkno, PIV_LOG_LOG, NULL) &&
			PageGetLSN((Page) disk_buf) >= cand->lsn)
			continue;

		/* re-read the winning copy; the scan buffer is long overwritten */
		DWBBatchFilePath(path, (int) cand->batch_idx);
		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		DWBApplyReadImage(fd, path, control->batch_pages, cand->slot_idx,
						  image_buf);
		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m", path)));

		/*
		 * The image was CRC-checked during the scan, but this is a second
		 * physical read; a divergence means the storage returned different
		 * bytes twice, and writing them over a data page would defeat the
		 * pass's whole purpose.
		 */
		if (!EQ_CRC32C(cand->image_crc, DWBImageCrc(image_buf)))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("page image in file \"%s\" slot %u failed verification on re-read",
							path, cand->slot_idx)));

		elog(DEBUG1, "double write buffer recovery: restoring page %u of relation %u/%u/%u fork %d from batch %u slot %u (LSN %X/%X)",
			 blkno, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
			 forknum, cand->batch_idx, cand->slot_idx,
			 LSN_FORMAT_ARGS(cand->lsn));

		smgrwrite(reln, forknum, blkno, image_buf, true);

		n_applied++;
		if (cand->lsn > applied_upto)
			applied_upto = cand->lsn;

		fkey.rlocator = rlocator;
		fkey.forknum = forknum;
		(void) hash_search(applied_forks, &fkey, HASH_ENTER, NULL);
	}

	/* make the repairs durable before the generation moves on */
	hash_seq_init(&seq, applied_forks);
	while ((fork = hash_seq_search(&seq)) != NULL)
		smgrimmedsync(smgropen(fork->rlocator, INVALID_PROC_NUMBER),
					  fork->forknum);

	ereport(LOG,
			(errmsg("double write buffer recovery: %d of %d candidate pages restored, generation " UINT64_FORMAT,
					n_applied, n_candidates, control->generation)));

	hash_destroy(candidates);
	hash_destroy(applied_forks);
	pfree(meta_buf);
	pfree(image_buf);
	pfree(disk_buf);

	return applied_upto;
}

/*
 * Durably remove the contents of pg_dwb/, keeping the directory (or the
 * symlink to it) in place, and report whether there was anything to
 * remove — a restored backup normally ships pg_dwb/ empty, and the
 * callers must not claim to have discarded ring contents that never
 * existed.  Callers: the restored-backup branches (a shipped ring must
 * not survive), the geometry change, and the cold-create sweep that
 * clears leftovers of an interrupted wipe.
 *
 * The control file goes first, durably: a crash in the middle of the batch
 * sweep must not leave a readable control beside missing batch files, or a
 * retried apply-pass would hard-fail on the ENOENT forever.  With the
 * control gone first, a retry takes the cold-create path instead, which is
 * correct for every caller, since the apply-pass, if one was needed, ran
 * to completion before any wipe starts.
 */
static bool
DWBWipeRing(void)
{
	struct stat st;
	DIR		   *dir;
	struct dirent *de;
	bool		had_contents = false;

	if (lstat(DWB_DIR, &st) < 0)
	{
		if (errno == ENOENT)
			return false;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat directory \"%s\": %m", DWB_DIR)));
	}

	dir = AllocateDir(DWB_DIR);
	while ((de = ReadDir(dir, DWB_DIR)) != NULL)
	{
		if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0)
		{
			had_contents = true;
			break;
		}
	}
	FreeDir(dir);

	if (!had_contents)
		return false;

	if (unlink(DWB_CONTROL_FILE) < 0 && errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						DWB_CONTROL_FILE)));
	fsync_fname(DWB_DIR, true);

	if (!rmtree(DWB_DIR, false))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not remove contents of directory \"%s\"",
						DWB_DIR)));
	fsync_fname(DWB_DIR, true);

	return true;
}

/*
 * Called from StartupXLOG before WAL recovery is initialized.  Creates or
 * validates the ring, enforces data checksums, repairs torn data pages
 * from the ring unless it was cleanly closed, performs the durable
 * generation bump and publishes ring_generation for the leaders' slot
 * metas.  Returns the highest LSN the apply-pass wrote to a data file, or
 * InvalidXLogRecPtr.
 *
 * Whether the apply-pass must run is decided by the ring's own RING_CLEAN
 * marker alone, never by the pg_control state.  The marker is the exact
 * certificate: it is set only after full retirement (so while it is set,
 * no slot covers a data write that has not reached disk) and cleared
 * before the ring reopens (so slots of the clearing run are covered until
 * the next clean shutdown re-sets it).  pg_control can be both cleaner and
 * dirtier than the ring: a standby's shutdown restartpoint can be skipped
 * entirely, leaving retirement fsyncs pending behind a clean pg_control —
 * and a crash under an interim full_pages/off run (which touches neither
 * the marker nor the generation) leaves an unclean pg_control over a fully
 * retired ring whose stale slots still match the current generation, where
 * an apply would resurrect ancient pages over blocks torn long after the
 * ring was closed.
 *
 * restoring_backup means the data directory is a restored base backup
 * (backup_label present, or pg_control still carrying backupStartPoint
 * after a crash mid-backup-recovery).  A ring found in that case — shipped
 * by a third-party backup tool, or this server's own from a crashed
 * backup-recovery run — must not be applied: its slots carry the restored
 * control's own generation, and the restored data files are legitimately
 * older than the slot copies, so both staleness defences pass — an
 * unguarded apply would push pages from the future of the backup into a
 * PITR target.  The WAL of the backup window carries forced full page
 * images instead, so the ring is not needed for this recovery; it is
 * wiped and recreated cold.
 */
XLogRecPtr
DWBStartup(bool restoring_backup)
{
	DWBControlFileData control;
	XLogRecPtr	applied_upto = InvalidXLogRecPtr;
	bool		created = false;
	bool		corrupt;
	bool		need_apply;

	if (!DWBIsEnabled())
	{
		/*
		 * The most dangerous mode must not be the quietest one: with "off"
		 * neither page images nor the ring protect data files, and the legacy
		 * full_page_writes GUC may still read "on".
		 */
		if (io_torn_pages_protection == DWB_PROTECT_OFF)
			ereport(LOG,
					(errmsg("torn page protection is disabled (io_torn_pages_protection = \"off\")"),
					 errdetail("WAL carries no full page images; \"full_page_writes\" is ignored in this mode.")));

		if (restoring_backup)
		{
			/*
			 * A foreign ring shipped in a restored backup is dangerous even
			 * lying dormant: a much later switch to double_writes would find
			 * it with a plausible control file.  Discard it now.
			 */
			if (DWBWipeRing())
				ereport(LOG,
						(errmsg("discarding double write buffer ring contents restored from a base backup")));
			return InvalidXLogRecPtr;
		}

		/*
		 * Mode-downgrade guard: a ring that was not cleanly closed may hold
		 * repairs of torn data pages that only a double_writes start can
		 * apply.  This intentionally does not consider pg_control: a clean
		 * server shutdown that failed to retire the ring (e.g. a soft fsync
		 * failure under data_sync_retry) leaves RING_CLEAN unset, and the
		 * pending data writes it covers are exactly as unprotected.
		 */
		if (DWBReadControlFile(&control, true, &corrupt))
		{
			if ((control.flags & DWB_CONTROL_RING_CLEAN) == 0)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("the double write buffer ring was not cleanly shut down, cannot start with \"io_torn_pages_protection=%s\"",
								DWBProtectionModeName(io_torn_pages_protection)),
						 errdetail("The ring in \"%s\" may hold repairs of torn data pages that have not been applied.",
								   DWB_DIR),
						 errhint("Start the server once with \"io_torn_pages_protection=double_writes\" and shut it down cleanly, or remove \"%s\" if you accept the risk of torn data pages.",
								 DWB_DIR)));
		}
		else if (corrupt)
		{
			/*
			 * An unreadable ring state must not block modes that never touch
			 * the ring with a bare low-level error: name the way out.  (A
			 * double_writes start would refuse too, so the only cure is
			 * removal.)
			 */
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the double write buffer ring state could not be validated, cannot start with \"io_torn_pages_protection=%s\"",
							DWBProtectionModeName(io_torn_pages_protection)),
					 errdetail("The control file in \"%s\" is unreadable or corrupt, and the ring may hold repairs of torn data pages that have not been applied.",
							   DWB_DIR),
					 errhint("Remove \"%s\" if you accept the risk of torn data pages.",
							 DWB_DIR)));
		}
		return InvalidXLogRecPtr;
	}

	/* 3.1.7: a torn page with an intact header must never pass unnoticed */
	if (!DataChecksumsEnabled())
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("io_torn_pages_protection = \"double_writes\" requires data checksums"),
				 errhint("Enable checksums with initdb -k or pg_checksums.")));

	if (restoring_backup)
	{
		if (DWBWipeRing())
			ereport(LOG,
					(errmsg("discarding double write buffer ring contents restored from a base backup")));
	}

	if (!DWBReadControlFile(&control, true, NULL))
	{
		/*
		 * Cold start: no ring yet.  Sweep the directory first — an
		 * interrupted wipe can leave batch files behind after the control
		 * file is gone.
		 */
		(void) DWBWipeRing();
		DWBCreateRing();
		created = true;
		if (!DWBReadControlFile(&control, false, NULL))
			pg_unreachable();
	}

	/*
	 * The RING_CLEAN marker alone decides (see the DWBStartup comment above
	 * for why pg_control must not weigh in).  A freshly created ring has
	 * nothing to apply even though its marker is unset.
	 */
	need_apply = !created &&
		(control.flags & DWB_CONTROL_RING_CLEAN) == 0;

	if (!created &&
		(control.num_batches != (uint32) dwb_num_batches ||
		 control.batch_pages != (uint32) dwb_batch_pages))
	{
		/*
		 * The geometry GUCs changed.  The old ring must still be applied
		 * first — its batch files follow the recorded geometry — and only
		 * then can the ring be recreated under the new one.  The fresh
		 * control restarts the generation from zero, which is safe exactly
		 * because the wipe left no slot behind.
		 */
		if (need_apply)
			applied_upto = DWBApplyPass(&control);
		ereport(LOG,
				(errmsg("recreating double write buffer ring: geometry changed from %u batches of %u pages to %d batches of %d pages",
						control.num_batches, control.batch_pages,
						dwb_num_batches, dwb_batch_pages)));
		(void) DWBWipeRing();
		DWBCreateRing();
		if (!DWBReadControlFile(&control, false, NULL))
			pg_unreachable();
	}
	else if (need_apply)
		applied_upto = DWBApplyPass(&control);

	control.generation++;
	control.flags &= ~DWB_CONTROL_RING_CLEAN;
	control.crc = DWBControlCrc(&control);
	DWBWriteControlFile(&control);

	DWBCtl->ring_generation = control.generation;

	ereport(LOG,
			(errmsg("double write buffer ring opened: %d batches of %d pages, generation " UINT64_FORMAT,
					dwb_num_batches, dwb_batch_pages, control.generation)));

	return applied_upto;
}

/*
 * True when no on-disk slot covers a data write that could still be
 * pending: every batch is FREE, or ALLOCATED (an open batch whose staged
 * copies live in shared memory only — nothing of it has been written to
 * the ring files, and its file still holds the fully retired slots of the
 * previous incarnation).
 */
static bool
DWBRingIsQuiescent(void)
{
	for (int i = 0; i < dwb_num_batches; i++)
	{
		DWBatchState state = DWBGetBatchState(i);

		if (state != DWB_FREE && state != DWB_ALLOCATED)
			return false;
	}
	return true;
}

/*
 * Called at the tail of a clean shutdown, after the shutdown checkpoint or
 * restartpoint.  Retires whatever the checkpoint left behind (a shutdown
 * restartpoint can be skipped entirely, leaving segment fsyncs pending)
 * and then sets RING_CLEAN in the ring's control file, entitling the next
 * start to skip the apply-pass and legalizing a start under a different
 * io_torn_pages_protection mode.  If the ring cannot be fully retired the
 * marker simply stays unset — the next start applies the ring, which is
 * always safe.
 */
void
DWBMarkCleanShutdown(void)
{
	DWBControlFileData control;

	if (!DWBIsEnabled())
		return;

	if (!DWBRingIsQuiescent())
	{
		DWBRetireAllSync();
		if (!DWBRingIsQuiescent())
		{
			ereport(LOG,
					(errmsg("double write buffer ring could not be fully retired; not marking it cleanly shut down")));
			return;
		}
	}

	if (!DWBReadControlFile(&control, false, NULL))
		pg_unreachable();
	control.flags |= DWB_CONTROL_RING_CLEAN;
	control.crc = DWBControlCrc(&control);
	DWBWriteControlFile(&control);
}
