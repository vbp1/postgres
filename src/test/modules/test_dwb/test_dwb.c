/*--------------------------------------------------------------------------
 *
 * test_dwb.c
 *		Test module for the short-lived double write buffer.
 *
 * Drives the DWB batch state machine directly with synthetic page tags and
 * images — independently of the FlushBuffer integration — and validates
 * the on-disk ring format independently of the server-side write path.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_dwb/test_dwb.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "varatt.h"

PG_MODULE_MAGIC;

static void
check_dwb_enabled(void)
{
	if (!DWBIsEnabled())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("test_dwb requires io_torn_pages_protection = \"double_writes\"")));
}

static void
wait_and_release(DWBSlotRef *refs, int nrefs)
{
	for (int i = 0; i < nrefs; i++)
	{
		DWBWaitBatchFsynced(&refs[i]);
		DWBReleaseSlot(&refs[i]);
	}
}

/* build a MAIN_FORKNUM page tag for (relnumber, blkno) in database dboid */
static BufferTag
make_tag(Oid dboid, Oid relnumber, BlockNumber blkno)
{
	BufferTag	tag;
	RelFileLocator rlocator;

	rlocator.spcOid = DEFAULTTABLESPACE_OID;
	rlocator.dbOid = dboid;
	rlocator.relNumber = relnumber;
	InitBufferTag(&tag, &rlocator, MAIN_FORKNUM, blkno);
	return tag;
}

/*
 * Acquire one slot for the tag, publish the image, seal the batch and wait
 * until it is durable: the shared prologue of the single-page scenarios.
 */
static void
stage_one_page(const BufferTag *tag, const char *image, XLogRecPtr page_lsn,
			   bool use_resowner, DWBSlotRef *ref)
{
	DWBAcquireSlot(tag, DWB_WCLASS_EVICTION, use_resowner, ref);
	DWBPublishImage(ref, image, page_lsn);
	if (!DWBTrySealBatch(ref->batch_idx))
		ereport(ERROR, (errmsg("could not seal the batch under test")));
	DWBWaitBatchFsynced(ref);
}

/*
 * Acquire (and optionally publish) npages slots and return with the refs
 * still pending: the shared body of the leak / abort-release scenarios.
 */
static void
leak_refs(int npages, bool do_publish, bool use_resowner, Oid relnumber)
{
	static char page[BLCKSZ];

	/* stay below the batch size so this backend never seals as leader */
	if (npages < 1 || npages >= dwb_batch_pages)
		ereport(ERROR, (errmsg("npages out of range")));

	for (int i = 0; i < npages; i++)
	{
		BufferTag	tag = make_tag(1, relnumber, (BlockNumber) i);
		DWBSlotRef	ref;

		DWBAcquireSlot(&tag, DWB_WCLASS_EVICTION, use_resowner, &ref);
		if (do_publish)
		{
			memset(page, 'L', BLCKSZ);
			DWBPublishImage(&ref, page, (XLogRecPtr) 0x2000000 + i);
		}
	}
}

/*
 * One full write cycle over npages synthetic pages: acquire, publish,
 * seal (by overflow or forced), wait durable, release, retire.
 * Returns the number of batches retired.
 */
static int
dwb_cycle_internal(int npages)
{
	DWBSlotRef	refs[DWB_BATCH_MAX_PAGES];
	int			nrefs = 0;
	int			last_batch = -1;
	int			retired = 0;
	static char page[BLCKSZ];

	for (int i = 0; i < npages; i++)
	{
		BufferTag	tag = make_tag(1, 90000 + (i % 3), (BlockNumber) i);
		DWBSlotRef	ref;

		CHECK_FOR_INTERRUPTS();

		DWBAcquireSlot(&tag, DWB_WCLASS_EVICTION, false, &ref);

		/*
		 * A batch switch means the previous batch overflowed and was sealed
		 * by its overflow writer: its refs can be waited for and dropped.
		 */
		if (last_batch >= 0 && ref.batch_idx != last_batch)
		{
			wait_and_release(refs, nrefs);
			nrefs = 0;
			retired += DWBRetireAllSync();
		}
		last_batch = ref.batch_idx;

		memset(page, 'A' + (i % 26), BLCKSZ);
		DWBPublishImage(&ref, page, (XLogRecPtr) 0x1000000 + i);
		refs[nrefs++] = ref;
	}

	/* seal the tail batch and drain */
	DWBForceSealOpenBatch(DWB_WCLASS_EVICTION);
	wait_and_release(refs, nrefs);
	retired += DWBRetireAllSync();
	return retired;
}

PG_FUNCTION_INFO_V1(test_dwb_cycle);
Datum
test_dwb_cycle(PG_FUNCTION_ARGS)
{
	int			npages = PG_GETARG_INT32(0);

	check_dwb_enabled();
	if (npages < 1 || npages > 100000)
		ereport(ERROR, (errmsg("npages out of range")));

	PG_RETURN_INT32(dwb_cycle_internal(npages));
}

PG_FUNCTION_INFO_V1(test_dwb_stress);
Datum
test_dwb_stress(PG_FUNCTION_ARGS)
{
	int			loops = PG_GETARG_INT32(0);
	int			npages = PG_GETARG_INT32(1);

	check_dwb_enabled();
	for (int i = 0; i < loops; i++)
	{
		CHECK_FOR_INTERRUPTS();
		(void) dwb_cycle_internal(npages);
	}
	PG_RETURN_VOID();
}

/*
 * Validate the on-disk ring the way the apply-pass will: read every batch
 * file, check header, then count slots passing meta_crc (+ generation if
 * current_only) + flags + image_crc.  With have_filter, count only slots
 * whose tag belongs to the given relation (any generation) — used to prove
 * that pages of a relation never entered the ring.
 */
static int
count_ring_slots(bool current_only, bool have_filter, Oid relnumber)
{
	DWBControlFileData control;
	Size		meta_region;
	DWSlotMeta *metas;
	char	   *image;
	int			valid = 0;

	if (!DWBReadControlFile(&control, false, NULL))
		pg_unreachable();

	meta_region = DWBMetaRegionSize(control.batch_pages);
	metas = palloc(control.batch_pages * sizeof(DWSlotMeta));
	image = palloc(BLCKSZ);

	for (uint32 b = 0; b < control.num_batches; b++)
	{
		char		path[MAXPGPATH];
		int			fd;
		ssize_t		r;
		DWBBatchHeader hdr;

		snprintf(path, MAXPGPATH, DWB_DIR "/batch_%04u", b);
		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));

		errno = 0;
		r = pg_pread(fd, &hdr, sizeof(hdr), 0);
		if (r != sizeof(hdr))
		{
			if (r < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("short read of header in \"%s\"", path)));
		}

		/* an all-zero (never written) batch fails the header check */
		if (hdr.magic == DWB_BATCH_MAGIC &&
			EQ_CRC32C(hdr.crc, DWBBatchHeaderCrc(&hdr)))
		{
			ssize_t		nbytes = hdr.n_slots * sizeof(DWSlotMeta);

			/*
			 * A CRC-valid header with out-of-range n_slots cannot happen
			 * under the startup geometry check; report the anomaly instead of
			 * silently contributing zero slots.
			 */
			if (hdr.n_slots > control.batch_pages)
			{
				ereport(WARNING,
						(errmsg("batch file \"%s\" has out-of-range n_slots %u",
								path, hdr.n_slots)));
				nbytes = -1;
			}

			if (nbytes >= 0)
			{
				errno = 0;
				r = pg_pread(fd, metas, nbytes, sizeof(DWBBatchHeader));
				if (r != nbytes)
				{
					if (r < 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not read metas in \"%s\": %m",
										path)));
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("short read of metas in \"%s\"", path)));
				}
			}

			for (uint32 i = 0; nbytes >= 0 && i < hdr.n_slots; i++)
			{
				DWSlotMeta *meta = &metas[i];

				if (!EQ_CRC32C(meta->meta_crc, DWBSlotMetaCrc(meta)))
					continue;
				if (meta->flags & DWB_SLOT_ABORTED)
					continue;
				if (current_only && meta->generation != control.generation)
					continue;
				if (have_filter &&
					BufTagGetRelNumber(&meta->tag) != (RelFileNumber) relnumber)
					continue;

				errno = 0;
				r = pg_pread(fd, image, BLCKSZ,
							 meta_region + (off_t) i * BLCKSZ);
				if (r != BLCKSZ)
				{
					if (r < 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not read image in \"%s\": %m",
										path)));
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("short read of image in \"%s\"", path)));
				}
				if (!EQ_CRC32C(meta->image_crc, DWBImageCrc(image)))
					continue;
				valid++;
			}
		}

		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m", path)));
	}

	pfree(metas);
	pfree(image);
	return valid;
}

PG_FUNCTION_INFO_V1(test_dwb_ring_slots);
Datum
test_dwb_ring_slots(PG_FUNCTION_ARGS)
{
	bool		current_only = PG_GETARG_BOOL(0);

	check_dwb_enabled();
	PG_RETURN_INT32(count_ring_slots(current_only, false, InvalidOid));
}

/* slots of one relation, any generation: 0 = never entered the ring */
PG_FUNCTION_INFO_V1(test_dwb_ring_rel_slots);
Datum
test_dwb_ring_rel_slots(PG_FUNCTION_ARGS)
{
	Oid			relnumber = PG_GETARG_OID(0);

	check_dwb_enabled();
	PG_RETURN_INT32(count_ring_slots(false, true, relnumber));
}

/*
 * Cumulative count of DWBOpenNewBatch iterations that went to sleep.  The
 * anti-spin regression in 003 asserts that a waiter parked on an unchanged
 * full ring accrues these at the 1s sleep-timeout pace instead of busily
 * rotating a wake token.
 */
PG_FUNCTION_INFO_V1(test_dwb_ring_wait_retries);
Datum
test_dwb_ring_wait_retries(PG_FUNCTION_ARGS)
{
	check_dwb_enabled();
	PG_RETURN_INT64((int64) pg_atomic_read_u64(&DWBCtl->ring_wait_retries));
}

PG_FUNCTION_INFO_V1(test_dwb_states);
Datum
test_dwb_states(PG_FUNCTION_ARGS)
{
	int			counts[DWB_OOM_RETIRING + 1] = {0};
	StringInfoData buf;

	check_dwb_enabled();
	for (int i = 0; i < dwb_num_batches; i++)
	{
		DWBatchState state = DWBGetBatchState(i);

		if (state <= DWB_OOM_RETIRING)
			counts[state]++;
	}

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "free=%d allocated=%d sealed=%d written=%d fsynced=%d data_written=%d retiring=%d",
					 counts[DWB_FREE], counts[DWB_ALLOCATED],
					 counts[DWB_SEALED], counts[DWB_WRITTEN],
					 counts[DWB_FSYNCED], counts[DWB_DATA_WRITTEN],
					 counts[DWB_RETIRING] + counts[DWB_OOM_RETIRING]);
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Acquire (and optionally publish) npages slots and return WITHOUT
 * releasing them: the refs stay pending, so closing the session exercises
 * DWBProcExit.  Unpublished slots get poisoned; published ownerless refs
 * just drop their batch ref (the repair write is reserved for
 * ResourceOwner-attached refs), leaving the batch completable by a later
 * seal.
 */
PG_FUNCTION_INFO_V1(test_dwb_leak);
Datum
test_dwb_leak(PG_FUNCTION_ARGS)
{
	int			npages = PG_GETARG_INT32(0);
	bool		do_publish = PG_GETARG_BOOL(1);

	check_dwb_enabled();
	leak_refs(npages, do_publish, false, 91000);
	PG_RETURN_VOID();
}

/*
 * Occupy the ring without blocking: acquire and publish slots until no
 * openable FREE batch remains and the open batch is full, keeping every ref
 * (the refs die with the session).  Sets up ring exhaustion for the
 * backpressure tests.  The sliced reserves (dwb.h) shape where each class
 * stops: an eviction fill may not consume the bottom DWB_BG_RESERVE FREE
 * batches, a background fill from a fresh ring stops above the middle
 * DWB_EVICT_RESERVE slice.  Meant for dwb_retire_workers = 0, where nothing
 * seals or retires behind our back.  Returns the number of slots taken.
 */
PG_FUNCTION_INFO_V1(test_dwb_fill_ring);
Datum
test_dwb_fill_ring(PG_FUNCTION_ARGS)
{
	bool		background = PG_GETARG_BOOL(0);
	int			wclass = background ? DWB_WCLASS_BACKGROUND : DWB_WCLASS_EVICTION;
	int			taken = 0;
	static char page[BLCKSZ];

	check_dwb_enabled();

	for (;;)
	{
		int			nfree = 0;
		bool		can_open;
		uint32		open_idx;
		BufferTag	tag;
		DWBSlotRef	ref;

		CHECK_FOR_INTERRUPTS();

		/* hard bound of the backend-local ref array */
		if (taken >= 2 * DWB_BATCH_MAX_PAGES - 1)
			break;

		for (int i = 0; i < dwb_num_batches; i++)
			if (DWBGetBatchState(i) == DWB_FREE)
				nfree++;
		can_open = background
			? (nfree > DWB_BG_RESERVE + DWB_EVICT_RESERVE ||
			   (nfree >= 1 && nfree <= DWB_BG_RESERVE))
			: nfree > DWB_BG_RESERVE;
		open_idx = pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]);
		if (!can_open &&
			(open_idx == DWB_INVALID_BATCH ||
			 (pg_atomic_read_u32(&DWBCtl->batches[open_idx].next_slot_idx) &
			  (DWB_SEAL_BIT | DWB_IDX_MASK)) >= (uint32) dwb_batch_pages))
			break;				/* one more acquire would block */

		tag = make_tag(1, 95000 + (background ? 1000 : 0), (BlockNumber) taken);

		DWBAcquireSlot(&tag, wclass, false, &ref);
		memset(page, 'X', BLCKSZ);
		DWBPublishImage(&ref, page, (XLogRecPtr) 0x6000000 + taken);
		taken++;
	}

	PG_RETURN_INT32(taken);
}

/*
 * Acquire (and optionally publish) npages slots WITH a ResourceOwner
 * attachment, then raise an ERROR: the transaction abort must release the
 * refs (poisoning unpublished slots), leaving the batch completable by a
 * later seal.  Exercises the abort path of the write path without a
 * process exit.
 */
PG_FUNCTION_INFO_V1(test_dwb_abort_release);
Datum
test_dwb_abort_release(PG_FUNCTION_ARGS)
{
	int			npages = PG_GETARG_INT32(0);
	bool		do_publish = PG_GETARG_BOOL(1);

	check_dwb_enabled();
	leak_refs(npages, do_publish, true, 93000);

	ereport(ERROR, (errmsg("test_dwb: deliberate abort with pending refs")));
	PG_RETURN_VOID();			/* unreachable */
}

/*
 * Abort AFTER the batch is durable: acquire one slot with a ResourceOwner
 * attachment, publish, seal, wait for DWB_FSYNCED, then ERROR.  This
 * exercises the REF HAND-OFF of the abort path: the fake relation makes the
 * repair exit through the dropped-relation branch, and the last-ref drop
 * must still finish the batch (publication, RETIRING).  The repair write
 * itself is exercised by test_dwb_torn_repair on a real relation.
 */
PG_FUNCTION_INFO_V1(test_dwb_abort_after_fsync);
Datum
test_dwb_abort_after_fsync(PG_FUNCTION_ARGS)
{
	BufferTag	tag = make_tag(1, 94000, 0);
	DWBSlotRef	ref;
	static char page[BLCKSZ];

	check_dwb_enabled();

	memset(page, 'F', BLCKSZ);
	stage_one_page(&tag, page, (XLogRecPtr) 0x5000000, true, &ref);

	ereport(ERROR, (errmsg("test_dwb: deliberate abort after batch fsync")));
	PG_RETURN_VOID();			/* unreachable */
}

/*
 * Torn-page repair end to end on a REAL relation: read the current on-disk
 * image of one block, stage it into the DWB with a ResourceOwner-attached
 * ref, make the batch durable, then deliberately tear the block on disk and
 * abort.  The ResourceOwner release must rewrite the block from the batch
 * copy (DWBRewriteAbandonedSlot); the TAP test verifies the on-disk content
 * after a restart, where a failed repair surfaces as a checksum error.
 */
PG_FUNCTION_INFO_V1(test_dwb_torn_repair);
Datum
test_dwb_torn_repair(PG_FUNCTION_ARGS)
{
	Oid			relnumber = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	BufferTag	tag = make_tag(MyDatabaseId, relnumber, blkno);
	DWBSlotRef	ref;
	RelPathStr	relpath;
	static PGAlignedBlock image;
	static char junk[BLCKSZ / 2];
	int			fd;

	check_dwb_enabled();

	relpath = relpathperm(BufTagGetRelFileLocator(&tag), MAIN_FORKNUM);
	fd = OpenTransientFile(relpath.str, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", relpath.str)));
	errno = 0;
	if (pg_pread(fd, image.data, BLCKSZ, (off_t) blkno * BLCKSZ) != BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read block %u of file \"%s\": %m",
						blkno, relpath.str)));

	/* stage the pristine image; the abort below must put it back */
	stage_one_page(&tag, image.data, PageGetLSN((Page) image.data), true, &ref);

	/* simulate a torn smgrwrite: clobber the second half of the block */
	memset(junk, 0x7F, sizeof(junk));
	errno = 0;
	if (pg_pwrite(fd, junk, sizeof(junk),
				  (off_t) blkno * BLCKSZ + BLCKSZ / 2) != (ssize_t) sizeof(junk))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not tear block %u of file \"%s\": %m",
						blkno, relpath.str)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", relpath.str)));

	ereport(ERROR,
			(errmsg("test_dwb: deliberate abort after tearing the data page")));
	PG_RETURN_VOID();			/* unreachable */
}

/*
 * Leave one batch RETIRING with a REAL segment in DWSegmentHash and a
 * pending checkpointer sync request for that segment: stage one real block,
 * make the batch durable, write the block through smgrwrite (which
 * registers the sync request), and release the ref.  With
 * dwb_retire_workers = 0 and no explicit test_dwb_retire() call, only the
 * checkpointer's ProcessSyncRequests -- wrapped by
 * DWBSegmentFsyncBegin/End -- can retire the batch: the TAP test asserts
 * that a CHECKPOINT alone frees the ring.
 */
PG_FUNCTION_INFO_V1(test_dwb_checkpoint_pending);
Datum
test_dwb_checkpoint_pending(PG_FUNCTION_ARGS)
{
	Oid			relnumber = PG_GETARG_OID(0);
	BufferTag	tag = make_tag(MyDatabaseId, relnumber, 0);
	DWBSlotRef	ref;
	SMgrRelation reln;
	static PGAlignedBlock image;

	check_dwb_enabled();

	reln = smgropen(BufTagGetRelFileLocator(&tag), INVALID_PROC_NUMBER);
	smgrread(reln, MAIN_FORKNUM, 0, image.data);

	stage_one_page(&tag, image.data, PageGetLSN((Page) image.data), false,
				   &ref);

	/* the data-file write; registers the checkpointer sync request */
	smgrwrite(reln, MAIN_FORKNUM, 0, image.data, false);

	DWBReleaseSlot(&ref);		/* last ref: publication, RETIRING */
	PG_RETURN_VOID();
}

/*
 * Acquire one ownerless ref, publish, seal and wait until the batch is
 * durable, then return WITHOUT releasing: closing the session leaves the
 * exit backstop holding the LAST ref of a DWB_FSYNCED batch, so the
 * FSYNCED -> RETIRING hand-off (seg_set publication under publish_lock and
 * DWBSegHashLock) runs inside the exit callback itself.  This is only
 * legal from before_shmem_exit, while the PGPROC is still alive.
 */
PG_FUNCTION_INFO_V1(test_dwb_leak_fsynced);
Datum
test_dwb_leak_fsynced(PG_FUNCTION_ARGS)
{
	BufferTag	tag = make_tag(1, 97000, 0);
	DWBSlotRef	ref;
	static char page[BLCKSZ];

	check_dwb_enabled();

	memset(page, 'E', BLCKSZ);
	stage_one_page(&tag, page, (XLogRecPtr) 0x8000000, false, &ref);
	PG_RETURN_VOID();
}

/*
 * Park one batch in RETIRING on a single fake segment (relnumber, block 0,
 * database oid 1): acquire, publish, seal, wait durable, release.  The
 * batch stays RETIRING until something fsyncs the segment (the fake
 * relation makes that an ENOENT = covered, unless the test planted a real
 * obstacle at the segment path).
 */
PG_FUNCTION_INFO_V1(test_dwb_park);
Datum
test_dwb_park(PG_FUNCTION_ARGS)
{
	Oid			relnumber = PG_GETARG_OID(0);
	BufferTag	tag = make_tag(1, relnumber, 0);
	DWBSlotRef	ref;
	static char page[BLCKSZ];

	check_dwb_enabled();

	memset(page, 'P', BLCKSZ);
	stage_one_page(&tag, page, (XLogRecPtr) 0x9000000, false, &ref);
	DWBReleaseSlot(&ref);		/* last ref: publication, RETIRING */
	PG_RETURN_VOID();
}

/*
 * Replay the checkpointer's stale-snapshot hazard against a parked batch:
 * DWBSegmentFsyncBegin for the parked segment WITHOUT the matching End
 * (exactly the state an fsync ERROR under data_sync_retry = on leaves
 * behind), then a successful Begin/End of an unrelated non-MD sync entry.
 * The leftover snapshot must be dropped, not consumed: the parked batch
 * has to stay RETIRING.
 */
PG_FUNCTION_INFO_V1(test_dwb_stale_snapshot);
Datum
test_dwb_stale_snapshot(PG_FUNCTION_ARGS)
{
	Oid			relnumber = PG_GETARG_OID(0);
	FileTag		md_tag;
	FileTag		clog_tag;

	check_dwb_enabled();

	memset(&md_tag, 0, sizeof(md_tag));
	md_tag.handler = SYNC_HANDLER_MD;
	md_tag.rlocator.spcOid = DEFAULTTABLESPACE_OID;
	md_tag.rlocator.dbOid = 1;
	md_tag.rlocator.relNumber = relnumber;
	md_tag.forknum = MAIN_FORKNUM;
	md_tag.segno = 0;

	/* arm the snapshot; no End, as if the fsync threw an ERROR */
	DWBSegmentFsyncBegin(&md_tag);

	/* an unrelated non-MD entry syncs successfully */
	memset(&clog_tag, 0, sizeof(clog_tag));
	clog_tag.handler = SYNC_HANDLER_CLOG;
	DWBSegmentFsyncBegin(&clog_tag);
	(void) DWBSegmentFsyncEnd(true);

	PG_RETURN_VOID();
}

/*
 * Publish nbatches full batches whose slots all point at DISTINCT fake
 * segments, so that RETIRING batches accumulate DWSegmentHash entries until
 * the hash overflows and publication degrades to the synchronous OOM retire
 * (WARNING "segment hash is full", DWB_OOM_RETIRING, batch freed by the
 * publisher).  Meant for dwb_retire_workers = 0 so the RETIRING batches
 * keep their entries pinned.  Returns the number of slots published.
 */
PG_FUNCTION_INFO_V1(test_dwb_fill_segments);
Datum
test_dwb_fill_segments(PG_FUNCTION_ARGS)
{
	int			nbatches = PG_GETARG_INT32(0);
	int			nsegs = 0;
	static char page[BLCKSZ];
	static uint32 next_relnumber = 200000;

	check_dwb_enabled();
	if (nbatches < 1 || nbatches > dwb_num_batches)
		ereport(ERROR, (errmsg("nbatches out of range")));

	for (int b = 0; b < nbatches; b++)
	{
		DWBSlotRef	refs[DWB_BATCH_MAX_PAGES];

		CHECK_FOR_INTERRUPTS();

		for (int i = 0; i < dwb_batch_pages; i++)
		{
			BufferTag	tag = make_tag(1, next_relnumber++, 0);

			DWBAcquireSlot(&tag, DWB_WCLASS_EVICTION, false, &refs[i]);
			/* the whole batch must be ours for the seal below to cover it */
			if (refs[i].batch_idx != refs[0].batch_idx)
				ereport(ERROR,
						(errmsg("segment-fill batch split unexpectedly")));
			memset(page, 'S', BLCKSZ);
			DWBPublishImage(&refs[i], page, (XLogRecPtr) 0x7000000 + nsegs);
			nsegs++;
		}
		if (!DWBTrySealBatch(refs[0].batch_idx))
			ereport(ERROR, (errmsg("could not seal a segment-fill batch")));
		DWBWaitBatchFsynced(&refs[0]);
		for (int i = 0; i < dwb_batch_pages; i++)
			DWBReleaseSlot(&refs[i]);
	}
	PG_RETURN_INT32(nsegs);
}

PG_FUNCTION_INFO_V1(test_dwb_force_seal);
Datum
test_dwb_force_seal(PG_FUNCTION_ARGS)
{
	bool		background = PG_GETARG_BOOL(0);

	check_dwb_enabled();
	PG_RETURN_BOOL(DWBForceSealOpenBatch(background ? DWB_WCLASS_BACKGROUND
										 : DWB_WCLASS_EVICTION));
}

PG_FUNCTION_INFO_V1(test_dwb_retire);
Datum
test_dwb_retire(PG_FUNCTION_ARGS)
{
	check_dwb_enabled();
	PG_RETURN_INT32(DWBRetireAllSync());
}

/*
 * Deterministic regression test for the stale-open ABA race: a writer that
 * bounced off a sealed batch calls DWBOpenNewBatch only after the ring has
 * reused the same index for a NEW live incarnation.  The replacement guard
 * must recognize the reuse and leave the live batch alone; the buggy
 * index-only comparison would repoint open_batch_idx and orphan it.
 *
 * Single-backend and timing-free: we replay the loser's exact interleaving
 * instead of racing two sessions.
 */
PG_FUNCTION_INFO_V1(test_dwb_open_stale);
Datum
test_dwb_open_stale(PG_FUNCTION_ARGS)
{
	uint32		stale_idx;
	uint32		reopened_idx;
	DWBSlotRef	ref;
	BufferTag	tag;
	static char page[BLCKSZ];

	check_dwb_enabled();

	/*
	 * Cycle once so the open batch goes through seal and retire:
	 * open_batch_idx afterwards still names it, sealed (SEAL_BIT is held
	 * through FREE) — exactly a bounced writer's stale view.
	 */
	(void) dwb_cycle_internal(1);
	stale_idx = pg_atomic_read_u32(&DWBCtl->open_batch_idx[DWB_WCLASS_EVICTION]);

	/*
	 * Acquire one slot: the reservation CAS bounces on SEAL_BIT and reopens
	 * the lowest FREE index — the same index again, as a new live
	 * incarnation.
	 */
	tag = make_tag(1, 92000, 0);
	DWBAcquireSlot(&tag, DWB_WCLASS_EVICTION, false, &ref);
	memset(page, 'S', BLCKSZ);
	DWBPublishImage(&ref, page, (XLogRecPtr) 0x3000000);

	reopened_idx = (uint32) ref.batch_idx;
	if (reopened_idx != stale_idx)
		ereport(ERROR,
				(errmsg("stale-open scenario not reproduced: reopened %u, stale %u",
						reopened_idx, stale_idx)));

	/*
	 * The ABA moment: a stale opener calls with old_idx naming the live
	 * reopened incarnation.  The guard must not replace it.
	 */
	DWBOpenNewBatch(DWB_WCLASS_EVICTION, stale_idx);

	if (pg_atomic_read_u32(&DWBCtl->open_batch_idx[DWB_WCLASS_EVICTION]) !=
		reopened_idx)
		ereport(ERROR,
				(errmsg("stale open hijacked the live open batch")));

	/* drain: seal, wait durable, release, retire */
	(void) DWBForceSealOpenBatch(DWB_WCLASS_EVICTION);
	DWBWaitBatchFsynced(&ref);
	DWBReleaseSlot(&ref);
	(void) DWBRetireAllSync();

	PG_RETURN_VOID();
}

/*
 * Write a synthetic single-slot batch file directly into pg_dwb/, bypassing
 * the ring state machine.  The apply-pass dedup scenarios need on-disk
 * layouts — the same page in two batches with chosen LSNs and batch_ids —
 * that the runtime write path cannot be steered into: a sequential writer
 * keeps reusing the lowest free ring index, so only the last copy of a page
 * survives on disk.
 *
 * The image is the target block's current on-disk content with the given
 * LSN, the marker planted in the page hole and the checksum recomputed, so
 * an applied image is a valid page the server can read back afterwards.
 * The caller keeps the batch index away from runtime traffic (quiet server,
 * high index) and must not hand out an LSN beyond the current WAL insert
 * position: it ends up as a real page LSN, and a later flush of that page
 * would ask XLogFlush for WAL that does not exist.
 */
PG_FUNCTION_INFO_V1(test_dwb_craft_batch);
Datum
test_dwb_craft_batch(PG_FUNCTION_ARGS)
{
	int			batch_idx = PG_GETARG_INT32(0);
	uint64		batch_id = (uint64) PG_GETARG_INT64(1);
	Oid			relnumber = PG_GETARG_OID(2);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(3);
	XLogRecPtr	lsn = PG_GETARG_LSN(4);
	text	   *marker = PG_GETARG_TEXT_PP(5);
	BufferTag	tag = make_tag(MyDatabaseId, relnumber, blkno);
	DWBControlFileData control;
	DWBBatchHeader hdr;
	DWSlotMeta	meta;
	static PGAlignedBlock image;
	PageHeader	ph = (PageHeader) image.data;
	RelPathStr	relpath;
	char		path[MAXPGPATH];
	char	   *region;
	Size		region_size;
	int			fd;

	check_dwb_enabled();

	if (!DWBReadControlFile(&control, false, NULL))
		pg_unreachable();
	if (batch_idx < 0 || (uint32) batch_idx >= control.num_batches)
		ereport(ERROR, (errmsg("batch index out of range")));

	/* base image: the block's current on-disk content */
	relpath = relpathperm(BufTagGetRelFileLocator(&tag), MAIN_FORKNUM);
	fd = OpenTransientFile(relpath.str, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", relpath.str)));
	errno = 0;
	if (pg_pread(fd, image.data, BLCKSZ, (off_t) blkno * BLCKSZ) != BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read block %u of file \"%s\": %m",
						blkno, relpath.str)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", relpath.str)));

	if (PageIsNew((Page) image.data))
		ereport(ERROR,
				(errmsg("block %u of \"%s\" is empty on disk; CHECKPOINT first",
						blkno, relpath.str)));
	if ((Size) (ph->pd_upper - ph->pd_lower) < VARSIZE_ANY_EXHDR(marker))
		ereport(ERROR, (errmsg("marker does not fit into the page hole")));

	memcpy(image.data + ph->pd_lower, VARDATA_ANY(marker),
		   VARSIZE_ANY_EXHDR(marker));
	PageSetLSN((Page) image.data, lsn);
	ph->pd_checksum = pg_checksum_page(image.data, blkno);

	memset(&meta, 0, sizeof(meta));
	meta.tag = tag;
	meta.page_lsn = lsn;
	meta.generation = control.generation;
	meta.image_crc = DWBImageCrc(image.data);
	meta.meta_crc = DWBSlotMetaCrc(&meta);

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = DWB_BATCH_MAGIC;
	hdr.version = DWB_VERSION;
	hdr.batch_id = batch_id;
	hdr.n_slots = 1;
	hdr.crc = DWBBatchHeaderCrc(&hdr);

	region_size = DWBMetaRegionSize(control.batch_pages);
	region = palloc0(region_size);
	memcpy(region, &hdr, sizeof(hdr));
	memcpy(region + sizeof(hdr), &meta, sizeof(meta));

	DWBBatchFilePath(path, batch_idx);
	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	errno = 0;
	if (pg_pwrite(fd, region, region_size, 0) != (ssize_t) region_size ||
		pg_pwrite(fd, image.data, BLCKSZ, (off_t) region_size) != BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", path)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));
	pfree(region);

	PG_RETURN_VOID();
}

/*
 * Rewrite pg_dwb/control with the given min_version (and a matching CRC):
 * the state a ring left behind by a newer server would present after a
 * binary downgrade.  The next start must refuse it with the format-version
 * FATAL, never with the "corrupt, remove pg_dwb" advice — the ring is
 * intact and may hold unapplied repairs only the newer server can read.
 */
PG_FUNCTION_INFO_V1(test_dwb_set_control_min_version);
Datum
test_dwb_set_control_min_version(PG_FUNCTION_ARGS)
{
	DWBControlFileData control;

	check_dwb_enabled();

	if (!DWBReadControlFile(&control, false, NULL))
		pg_unreachable();
	control.min_version = (uint32) PG_GETARG_INT32(0);
	control.crc = DWBControlCrc(&control);
	DWBWriteControlFile(&control);

	PG_RETURN_VOID();
}
