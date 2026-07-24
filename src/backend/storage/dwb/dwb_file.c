/*-------------------------------------------------------------------------
 *
 * dwb_file.c
 *	  On-disk format of the short-lived double write buffer: ring creation
 *	  and preallocation, control file, batch writes.
 *
 * Ring files are fully preallocated with zeros at creation (same contract
 * as WAL segments): every block is allocated and the file size never
 * changes, so batch durability only needs fdatasync.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_file.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "common/file_utils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/* backend-local cache of open batch-file VFDs; only leaders and retire
 * workers ever open batch files */
static File *batch_files = NULL;

/* IO-aligned meta-region assembly buffer, allocated by DWBPrepareBatchWrite */
static char *meta_buf = NULL;

pg_crc32c
DWBImageCrc(const char *image)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, image, BLCKSZ);
	FIN_CRC32C(crc);
	return crc;
}

pg_crc32c
DWBSlotMetaCrc(const DWSlotMeta *meta)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, meta, offsetof(DWSlotMeta, meta_crc));
	FIN_CRC32C(crc);
	return crc;
}

pg_crc32c
DWBControlCrc(const DWBControlFileData *control)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, control, offsetof(DWBControlFileData, crc));
	FIN_CRC32C(crc);
	return crc;
}

pg_crc32c
DWBBatchHeaderCrc(const DWBBatchHeader *hdr)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, hdr, offsetof(DWBBatchHeader, crc));
	FIN_CRC32C(crc);
	return crc;
}

static void
DWBBatchFilePath(char *path, int batch_idx)
{
	snprintf(path, MAXPGPATH, DWB_DIR "/batch_%04d", batch_idx);
}

/*
 * Read pg_dwb/control.  Returns false if the file does not exist and
 * missing_ok; any other failure (including a CRC mismatch) is FATAL —
 * a damaged control file must not silently degrade the apply-pass.
 */
bool
DWBReadControlFile(DWBControlFileData *control, bool missing_ok)
{
	int			fd;
	int			r;

	fd = OpenTransientFile(DWB_CONTROL_FILE, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT && missing_ok)
			return false;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", DWB_CONTROL_FILE)));
	}

	pgstat_report_wait_start(WAIT_EVENT_DWB_CONTROL_READ);
	errno = 0;
	r = read(fd, control, sizeof(DWBControlFileData));
	pgstat_report_wait_end();
	if (r != sizeof(DWBControlFileData))
	{
		/* distinguish a real read error from a truncated file */
		if (r < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							DWB_CONTROL_FILE)));
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read file \"%s\": read %d of %zu",
						DWB_CONTROL_FILE, r, sizeof(DWBControlFileData))));
	}
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", DWB_CONTROL_FILE)));

	if (control->magic != DWB_CONTROL_MAGIC ||
		!EQ_CRC32C(control->crc, DWBControlCrc(control)))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid checksum or magic number in file \"%s\"",
						DWB_CONTROL_FILE)));
	if (control->min_version > DWB_VERSION)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("file \"%s\" requires format version at least %u, but this server supports %u",
						DWB_CONTROL_FILE, control->min_version, DWB_VERSION)));

	return true;
}

/*
 * Write pg_dwb/control atomically: tmp file + fsync + durable_rename,
 * all inside pg_dwb/.
 */
void
DWBWriteControlFile(const DWBControlFileData *control)
{
	const char *tmppath = DWB_DIR "/control.tmp";
	int			fd;

	fd = OpenTransientFile(tmppath,
						   O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	pgstat_report_wait_start(WAIT_EVENT_DWB_CONTROL_WRITE);
	errno = 0;
	if (write(fd, control, sizeof(DWBControlFileData)) !=
		sizeof(DWBControlFileData))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmppath)));
	}
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_DWB_CONTROL_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	durable_rename(tmppath, DWB_CONTROL_FILE, FATAL);
}

/*
 * Create pg_dwb/ from scratch: directory, zero-preallocated batch files,
 * control with generation 0.
 */
void
DWBCreateRing(void)
{
	DWBControlFileData control;

	if (MakePGDirectory(DWB_DIR) < 0 && errno != EEXIST)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", DWB_DIR)));

	for (int i = 0; i < dwb_num_batches; i++)
	{
		char		path[MAXPGPATH];
		int			fd;
		int			rc;

		DWBBatchFilePath(path, i);
		fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_RDWR | PG_BINARY);
		if (fd < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));

		pgstat_report_wait_start(WAIT_EVENT_DWB_RING_INIT);
		rc = pg_pwrite_zeros(fd, DWBBatchFileSize(dwb_batch_pages), 0);
		if (rc < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", path)));
		if (pg_fsync(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m", path)));
		pgstat_report_wait_end();

		if (CloseTransientFile(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m", path)));
	}

	memset(&control, 0, sizeof(control));
	control.magic = DWB_CONTROL_MAGIC;
	control.version = DWB_VERSION;
	control.min_version = DWB_MIN_VERSION;
	control.num_batches = dwb_num_batches;
	control.batch_pages = dwb_batch_pages;
	control.generation = 0;
	control.crc = DWBControlCrc(&control);
	DWBWriteControlFile(&control);

	fsync_fname(DWB_DIR, true);
}

/*
 * Return an open VFD for a batch file, from the backend-local cache.
 */
int
DWBOpenBatchFile(int batch_idx)
{
	char		path[MAXPGPATH];

	if (batch_files == NULL)
	{
		batch_files = (File *)
			MemoryContextAllocZero(TopMemoryContext,
								   dwb_num_batches * sizeof(File));
		for (int i = 0; i < dwb_num_batches; i++)
			batch_files[i] = -1;
	}

	if (batch_files[batch_idx] >= 0)
		return batch_files[batch_idx];

	DWBBatchFilePath(path, batch_idx);
	batch_files[batch_idx] = PathNameOpenFile(path, O_RDWR | PG_BINARY);
	if (batch_files[batch_idx] < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	return batch_files[batch_idx];
}

/*
 * Pre-open the batch file and pre-allocate the meta-region buffer, so that
 * DWBWriteBatch can run inside the leader's critical section without
 * allocating anything.  Called before the seal is attempted, where an
 * ERROR is still harmless.
 */
void
DWBPrepareBatchWrite(int batch_idx)
{
	if (meta_buf == NULL)
		meta_buf = MemoryContextAllocAligned(TopMemoryContext,
											 DWBMetaRegionSize(dwb_batch_pages),
											 PG_IO_ALIGN_SIZE, 0);
	(void) DWBOpenBatchFile(batch_idx);
}

/*
 * Read one slot's page image back from a batch file.  Used by the abort
 * cleanup of a published ref whose batch is already durable (>= FSYNCED):
 * the staged copy in shmem is gone by then, the batch file is the
 * authoritative source.  Failure — the open included — is PANIC: the
 * caller is about to repair a possibly-torn data page, has no fallback,
 * and may be running from a ResourceOwner release callback.
 *
 * Deliberately avoids the VFD layer (BasicOpenFile + raw pg_pread): a
 * release callback must not fail, and PathNameOpenFile can throw ERROR
 * from its internal allocations.  BasicOpenFile allocates nothing and
 * still recovers from EMFILE/ENFILE by closing LRU VFDs.
 */
void
DWBReadSlotImage(int batch_idx, int slot_idx, char *dst)
{
	char		path[MAXPGPATH];
	int			fd;
	off_t		off = DWBMetaRegionSize(dwb_batch_pages) +
		(off_t) slot_idx * BLCKSZ;
	ssize_t		r;

	DWBBatchFilePath(path, batch_idx);
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	pgstat_report_wait_start(WAIT_EVENT_DWB_BATCH_READ);
	r = pg_pread(fd, dst, BLCKSZ, off);
	pgstat_report_wait_end();
	if (r != BLCKSZ)
	{
		if (r < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read slot %d of batch %d in \"%s\": %m",
							slot_idx, batch_idx, DWB_DIR)));
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read slot %d of batch %d in \"%s\": read %zd of %d",
						slot_idx, batch_idx, DWB_DIR, r, BLCKSZ)));
	}
	if (close(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));
}

/*
 * Leader write of one batch: (a) one contiguous pwrite of the image stream
 * from staging, (b) one pwrite of the meta region, (c) fdatasync.  Exactly
 * this order: a crash while reusing a slot must never leave valid-looking
 * meta over a torn or foreign image (any partially-persistent mix is
 * rejected locally by meta_crc/generation/image_crc, see 3.2/3.4).
 *
 * Runs inside the leader's critical section: every ereport here escalates
 * to PANIC, which is deliberate — an incomplete leader write cannot be
 * unwound (see DWBSealBatch).
 */
void
DWBWriteBatch(int batch_idx, const DWBBatchHeader *hdr,
			  const DWSlotMeta *metas, const char *images)
{
	Size		meta_region = DWBMetaRegionSize(dwb_batch_pages);
	File		file = DWBOpenBatchFile(batch_idx);
	Size		image_bytes = (Size) hdr->n_slots * BLCKSZ;
	ssize_t		nwritten;
	int			fd;
	instr_time	io_start;

	/* DWBPrepareBatchWrite has run */
	Assert(meta_buf != NULL);

	io_start = pgstat_prepare_io_time(track_io_timing);

	nwritten = FileWrite(file, images, image_bytes, meta_region,
						 WAIT_EVENT_DWB_BATCH_WRITE);
	if (nwritten != (ssize_t) image_bytes)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write batch %d of \"%s\": %m",
						batch_idx, DWB_DIR)));

	memset(meta_buf, 0, meta_region);
	memcpy(meta_buf, hdr, sizeof(DWBBatchHeader));
	memcpy(meta_buf + sizeof(DWBBatchHeader), metas,
		   hdr->n_slots * sizeof(DWSlotMeta));

	nwritten = FileWrite(file, meta_buf, meta_region, 0,
						 WAIT_EVENT_DWB_BATCH_WRITE);
	if (nwritten != (ssize_t) meta_region)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write batch %d of \"%s\": %m",
						batch_idx, DWB_DIR)));

	pgstat_count_io_op_time(IOOBJECT_DWB, IOCONTEXT_NORMAL, IOOP_WRITE,
							io_start, 1, image_bytes + meta_region);

	/*
	 * fdatasync suffices: the file was fully preallocated at ring creation,
	 * its size and block layout never change (WAL-segment contract).
	 */
	io_start = pgstat_prepare_io_time(track_io_timing);
	pgstat_report_wait_start(WAIT_EVENT_DWB_BATCH_SYNC);
	fd = FileGetRawDesc(file);
	if (fd < 0 || pg_fdatasync(fd) != 0)
		ereport(data_sync_elevel(ERROR),
				(errcode_for_file_access(),
				 errmsg("could not fsync batch %d of \"%s\": %m",
						batch_idx, DWB_DIR)));
	pgstat_report_wait_end();
	pgstat_count_io_op_time(IOOBJECT_DWB, IOCONTEXT_NORMAL, IOOP_FSYNC,
							io_start, 1, 0);
}
