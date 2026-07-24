/*--------------------------------------------------------------------------
 *
 * test_dwb.c
 *		Test module for the short-lived double write buffer.
 *
 * Drives the DWB batch state machine directly (Stage 1: FlushBuffer is not
 * wired in yet) with synthetic page tags and images, and validates the
 * on-disk ring format independently of the server-side write path.
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
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "utils/builtins.h"

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
		BufferTag	tag;
		DWBSlotRef	ref;
		RelFileLocator rlocator;

		CHECK_FOR_INTERRUPTS();

		rlocator.spcOid = DEFAULTTABLESPACE_OID;
		rlocator.dbOid = 1;
		rlocator.relNumber = 90000 + (i % 3);
		InitBufferTag(&tag, &rlocator, MAIN_FORKNUM, (BlockNumber) i);

		DWBAcquireSlot(&tag, &ref);

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
 * current_only) + flags + image_crc.
 */
PG_FUNCTION_INFO_V1(test_dwb_ring_slots);
Datum
test_dwb_ring_slots(PG_FUNCTION_ARGS)
{
	bool		current_only = PG_GETARG_BOOL(0);
	DWBControlFileData control;
	Size		meta_region;
	DWSlotMeta *metas;
	char	   *image;
	int			valid = 0;

	check_dwb_enabled();
	if (!DWBReadControlFile(&control, false))
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
			 * under the startup geometry check; report the anomaly instead
			 * of silently contributing zero slots.
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
	PG_RETURN_INT32(valid);
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
 * DWBProcExit's poison (unpublished) or orphan (published) path.
 */
PG_FUNCTION_INFO_V1(test_dwb_leak);
Datum
test_dwb_leak(PG_FUNCTION_ARGS)
{
	int			npages = PG_GETARG_INT32(0);
	bool		do_publish = PG_GETARG_BOOL(1);
	static char page[BLCKSZ];

	check_dwb_enabled();
	/* stay below the batch size so this backend never seals as leader */
	if (npages < 1 || npages >= dwb_batch_pages)
		ereport(ERROR, (errmsg("npages out of range")));

	for (int i = 0; i < npages; i++)
	{
		BufferTag	tag;
		DWBSlotRef	ref;
		RelFileLocator rlocator;

		rlocator.spcOid = DEFAULTTABLESPACE_OID;
		rlocator.dbOid = 1;
		rlocator.relNumber = 91000;
		InitBufferTag(&tag, &rlocator, MAIN_FORKNUM, (BlockNumber) i);

		DWBAcquireSlot(&tag, &ref);
		if (do_publish)
		{
			memset(page, 'L', BLCKSZ);
			DWBPublishImage(&ref, page, (XLogRecPtr) 0x2000000 + i);
		}
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_dwb_force_seal);
Datum
test_dwb_force_seal(PG_FUNCTION_ARGS)
{
	check_dwb_enabled();
	PG_RETURN_BOOL(DWBForceSealOpenBatch(DWB_WCLASS_EVICTION));
}

PG_FUNCTION_INFO_V1(test_dwb_retire);
Datum
test_dwb_retire(PG_FUNCTION_ARGS)
{
	check_dwb_enabled();
	PG_RETURN_INT32(DWBRetireAllSync());
}
