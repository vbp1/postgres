/*-------------------------------------------------------------------------
 *
 * dwb_ctl.c
 *	  Shared-memory state and GUC variables of the short-lived double
 *	  write buffer.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_ctl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/dwb.h"
#include "storage/shmem.h"
#include "utils/guc.h"

/* GUC variables (see 3.10 of the design plan) */
int			io_torn_pages_protection = DWB_PROTECT_FULL_PAGES;
int			dwb_num_batches = 64;
int			dwb_batch_pages = 64;
int			dwb_max_segments = 4096;
int			dwb_retire_workers = 1;
int			dwb_cleaner_workers = 0;
int			dwb_retire_sync_method = DWB_RETIRE_SYNC_METHOD_DEFAULT;
int			dwb_batch_timeout_ms = 10;
int			dwb_retire_interval_ms = 50;
int			dwb_writeback_after = DEFAULT_DWB_WRITEBACK_AFTER;
int			dwb_slow_warn_ms = 5000;
int			dwb_slot_stuck_timeout_ms = 30000;
int			dwb_write_timeout_ms = 60000;
int			dwb_on_stall = DWB_ON_STALL_PANIC;

DWCtl	   *DWBCtl = NULL;
char	   *DWBStagingBase = NULL;
HTAB	   *DWSegmentHash = NULL;

static Size
DWBCtlSize(void)
{
	return offsetof(DWCtl, batches) +
		mul_size(dwb_num_batches, sizeof(DWBatchCtl));
}

static Size
DWBSegEntrySize(void)
{
	return offsetof(DWSegEntry, batch_bitmap) +
		mul_size(DWBSegBitmapWords(), sizeof(pg_atomic_uint64));
}

static Size
DWBStagingSize(void)
{
	/* IO-aligned staging buffers of one batch worth of pages each */
	return add_size(mul_size(DWB_STAGING_BUFFERS,
							 mul_size(dwb_batch_pages, BLCKSZ)),
					PG_IO_ALIGN_SIZE);
}

Size
DWBShmemSize(void)
{
	Size		size;

	if (!DWBIsEnabled())
		return 0;

	size = add_size(DWBCtlSize(), DWBStagingSize());
	size = add_size(size, hash_estimate_size(dwb_max_segments,
											 DWBSegEntrySize()));
	size = add_size(size, DWBCleanerShmemSize());
	return size;
}

void
DWBShmemInit(void)
{
	bool		found;

	if (!DWBIsEnabled())
		return;

	DWBCtl = (DWCtl *) ShmemInitStruct("DWB Ctl", DWBCtlSize(), &found);

	if (!found)
	{
		memset(DWBCtl, 0, DWBCtlSize());

		for (int i = 0; i < DWB_NUM_WCLASSES; i++)
			pg_atomic_init_u32(&DWBCtl->open_batch_idx[i], DWB_INVALID_BATCH);
		pg_atomic_init_u64(&DWBCtl->next_batch_id, 1);
		pg_atomic_init_u64(&DWBCtl->freed_events, 0);
		pg_atomic_init_u64(&DWBCtl->ring_wait_retries, 0);
		for (int c = 0; c < DWB_NUM_WCLASSES; c++)
			for (int r = 0; r < DWB_SEAL_NREASONS; r++)
			{
				pg_atomic_init_u64(&DWBCtl->seal_count[c][r], 0);
				pg_atomic_init_u64(&DWBCtl->seal_pages[c][r], 0);
			}
		for (int c = 0; c < DWB_NUM_WCLASSES; c++)
			pg_atomic_init_u64(&DWBCtl->last_overflow_seal[c], 0);
		for (int c = 0; c < DWB_NUM_WCLASSES; c++)
			ConditionVariableInit(&DWBCtl->cv_want_batch[c]);
		ConditionVariableInit(&DWBCtl->cv_retire_wake);
		SpinLockInit(&DWBCtl->staging_lock);
		DWBCtl->staging_free = (1U << DWB_STAGING_BUFFERS) - 1;

		for (int i = 0; i < dwb_num_batches; i++)
		{
			DWBatchCtl *batch = &DWBCtl->batches[i];

			pg_atomic_init_u32(&batch->state, DWB_FREE);
			pg_atomic_init_u32(&batch->next_slot_idx, 0);
			pg_atomic_init_u32(&batch->capped_slots, 0);
			for (int w = 0; w < DWB_BITMAP_WORDS; w++)
				pg_atomic_init_u64(&batch->slots_written_bitmap[w], 0);
			pg_atomic_init_u32(&batch->ref_count, 0);
			pg_atomic_init_u32(&batch->seg_pending_count, 0);
			LWLockInitialize(&batch->publish_lock, LWTRANCHE_DWB_PUBLISH);
			ConditionVariableInit(&batch->cv_state);
			batch->staging_idx = -1;
		}
	}

	{
		char	   *base;

		base = (char *) ShmemInitStruct("DWB Staging", DWBStagingSize(),
										&found);
		DWBStagingBase = (char *) TYPEALIGN(PG_IO_ALIGN_SIZE, base);
	}

	{
		HASHCTL		info;

		info.keysize = sizeof(DWSegRef);
		info.entrysize = DWBSegEntrySize();

		DWSegmentHash = ShmemInitHash("DWB Segment Hash",
									  dwb_max_segments, dwb_max_segments,
									  &info,
									  HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
	}

	DWBCleanerShmemInit();
}
