/*-------------------------------------------------------------------------
 *
 * dwb.c
 *	  Batch state machine of the short-lived double write buffer.
 *
 * Batch lifecycle: FREE -> ALLOCATED -> SEALED -> WRITTEN -> FSYNCED ->
 * DATA_WRITTEN -> RETIRING -> FREE.  Writers reserve slots with an atomic
 * fetch_add on next_slot_idx (31-bit index + SEAL_BIT sentinel), publish
 * their page image with a plain memcpy into the batch's staging buffer and
 * set their bit in slots_written_bitmap.  The SEAL initiator becomes the
 * leader: it waits for bitmap coverage of capped_slots, then writes the
 * whole batch — in this write order: the contiguous image stream, then the
 * meta region, then fdatasync (the on-disk layout puts the meta region
 * first; see dwb.h) — and broadcasts DWB_FSYNCED.
 *
 * Stage 1 scope: the state machine is complete but not yet wired into
 * FlushBuffer; retirement is synchronous (DWBRetireAllSync) — the retire
 * worker pool and the segment back-reference hash arrive in Stage 2.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/dwb.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Slot refs held by this backend, for cleanup on process exit.  A ref lives
 * from DWBAcquireSlot to DWBReleaseSlot.  Sized to two full batches because
 * a backend can hold refs on a sealed batch and on its successor at the
 * same time, releasing the former only after the switch.  (Stage 2
 * additionally attaches refs to the ResourceOwner so that a transaction
 * abort — e.g. an ERROR out of smgrwrite — releases them too.)
 */
static DWBSlotRef pendingRefs[2 * DWB_BATCH_MAX_PAGES];
static int	nPendingRefs = 0;
static bool cleanup_registered = false;

/* leader-side meta assembly area, allocated before the seal is attempted */
static DWSlotMeta *leader_metas = NULL;

static void DWBProcExit(int code, Datum arg);
static void DWBLeaderWriteBatch(int batch_idx);
static void DWBFinishBatchData(DWBatchCtl *batch);

static inline char *
DWBStagingSlotPtr(int staging_idx, int slot_idx)
{
	return DWBStagingBase +
		(Size) staging_idx * dwb_batch_pages * BLCKSZ +
		(Size) slot_idx * BLCKSZ;
}

/* ----------------------------------------------------------------
 * staging pool
 * ----------------------------------------------------------------
 */
static int
DWBStagingAlloc(void)
{
	int			idx;

	for (;;)
	{
		idx = -1;

		SpinLockAcquire(&DWBCtl->staging_lock);
		if (DWBCtl->staging_free != 0)
		{
			idx = pg_rightmost_one_pos32(DWBCtl->staging_free);
			DWBCtl->staging_free &= ~(1U << idx);
		}
		SpinLockRelease(&DWBCtl->staging_lock);

		if (idx >= 0)
			break;

		/*
		 * A buffer frees once its leader finishes the image pwrite;
		 * retirement broadcasts cv_free_batch too, so just re-check on
		 * every wake-up.
		 */
		ConditionVariableSleep(&DWBCtl->cv_free_batch,
							   WAIT_EVENT_DWB_FREE_BATCH);
	}
	ConditionVariableCancelSleep();
	return idx;
}

static void
DWBStagingRelease(int idx)
{
	SpinLockAcquire(&DWBCtl->staging_lock);
	DWBCtl->staging_free |= 1U << idx;
	SpinLockRelease(&DWBCtl->staging_lock);
	ConditionVariableBroadcast(&DWBCtl->cv_free_batch);
}

/* ----------------------------------------------------------------
 * batch opening
 * ----------------------------------------------------------------
 */

/*
 * Make open_batch_idx[wclass] point at an ALLOCATED batch, if it currently
 * points at old_idx (a sealed or invalid batch).  Serialized by
 * DWBRingOpenLock; sleeps on cv_free_batch when the whole ring is busy.
 *
 * Ordering note for stale writers: a batch keeps SEAL_BIT in next_slot_idx
 * from its SEAL until we finish re-initializing it here, so a stale
 * fetch_add against a reused batch either sees SEAL_BIT (and retries) or
 * lands on a valid slot of the new incarnation — never on a slot that a
 * concurrent reset can wipe.
 */
static void
DWBOpenNewBatch(int wclass, uint32 old_idx)
{
	for (;;)
	{
		int			free_idx = -1;
		int			staging_idx;

		/*
		 * Reserve the staging buffer before taking the lock: the wait for a
		 * free buffer can be long, and no sleeping (or interruptible) point
		 * may exist below, where we hold DWBRingOpenLock with a batch
		 * already taken out of DWB_FREE.
		 */
		staging_idx = DWBStagingAlloc();

		LWLockAcquire(DWBRingOpenLock, LW_EXCLUSIVE);

		/* someone else already replaced the open batch: done */
		if (pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]) != old_idx)
		{
			LWLockRelease(DWBRingOpenLock);
			DWBStagingRelease(staging_idx);
			ConditionVariableCancelSleep();
			return;
		}

		for (int i = 0; i < dwb_num_batches; i++)
		{
			uint32		expected = DWB_FREE;

			if (pg_atomic_compare_exchange_u32(&DWBCtl->batches[i].state,
											   &expected, DWB_ALLOCATED))
			{
				free_idx = i;
				break;
			}
		}

		if (free_idx >= 0)
		{
			DWBatchCtl *batch = &DWBCtl->batches[free_idx];

			for (int w = 0; w < DWB_BITMAP_WORDS; w++)
				pg_atomic_write_u64(&batch->slots_written_bitmap[w], 0);
			pg_atomic_write_u32(&batch->capped_slots, 0);
			pg_atomic_write_u32(&batch->ref_count, 0);
			pg_atomic_write_u32(&batch->seg_pending_count, 0);
			pg_atomic_write_u32(&batch->orphaned_refs_count, 0);
			batch->n_segs = 0;
			batch->max_page_lsn = InvalidXLogRecPtr;
			batch->batch_id = pg_atomic_fetch_add_u64(&DWBCtl->next_batch_id, 1);
			batch->open_time = GetCurrentTimestamp();
			batch->staging_idx = staging_idx;

			/*
			 * Open for reservations only after everything above is visible:
			 * clearing SEAL_BIT is the point where writers may enter.
			 */
			pg_write_barrier();
			pg_atomic_write_u32(&batch->next_slot_idx, 0);

			pg_atomic_write_u32(&DWBCtl->open_batch_idx[wclass], free_idx);
			LWLockRelease(DWBRingOpenLock);
			ConditionVariableCancelSleep();
			return;
		}

		LWLockRelease(DWBRingOpenLock);
		DWBStagingRelease(staging_idx);

		/* whole ring busy: wait for a retirement, then retry */
		ConditionVariableSleep(&DWBCtl->cv_free_batch,
							   WAIT_EVENT_DWB_FREE_BATCH);
	}
}

/* ----------------------------------------------------------------
 * sealing and the leader write
 * ----------------------------------------------------------------
 */

/*
 * Seal a batch.  Returns true if we won the seal race and performed the
 * leader duties (the batch is DWB_FSYNCED — or fully cascaded to FREE for
 * the defensive capped_slots == 0 case — on return).
 */
static bool
DWBSealBatch(int batch_idx)
{
	DWBatchCtl *batch = &DWBCtl->batches[batch_idx];
	uint32		prev;
	uint32		capped;
	uint32		expected;

	/*
	 * Get everything the critical section below could fail at out of the
	 * way while failure is still harmless (the seal has not been attempted
	 * yet, so on ERROR the batch stays ALLOCATED and any other writer can
	 * seal it later): the one-time leader allocations, the batch file VFD,
	 * and this backend's condition-variable wait event set (the first
	 * ConditionVariablePrepareToSleep of a backend allocates it).
	 */
	if (leader_metas == NULL)
		leader_metas = MemoryContextAllocZero(TopMemoryContext,
											  DWB_BATCH_MAX_PAGES * sizeof(DWSlotMeta));
	DWBPrepareBatchWrite(batch_idx);
	ConditionVariablePrepareToSleep(&batch->cv_state);
	ConditionVariableCancelSleep();

	prev = pg_atomic_fetch_or_u32(&batch->next_slot_idx, DWB_SEAL_BIT);
	if (prev & DWB_SEAL_BIT)
		return false;			/* somebody else is the leader */

	/*
	 * We are the leader: nobody else can advance this batch anymore.  A
	 * failure between here and DWB_FSYNCED would leave the batch wedged
	 * forever, its waiters stuck and its staging buffer lost, so run the
	 * whole span as a critical section: any error escalates to PANIC and
	 * crash recovery resets the ring.  (This also suspends interrupt
	 * processing, making the coverage wait in DWBLeaderWriteBatch
	 * non-interruptible; its dwb_slot_stuck_timeout_ms PANIC is the
	 * backstop.)
	 */
	START_CRIT_SECTION();

	capped = Min(prev & DWB_IDX_MASK, (uint32) dwb_batch_pages);
	pg_atomic_write_u32(&batch->capped_slots, capped);
	pg_write_barrier();

	expected = DWB_ALLOCATED;
	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected, DWB_SEALED))
		elog(PANIC, "DWB batch %d sealed in unexpected state %u",
			 batch_idx, expected);
	ConditionVariableBroadcast(&batch->cv_state);

	if (capped == 0)
	{
		/*
		 * Defensive: a seal without a single reservation.  No leader write;
		 * cascade to FREE without I/O or publication.
		 */
		DWBStagingRelease(batch->staging_idx);
		batch->staging_idx = -1;
		pg_atomic_write_u32(&batch->state, DWB_FREE);
		ConditionVariableBroadcast(&DWBCtl->cv_free_batch);
		END_CRIT_SECTION();
		return true;
	}

	/*
	 * Pin the batch with a leader ref for the duration of the write.  All
	 * writers may exit while we write (dropping their refs), and the
	 * FSYNCED -> RETIRING hand-off runs when the last ref drops: the pin
	 * guarantees ref_count stays above zero until DWB_FSYNCED is reached,
	 * so the hand-off always has exactly one well-defined owner.
	 */
	pg_atomic_fetch_add_u32(&batch->ref_count, 1);

	DWBLeaderWriteBatch(batch_idx);

	END_CRIT_SECTION();

	if (pg_atomic_fetch_sub_u32(&batch->ref_count, 1) == 1)
		DWBFinishBatchData(batch);

	return true;
}

/*
 * Leader: wait for bitmap coverage of capped_slots, write the batch,
 * fdatasync, broadcast DWB_FSYNCED.
 *
 * Runs inside the leader's critical section (see DWBSealBatch); everything
 * it needs was allocated and opened before the seal, so no palloc happens
 * here.
 */
static void
DWBLeaderWriteBatch(int batch_idx)
{
	DWBatchCtl *batch = &DWBCtl->batches[batch_idx];
	uint32		capped = pg_atomic_read_u32(&batch->capped_slots);
	DWBBatchHeader hdr;
	TimestampTz wait_start = GetCurrentTimestamp();
	uint32		expected;

	Assert(CritSectionCount > 0);
	Assert(leader_metas != NULL);

	/*
	 * Coverage wait is memcpy-bound: writers do no I/O between reserving a
	 * slot and setting their bit.  The timeout is a defensive backstop
	 * (e.g. a writer stopped in a debugger); dead writers are covered by
	 * ref cleanup marking their slots DWB_SLOT_ABORTED.
	 */
	ConditionVariablePrepareToSleep(&batch->cv_state);
	for (;;)
	{
		bool		covered = true;
		uint32		full_words = capped / 64;
		uint32		tail_bits = capped % 64;

		for (uint32 i = 0; covered && i < full_words; i++)
			if (pg_atomic_read_u64(&batch->slots_written_bitmap[i]) !=
				PG_UINT64_MAX)
				covered = false;
		if (covered && tail_bits > 0)
		{
			uint64		mask = (UINT64CONST(1) << tail_bits) - 1;

			if ((pg_atomic_read_u64(&batch->slots_written_bitmap[full_words]) &
				 mask) != mask)
				covered = false;
		}
		if (covered)
			break;

		if (ConditionVariableTimedSleep(&batch->cv_state,
										dwb_slot_stuck_timeout_ms,
										WAIT_EVENT_DWB_BATCH_COVERAGE) &&
			TimestampDifferenceExceeds(wait_start, GetCurrentTimestamp(),
									   dwb_slot_stuck_timeout_ms))
			elog(PANIC, "DWB batch %d coverage wait exceeded %d ms",
				 batch_idx, dwb_slot_stuck_timeout_ms);
	}
	ConditionVariableCancelSleep();

	expected = DWB_SEALED;
	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected, DWB_WRITTEN))
		elog(PANIC, "DWB batch %d written in unexpected state %u",
			 batch_idx, expected);

	/* assemble slot metas entirely from shmem arrays */
	memset(leader_metas, 0, capped * sizeof(DWSlotMeta));
	batch->max_page_lsn = InvalidXLogRecPtr;
	for (uint32 i = 0; i < capped; i++)
	{
		DWSlotMeta *meta = &leader_metas[i];

		meta->tag = batch->pages[i];
		meta->page_lsn = batch->page_lsns[i];
		meta->generation = DWBCtl->ring_generation;
		meta->flags = batch->slot_flags[i];
		meta->image_crc = batch->image_crcs[i];
		meta->meta_crc = DWBSlotMetaCrc(meta);
		if (batch->page_lsns[i] > batch->max_page_lsn)
			batch->max_page_lsn = batch->page_lsns[i];
	}

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = DWB_BATCH_MAGIC;
	hdr.version = DWB_VERSION;
	hdr.batch_id = batch->batch_id;
	hdr.n_slots = capped;
	hdr.crc = DWBBatchHeaderCrc(&hdr);

	DWBWriteBatch(batch_idx, &hdr, leader_metas,
				  DWBStagingSlotPtr(batch->staging_idx, 0));

	/* image pwrite done — staging can serve the next batch */
	DWBStagingRelease(batch->staging_idx);
	batch->staging_idx = -1;

	expected = DWB_WRITTEN;
	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected, DWB_FSYNCED))
		elog(PANIC, "DWB batch %d fsynced in unexpected state %u",
			 batch_idx, expected);
	ConditionVariableBroadcast(&batch->cv_state);
}

/* ----------------------------------------------------------------
 * writer API
 * ----------------------------------------------------------------
 */

/*
 * Reserve a slot in the open batch (Stage 1: single writer class), record
 * the page tag and the segment ref, and take a batch ref.
 */
void
DWBAcquireSlot(const BufferTag *tag, DWBSlotRef *ref)
{
	const int	wclass = DWB_WCLASS_EVICTION;

	Assert(DWBIsEnabled());

	/* hard bound: overflowing the static array would corrupt memory */
	if (nPendingRefs >= (int) lengthof(pendingRefs))
		elog(ERROR, "too many pending double write buffer slot refs held by one backend");

	if (!cleanup_registered)
	{
		on_proc_exit(DWBProcExit, 0);
		cleanup_registered = true;
	}

	for (;;)
	{
		uint32		idx = pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]);
		DWBatchCtl *batch;
		uint32		prev;
		uint32		slot;

		if (idx == DWB_INVALID_BATCH)
		{
			DWBOpenNewBatch(wclass, idx);
			continue;
		}

		batch = &DWBCtl->batches[idx];
		prev = pg_atomic_fetch_add_u32(&batch->next_slot_idx, 1);

		if (prev & DWB_SEAL_BIT)
		{
			/* already sealed; the extra increment is harmless (3.4) */
			DWBOpenNewBatch(wclass, idx);
			continue;
		}

		slot = prev & DWB_IDX_MASK;
		if (slot >= (uint32) dwb_batch_pages)
		{
			/* overflow: this writer seals and (if it wins) leads */
			DWBSealBatch(idx);
			DWBOpenNewBatch(wclass, idx);
			continue;
		}

		/* valid reservation */
		batch->pages[slot] = *tag;
		batch->slot_flags[slot] = 0;

		{
			DWSegRef	seg;
			bool		found = false;

			seg.rlocator = BufTagGetRelFileLocator(tag);
			seg.forknum = BufTagGetForkNum(tag);
			seg.segno = tag->blockNum / RELSEG_SIZE;

			/*
			 * The dedup scan is O(n_segs), far too long for a spinlock;
			 * publish_lock is this batch's LWLock over n_segs/seg_set.
			 */
			LWLockAcquire(&batch->publish_lock, LW_EXCLUSIVE);
			for (uint32 i = 0; i < batch->n_segs; i++)
			{
				if (RelFileLocatorEquals(batch->seg_set[i].rlocator, seg.rlocator) &&
					batch->seg_set[i].forknum == seg.forknum &&
					batch->seg_set[i].segno == seg.segno)
				{
					found = true;
					break;
				}
			}
			if (!found)
				batch->seg_set[batch->n_segs++] = seg;
			LWLockRelease(&batch->publish_lock);
		}

		pg_atomic_fetch_add_u32(&batch->ref_count, 1);

		ref->batch_idx = (int) idx;
		ref->slot_idx = (int) slot;
		ref->batch_id = batch->batch_id;
		pendingRefs[nPendingRefs++] = *ref;
		return;
	}
}

/*
 * Publish the page image: memcpy into the batch's staging slot, record
 * LSN and image CRC in shmem, set our bitmap bit.
 */
void
DWBPublishImage(const DWBSlotRef *ref, const char *image, XLogRecPtr page_lsn)
{
	DWBatchCtl *batch = &DWBCtl->batches[ref->batch_idx];

	/* a held ref pins the batch, so its incarnation cannot have changed */
	Assert(ref->batch_id == batch->batch_id);

	memcpy(DWBStagingSlotPtr(batch->staging_idx, ref->slot_idx),
		   image, BLCKSZ);
	batch->page_lsns[ref->slot_idx] = page_lsn;
	batch->image_crcs[ref->slot_idx] = DWBImageCrc(image);

	pg_write_barrier();
	pg_atomic_fetch_or_u64(&batch->slots_written_bitmap[ref->slot_idx / 64],
						   UINT64CONST(1) << (ref->slot_idx % 64));
	ConditionVariableBroadcast(&batch->cv_state);
}

/*
 * Wait until the batch's DWB copy is durable.  The caller holds a batch
 * ref, so the batch cannot be retired or reused under us.
 */
void
DWBWaitBatchFsynced(const DWBSlotRef *ref)
{
	DWBatchCtl *batch = &DWBCtl->batches[ref->batch_idx];

	Assert(ref->batch_id == batch->batch_id);

	ConditionVariablePrepareToSleep(&batch->cv_state);
	while (pg_atomic_read_u32(&batch->state) < DWB_FSYNCED)
		ConditionVariableSleep(&batch->cv_state, WAIT_EVENT_DWB_BATCH_FSYNC);
	ConditionVariableCancelSleep();
}

/*
 * Step 7 of the write path: the last ref hands the batch over to
 * retirement (FSYNCED -> DATA_WRITTEN -> RETIRING).  (Stage 2 publishes
 * the segment set into DWSegmentHash here; Stage 1 retirement is
 * DWBRetireAllSync.)
 */
static void
DWBFinishBatchData(DWBatchCtl *batch)
{
	uint32		expected = DWB_FSYNCED;

	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
										DWB_DATA_WRITTEN))
		elog(PANIC, "DWB batch data-written in unexpected state %u", expected);

	pg_atomic_write_u32(&batch->seg_pending_count, batch->n_segs);
	pg_write_barrier();

	expected = DWB_DATA_WRITTEN;
	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
										DWB_RETIRING))
		elog(PANIC, "DWB batch retiring in unexpected state %u", expected);
	ConditionVariableSignal(&DWBCtl->cv_retire_wake);
}

/*
 * Drop our batch ref after the data-file write.  The last ref finishes the
 * batch (see DWBFinishBatchData).
 */
void
DWBReleaseSlot(const DWBSlotRef *ref)
{
	DWBatchCtl *batch = &DWBCtl->batches[ref->batch_idx];

	Assert(ref->batch_id == batch->batch_id);

	for (int i = 0; i < nPendingRefs; i++)
	{
		if (pendingRefs[i].batch_idx == ref->batch_idx &&
			pendingRefs[i].slot_idx == ref->slot_idx)
		{
			pendingRefs[i] = pendingRefs[--nPendingRefs];
			break;
		}
	}

	if (pg_atomic_fetch_sub_u32(&batch->ref_count, 1) == 1)
		DWBFinishBatchData(batch);
}

/*
 * Force-seal the currently open batch of a writer class (used by tests
 * now; the retire worker's dwb_batch_timeout_ms path in Stage 2).
 * Returns true if a batch was sealed by us.
 */
bool
DWBForceSealOpenBatch(int wclass)
{
	uint32		idx = pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]);

	if (idx == DWB_INVALID_BATCH)
		return false;
	if (pg_atomic_read_u32(&DWBCtl->batches[idx].next_slot_idx) == 0)
		return false;			/* empty batch: nothing to seal */
	return DWBSealBatch((int) idx);
}

/*
 * Synchronously retire every RETIRING batch.  Stage 1: batch images are
 * already durable in the ring and the test pages have no real relation
 * segments to fsync, so retirement is pure state bookkeeping.  Stage 2
 * replaces this with segment fsyncs by the retire worker pool and
 * ProcessSyncRequests.
 */
int
DWBRetireAllSync(void)
{
	int			retired = 0;

	for (int i = 0; i < dwb_num_batches; i++)
	{
		DWBatchCtl *batch = &DWBCtl->batches[i];
		uint32		expected = DWB_RETIRING;

		if (pg_atomic_read_u32(&batch->state) != DWB_RETIRING)
			continue;

		/* Stage 2: smgrimmedsync of each seg_set entry goes here */

		pg_atomic_write_u32(&batch->seg_pending_count, 0);
		if (pg_atomic_compare_exchange_u32(&batch->state, &expected,
										   DWB_FREE))
		{
			retired++;
			ConditionVariableBroadcast(&DWBCtl->cv_free_batch);
		}
	}
	return retired;
}

DWBatchState
DWBGetBatchState(int batch_idx)
{
	return (DWBatchState) pg_atomic_read_u32(&DWBCtl->batches[batch_idx].state);
}

/* ----------------------------------------------------------------
 * process exit cleanup
 * ----------------------------------------------------------------
 */

/*
 * Runs strictly on shmem DWB state: by the time on_proc_exit callbacks run,
 * LWLockReleaseAll has already dropped any content locks (ipc.c) and the
 * private page copy died with the process, so the staged copy in the batch
 * is the authoritative source for our slots.
 */
static void
DWBProcExit(int code, Datum arg)
{
	while (nPendingRefs > 0)
	{
		DWBSlotRef	ref = pendingRefs[--nPendingRefs];
		DWBatchCtl *batch = &DWBCtl->batches[ref.batch_idx];
		uint64		bit = UINT64CONST(1) << (ref.slot_idx % 64);
		pg_atomic_uint64 *word =
			&batch->slots_written_bitmap[ref.slot_idx / 64];

		if (!(pg_atomic_read_u64(word) & bit))
		{
			/* copy never published: poison the slot so the seal-waiter
			 * wakes up and recovery ignores it */
			batch->slot_flags[ref.slot_idx] |= DWB_SLOT_ABORTED;
			pg_write_barrier();
			pg_atomic_fetch_or_u64(word, bit);
			ConditionVariableBroadcast(&batch->cv_state);
		}
		else
		{
			/* copy published but smgrwrite may not have happened: hand the
			 * write over to the retire worker (Stage 2 completes orphans) */
			uint32		n = pg_atomic_fetch_add_u32(&batch->orphaned_refs_count, 1);

			batch->orphan_tags[n] = batch->pages[ref.slot_idx];
		}

		/*
		 * The last ref finishes the batch only once it is FSYNCED.  A
		 * sealed batch cannot lose its last ref earlier — the leader holds
		 * its own pin from SEAL to FSYNCED (see DWBSealBatch) — so reaching
		 * zero refs in an earlier state means the batch is not sealed yet:
		 * it stays open and a later seal completes it normally.
		 */
		if (pg_atomic_fetch_sub_u32(&batch->ref_count, 1) == 1 &&
			pg_atomic_read_u32(&batch->state) == DWB_FSYNCED)
			DWBFinishBatchData(batch);
	}
}
