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
 * FlushBuffer drives this through DWBStagePageWrite/DWBFinishPageWrite;
 * retirement (segment fsyncs, the worker pool) lives in dwb_retire.c.
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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/relpath.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Slot refs held by this backend.  A ref lives from DWBAcquireSlot to
 * DWBReleaseSlot; the write path also attaches it to the current
 * ResourceOwner, so that a transaction abort (e.g. an ERROR out of
 * smgrwrite) releases it long before process exit.  Entries have stable
 * addresses — the ResourceOwner remembers a pointer — so freeing is a flag,
 * not compaction.  Sized to two full batches because a backend can hold
 * refs on a sealed batch and on its successor at the same time.
 */
typedef struct DWBPendingRef
{
	DWBSlotRef	ref;
	ResourceOwner owner;		/* owner the ref is registered with, or NULL */
	bool		in_use;
} DWBPendingRef;

static DWBPendingRef pendingRefs[2 * DWB_BATCH_MAX_PAGES];
static bool cleanup_registered = false;

/* leader-side meta assembly area, allocated before the seal is attempted */
static DWSlotMeta *leader_metas = NULL;

/* Stage A pause of the bgwriter (see DWBWritesPaused) */
static bool bgwriter_paused = false;
static uint64 bgwriter_pause_snap = 0;

/*
 * Escalation clock of one wait for ring space.  The clock re-arms whenever
 * freed_events moves: escalation fires only when retirement as a whole has
 * made no progress for the full window, i.e. "broken", not "slow".
 */
typedef struct DWBStallState
{
	TimestampTz start;
	uint64		freed_snap;
	bool		warned;
} DWBStallState;

static void DWBProcExit(int code, Datum arg);
static void DWBLeaderWriteBatch(int batch_idx);
static bool DWBSealBatch(int batch_idx);
static void DWBFinishBatchData(DWBatchCtl *batch);
static void DWBAbandonRef(DWBPendingRef *pref);
static void ResOwnerReleaseDWBRef(Datum res);

/*
 * Released BEFORE the buffer-IO cleanup (smaller priority runs first):
 * DWBAbandonRef may repair the data page from the batch file, which is only
 * race-free while BM_IO_IN_PROGRESS of the aborted flush is still ours.
 */
static const ResourceOwnerDesc dwb_ref_resowner_desc =
{
	.name = "double write buffer slot ref",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_BUFFER_IOS - 10,
	.ReleaseResource = ResOwnerReleaseDWBRef,
	.DebugPrint = NULL,
};

static inline char *
DWBStagingSlotPtr(int staging_idx, int slot_idx)
{
	return DWBStagingBase +
		(Size) staging_idx * dwb_batch_pages * BLCKSZ +
		(Size) slot_idx * BLCKSZ;
}

/* ----------------------------------------------------------------
 * backpressure (3.6)
 * ----------------------------------------------------------------
 */

static void
DWBStallInit(DWBStallState *st)
{
	st->start = GetCurrentTimestamp();
	st->freed_snap = pg_atomic_read_u64(&DWBCtl->freed_events);
	st->warned = false;
}

/*
 * Escalate one iteration of a ring-space wait.  Stage A after
 * dwb_slow_warn_ms: WARNING, and the bgwriter additionally pauses its own
 * future flush rounds (DWBWritesPaused).  Stage B after
 * dwb_write_timeout_ms without a single retired batch: dwb_on_stall, except
 * that the checkpointer and the startup process always PANIC — an ERROR
 * there would fail the checkpoint or recovery anyway, without the fresh
 * start that crash recovery gives (the explicit safety policy of 3.6,
 * consistent with data_sync_elevel for a checkpoint-phase fsync failure).
 *
 * The dwb-force-stall injection point makes the current wait escalate to
 * Stage B immediately: the role policy and dwb_on_stall handling stay
 * exactly the production code paths, only the clock is bypassed.
 */
static void
DWBStallCheck(DWBStallState *st)
{
	uint64		freed = pg_atomic_read_u64(&DWBCtl->freed_events);
	TimestampTz now = GetCurrentTimestamp();
	long		waited;
	bool		forced;

	if (freed != st->freed_snap)
	{
		/* retirement made progress: re-arm */
		st->freed_snap = freed;
		st->start = now;
		st->warned = false;
		return;
	}

	waited = TimestampDifferenceMilliseconds(st->start, now);
	forced = IS_INJECTION_POINT_ATTACHED("dwb-force-stall");

	if (waited >= dwb_slow_warn_ms && !st->warned)
	{
		st->warned = true;
		ereport(WARNING,
				(errmsg("double write buffer has no free batch after %ld ms",
						waited)));
		if (MyBackendType == B_BG_WRITER)
		{
			bgwriter_paused = true;
			bgwriter_pause_snap = freed;
		}
	}

	if (waited >= dwb_write_timeout_ms || forced)
	{
		if (AmCheckpointerProcess() || AmStartupProcess() ||
			CritSectionCount > 0 || dwb_on_stall == DWB_ON_STALL_PANIC)
			ereport(PANIC,
					(errmsg("double write buffer retirement made no progress within \"dwb_write_timeout_ms\""),
					 errdetail("No batch was retired while a %s process waited for ring space.",
							   GetBackendTypeDesc(MyBackendType))));
		if (dwb_on_stall == DWB_ON_STALL_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("double write buffer retirement made no progress within \"dwb_write_timeout_ms\"")));
		/* DWB_ON_STALL_WARN: complain and keep waiting */
		ereport(WARNING,
				(errmsg("double write buffer retirement made no progress within \"dwb_write_timeout_ms\", still waiting")));
		st->start = now;
	}
}

/*
 * Has a Stage A stall told the bgwriter to sit out its flush rounds?
 * Clears itself as soon as any batch retires.
 */
bool
DWBWritesPaused(void)
{
	if (!bgwriter_paused)
		return false;
	if (pg_atomic_read_u64(&DWBCtl->freed_events) != bgwriter_pause_snap)
	{
		bgwriter_paused = false;
		return false;
	}
	return true;
}

/* ----------------------------------------------------------------
 * staging pool
 * ----------------------------------------------------------------
 */
static int
DWBStagingAlloc(void)
{
	int			idx;
	DWBStallState stall;

	DWBStallInit(&stall);
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
		 * retirement broadcasts cv_free_batch too, so just re-check on every
		 * wake-up.  The timeout only paces the stall clock.
		 */
		(void) ConditionVariableTimedSleep(&DWBCtl->cv_free_batch, 1000,
										   WAIT_EVENT_DWB_FREE_BATCH);
		DWBStallCheck(&stall);
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
 *
 * Non-static only for test_dwb's stale-open regression test.
 */
void
DWBOpenNewBatch(int wclass, uint32 old_idx)
{
	DWBStallState stall;

	DWBStallInit(&stall);
	for (;;)
	{
		int			free_idx = -1;
		int			nfree = 0;
		int			staging_idx;

		/*
		 * Reserve the staging buffer before taking the lock: the wait for a
		 * free buffer can be long, and no sleeping (or interruptible) point
		 * may exist below, where we hold DWBRingOpenLock with a batch already
		 * taken out of DWB_FREE.
		 */
		staging_idx = DWBStagingAlloc();

		LWLockAcquire(DWBRingOpenLock, LW_EXCLUSIVE);

		/*
		 * Someone else already replaced the open batch: done.  Comparing the
		 * index alone is not enough: the ring reuses indexes, so by the time
		 * a slow opener gets here, old_idx may name a NEW live incarnation of
		 * the same slot (sealed, retired, freed and reopened behind our
		 * back), and replacing it would orphan that live batch together with
		 * its staging buffer.  SEAL_BIT disambiguates the incarnations: it is
		 * set from SEAL through FREE and cleared only by the
		 * re-initialization below, under this same lock — so the open batch
		 * needs replacing if and only if its SEAL_BIT is set.
		 */
		{
			uint32		cur = pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]);

			if (cur != old_idx ||
				(cur != DWB_INVALID_BATCH &&
				 !(pg_atomic_read_u32(&DWBCtl->batches[cur].next_slot_idx) &
				   DWB_SEAL_BIT)))
			{
				LWLockRelease(DWBRingOpenLock);
				DWBStagingRelease(staging_idx);
				ConditionVariableCancelSleep();
				return;
			}
		}

		/*
		 * Count FREE batches first: a background-class open must leave
		 * DWB_EVICT_RESERVE of them for user evictions, so that a
		 * checkpoint's BufferSync storm cannot eat the ring from under
		 * latency-critical paths.  FREE->ALLOCATED happens only under
		 * DWBRingOpenLock, and concurrent retirements only grow the count, so
		 * the check cannot overestimate.
		 */
		for (int i = 0; i < dwb_num_batches; i++)
			if (pg_atomic_read_u32(&DWBCtl->batches[i].state) == DWB_FREE)
				nfree++;

		if (nfree > (wclass == DWB_WCLASS_BACKGROUND ? DWB_EVICT_RESERVE : 0))
		{
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
			Assert(free_idx >= 0);
		}

		if (free_idx >= 0)
		{
			DWBatchCtl *batch = &DWBCtl->batches[free_idx];

			for (int w = 0; w < DWB_BITMAP_WORDS; w++)
				pg_atomic_write_u64(&batch->slots_written_bitmap[w], 0);
			pg_atomic_write_u32(&batch->capped_slots, 0);
			pg_atomic_write_u32(&batch->ref_count, 0);
			pg_atomic_write_u32(&batch->seg_pending_count, 0);
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

			/* let retire workers re-time the force-seal deadline */
			ConditionVariableBroadcast(&DWBCtl->cv_retire_wake);
			return;
		}

		LWLockRelease(DWBRingOpenLock);
		DWBStagingRelease(staging_idx);

		/*
		 * No usable FREE batch.  Help ourselves before waiting: sweep the
		 * RETIRING batches synchronously.  Under normal operation the worker
		 * pool keeps the ring ahead of the writers and this path is rare;
		 * when it does run, the per-segment claim keeps us and the workers
		 * from duplicating fsyncs.  This is also what keeps the ring alive
		 * with dwb_retire_workers = 0 and in single-user mode.
		 */
		if (DWBRetireAllSync() > 0)
			continue;

		(void) ConditionVariableTimedSleep(&DWBCtl->cv_free_batch, 1000,
										   WAIT_EVENT_DWB_FREE_BATCH);
		DWBStallCheck(&stall);
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
	 * Get everything the critical section below could fail at out of the way
	 * while failure is still harmless (the seal has not been attempted yet,
	 * so on ERROR the batch stays ALLOCATED and any other writer can seal it
	 * later): the one-time leader allocations, the batch file VFD, and this
	 * backend's condition-variable wait event set (the first
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
		pg_atomic_fetch_add_u64(&DWBCtl->freed_events, 1);
		ConditionVariableBroadcast(&DWBCtl->cv_free_batch);
		END_CRIT_SECTION();
		return true;
	}

	/*
	 * Pin the batch with a leader ref for the duration of the write.  All
	 * writers may exit while we write (dropping their refs), and the FSYNCED
	 * -> RETIRING hand-off runs when the last ref drops: the pin guarantees
	 * ref_count stays above zero until DWB_FSYNCED is reached, so the
	 * hand-off always has exactly one well-defined owner.
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
	 * slot and setting their bit.  The timeout is a defensive backstop (e.g.
	 * a writer stopped in a debugger); dead writers are covered by ref
	 * cleanup marking their slots DWB_SLOT_ABORTED.
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
 * Try to seal a batch if it is still an open, non-empty ALLOCATED one; the
 * guards make it safe to call speculatively against any state.  Entry point
 * of the decentralized seal triggers: the lone-writer fast seal and the
 * timeout seal in DWBWaitBatchFsynced, the no-worker-pool seal in
 * DWBStagePageWrite, DWBForceSealOpenBatch, and the retire workers'
 * force-seal on dwb_batch_timeout_ms.  The overflow writer in
 * DWBAcquireSlot calls DWBSealBatch directly instead: it has just consumed
 * the first slot index past the cap, so it already knows the batch is full
 * and non-empty.
 */
bool
DWBTrySealBatch(int batch_idx)
{
	DWBatchCtl *batch = &DWBCtl->batches[batch_idx];
	uint32		nsi = pg_atomic_read_u32(&batch->next_slot_idx);

	if (nsi & DWB_SEAL_BIT)
		return false;			/* sealed already (or FREE: the bit is held
								 * through FREE until reopen) */
	if ((nsi & DWB_IDX_MASK) == 0)
		return false;			/* empty: sealing buys nothing */
	if (pg_atomic_read_u32(&batch->state) != DWB_ALLOCATED)
		return false;
	return DWBSealBatch(batch_idx);
}

/*
 * Reserve a slot in the open batch of the given writer class, record the
 * page tag and the segment ref, and take a batch ref.  With use_resowner
 * the ref is also attached to CurrentResourceOwner, so a transaction abort
 * releases it (the write path always does this; tests exercising proc-exit
 * cleanup do not).
 */
void
DWBAcquireSlot(const BufferTag *tag, int wclass, bool use_resowner,
			   DWBSlotRef *ref)
{
	DWBPendingRef *pref = NULL;

	Assert(DWBIsEnabled());
	Assert(wclass >= 0 && wclass < DWB_NUM_WCLASSES);

	for (int i = 0; i < (int) lengthof(pendingRefs); i++)
	{
		if (!pendingRefs[i].in_use)
		{
			pref = &pendingRefs[i];
			break;
		}
	}
	/* hard bound: overflowing the static array would corrupt memory */
	if (pref == NULL)
		elog(ERROR, "too many pending double write buffer slot refs held by one backend");

	/* no failure window between the reservation below and remembering it */
	if (use_resowner)
		ResourceOwnerEnlarge(CurrentResourceOwner);

	if (!cleanup_registered)
	{
		/*
		 * before_shmem_exit, NOT on_proc_exit: dropping the last ref of a
		 * durable batch publishes its seg_set under LWLocks, which is only
		 * legal while our PGPROC is alive — on_proc_exit callbacks run
		 * after ProcKill has released it.
		 */
		before_shmem_exit(DWBProcExit, 0);
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

			/* becomes a HASH_BLOBS key at publication: no padding garbage */
			memset(&seg, 0, sizeof(seg));
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

		pref->ref = *ref;
		pref->owner = use_resowner ? CurrentResourceOwner : NULL;
		pref->in_use = true;
		if (pref->owner)
			ResourceOwnerRemember(pref->owner, PointerGetDatum(pref),
								  &dwb_ref_resowner_desc);
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
 *
 * A batch that nobody seals would leave its writers waiting forever, so
 * after dwb_batch_timeout_ms of waiting on a still-open batch the waiter
 * seals it itself.  The retire workers force-seal on the same timeout;
 * this decentralized backstop keeps the write path independent of the
 * worker pool (dwb_retire_workers = 0, single-user mode, a stuck worker).
 */
void
DWBWaitBatchFsynced(const DWBSlotRef *ref)
{
	DWBatchCtl *batch = &DWBCtl->batches[ref->batch_idx];

	Assert(ref->batch_id == batch->batch_id);

	/*
	 * A lone writer has nobody to batch with: sequential flush streams
	 * (recovery, a backend evicting page after page, BufferSync) reach this
	 * wait one page at a time, and paying dwb_batch_timeout_ms per page would
	 * dominate the stream.  If our ref is the only one on a still-open batch,
	 * seal right away; under concurrency ref_count > 1 keeps the rendezvous
	 * window open for the timeout.  A racing second writer merely bounces to
	 * the next batch — sealing is valid at any moment.
	 */
	if (pg_atomic_read_u32(&batch->ref_count) == 1)
		(void) DWBTrySealBatch(ref->batch_idx);

	ConditionVariablePrepareToSleep(&batch->cv_state);
	while (pg_atomic_read_u32(&batch->state) < DWB_FSYNCED)
	{
		if (ConditionVariableTimedSleep(&batch->cv_state,
										dwb_batch_timeout_ms,
										WAIT_EVENT_DWB_BATCH_FSYNC))
			(void) DWBTrySealBatch(ref->batch_idx);
	}
	ConditionVariableCancelSleep();
}

/*
 * Step 7 of the write path: the last ref hands the batch over to
 * retirement — FSYNCED -> DATA_WRITTEN, then the seg_set publication into
 * DWSegmentHash and the RETIRING transition (dwb_retire.c).
 */
static void
DWBFinishBatchData(DWBatchCtl *batch)
{
	uint32		expected = DWB_FSYNCED;

	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
										DWB_DATA_WRITTEN))
		elog(PANIC, "DWB batch data-written in unexpected state %u", expected);

	DWBPublishBatchSegSet((int) (batch - DWBCtl->batches));
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

	for (int i = 0; i < (int) lengthof(pendingRefs); i++)
	{
		DWBPendingRef *pref = &pendingRefs[i];

		if (pref->in_use &&
			pref->ref.batch_idx == ref->batch_idx &&
			pref->ref.slot_idx == ref->slot_idx)
		{
			if (pref->owner != NULL)
				ResourceOwnerForget(pref->owner, PointerGetDatum(pref),
									&dwb_ref_resowner_desc);
			pref->owner = NULL;
			pref->in_use = false;
			break;
		}
	}

	if (pg_atomic_fetch_sub_u32(&batch->ref_count, 1) == 1)
		DWBFinishBatchData(batch);
}

/*
 * Force-seal the currently open batch of a writer class.  Returns true if
 * a batch was sealed by us.
 */
bool
DWBForceSealOpenBatch(int wclass)
{
	uint32		idx = pg_atomic_read_u32(&DWBCtl->open_batch_idx[wclass]);

	if (idx == DWB_INVALID_BATCH)
		return false;
	return DWBTrySealBatch((int) idx);
}

DWBatchState
DWBGetBatchState(int batch_idx)
{
	return (DWBatchState) pg_atomic_read_u32(&DWBCtl->batches[batch_idx].state);
}

/* ----------------------------------------------------------------
 * FlushBuffer entry points
 * ----------------------------------------------------------------
 */

/*
 * The checkpointer's BufferSync and the bgwriter's flush rounds form the
 * background stream; everything else — ordinary backend evictions above
 * all — is the latency-critical class with first claim on FREE batches.
 */
static int
DWBWriterClass(void)
{
	if (MyBackendType == B_CHECKPOINTER || MyBackendType == B_BG_WRITER)
		return DWB_WCLASS_BACKGROUND;
	return DWB_WCLASS_EVICTION;
}

/*
 * Steps 3-5 of the write path (3.4): reserve a slot in this writer class's
 * open batch, publish the private page copy, and wait until the batch copy
 * is durable in pg_dwb/.  On return the caller may write the same copy to
 * the data file.  The caller must already have flushed WAL up to page_lsn.
 */
void
DWBStagePageWrite(const BufferTag *tag, const char *image,
				  XLogRecPtr page_lsn, DWBSlotRef *ref)
{
	Assert(DWBCtl->ring_generation > 0);

	DWBAcquireSlot(tag, DWBWriterClass(), true, ref);
	DWBPublishImage(ref, image, page_lsn);

	/*
	 * With no worker pool (dwb_retire_workers = 0, single-user mode) a lonely
	 * batch would only seal via the wait timeout below; seal it right away
	 * instead of paying dwb_batch_timeout_ms per page.
	 */
	if (dwb_retire_workers == 0 || !IsUnderPostmaster)
		(void) DWBTrySealBatch(ref->batch_idx);

	DWBWaitBatchFsynced(ref);

	INJECTION_POINT("dwb-after-batch-fsynced", NULL);
}

/*
 * Step 7: release the ref after smgrwrite returned.  Without a worker
 * pool, also retire synchronously so the ring keeps circulating (and, in
 * the TAP tests, returns to all-FREE after every flush).
 */
void
DWBFinishPageWrite(const DWBSlotRef *ref)
{
	DWBReleaseSlot(ref);

	if (dwb_retire_workers == 0 || !IsUnderPostmaster)
		(void) DWBRetireAllSync();
}

/* ----------------------------------------------------------------
 * abort / process exit cleanup
 * ----------------------------------------------------------------
 */

/*
 * Repair the data page of an abandoned ref from the batch file.
 *
 * If the writer died out of a failed smgrwrite (step 6), the data page may
 * be torn on disk while the batch — and with it the only whole copy — is
 * about to retire and recycle.  Overwriting the page with the durable batch
 * copy makes the disk page whole again; the shared buffer is still dirty
 * (the abort path never clears BM_DIRTY), so newer content still reaches
 * the disk through a later flush.
 *
 * This runs for refs that were attached to a ResourceOwner, either from the
 * owner's release (BEFORE the buffer-IO cleanup) or from the proc-exit
 * backstop when abort cleanup was cut short (see DWBProcExit): in both
 * cases BM_IO_IN_PROGRESS of the failed flush is still ours, so no
 * concurrent flush of the same page can be in flight and writing the
 * (possibly stale) batch copy cannot overwrite a newer image.  For the same
 * reason the relation cannot be dropped or truncated under us — both
 * invalidate the buffer first and that waits for our IO flag — so the
 * ENOENT/short-file exits are pure defense (and serve test refs pointing at
 * fake relations).
 *
 * Durability: our segment is in the batch's seg_set, and the seg_set is
 * published only after every ref (ours included) is gone, so retirement
 * fsyncs this segment strictly after this write.
 *
 * Runs from release callbacks that must not fail, so the whole path is
 * allocation-free: BasicOpenFile + raw pg_pwrite here (and the same inside
 * DWBReadSlotImage) instead of OpenTransientFile/VFD, whose descriptor
 * reservation and name bookkeeping can throw ERROR.  Every failure other
 * than the dropped/truncated-relation exits is PANIC.
 */
static void
DWBRewriteAbandonedSlot(const DWBSlotRef *ref)
{
	DWBatchCtl *batch = &DWBCtl->batches[ref->batch_idx];
	BufferTag	tag = batch->pages[ref->slot_idx];
	uint32		segno = tag.blockNum / ((BlockNumber) RELSEG_SIZE);
	PGAlignedBlock image;
	RelPathStr	relpath;
	char		path[MAXPGPATH];
	int			fd;
	struct stat st;
	off_t		off;
	ssize_t		written;

	DWBReadSlotImage(ref->batch_idx, ref->slot_idx, image.data);

	relpath = relpathperm(BufTagGetRelFileLocator(&tag),
						  BufTagGetForkNum(&tag));
	if (segno == 0)
		snprintf(path, MAXPGPATH, "%s", relpath.str);
	else
		snprintf(path, MAXPGPATH, "%s.%u", relpath.str, segno);

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return;				/* relation dropped: the write is moot */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" to repair an abandoned double write buffer slot: %m",
						path)));
	}

	off = (off_t) (tag.blockNum % ((BlockNumber) RELSEG_SIZE)) * BLCKSZ;
	if (fstat(fd, &st) < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));
	if (off + BLCKSZ > st.st_size)
	{
		/* segment truncated: the write is moot */
		close(fd);
		return;
	}

	errno = 0;
	written = pg_pwrite(fd, image.data, BLCKSZ, off);
	if (written != BLCKSZ)
	{
		if (errno == 0)
			errno = ENOSPC;
		/* the page may now be torn with its DWB copy about to recycle */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not repair block %u of file \"%s\" from the double write buffer: %m",
						tag.blockNum, path)));
	}

	if (close(fd) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));
}

/*
 * Release a ref whose owner is going away without the normal
 * DWBReleaseSlot: transaction abort (ResourceOwner) or process exit.  No
 * ResourceOwnerForget here — the ResourceOwner path removes the resource
 * implicitly, and at process exit the owner dies with the process.  Works
 * strictly on shmem DWB state plus the batch file — the private page copy
 * of the writer is gone.
 */
static void
DWBAbandonRef(DWBPendingRef *pref)
{
	DWBSlotRef	ref = pref->ref;
	DWBatchCtl *batch = &DWBCtl->batches[ref.batch_idx];
	uint64		bit = UINT64CONST(1) << (ref.slot_idx % 64);
	pg_atomic_uint64 *word =
		&batch->slots_written_bitmap[ref.slot_idx / 64];
	bool		had_owner = (pref->owner != NULL);

	/*
	 * Idempotent: the exit backstop (before_shmem_exit) and a later
	 * ResourceOwner release may both reach the same entry — the relative
	 * order of exit callbacks is not fixed across process types.
	 */
	if (!pref->in_use)
		return;

	pref->owner = NULL;
	pref->in_use = false;

	/* a held ref pins the batch, so its incarnation cannot have changed */
	Assert(ref.batch_id == batch->batch_id);

	if (!(pg_atomic_read_u64(word) & bit))
	{
		/*
		 * Copy never published: poison the slot so the seal-waiter wakes up
		 * and recovery ignores it.
		 */
		batch->slot_flags[ref.slot_idx] |= DWB_SLOT_ABORTED;
		pg_write_barrier();
		pg_atomic_fetch_or_u64(word, bit);
		ConditionVariableBroadcast(&batch->cv_state);
	}
	else if (had_owner &&
			 pg_atomic_read_u32(&batch->state) >= DWB_FSYNCED)
	{
		/*
		 * Copy published and the batch is durable, which means the writer was
		 * at or past step 6: its smgrwrite may have failed halfway. Make the
		 * data page whole again from the batch copy.
		 *
		 * Only for refs that were attached to a ResourceOwner: those are real
		 * write-path refs, and their BM_IO_IN_PROGRESS is still held here (on
		 * the proc-exit path too, see DWBProcExit).  An ownerless (test) ref
		 * never had the buffer-IO interlock, so the repair write would race a
		 * concurrent flush of the same page.
		 */
		DWBRewriteAbandonedSlot(&ref);
	}

	/*
	 * The last ref finishes the batch only once it is FSYNCED.  A sealed
	 * batch cannot lose its last ref earlier — the leader holds its own pin
	 * from SEAL to FSYNCED (see DWBSealBatch) — so reaching zero refs in an
	 * earlier state means the batch is not sealed yet: it stays open and a
	 * later seal completes it normally.
	 */
	if (pg_atomic_fetch_sub_u32(&batch->ref_count, 1) == 1 &&
		pg_atomic_read_u32(&batch->state) == DWB_FSYNCED)
		DWBFinishBatchData(batch);
}

/*
 * ResourceOwner release of one ref: the abort path of the write path.
 */
static void
ResOwnerReleaseDWBRef(Datum res)
{
	DWBAbandonRef((DWBPendingRef *) DatumGetPointer(res));
}

/*
 * Exit backstop for refs that no ResourceOwner released.  Runs as a
 * before_shmem_exit callback: the PGPROC is still alive, so the LWLocks
 * taken by a last-ref publication (publish_lock, DWBSegHashLock) are legal
 * here — unlike in on_proc_exit callbacks, which run after ProcKill.  It
 * may run BEFORE the ResourceOwner release of the same refs (callback
 * registration order); DWBAbandonRef is idempotent, so whichever side runs
 * second is a no-op.
 *
 * An owned ref can only get here when abort cleanup was cut short before
 * the ResourceOwner release phase (e.g. a FATAL thrown out of the abort
 * path itself).  In that case the buffer-IO resource of the failed flush
 * was not released either: it lives in the SAME owner and releases AFTER
 * the DWB ref (ascending priority within the phase, RELEASE_PRIO_BUFFER_IOS
 * - 10 before RELEASE_PRIO_BUFFER_IOS; on the success path
 * DWBFinishPageWrite likewise precedes TerminateBufferIO).  So whenever an
 * owned ref is still alive, BM_IO_IN_PROGRESS is still ours and the
 * abandoned-slot repair is exactly as race-free as on the ResourceOwner
 * path.  It is also the last chance to repair: a FATAL exit does not
 * trigger crash recovery, so no apply-pass would ever fix a torn page.
 *
 * Ownerless refs are test refs (DWBAcquireSlot with use_resowner = false);
 * they never had the interlock and DWBAbandonRef skips the repair write for
 * them.
 */
static void
DWBProcExit(int code, Datum arg)
{
	for (int i = 0; i < (int) lengthof(pendingRefs); i++)
	{
		if (pendingRefs[i].in_use)
			DWBAbandonRef(&pendingRefs[i]);
	}
}
