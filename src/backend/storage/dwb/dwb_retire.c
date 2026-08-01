/*-------------------------------------------------------------------------
 *
 * dwb_retire.c
 *	  Retirement of double write buffer batches: the segment->batch
 *	  back-reference hash, the durability accounting that frees batches,
 *	  and the retire worker pool.
 *
 * A batch reaches FREE only after every segment in its seg_set has been
 * fsynced after the batch's data-file writes.  The accounting protocol
 * (3.5 of the design plan):
 *
 *	 - publication: at DATA_WRITTEN -> RETIRING, under the batch's
 *	   publish_lock, seg_pending_count := n_segs and one bit per segment is
 *	   set in the segment's DWSegmentHash entry;
 *	 - decrement: a successful segment fsync clears the bits that were
 *	   already set BEFORE the fsync started (a bit published mid-fsync may
 *	   cover a write the fsync missed) and decrements seg_pending_count of
 *	   the owning batches; the decrement to zero frees the batch;
 *	 - ABA guard: bits address batches by ring index, which is reused, so
 *	   the fsyncer snapshots (batch_idx, batch_id) before the fsync and,
 *	   under that batch's publish_lock, decrements only if batch_id still
 *	   matches.
 *
 * Fsyncs come from three independent sources: the retire worker pool
 * (proactive, partitioned by segment hash), ProcessSyncRequests in the
 * checkpointer (opportunistic, wrapped by DWBSegmentFsyncBegin/End), and
 * writers stuck on a full ring helping themselves (DWBRetireAllSync).
 * All of them share this accounting; duplicate fsyncs are wasted work at
 * worst, never a correctness problem.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_retire.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "common/int.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dwb.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/sync.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

static void DWBMaybeRemoveSegEntry(DWSegEntry *entry);
static int	DWBRetireSweep(int worker_id);

/*
 * Snapshot of one segment's back-references, taken before an fsync of that
 * segment and consumed after it.  One at a time per process: a process
 * fsyncs one segment at a time on every path that uses this.
 */
typedef struct DWBSegSyncSnap
{
	bool		active;
	DWSegRef	seg;
	int			npairs;
	struct
	{
		int			batch_idx;
		uint64		batch_id;
	}			pairs[DWB_NUM_BATCHES_MAX];
} DWBSegSyncSnap;

static DWBSegSyncSnap seg_sync_snap;

static DWSegRef
DWBSegRefFromFileTag(const FileTag *ftag)
{
	DWSegRef	seg;

	memset(&seg, 0, sizeof(seg));	/* keyed by memcmp: no padding garbage */
	seg.rlocator = ftag->rlocator;
	seg.forknum = (ForkNumber) ftag->forknum;
	seg.segno = (uint32) ftag->segno;
	return seg;
}

static FileTag
DWBFileTagFromSegRef(const DWSegRef *seg)
{
	FileTag		tag;

	memset(&tag, 0, sizeof(tag));
	tag.handler = SYNC_HANDLER_MD;
	tag.forknum = (int16) seg->forknum;
	tag.rlocator = seg->rlocator;
	tag.segno = seg->segno;
	return tag;
}

/* ----------------------------------------------------------------
 * publication
 * ----------------------------------------------------------------
 */

/*
 * Free a batch and wake one would-be opener per writer class.  The caller
 * has already moved the state to DWB_FREE.
 */
static void
DWBNoteBatchFreed(void)
{
	pg_atomic_fetch_add_u64(&DWBCtl->freed_events, 1);
	DWBWakeRingWaiters();
}

/*
 * Fsync one segment for retirement purposes, tolerating a concurrently
 * dropped relation: the data-file writes of a dropped segment are moot, so
 * ENOENT counts as covered.  Returns true if the segment is covered (fsynced
 * or dropped).
 *
 * A real fsync failure follows the vanilla data_sync_retry policy: PANIC by
 * default, but with data_sync_retry = on the kernel is trusted to keep the
 * dirty pages, so this must NOT throw -- the callers hold accounting state
 * (the advisory fsync claim, the OOM batch state) that a longjmp would leak
 * forever.  Instead it WARNs and returns false; the segment's back-reference
 * bits stay set and a later fsyncer retries.  force_panic is for the one
 * caller that has no later fsyncer to fall back on (DWBRetireBatchSyncOOM).
 */
static bool
DWBRetireSyncSegment(const DWSegRef *seg, bool force_panic)
{
	FileTag		tag = DWBFileTagFromSegRef(seg);
	char		path[MAXPGPATH];

	if (mdsyncfiletag(&tag, path) < 0)
	{
		if (errno == ENOENT)
		{
			elog(DEBUG1, "DWB: segment \"%s\" dropped during retire, skipping fsync",
				 path);
			return true;
		}
		ereport(force_panic ? PANIC : data_sync_elevel(WARNING),
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", path)));
		return false;
	}
	return true;
}

/*
 * Synchronous retire of a batch whose seg_set could not be published into a
 * full DWSegmentHash: the publisher itself fsyncs every segment and frees
 * the batch, so an undersized hash degrades throughput instead of wedging
 * the ring.  Runs with no locks held; the DWB_OOM_RETIRING state keeps
 * everyone else away from the batch.
 *
 * An fsync failure here is a PANIC even under data_sync_retry = on: nothing
 * ever revisits a DWB_OOM_RETIRING batch (retire sweeps only collect
 * DWB_RETIRING, and the hash bits were rolled back), so a soft failure would
 * leak the batch until restart -- and this can run inside a ResourceOwner
 * release callback, which must not fail (resowner.h).
 */
static void
DWBRetireBatchSyncOOM(DWBatchCtl *batch)
{
	uint32		expected;

	for (uint32 i = 0; i < batch->n_segs; i++)
		(void) DWBRetireSyncSegment(&batch->seg_set[i], true);

	expected = DWB_OOM_RETIRING;
	if (!pg_atomic_compare_exchange_u32(&batch->state, &expected, DWB_FREE))
		elog(PANIC, "DWB batch freed in unexpected state %u", expected);
	DWBNoteBatchFreed();

	ereport(WARNING,
			(errmsg("double write buffer segment hash is full"),
			 errhint("Consider increasing \"dwb_max_segments\".")));
}

/*
 * Publish a batch's seg_set into DWSegmentHash and hand the batch over to
 * retirement (DATA_WRITTEN -> RETIRING).  Called by whoever drops the last
 * ref (see DWBFinishBatchData).
 */
void
DWBPublishBatchSegSet(int batch_idx)
{
	DWBatchCtl *batch = &DWBCtl->batches[batch_idx];
	uint32		published = 0;
	bool		oom = false;
	uint32		expected;

	/*
	 * A sealed non-empty batch always has at least one segment (every slot
	 * reservation dedup-inserts its segment).  Publishing an empty seg_set
	 * would move the batch to DWB_RETIRING with nothing to ever decrement it
	 * to FREE.
	 */
	Assert(batch->n_segs > 0);

	LWLockAcquire(&batch->publish_lock, LW_EXCLUSIVE);

	pg_atomic_write_u32(&batch->seg_pending_count, batch->n_segs);
	pg_write_barrier();

	LWLockAcquire(DWBSegHashLock, LW_EXCLUSIVE);
	for (published = 0; published < batch->n_segs; published++)
	{
		bool		found;
		DWSegEntry *entry;

		entry = (DWSegEntry *) hash_search(DWSegmentHash,
										   &batch->seg_set[published],
										   HASH_ENTER_NULL, &found);
		if (entry == NULL)
		{
			oom = true;
			break;
		}
		if (!found)
		{
			pg_atomic_init_u32(&entry->fsync_in_progress, 0);
			for (uint32 w = 0; w < DWBSegBitmapWords(); w++)
				pg_atomic_init_u64(&entry->batch_bitmap[w], 0);
		}
		pg_atomic_fetch_or_u64(&entry->batch_bitmap[batch_idx / 64],
							   UINT64CONST(1) << (batch_idx % 64));
	}

	if (oom)
	{
		/*
		 * Take the partial publication back.  Nobody saw those bits: they
		 * were set and are removed under one continuous exclusive hold of
		 * DWBSegHashLock.
		 */
		for (uint32 i = 0; i < published; i++)
		{
			bool		found;
			DWSegEntry *entry;

			entry = (DWSegEntry *) hash_search(DWSegmentHash,
											   &batch->seg_set[i],
											   HASH_FIND, &found);
			if (entry == NULL)
				continue;
			pg_atomic_fetch_and_u64(&entry->batch_bitmap[batch_idx / 64],
									~(UINT64CONST(1) << (batch_idx % 64)));
			DWBMaybeRemoveSegEntry(entry);
		}
		pg_atomic_write_u32(&batch->seg_pending_count, 0);

		expected = DWB_DATA_WRITTEN;
		if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
											DWB_OOM_RETIRING))
			elog(PANIC, "DWB batch OOM-retiring in unexpected state %u",
				 expected);
	}
	LWLockRelease(DWBSegHashLock);

	if (!oom)
	{
		pg_write_barrier();
		expected = DWB_DATA_WRITTEN;
		if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
											DWB_RETIRING))
			elog(PANIC, "DWB batch retiring in unexpected state %u", expected);
	}
	LWLockRelease(&batch->publish_lock);

	if (oom)
		DWBRetireBatchSyncOOM(batch);
	else
		ConditionVariableBroadcast(&DWBCtl->cv_retire_wake);
}

/* ----------------------------------------------------------------
 * decrement
 * ----------------------------------------------------------------
 */

/*
 * Remove a segment entry once its bitmap is empty.  Caller holds
 * DWBSegHashLock exclusive.  Safe regardless of fsync_in_progress: nobody
 * keeps entry pointers across the lock, claim holders re-look-up by key.
 */
static void
DWBMaybeRemoveSegEntry(DWSegEntry *entry)
{
	for (uint32 w = 0; w < DWBSegBitmapWords(); w++)
		if (pg_atomic_read_u64(&entry->batch_bitmap[w]) != 0)
			return;
	if (hash_search(DWSegmentHash, &entry->key, HASH_REMOVE, NULL) == NULL)
		elog(PANIC, "DWB segment hash entry vanished under exclusive lock");
}

/*
 * Snapshot the back-references of one segment before fsyncing it.
 */
static void
DWBSegSnapBegin(const DWSegRef *seg)
{
	DWSegEntry *entry;

	/*
	 * overwriting a leftover snapshot (an fsync that errored out between
	 * Begin and End) is a correct drop: its bits were never cleared and a
	 * later fsyncer covers them; see also DWBSegmentFsyncBegin
	 */
	seg_sync_snap.active = true;
	seg_sync_snap.seg = *seg;
	seg_sync_snap.npairs = 0;

	LWLockAcquire(DWBSegHashLock, LW_SHARED);
	entry = (DWSegEntry *) hash_search(DWSegmentHash, seg, HASH_FIND, NULL);
	if (entry != NULL)
	{
		for (uint32 w = 0; w < DWBSegBitmapWords(); w++)
		{
			uint64		word = pg_atomic_read_u64(&entry->batch_bitmap[w]);

			while (word != 0)
			{
				int			bit = pg_rightmost_one_pos64(word);
				int			idx = (int) (w * 64) + bit;

				word &= word - 1;
				Assert(seg_sync_snap.npairs < (int) lengthof(seg_sync_snap.pairs));
				seg_sync_snap.pairs[seg_sync_snap.npairs].batch_idx = idx;

				/*
				 * Racy read of a 64-bit batch_id outside the publish_lock: a
				 * torn or stale value only makes the guarded re-check below
				 * skip the decrement, never decrement a wrong batch.
				 */
				seg_sync_snap.pairs[seg_sync_snap.npairs].batch_id =
					DWBCtl->batches[idx].batch_id;
				seg_sync_snap.npairs++;
			}
		}
	}
	LWLockRelease(DWBSegHashLock);
}

/*
 * Consume the snapshot after the fsync.  If synced is false (the fsync did
 * not happen and the segment still exists), the snapshot is discarded and
 * the bits stay for a later fsyncer.  Returns the number of batches this
 * call moved RETIRING -> FREE.
 */
static int
DWBSegSnapEnd(bool synced)
{
	int			freed = 0;

	Assert(seg_sync_snap.active);
	seg_sync_snap.active = false;

	if (!synced)
		return 0;

	for (int i = 0; i < seg_sync_snap.npairs; i++)
	{
		int			idx = seg_sync_snap.pairs[i].batch_idx;
		DWBatchCtl *batch = &DWBCtl->batches[idx];
		bool		cleared = false;

		LWLockAcquire(&batch->publish_lock, LW_EXCLUSIVE);
		if (batch->batch_id == seg_sync_snap.pairs[i].batch_id)
		{
			DWSegEntry *entry;
			uint64		bit = UINT64CONST(1) << (idx % 64);

			LWLockAcquire(DWBSegHashLock, LW_EXCLUSIVE);
			entry = (DWSegEntry *) hash_search(DWSegmentHash,
											   &seg_sync_snap.seg,
											   HASH_FIND, NULL);
			if (entry != NULL)
			{
				uint64		prev;

				prev = pg_atomic_fetch_and_u64(&entry->batch_bitmap[idx / 64],
											   ~bit);
				if (prev & bit)
				{
					cleared = true;
					DWBMaybeRemoveSegEntry(entry);
				}
			}
			LWLockRelease(DWBSegHashLock);

			if (cleared &&
				pg_atomic_fetch_sub_u32(&batch->seg_pending_count, 1) == 1)
			{
				uint32		expected = DWB_RETIRING;

				if (!pg_atomic_compare_exchange_u32(&batch->state, &expected,
													DWB_FREE))
					elog(PANIC, "DWB batch freed in unexpected state %u",
						 expected);
				freed++;
			}
		}
		LWLockRelease(&batch->publish_lock);
	}

	if (freed > 0)
		DWBNoteBatchFreed();

	return freed;
}

/*
 * Wrap an external fsync of a relation segment (ProcessSyncRequests in the
 * checkpointer).  Begin before the fsync attempt; End after it, with
 * synced = true if the segment was fsynced OR turned out to be dropped
 * (a dropped segment's writes are moot).  Non-md tags and disabled DWB are
 * handled here so the caller stays a two-liner.
 */
void
DWBSegmentFsyncBegin(const FileTag *ftag)
{
	DWSegRef	seg;

	/*
	 * Drop any leftover snapshot BEFORE deciding whether to take a new one.
	 * If a previous fsync ERROR'ed out between Begin and End (possible in the
	 * checkpointer with data_sync_retry = on, which survives the ERROR and
	 * keeps this process-local state), the early return below would otherwise
	 * leave the stale snapshot armed, and the End of the next successful
	 * fsync of an unrelated non-MD tag would decrement the stale segment's
	 * back-references -- freeing batches whose data-file fsync never
	 * succeeded.
	 */
	seg_sync_snap.active = false;

	if (!DWBIsEnabled() || ftag->handler != SYNC_HANDLER_MD)
		return;

	seg = DWBSegRefFromFileTag(ftag);
	DWBSegSnapBegin(&seg);
}

int
DWBSegmentFsyncEnd(bool synced)
{
	if (!seg_sync_snap.active)
		return 0;
	return DWBSegSnapEnd(synced);
}

/* ----------------------------------------------------------------
 * proactive retire
 * ----------------------------------------------------------------
 */

/*
 * Copy the seg_set of a batch if it is still the expected RETIRING
 * incarnation.  Returns the number of segments, 0 if the batch moved on.
 */
static uint32
DWBCollectBatchSegs(int batch_idx, uint64 batch_id, DWSegRef *segs)
{
	DWBatchCtl *batch = &DWBCtl->batches[batch_idx];
	uint32		n = 0;

	LWLockAcquire(&batch->publish_lock, LW_SHARED);
	if (batch->batch_id == batch_id &&
		pg_atomic_read_u32(&batch->state) == DWB_RETIRING)
	{
		n = batch->n_segs;
		memcpy(segs, batch->seg_set, n * sizeof(DWSegRef));
	}
	LWLockRelease(&batch->publish_lock);
	return n;
}

/*
 * Fsync one segment of a RETIRING batch and decrement its back-references.
 * Skips the segment when another fsyncer holds the claim (they will cover
 * it) or when its bits are already gone.  Returns batches freed.
 */
static int
DWBRetireSegment(const DWSegRef *seg)
{
	DWSegEntry *entry;
	bool		claimed = false;
	bool		covered;
	uint32		zero = 0;
	int			freed;

	/* claim the segment; a busy or vanished entry means nothing to do */
	LWLockAcquire(DWBSegHashLock, LW_SHARED);
	entry = (DWSegEntry *) hash_search(DWSegmentHash, seg, HASH_FIND, NULL);
	if (entry != NULL)
		claimed = pg_atomic_compare_exchange_u32(&entry->fsync_in_progress,
												 &zero, 1);
	LWLockRelease(DWBSegHashLock);
	if (!claimed)
		return 0;

	/*
	 * DWBRetireSyncSegment does not throw on a soft (data_sync_retry = on)
	 * fsync failure, so the claim reset below always runs; on covered = false
	 * the snapshot is discarded and the bits stay for a retry.
	 */
	DWBSegSnapBegin(seg);
	covered = DWBRetireSyncSegment(seg, false);
	freed = DWBSegSnapEnd(covered);

	/*
	 * Release the claim.  The entry may have been removed (and even
	 * re-created for a new batch) meanwhile; re-look-up by key and reset
	 * whatever is there -- the flag is advisory, an over-reset only costs a
	 * duplicate fsync.
	 */
	LWLockAcquire(DWBSegHashLock, LW_SHARED);
	entry = (DWSegEntry *) hash_search(DWSegmentHash, seg, HASH_FIND, NULL);
	if (entry != NULL)
		pg_atomic_write_u32(&entry->fsync_in_progress, 0);
	LWLockRelease(DWBSegHashLock);

	return freed;
}

/*
 * One retire sweep over all RETIRING batches, oldest first.  worker_id >= 0
 * restricts the sweep to that worker's segment partition; -1 sweeps
 * everything: the self-help of a writer stuck on a full ring
 * (DWBOpenNewBatch) and the synchronous retire in DWBFinishPageWrite when
 * there is no worker pool (dwb_retire_workers = 0, single-user mode).
 * Returns batches freed.
 */
int
DWBRetireAllSync(void)
{
	return DWBRetireSweep(-1);
}

typedef struct DWBRetiringBatch
{
	int			idx;
	uint64		id;
} DWBRetiringBatch;

static int
dwb_retiring_batch_cmp(const void *a, const void *b)
{
	return pg_cmp_u64(((const DWBRetiringBatch *) a)->id,
					  ((const DWBRetiringBatch *) b)->id);
}

static int
DWBRetireSweep(int worker_id)
{
	DWBRetiringBatch *retiring;
	int			nretiring = 0;
	int			freed = 0;
	DWSegRef   *segs;

	retiring = palloc(dwb_num_batches * sizeof(DWBRetiringBatch));
	segs = palloc(dwb_batch_pages * sizeof(DWSegRef));

	for (int i = 0; i < dwb_num_batches; i++)
	{
		if (pg_atomic_read_u32(&DWBCtl->batches[i].state) == DWB_RETIRING)
		{
			retiring[nretiring].idx = i;
			retiring[nretiring].id = DWBCtl->batches[i].batch_id;
			nretiring++;
		}
	}

	/* oldest first: smaller batch_id was opened earlier */
	qsort(retiring, nretiring, sizeof(DWBRetiringBatch),
		  dwb_retiring_batch_cmp);

	for (int i = 0; i < nretiring; i++)
	{
		uint32		nsegs = DWBCollectBatchSegs(retiring[i].idx,
												retiring[i].id, segs);

		for (uint32 s = 0; s < nsegs; s++)
		{
			if (worker_id >= 0 &&
				(int) (hash_bytes((const unsigned char *) &segs[s],
								  sizeof(DWSegRef)) %
					   (uint32) dwb_retire_workers) != worker_id)
				continue;
			freed += DWBRetireSegment(&segs[s]);
		}
	}

	pfree(retiring);
	pfree(segs);
	return freed;
}

/* ----------------------------------------------------------------
 * retire worker pool
 * ----------------------------------------------------------------
 */

/*
 * Register dwb_retire_workers static background workers.  Called from
 * PostmasterMain before extensions get a chance at the worker slots.
 */
void
DWBRetireWorkersRegister(void)
{
	BackgroundWorker bgw;
	int			free_slots;

	if (!DWBIsEnabled() || dwb_retire_workers == 0)
		return;

	/*
	 * RegisterBackgroundWorker only LOGs on overflow, so check the slots
	 * actually left after the earlier internal registrations (the logical
	 * replication launcher above all) and fail loudly: a silently missing
	 * retire worker would ship a smaller pool than the operator configured.
	 */
	free_slots = max_worker_processes - GetNumRegisteredBackgroundWorkers();
	if (dwb_retire_workers > free_slots)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"dwb_retire_workers\" (%d) needs more \"max_worker_processes\" slots than remain free (%d)",
						dwb_retire_workers, free_slots),
				 errhint("Increase \"max_worker_processes\" or decrease \"dwb_retire_workers\".")));

	for (int i = 0; i < dwb_retire_workers; i++)
	{
		memset(&bgw, 0, sizeof(bgw));

		/*
		 * The database-less connection gives the worker a pg_stat_activity
		 * entry; it forces BgWorkerStart_ConsistentState, so during the
		 * pre-consistency part of recovery the write path relies on its
		 * built-in self service (waiters seal on timeout, ring-full writers
		 * retire inline) — the pool is throughput, not correctness.
		 */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_ConsistentState;
		snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DWBRetireWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "dwb retire worker %d", i);
		snprintf(bgw.bgw_type, BGW_MAXLEN, "dwb retire worker");
		bgw.bgw_restart_time = 1;
		bgw.bgw_notify_pid = 0;
		bgw.bgw_main_arg = Int32GetDatum(i);

		RegisterBackgroundWorker(&bgw);
	}
}

/*
 * Main loop: force-SEAL non-empty batches that outlived
 * dwb_batch_timeout_ms, then proactively fsync this worker's segment
 * partition of every RETIRING batch.  Woken by cv_retire_wake (publication
 * of a seg_set, opening of a batch) or by timeout.
 */
void
DWBRetireWorkerMain(Datum main_arg)
{
	int			my_id = DatumGetInt32(main_arg);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/* no database, just shared state and pg_stat_activity visibility */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ConditionVariablePrepareToSleep(&DWBCtl->cv_retire_wake);

	for (;;)
	{
		long		timeout;
		TimestampTz now;

		if (ShutdownRequestPending)
			break;
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Force-SEAL pass: writers waiting on a half-filled batch seal it
		 * themselves after the same timeout, so this only matters for batches
		 * whose writers all went away before sealing.  open_time is read
		 * unlocked; a torn read can only mis-time the seal, which is always a
		 * valid action on a non-empty ALLOCATED batch.
		 */
		now = GetCurrentTimestamp();
		timeout = dwb_retire_interval_ms;
		for (int i = 0; i < dwb_num_batches; i++)
		{
			DWBatchCtl *batch = &DWBCtl->batches[i];
			long		age_ms;

			if (pg_atomic_read_u32(&batch->state) != DWB_ALLOCATED)
				continue;
			if ((pg_atomic_read_u32(&batch->next_slot_idx) & DWB_IDX_MASK) == 0)
				continue;		/* empty: sealing it buys nothing */

			age_ms = TimestampDifferenceMilliseconds(batch->open_time, now);
			if (age_ms >= dwb_batch_timeout_ms)
				(void) DWBTrySealBatch(i);
			else if (dwb_batch_timeout_ms - age_ms < timeout)
				timeout = dwb_batch_timeout_ms - age_ms;
		}

		(void) DWBRetireSweep(my_id);

		(void) ConditionVariableTimedSleep(&DWBCtl->cv_retire_wake,
										   Max(timeout, 1),
										   WAIT_EVENT_DWB_RETIRE_MAIN);
	}

	ConditionVariableCancelSleep();
	proc_exit(0);
}
