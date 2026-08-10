/*-------------------------------------------------------------------------
 *
 * dwb_cleaner.c
 *	  Cleaner worker pool of the short-lived double write buffer: takes
 *	  the flush bins the bgwriter's LRU scan produces and executes them,
 *	  so the scan's issue rate is no longer capped by one process
 *	  serially waiting out a batch fdatasync per bin.
 *
 * The bgwriter stays the only LRU scanner and pacing estimator (the
 * allocation counter of StrategySyncStart is consumed on read, so the
 * estimator cannot be split across processes).  What scales here is
 * execution only: bins travel through a small shared-memory queue and
 * any pool worker flushes them through FlushBufferBin's opportunistic
 * mode.
 *
 * Queue entries are hints, not obligations.  Every claim is
 * reclassified under the buffer header lock right before the write; a
 * buffer that was recycled, became hot, went clean or is busy with
 * somebody's I/O is skipped, never waited on.  Hence no draining on
 * shutdown and no meaning to queue contents after a crash: whatever was
 * queued is still dirty and the next checkpoint covers it.
 *
 * Backpressure is the enqueue refusing a full queue: the bgwriter keeps
 * the bin, stops scanning and re-offers it next round.  With an active
 * pool the bgwriter never writes data pages itself — a scan stalled
 * behind serial batch fsyncs starves the pool of fresh bins and lets
 * the strategy clock hand catch the scan point, pushing evictions onto
 * the backends.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_cleaner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/dwb.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

DWBCleanerCtl *DWBCleanerQueue = NULL;

/* set for the lifetime of a cleaner worker; DWBWriterClass consults it */
bool		DWBAmCleanerWorker = false;

/*
 * Queue capacity in bins.  A fixed burst absorber, deliberately not
 * scaled by the worker count: at observed pool drain rates even a full
 * queue empties in tens of milliseconds, so entries stay fresh, while a
 * queue sized to the pool would overflow on demand bursts exactly when
 * the scan must not stall.  64 bins is ~17 kB of shared memory.
 */
#define DWB_CLEANER_QUEUE_CAPACITY	64

Size
DWBCleanerShmemSize(void)
{
	if (!DWBIsEnabled() || dwb_cleaner_workers == 0)
		return 0;

	return add_size(offsetof(DWBCleanerCtl, bins),
					mul_size(DWB_CLEANER_QUEUE_CAPACITY,
							 sizeof(DWBCleanerBin)));
}

void
DWBCleanerShmemInit(void)
{
	bool		found;

	if (!DWBIsEnabled() || dwb_cleaner_workers == 0)
		return;

	DWBCleanerQueue = (DWBCleanerCtl *)
		ShmemInitStruct("DWB Cleaner Queue", DWBCleanerShmemSize(), &found);

	if (!found)
	{
		memset(DWBCleanerQueue, 0, DWBCleanerShmemSize());
		pg_atomic_init_u64(&DWBCleanerQueue->enqueued_pages, 0);
		pg_atomic_init_u64(&DWBCleanerQueue->pool_written, 0);
		pg_atomic_init_u64(&DWBCleanerQueue->pool_written_total, 0);
		pg_atomic_init_u64(&DWBCleanerQueue->skipped_pages, 0);
		pg_atomic_init_u64(&DWBCleanerQueue->deferred_bins, 0);
		pg_atomic_init_u64(&DWBCleanerQueue->pressure_naps, 0);
		pg_atomic_init_u32(&DWBCleanerQueue->depth, 0);
		ConditionVariableInit(&DWBCleanerQueue->cv_work);
		DWBCleanerQueue->capacity = DWB_CLEANER_QUEUE_CAPACITY;
	}
}

/*
 * True when bins may be handed to the pool instead of flushed in place.
 */
bool
DWBCleanersActive(void)
{
	return DWBCleanerQueue != NULL;
}

/*
 * Hand one bin to the pool.  The queue lock is taken unconditionally —
 * the critical section is one bin copy — so a false return means
 * exactly one thing: the queue is full.  The caller keeps the bin and
 * re-offers it later (the bgwriter counts the refusal as a deferral).
 */
bool
DWBCleanerEnqueueBin(const int *buf_ids, int nbuf)
{
	DWBCleanerCtl *ctl = DWBCleanerQueue;
	DWBCleanerBin *bin;

	Assert(ctl != NULL);
	Assert(nbuf > 0 && nbuf <= DWB_FLUSH_BIN_MAX);

	LWLockAcquire(DWBCleanerQueueLock, LW_EXCLUSIVE);
	if (ctl->nqueued == ctl->capacity)
	{
		LWLockRelease(DWBCleanerQueueLock);
		return false;
	}

	bin = &ctl->bins[(ctl->head + ctl->nqueued) % ctl->capacity];
	bin->nbuf = nbuf;
	memcpy(bin->buf_ids, buf_ids, nbuf * sizeof(int));
	ctl->nqueued++;
	pg_atomic_write_u32(&ctl->depth, ctl->nqueued);
	LWLockRelease(DWBCleanerQueueLock);

	pg_atomic_fetch_add_u64(&ctl->enqueued_pages, nbuf);
	ConditionVariableSignal(&ctl->cv_work);
	return true;
}

/*
 * Take the oldest bin, if any.  A local copy is returned so the queue
 * lock is never held across the flush.
 */
static bool
DWBCleanerDequeueBin(DWBCleanerBin *bin)
{
	DWBCleanerCtl *ctl = DWBCleanerQueue;
	bool		got = false;

	LWLockAcquire(DWBCleanerQueueLock, LW_EXCLUSIVE);
	if (ctl->nqueued > 0)
	{
		*bin = ctl->bins[ctl->head];
		ctl->head = (ctl->head + 1) % ctl->capacity;
		ctl->nqueued--;
		pg_atomic_write_u32(&ctl->depth, ctl->nqueued);
		got = true;
	}
	LWLockRelease(DWBCleanerQueueLock);
	return got;
}

/*
 * Advisory pressure signal for the checkpointer: is the bin queue at
 * least half full?  Reads only the lock-free depth mirror — a stale
 * answer merely shifts one 100ms pacing decision, so no lock is taken;
 * nqueued itself stays under DWBCleanerQueueLock.
 */
bool
DWBCleanerQueueHot(void)
{
	DWBCleanerCtl *ctl = DWBCleanerQueue;

	if (ctl == NULL || !DWBCleanersActive())
		return false;
	return pg_atomic_read_u32(&ctl->depth) >= ctl->capacity / 2;
}

/*
 * Count a checkpointer nap taken only because the queue was hot (the
 * base schedule check alone would have kept writing).
 */
void
DWBCleanerCountPressureNap(void)
{
	if (DWBCleanerQueue != NULL)
		pg_atomic_fetch_add_u64(&DWBCleanerQueue->pressure_naps, 1);
}

/*
 * The bgwriter folds the pool's completed writes into
 * PendingBgWriterStats.buf_written_clean once per round, keeping
 * pg_stat_bgwriter's counter "pages written by LRU cleaning" no matter
 * which process executed the write.
 */
uint64
DWBCleanerFetchPoolWritten(void)
{
	if (!DWBCleanersActive())
		return 0;
	return pg_atomic_exchange_u64(&DWBCleanerQueue->pool_written, 0);
}

/*
 * The bgwriter counts a bin the pool's queue refused; the bin itself is
 * carried over to the next round, so this is a pure saturation gauge —
 * nothing gets written on this path.  Counted by the bgwriter, not
 * inside the failed enqueue: a refused test claim defers nothing.
 */
void
DWBCleanerCountDeferral(void)
{
	pg_atomic_fetch_add_u64(&DWBCleanerQueue->deferred_bins, 1);
}

/*
 * Register dwb_cleaner_workers static background workers.  Called from
 * PostmasterMain right after the retire pool registration, before
 * extensions get a chance at the worker slots.
 */
void
DWBCleanerWorkersRegister(void)
{
	BackgroundWorker bgw;
	int			free_slots;

	if (!DWBIsEnabled() || dwb_cleaner_workers == 0)
		return;

	/*
	 * RegisterBackgroundWorker only LOGs on overflow, so check the slots
	 * actually left and fail loudly: a silently missing cleaner would ship a
	 * smaller pool than the operator configured.
	 */
	free_slots = max_worker_processes - GetNumRegisteredBackgroundWorkers();
	if (dwb_cleaner_workers > free_slots)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"dwb_cleaner_workers\" (%d) needs more \"max_worker_processes\" slots than remain free (%d)",
						dwb_cleaner_workers, free_slots),
				 errhint("Increase \"max_worker_processes\" or decrease \"dwb_cleaner_workers\".")));

	for (int i = 0; i < dwb_cleaner_workers; i++)
	{
		memset(&bgw, 0, sizeof(bgw));

		/*
		 * The database-less connection gives the worker a pg_stat_activity
		 * entry; it forces BgWorkerStart_ConsistentState, which is fine: the
		 * bgwriter that feeds the queue starts even later, and until then the
		 * queue simply stays empty.
		 */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_ConsistentState;
		snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DWBCleanerWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "dwb cleaner %d", i);
		snprintf(bgw.bgw_type, BGW_MAXLEN, "dwb cleaner");
		bgw.bgw_restart_time = 1;
		bgw.bgw_notify_pid = 0;
		bgw.bgw_main_arg = Int32GetDatum(i);

		RegisterBackgroundWorker(&bgw);
	}
}

/*
 * Main loop: flush queued bins, sleep when the queue is empty.
 */
void
DWBCleanerWorkerMain(Datum main_arg)
{
	WritebackContext wb_context;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);

	/*
	 * die, not a shutdown flag: a worker that exits with code 0 is
	 * unregistered for good, so one stray SIGTERM would permanently shrink
	 * the pool.  The FATAL exit restarts after bgw_restart_time outside a
	 * postmaster shutdown and is simply the end during one.
	 */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* no database, just shared state and pg_stat_activity visibility */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/*
	 * FlushBufferBin pins buffers and registers its buffer I/O with
	 * CurrentResourceOwner — that registration is what repairs an
	 * interrupted data-file write from the batch copy if an ERROR throws the
	 * worker out mid-bin.  The aux-process owner provides both the owner and
	 * its shmem-exit release.
	 */
	CreateAuxProcessResourceOwner();

	DWBAmCleanerWorker = true;
	WritebackContextInit(&wb_context, &bgwriter_flush_after);

	for (;;)
	{
		DWBCleanerBin bin;
		int			written;

		/* the CFI is what turns a pending die() into the FATAL exit */
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		INJECTION_POINT("dwb-cleaner-loop", NULL);

		if (!DWBCleanerDequeueBin(&bin))
		{
			/*
			 * Sleep without losing a wakeup: get onto the wait list first,
			 * then recheck under the queue lock, then sleep.  A signal sent
			 * after the recheck is kept by the prepared state; a bin enqueued
			 * before it is seen by the recheck.  The sleep itself checks for
			 * interrupts, so a pending die() cuts it short.
			 */
			ConditionVariablePrepareToSleep(&DWBCleanerQueue->cv_work);
			if (!DWBCleanerDequeueBin(&bin))
			{
				IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);

				/*
				 * Flush I/O statistics while idle: nothing else in this loop
				 * reports them, and the worker's pg_stat_io rows are how an
				 * operator sees the pool actually writing.  Forced, because a
				 * deferred report would sit on local counters through the
				 * whole open-ended sleep that follows.  The injection point
				 * lets a test park the worker right after the report; a
				 * parked worker switches its prepared condition-variable
				 * sleep to the injection one, which the sleep below repairs
				 * by re-preparing and returning for another loop.
				 */
				pgstat_report_stat(true);
				INJECTION_POINT("dwb-cleaner-reported", NULL);
				ConditionVariableSleep(&DWBCleanerQueue->cv_work,
									   WAIT_EVENT_DWB_CLEANER_MAIN);
				continue;
			}
		}

		/* off the wait list while flushing (no-op if never prepared) */
		ConditionVariableCancelSleep();

		/*
		 * In opportunistic mode every bin member ends up either written or
		 * skipped, so the skip count needs no extra plumbing.
		 */
		written = FlushBufferBin(bin.buf_ids, bin.nbuf, true, &wb_context);
		pg_atomic_fetch_add_u64(&DWBCleanerQueue->pool_written, written);
		pg_atomic_fetch_add_u64(&DWBCleanerQueue->pool_written_total, written);
		pg_atomic_fetch_add_u64(&DWBCleanerQueue->skipped_pages,
								bin.nbuf - written);
	}
}
