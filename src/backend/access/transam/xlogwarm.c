/*-------------------------------------------------------------------------
 *
 * xlogwarm.c
 *	  Replay prefetch worker pool: reads the pages replay is about to need
 *	  into shared buffers, so the startup process does not spend its single
 *	  core fetching them.
 *
 * Without full-page images in the WAL stream — the standby of a cluster
 * running io_torn_pages_protection = double_writes — replay has to fetch
 * every page it modifies.  Doing that from the startup process costs about
 * half of its core: issuing kernel advice for each block, then reading the
 * page with its copy out of the page cache, then verifying its checksum,
 * then finding a victim buffer to put it in.  All of that is work another
 * process can do in parallel, ahead of replay.
 *
 * The prefetcher already decodes WAL ahead of replay and already filters
 * the blocks that must not be touched.  Where it would issue advice, it
 * instead publishes the block here; a worker reads it into a shared buffer
 * and records which buffer that was.  Replay picks the answer up as a
 * recent-buffer hint, which XLogReadBufferExtended() already knows how to
 * validate, so the redo path itself is unchanged.
 *
 * Nothing here is an obligation.  A slot that no worker got to, a read that
 * failed, a buffer that was evicted before replay reached it — each simply
 * means replay reads the page itself, exactly as it does with the pool
 * disabled.  Replay never sleeps on a slot.
 *
 * The queue is a ring of slots recycled in publication order.  One
 * publisher (the startup process) and several consumers coordinate through
 * the slot state alone:
 *
 *		FREE/DONE/FAILED --(publisher)--> PUBLISHED
 *		PUBLISHED        --(worker)----->  CLAIMED
 *		CLAIMED          --(worker)----->  DONE | FAILED
 *
 * Both worker transitions are compare-and-swap, so several consumers cannot
 * claim one slot.  A claimed slot belongs to its worker until that worker
 * leaves it: nothing else ever writes it, which is what keeps the result and
 * the request id it was produced for a consistent pair.  The publisher
 * ignores an answer whose id is not the one it published, so an answer that
 * arrives after replay has moved on is simply not picked up.
 *
 * A worker that exits while holding a slot returns it on the way out, so the
 * ring does not shrink when a worker is signalled or throws a FATAL error; a
 * worker that dies in an uglier way takes the whole cluster through a restart
 * cycle, which rebuilds this ring from scratch.  The one case that does cost
 * a slot for good is a worker wedged inside a read that never returns, and
 * replay would be wedged on that page too.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xlogwarm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogwarm.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

/* GUCs */
int			replay_warm_workers = 0;
int			replay_warm_queue_size = 256;


typedef enum XLogWarmState
{
	XLOGWARM_FREE = 0,
	XLOGWARM_PUBLISHED,
	XLOGWARM_CLAIMED,
	XLOGWARM_DONE,
	XLOGWARM_FAILED,
}			XLogWarmState;

typedef struct XLogWarmSlot
{
	pg_atomic_uint32 state;		/* XLogWarmState */

	/* payload, written by the publisher while it owns the slot */
	uint64		request_id;
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;

	/* result, written by the claiming worker */
	uint64		result_id;
	Buffer		result_buffer;
}			XLogWarmSlot;

typedef struct XLogWarmCtl
{
	ConditionVariable cv_work;

	/* publisher side */
	pg_atomic_uint64 published;
	pg_atomic_uint64 dropped_full;
	pg_atomic_uint64 collected;
	pg_atomic_uint64 missed;
	pg_atomic_uint64 stale;
	pg_atomic_uint64 cancelled;
	pg_atomic_uint64 released;

	/* worker side */
	pg_atomic_uint64 claimed;
	pg_atomic_uint64 reads;
	pg_atomic_uint64 hits;
	pg_atomic_uint64 failed;
	pg_atomic_uint64 vanished;
	pg_atomic_uint64 discarded;

	int			capacity;
	pg_atomic_uint32 hand;		/* where consumers start scanning */

	/*
	 * Bumped under ReplayWarmReadLock whenever a relation or a database is
	 * about to lose its files.  A worker has no database connection and so
	 * receives no cache invalidations: without this it could keep a
	 * relation's cached size — or its open segments — from before the
	 * file changed underneath it.  Read under the lock held shared.
	 */
	uint64		drop_epoch;

	/*
	 * Worker pids, published by the workers themselves.  Without a database
	 * connection there is no pg_stat_activity row to see them in, and a pool
	 * nobody can see is a pool nobody can diagnose.
	 */
	pg_atomic_uint32 worker_pids[XLOGWARM_MAX_WORKERS];

	XLogWarmSlot slots[FLEXIBLE_ARRAY_MEMBER];
}			XLogWarmCtl;

static XLogWarmCtl * XLogWarmQueue = NULL;

static void XLogWarmWorkerExit(int code, Datum arg);

/* publisher-private state */
static uint64 next_request_id = 1;
static int	publish_hand = 0;

/*
 * Worker-private: the drop epoch this worker's smgr state is good for.
 */
static uint64 my_drop_epoch = 0;

/*
 * Worker-private: relation sizes this worker has measured.
 *
 * smgr keeps such a cache too, but hands it out only to the startup process
 * (smgrnblocks_cached(), "due to lack of a shared invalidation mechanism for
 * changes in file size").  This pool has that mechanism — the drop epoch —
 * so it can keep its own answers, and it has to: measuring walks the segment
 * chain, which on a terabyte relation is a thousand file opens, and paying
 * that per request leaves a worker doing nothing else.
 *
 * A remembered size is only ever too small, never too large: within an epoch
 * no relation lost blocks, so the entry is trusted for "the block is inside
 * the relation" and re-measured for anything else.  Direct-mapped and small
 * on purpose — replay works through a handful of relations at a time.
 */
#define XLOGWARM_SIZES	16

typedef struct XLogWarmSize
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber nblocks;
	uint64		epoch;
	bool		valid;
}			XLogWarmSize;

static XLogWarmSize my_sizes[XLOGWARM_SIZES];

/*
 * Worker-private: the slot this worker holds, or -1.  Read on the way out to
 * hand the slot back, so a worker that is signalled away does not take a slot
 * of the ring with it.
 */
static int	my_claimed_slot = -1;

Size
XLogWarmShmemSize(void)
{
	if (replay_warm_workers == 0)
		return 0;

	return add_size(offsetof(XLogWarmCtl, slots),
					mul_size(replay_warm_queue_size, sizeof(XLogWarmSlot)));
}

void
XLogWarmShmemInit(void)
{
	bool		found;

	if (replay_warm_workers == 0)
		return;

	XLogWarmQueue = (XLogWarmCtl *)
		ShmemInitStruct("Replay Warm Queue", XLogWarmShmemSize(), &found);

	if (!found)
	{
		memset(XLogWarmQueue, 0, XLogWarmShmemSize());
		ConditionVariableInit(&XLogWarmQueue->cv_work);
		pg_atomic_init_u64(&XLogWarmQueue->published, 0);
		pg_atomic_init_u64(&XLogWarmQueue->dropped_full, 0);
		pg_atomic_init_u64(&XLogWarmQueue->collected, 0);
		pg_atomic_init_u64(&XLogWarmQueue->missed, 0);
		pg_atomic_init_u64(&XLogWarmQueue->stale, 0);
		pg_atomic_init_u64(&XLogWarmQueue->cancelled, 0);
		pg_atomic_init_u64(&XLogWarmQueue->released, 0);
		pg_atomic_init_u64(&XLogWarmQueue->claimed, 0);
		pg_atomic_init_u64(&XLogWarmQueue->reads, 0);
		pg_atomic_init_u64(&XLogWarmQueue->hits, 0);
		pg_atomic_init_u64(&XLogWarmQueue->failed, 0);
		pg_atomic_init_u64(&XLogWarmQueue->vanished, 0);
		pg_atomic_init_u64(&XLogWarmQueue->discarded, 0);
		pg_atomic_init_u32(&XLogWarmQueue->hand, 0);
		XLogWarmQueue->drop_epoch = 0;
		XLogWarmQueue->capacity = replay_warm_queue_size;

		for (int i = 0; i < XLOGWARM_MAX_WORKERS; i++)
			pg_atomic_init_u32(&XLogWarmQueue->worker_pids[i], 0);

		for (int i = 0; i < replay_warm_queue_size; i++)
			pg_atomic_init_u32(&XLogWarmQueue->slots[i].state, XLOGWARM_FREE);
	}
}

/*
 * True when blocks may be handed to the pool.  The pool exists for the
 * duration of the postmaster; it is idle whenever nothing publishes.
 */
bool
XLogWarmPoolActive(void)
{
	return XLogWarmQueue != NULL;
}

/*
 * Publish one block for a worker to read.
 *
 * Returns the slot the request went into and sets *request_id, or
 * XLOGWARM_NO_SLOT when the ring has no reusable slot, in which case the
 * caller simply leaves the block unwarmed.
 */
int
XLogWarmPublish(RelFileLocator rlocator, ForkNumber forknum,
				BlockNumber blkno, uint64 *request_id)
{
	XLogWarmSlot *slot = NULL;
	int			capacity = XLogWarmQueue->capacity;
	int			slot_no = XLOGWARM_NO_SLOT;

	Assert(XLogWarmQueue != NULL);

	/*
	 * A slot still PUBLISHED or CLAIMED belongs to an earlier request that
	 * has not been dealt with yet, and taking it back is never right — a
	 * claimed slot is being written by its worker, and a second writer would
	 * tear the result apart.  So the publisher writes only slots nobody
	 * holds, and looks past the ones somebody does: a single slow worker must
	 * not stop the ring, which it would if publication insisted on one slot.
	 */
	for (int i = 0; i < capacity; i++)
	{
		int			candidate = (publish_hand + i) % capacity;
		uint32		state = pg_atomic_read_u32(&XLogWarmQueue->slots[candidate].state);

		if (state != XLOGWARM_PUBLISHED && state != XLOGWARM_CLAIMED)
		{
			slot_no = candidate;
			slot = &XLogWarmQueue->slots[candidate];
			break;
		}
	}

	/*
	 * Every slot is spoken for: the pool is behind, and this block goes
	 * unwarmed.  That is the pool's back-pressure — replay reads the block
	 * itself and never waits for a worker.
	 */
	if (slot == NULL)
	{
		pg_atomic_fetch_add_u64(&XLogWarmQueue->dropped_full, 1);
		return XLOGWARM_NO_SLOT;
	}

	slot->request_id = next_request_id;
	slot->rlocator = rlocator;
	slot->forknum = forknum;
	slot->blkno = blkno;

	/* the payload must be visible before a worker can see the state */
	pg_write_barrier();
	pg_atomic_write_u32(&slot->state, XLOGWARM_PUBLISHED);

	*request_id = next_request_id++;
	publish_hand = (slot_no + 1) % XLogWarmQueue->capacity;
	pg_atomic_fetch_add_u64(&XLogWarmQueue->published, 1);

	ConditionVariableSignal(&XLogWarmQueue->cv_work);

	return slot_no;
}

/*
 * Collect the buffer a worker read for this request, if it has one.
 *
 * Returns InvalidBuffer when the request was never claimed, is still being
 * read, failed, or the slot has moved on to another request: in every one
 * of those cases replay reads the page itself.  The returned buffer is only
 * a hint and is not pinned — the caller validates it, as it does for any
 * recent-buffer hint.
 */
Buffer
XLogWarmCollect(int slot_no, uint64 request_id)
{
	XLogWarmSlot *slot;
	Buffer		buffer;

	Assert(XLogWarmQueue != NULL);
	Assert(slot_no >= 0 && slot_no < XLogWarmQueue->capacity);

	slot = &XLogWarmQueue->slots[slot_no];

	if (pg_atomic_read_u32(&slot->state) != XLOGWARM_DONE)
	{
		pg_atomic_fetch_add_u64(&XLogWarmQueue->missed, 1);
		return InvalidBuffer;
	}

	/* the state must be observed before the result it advertises */
	pg_read_barrier();

	if (slot->result_id != request_id)
	{
		pg_atomic_fetch_add_u64(&XLogWarmQueue->missed, 1);
		return InvalidBuffer;
	}

	buffer = slot->result_buffer;
	if (!BufferIsValid(buffer))
	{
		pg_atomic_fetch_add_u64(&XLogWarmQueue->missed, 1);
		return InvalidBuffer;
	}

	pg_atomic_fetch_add_u64(&XLogWarmQueue->collected, 1);
	return buffer;
}

/*
 * Withdraw every outstanding request.
 *
 * The prefetcher throws its decoded records away when the read position
 * moves (XLogPrefetcherBeginRead) and at the end of recovery, taking the
 * slot references with them.  Requests nobody claimed are freed here;
 * requests a worker is inside of are left alone — that worker will finish
 * into DONE, and the slot becomes reusable on the next pass of the ring.
 */
void
XLogWarmCancelAll(void)
{
	if (XLogWarmQueue == NULL)
		return;

	for (int i = 0; i < XLogWarmQueue->capacity; i++)
	{
		XLogWarmSlot *slot = &XLogWarmQueue->slots[i];
		uint32		expected = XLOGWARM_PUBLISHED;

		if (pg_atomic_compare_exchange_u32(&slot->state, &expected,
										   XLOGWARM_FREE))
			pg_atomic_fetch_add_u64(&XLogWarmQueue->cancelled, 1);
	}

	publish_hand = 0;
}

/*
 * Take the pool out of the way of a relation about to lose its buffers.
 *
 * DropRelationBuffers() requires that no other process be loading pages of
 * the relation into buffers while it runs (bufmgr.c:5081-5083), and a worker
 * reading ahead of replay is exactly such a process — one that usually wins
 * the race, since its file descriptor keeps reaching an unlinked file and the
 * read succeeds.  Held across the buffer drop *and* the file operation that
 * follows it, this leaves a worker two possibilities and no third: it either
 * finished before the drop scanned the pool, in which case the scan removes
 * its page, or it starts afterwards and finds the relation gone.
 *
 * The wait is one page read long, and only relation drops and truncations
 * ever wait at all.
 */
void
XLogWarmDropBegin(void)
{
	if (XLogWarmQueue == NULL)
		return;

	LWLockAcquire(ReplayWarmReadLock, LW_EXCLUSIVE);
	XLogWarmQueue->drop_epoch++;
}

void
XLogWarmDropEnd(void)
{
	if (XLogWarmQueue != NULL)
		LWLockRelease(ReplayWarmReadLock);
}

/*
 * A warmed buffer no longer held the page by the time replay asked for it.
 * Only the recent-buffer validation can tell, so it reports it here.
 */
void
XLogWarmCountStale(void)
{
	if (XLogWarmQueue != NULL)
		pg_atomic_fetch_add_u64(&XLogWarmQueue->stale, 1);
}

/*
 * Report the pids of the running workers into caller-provided storage of
 * XLOGWARM_MAX_WORKERS entries, returning how many were found.
 */
int
XLogWarmGetWorkerPids(int *pids)
{
	int			found = 0;

	if (XLogWarmQueue == NULL)
		return 0;

	for (int i = 0; i < XLOGWARM_MAX_WORKERS; i++)
	{
		uint32		pid = pg_atomic_read_u32(&XLogWarmQueue->worker_pids[i]);

		if (pid != 0)
			pids[found++] = (int) pid;
	}

	return found;
}

/*
 * Read the counters out, for monitoring and for the tests.  Returns false
 * when the pool is not configured, leaving *stats untouched.
 */
bool
XLogWarmGetStats(XLogWarmStats * stats)
{
	if (XLogWarmQueue == NULL)
		return false;

	stats->published = pg_atomic_read_u64(&XLogWarmQueue->published);
	stats->dropped_full = pg_atomic_read_u64(&XLogWarmQueue->dropped_full);
	stats->collected = pg_atomic_read_u64(&XLogWarmQueue->collected);
	stats->missed = pg_atomic_read_u64(&XLogWarmQueue->missed);
	stats->stale = pg_atomic_read_u64(&XLogWarmQueue->stale);
	stats->cancelled = pg_atomic_read_u64(&XLogWarmQueue->cancelled);
	stats->released = pg_atomic_read_u64(&XLogWarmQueue->released);
	stats->claimed = pg_atomic_read_u64(&XLogWarmQueue->claimed);
	stats->reads = pg_atomic_read_u64(&XLogWarmQueue->reads);
	stats->hits = pg_atomic_read_u64(&XLogWarmQueue->hits);
	stats->failed = pg_atomic_read_u64(&XLogWarmQueue->failed);
	stats->vanished = pg_atomic_read_u64(&XLogWarmQueue->vanished);
	stats->discarded = pg_atomic_read_u64(&XLogWarmQueue->discarded);

	return true;
}

/*
 * How many slots are waiting for a worker, and how many a worker holds.
 *
 * A running total says what the pool has done; this says what it is doing,
 * which is what a test needs to arrange anything around a request in flight.
 */
void
XLogWarmGetSlotCounts(int *published, int *claimed)
{
	*published = 0;
	*claimed = 0;

	if (XLogWarmQueue == NULL)
		return;

	for (int i = 0; i < XLogWarmQueue->capacity; i++)
	{
		switch (pg_atomic_read_u32(&XLogWarmQueue->slots[i].state))
		{
			case XLOGWARM_PUBLISHED:
				(*published)++;
				break;
			case XLOGWARM_CLAIMED:
				(*claimed)++;
				break;
			default:
				break;
		}
	}
}

/*
 * Read one published block into shared buffers.
 *
 * Runs inside the worker's own resource owner: replay may drop or truncate
 * the relation between publication and this read, so any smgr error has to
 * be survivable.
 */
static void
XLogWarmDoOne(XLogWarmSlot * slot, uint64 request_id,
			  RelFileLocator rlocator, ForkNumber forknum, BlockNumber blkno)
{
	SMgrRelation smgr;
	XLogWarmSize *size;
	Buffer		buffer = InvalidBuffer;
	uint32		expected;
	bool		failed = false;

	/*
	 * A test can park a worker here to hold a slot claimed while it arranges
	 * what happens next.  It sits outside the interlock below on purpose: a
	 * worker parked while holding that lock would stop replay from dropping
	 * anything at all.
	 */
	INJECTION_POINT("replay-warm-before-read", NULL);

	PG_TRY();
	{
		/* see XLogWarmDropBegin(): this is the whole reason it exists */
		LWLockAcquire(ReplayWarmReadLock, LW_SHARED);

		/*
		 * Something lost its files since this worker last looked.  Nothing
		 * tells a process without a database connection that, so it throws
		 * its own smgr state away and starts from the files as they are now.
		 */
		if (XLogWarmQueue->drop_epoch != my_drop_epoch)
		{
			smgrreleaseall();
			memset(my_sizes, 0, sizeof(my_sizes));
			my_drop_epoch = XLogWarmQueue->drop_epoch;
		}

		smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

		/*
		 * Is the block still there?  Replay may have dropped or truncated the
		 * relation between publication and now — the ordinary outcome of
		 * running ahead of it, and the outcome the interlock guarantees for a
		 * request that gets here after a drop.
		 *
		 * Asking outright costs more than it looks: smgrexists() closes the
		 * fork first (mdexists() skips that only in the startup process) and
		 * smgrnblocks() then walks the segment chain from the beginning, so
		 * on a terabyte relation one question is a thousand file opens.  The
		 * answer is therefore remembered per epoch, and only the first
		 * request for a fork, or one that lands past a remembered end, pays
		 * for asking again.
		 */
		size = &my_sizes[rlocator.relNumber % XLOGWARM_SIZES];

		if (!size->valid || size->epoch != my_drop_epoch ||
			size->forknum != forknum ||
			!RelFileLocatorEquals(size->rlocator, rlocator))
		{
			if (!smgrexists(smgr, forknum))
			{
				failed = true;
				pg_atomic_fetch_add_u64(&XLogWarmQueue->vanished, 1);
			}
			else
			{
				size->rlocator = rlocator;
				size->forknum = forknum;
				size->nblocks = smgrnblocks(smgr, forknum);
				size->epoch = my_drop_epoch;
				size->valid = true;
			}
		}

		if (!failed && blkno >= size->nblocks)
		{
			/* the remembered size may simply predate an extension */
			size->nblocks = smgrnblocks(smgr, forknum);

			if (blkno >= size->nblocks)
			{
				failed = true;
				pg_atomic_fetch_add_u64(&XLogWarmQueue->vanished, 1);
			}
		}

		if (failed)
		{
			/* the block is gone; there is nothing to warm */
		}
		else if (BufferIsValid(buffer = LookupSharedBuffer(smgr, forknum, blkno)))
		{
			/*
			 * Already resident: not a read, but still the answer replay
			 * wants, so hand the buffer on as if we had read it.
			 */
			pg_atomic_fetch_add_u64(&XLogWarmQueue->hits, 1);
		}
		else
		{
			buffer = ReadBufferWithoutRelcache(rlocator, forknum, blkno,
											   RBM_NORMAL, NULL, true);
			pg_atomic_fetch_add_u64(&XLogWarmQueue->reads, 1);

			/*
			 * Hand the buffer number on and let go: holding pins ahead of
			 * replay would pin down a slice of the buffer pool, and replay
			 * validates the hint anyway.
			 */
			ReleaseBuffer(buffer);
		}

		LWLockRelease(ReplayWarmReadLock);
	}
	PG_CATCH();
	{
		/* an unreadable block is not this pool's problem to solve */
		buffer = InvalidBuffer;
		failed = true;
		pg_atomic_fetch_add_u64(&XLogWarmQueue->failed, 1);

		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		/*
		 * Whatever the failed read was holding goes back here: the
		 * lightweight locks it took, including the interlock above, and then
		 * the pins and the buffer I/O owned by the aux-process resource
		 * owner.  Releasing the owner is what repairs a read interrupted
		 * mid-flight — it hands the buffer's I/O back, so whoever waits on
		 * that buffer can retry instead of waiting on a process that is no
		 * longer reading.
		 */
		LWLockReleaseAll();
		ReleaseAuxProcessResources(false);
	}
	PG_END_TRY();

	slot->result_id = request_id;
	slot->result_buffer = buffer;

	/* the result must be visible before the state that advertises it */
	pg_write_barrier();

	/*
	 * The slot is ours until we leave it, so this compare-and-swap is a
	 * statement of that invariant rather than a race to win; a failure would
	 * mean somebody else wrote a claimed slot, and the counter says so.
	 */
	expected = XLOGWARM_CLAIMED;
	if (!pg_atomic_compare_exchange_u32(&slot->state, &expected,
										failed ? XLOGWARM_FAILED : XLOGWARM_DONE))
		pg_atomic_fetch_add_u64(&XLogWarmQueue->discarded, 1);
}

/*
 * Leave the pool tidily.
 *
 * Two things outlive this process if it does not: the pid it advertised,
 * which would point at a process that no longer exists, and the slot it
 * holds, which no one else may write and which would therefore shrink the
 * ring for the rest of the cluster's life.  This runs on the way out of a
 * signalled or FATAL exit — the paths that leave shared memory in place.
 */
static void
XLogWarmWorkerExit(int code, Datum arg)
{
	int			worker_id = DatumGetInt32(arg);

	if (XLogWarmQueue == NULL)
		return;

	if (my_claimed_slot >= 0)
	{
		XLogWarmSlot *slot = &XLogWarmQueue->slots[my_claimed_slot];
		uint32		expected = XLOGWARM_CLAIMED;

		my_claimed_slot = -1;

		if (pg_atomic_compare_exchange_u32(&slot->state, &expected,
										   XLOGWARM_FAILED))
			pg_atomic_fetch_add_u64(&XLogWarmQueue->released, 1);
	}

	pg_atomic_write_u32(&XLogWarmQueue->worker_pids[worker_id], 0);
}

/*
 * Claim and serve one published slot.  Returns false when the ring holds
 * nothing to do.
 */
static bool
XLogWarmServeOne(void)
{
	int			capacity = XLogWarmQueue->capacity;
	uint32		start = pg_atomic_fetch_add_u32(&XLogWarmQueue->hand, 1);

	for (int i = 0; i < capacity; i++)
	{
		XLogWarmSlot *slot = &XLogWarmQueue->slots[(start + i) % capacity];
		uint32		expected = XLOGWARM_PUBLISHED;
		uint64		request_id;
		RelFileLocator rlocator;
		ForkNumber	forknum;
		BlockNumber blkno;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected,
											XLOGWARM_CLAIMED))
			continue;

		/* the state was observed before the payload it advertises */
		pg_read_barrier();

		request_id = slot->request_id;
		rlocator = slot->rlocator;
		forknum = slot->forknum;
		blkno = slot->blkno;

		/*
		 * From here until the slot is finished this worker owns it, and says
		 * so where its exit callback can see it.
		 */
		my_claimed_slot = (start + i) % capacity;

		pg_atomic_fetch_add_u64(&XLogWarmQueue->claimed, 1);
		XLogWarmDoOne(slot, request_id, rlocator, forknum, blkno);

		my_claimed_slot = -1;
		return true;
	}

	return false;
}

/*
 * Register the pool.  Like the DWB cleaner pool, a worker slot shortage is
 * fatal rather than silent: a smaller pool than the operator configured is
 * a performance surprise nobody asked for.
 */
void
XLogWarmWorkersRegister(void)
{
	BackgroundWorker bgw;
	int			free_slots;

	if (replay_warm_workers == 0)
		return;

	free_slots = max_worker_processes - GetNumRegisteredBackgroundWorkers();
	if (replay_warm_workers > free_slots)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"replay_warm_workers\" (%d) needs more \"max_worker_processes\" slots than remain free (%d)",
						replay_warm_workers, free_slots),
				 errhint("Increase \"max_worker_processes\" or decrease \"replay_warm_workers\".")));

	for (int i = 0; i < replay_warm_workers; i++)
	{
		memset(&bgw, 0, sizeof(bgw));

		/*
		 * No database connection: the workers deal in relation locators and
		 * shared buffers only.  That also lets them start at postmaster
		 * start, so they serve crash recovery on a primary from the first
		 * record, not only a standby past consistency.
		 */
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
		bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
		snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "XLogWarmWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "replay warm worker %d", i);
		snprintf(bgw.bgw_type, BGW_MAXLEN, "replay warm worker");
		bgw.bgw_restart_time = 1;
		bgw.bgw_notify_pid = 0;
		bgw.bgw_main_arg = Int32GetDatum(i);

		RegisterBackgroundWorker(&bgw);
	}
}

/*
 * Main loop: serve published slots, sleep when there is nothing published.
 */
void
XLogWarmWorkerMain(Datum main_arg)
{
	int			worker_id;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);

	/*
	 * die, not a shutdown flag: a worker exiting with code 0 is unregistered
	 * for good, so one stray SIGTERM would permanently shrink the pool.
	 */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * ReadBufferWithoutRelcache pins buffers and registers its buffer I/O
	 * with CurrentResourceOwner; that registration is what releases an
	 * interrupted read if an ERROR throws the worker out of a slot.  The
	 * aux-process owner provides both the owner and its shmem-exit release.
	 */
	CreateAuxProcessResourceOwner();

	Assert(XLogWarmQueue != NULL);

	worker_id = DatumGetInt32(main_arg);
	Assert(worker_id >= 0 && worker_id < XLOGWARM_MAX_WORKERS);
	pg_atomic_write_u32(&XLogWarmQueue->worker_pids[worker_id], MyProcPid);
	before_shmem_exit(XLogWarmWorkerExit, Int32GetDatum(worker_id));

	for (;;)
	{
		/* the CFI is what turns a pending die() into the FATAL exit */
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!XLogWarmServeOne())
		{
			/*
			 * Sleep without losing a wakeup: get onto the wait list first,
			 * then recheck, then sleep.  A signal sent after the recheck is
			 * kept by the prepared state; a request published before it is
			 * seen by the recheck.
			 */
			ConditionVariablePrepareToSleep(&XLogWarmQueue->cv_work);
			if (!XLogWarmServeOne())
			{
				ConditionVariableSleep(&XLogWarmQueue->cv_work,
									   WAIT_EVENT_REPLAY_WARM_MAIN);
				continue;
			}
		}

		/* off the wait list while serving (no-op if never prepared) */
		ConditionVariableCancelSleep();
	}
}
