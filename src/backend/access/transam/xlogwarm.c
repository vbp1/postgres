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

#include "access/xlogprefetcher.h"
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
	 * What keeps the publisher from spending a system call per block.
	 *
	 * Replay publishes on the order of a hundred thousand blocks a second,
	 * and a condition variable signal that finds a sleeper costs a kill(2)
	 * every time.  So the publisher only signals when nobody is looking at
	 * the ring, and a worker that stops looking hands the ring over in its
	 * place.  Between them the wakeup happens once per idle pool rather than
	 * once per block.
	 *
	 * scanners counts workers searching the ring, and deliberately not the
	 * ones inside a page read: a worker in a read cannot take new work, so
	 * counting it would let one busy worker silence the wakeups for a pool
	 * that is otherwise asleep.
	 *
	 * pending counts published requests nobody has claimed.  It is what a
	 * searching worker reads instead of walking every slot, and what tells
	 * the process leaving the ring whether the ring still needs somebody. It
	 * is raised before its slot becomes visible, so it is never lower than
	 * the number of published slots and a claimer never takes it below zero;
	 * the two are equal only when no publication is in flight.
	 *
	 * sleepers decides nothing.  It exists so that "the whole pool is asleep"
	 * is an observable fact from outside the pool.
	 */
	pg_atomic_uint32 scanners;
	pg_atomic_uint32 pending;
	pg_atomic_uint32 sleepers;

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

	/*
	 * The slot each worker holds, one past its index, or zero for none. A
	 * request that stops moving belongs to somebody, and this is how the
	 * owner is found: a worker inside a read that never returns is the one
	 * case that costs the ring a slot for good, and without this the pid to
	 * look at is a guess.
	 */
	pg_atomic_uint32 worker_slots[XLOGWARM_MAX_WORKERS];

	XLogWarmSlot slots[FLEXIBLE_ARRAY_MEMBER];
}			XLogWarmCtl;

static XLogWarmCtl * XLogWarmQueue = NULL;

/* one claimed request, in the hands of the worker that claimed it */
typedef struct XLogWarmRequest
{
	XLogWarmSlot *slot;
	uint64		request_id;
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;
}			XLogWarmRequest;

static void XLogWarmWorkerExit(int code, Datum arg);
static void XLogWarmHandOff(void);

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
/*
 * How long a worker keeps searching an empty ring before it sleeps, in
 * pg_spin_delay() rounds.  It buys the publisher its silence: a worker that
 * stays in the search over the gap between two publications is one the
 * publisher does not have to wake, and at replay's rate those gaps are
 * microseconds.  Large enough to cover them, small enough that a standby
 * with nothing to replay settles into sleeping workers within a moment.
 */
#define XLOGWARM_SPINS	1000

/*
 * Worker-private: the slot this worker holds, or -1.  Read on the way out to
 * hand the slot back, so a worker that is signalled away does not take a slot
 * of the ring with it.
 */
static int	my_claimed_slot = -1;

/*
 * Worker-private: which of the two shared counts this worker is part of.
 *
 * Both are read on the way out.  A worker can be signalled away from either
 * state, and a count left standing would say the pool has a searcher, or a
 * sleeper, that no longer exists — the first silences the publisher's
 * wakeups, the second makes "the whole pool is asleep" untrue where it is
 * relied upon.
 */
static bool my_scanning = false;
static bool my_sleeping = false;

/* Worker-private: this worker's index in the pool, or -1 outside one. */
static int	my_worker_id = -1;

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
		pg_atomic_init_u32(&XLogWarmQueue->scanners, 0);
		pg_atomic_init_u32(&XLogWarmQueue->pending, 0);
		pg_atomic_init_u32(&XLogWarmQueue->sleepers, 0);
		XLogWarmQueue->drop_epoch = 0;
		XLogWarmQueue->capacity = replay_warm_queue_size;

		for (int i = 0; i < XLOGWARM_MAX_WORKERS; i++)
		{
			pg_atomic_init_u32(&XLogWarmQueue->worker_pids[i], 0);
			pg_atomic_init_u32(&XLogWarmQueue->worker_slots[i], 0);
		}

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

	/*
	 * The count covers the slot before anybody else can see it.  Raising it
	 * afterwards would let a worker claim the slot and lower a count that had
	 * not been raised yet, which on an unsigned counter is not a small error.
	 */
	pg_atomic_fetch_add_u32(&XLogWarmQueue->pending, 1);

	/* the payload must be visible before a worker can see the state */
	pg_write_barrier();
	pg_atomic_write_u32(&slot->state, XLOGWARM_PUBLISHED);

	*request_id = next_request_id++;
	publish_hand = (slot_no + 1) % XLogWarmQueue->capacity;
	pg_atomic_fetch_add_u64(&XLogWarmQueue->published, 1);

	/*
	 * Somebody already searching the ring will find this request without
	 * being told, so the signal — and the system call inside it — is only
	 * for a pool where nobody is.  The barrier is what makes the two sides
	 * meet: a write barrier would order the publication, but not this load
	 * against it, and the pairing needs the load to come after.  A worker on
	 * its way out of the search closes the other half of the window in
	 * XLogWarmHandOff().
	 */
	pg_memory_barrier();
	if (pg_atomic_read_u32(&XLogWarmQueue->scanners) == 0)
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
		{
			/* the request is gone, and so is the need for somebody to take it */
			pg_atomic_fetch_sub_u32(&XLogWarmQueue->pending, 1);
			pg_atomic_fetch_add_u64(&XLogWarmQueue->cancelled, 1);
		}
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
XLogWarmGetWorkerPids(int *pids, int *slots)
{
	int			found = 0;

	if (XLogWarmQueue == NULL)
		return 0;

	for (int i = 0; i < XLOGWARM_MAX_WORKERS; i++)
	{
		uint32		pid = pg_atomic_read_u32(&XLogWarmQueue->worker_pids[i]);

		if (pid != 0)
		{
			if (slots != NULL)
				slots[found] =
					(int) pg_atomic_read_u32(&XLogWarmQueue->worker_slots[i]) - 1;
			pids[found++] = (int) pid;
		}
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

	/*
	 * This walk is not a snapshot: slots change state under it, so what it
	 * returns is what the ring looked like slot by slot rather than at any
	 * one instant.  Good enough to see a request in flight, and not good
	 * enough to check a counter against while the pool is working.
	 */

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
 * What the pool's processes are doing right now: how many are searching the
 * ring, how many requests are waiting for one of them, and how many are
 * asleep.  Unlike the slot walk above these are single counters, so each is
 * a real value rather than a scan; "the whole pool is asleep" is a fact a
 * test can wait for here.
 */
void
XLogWarmGetPoolState(int *scanners, int *pending, int *sleepers)
{
	*scanners = 0;
	*pending = 0;
	*sleepers = 0;

	if (XLogWarmQueue == NULL)
		return;

	*scanners = (int) pg_atomic_read_u32(&XLogWarmQueue->scanners);
	*pending = (int) pg_atomic_read_u32(&XLogWarmQueue->pending);
	*sleepers = (int) pg_atomic_read_u32(&XLogWarmQueue->sleepers);
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
	BlockNumber nblocks;
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
			my_drop_epoch = XLogWarmQueue->drop_epoch;
		}

		smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

		/*
		 * Is the block still there?  Replay may have dropped or truncated the
		 * relation between publication and now — the ordinary outcome of
		 * running ahead of it, and the outcome the interlock guarantees for a
		 * request that gets here after a drop.
		 *
		 * Asking the file system outright costs more than it looks:
		 * smgrexists() closes the fork before answering (mdexists() skips
		 * that only in the startup process) and the smgrnblocks() behind it
		 * then reopens the segment chain from the beginning, so on a terabyte
		 * relation one question is a thousand file opens.
		 *
		 * The size this worker last saw is therefore taken from the relation
		 * itself: smgrnblocks() records it there, smgrrelease() clears it,
		 * and the smgrreleaseall() above is what clears it after a drop.
		 * Reading the field directly is how the rest of the tree uses it —
		 * see the comment on smgrnblocks_cached(), whose InRecovery test is
		 * about the startup process and so never lets a worker in.
		 */
		nblocks = smgr->smgr_cached_nblocks[forknum];

		if (nblocks == InvalidBlockNumber)
		{
			/*
			 * Nothing known about this fork: either the worker has not
			 * touched it since the last drop, or it has never touched it at
			 * all.  This is the one place that pays for the expensive
			 * question, and it is also the only place that can tell a
			 * relation whose files are gone from one that is merely shorter
			 * than the request expects.
			 */
			if (!smgrexists(smgr, forknum))
			{
				failed = true;
				pg_atomic_fetch_add_u64(&XLogWarmQueue->vanished, 1);
			}
			else
				nblocks = smgrnblocks(smgr, forknum);
		}

		if (!failed && blkno >= nblocks)
		{
			/*
			 * The remembered size may simply predate an extension: nothing
			 * tells a worker that replay has grown a relation, so a known
			 * size is a lower bound.  mdnblocks() resumes from the last open
			 * segment, which makes this an lseek rather than another walk.
			 */
			nblocks = smgrnblocks(smgr, forknum);

			if (blkno >= nblocks)
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
			 *
			 * This is also where that question gets answered for
			 * pg_stat_recovery_prefetch.  Replay used to ask it before
			 * publishing and count the answer itself; with the pool running
			 * it no longer asks, so the count belongs to whoever does.
			 */
			pg_atomic_fetch_add_u64(&XLogWarmQueue->hits, 1);
			XLogPrefetchCountHit();
		}
		else
		{
			buffer = ReadBufferWithoutRelcache(rlocator, forknum, blkno,
											   RBM_NORMAL, NULL, true);
			pg_atomic_fetch_add_u64(&XLogWarmQueue->reads, 1);
			XLogPrefetchCountPrefetch();

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

	/*
	 * Leave the pool's counts, and leave them in the order a live worker
	 * would.  This callback runs in the before_shmem_exit phase, and the
	 * teardown that takes a process off a condition variable's wait list
	 * happens later, in ProcKill(): until then a publisher's signal can still
	 * land on this process, which is about to stop reading its latch.  So the
	 * wait list goes first, then the counts, then the hand-off — a searcher
	 * that leaves without one takes the ring's only promised searcher with
	 * it.
	 */
	if (my_scanning || my_sleeping)
	{
		ConditionVariableCancelSleep();

		if (my_sleeping)
		{
			pg_atomic_fetch_sub_u32(&XLogWarmQueue->sleepers, 1);
			my_sleeping = false;
		}
		if (my_scanning)
		{
			pg_atomic_fetch_sub_u32(&XLogWarmQueue->scanners, 1);
			my_scanning = false;
		}

		XLogWarmHandOff();
	}

	if (my_claimed_slot >= 0)
	{
		XLogWarmSlot *slot = &XLogWarmQueue->slots[my_claimed_slot];
		uint32		expected = XLOGWARM_CLAIMED;

		my_claimed_slot = -1;

		if (pg_atomic_compare_exchange_u32(&slot->state, &expected,
										   XLOGWARM_FAILED))
			pg_atomic_fetch_add_u64(&XLogWarmQueue->released, 1);
	}

	pg_atomic_write_u32(&XLogWarmQueue->worker_slots[worker_id], 0);
	pg_atomic_write_u32(&XLogWarmQueue->worker_pids[worker_id], 0);
}

/*
 * Claim one published slot.  Returns false when the ring holds nothing to do.
 *
 * The caller must be counted in scanners while this runs: that is what tells
 * a publisher it need not spend a wakeup, and the promise behind it is that
 * this process looks at the ring after the publication became visible.
 */
static bool
XLogWarmClaimOne(XLogWarmRequest * req)
{
	int			capacity = XLogWarmQueue->capacity;
	uint32		start;

	/*
	 * No request outstanding, and the counter says so without touching a
	 * slot.  A worker that searched the whole ring every time it looked would
	 * spend the pool's cores dragging several hundred shared cache lines
	 * between them, which is what makes waiting here cheap enough to prefer
	 * to sleeping.
	 */
	if (pg_atomic_read_u32(&XLogWarmQueue->pending) == 0)
		return false;

	start = pg_atomic_fetch_add_u32(&XLogWarmQueue->hand, 1);

	for (int i = 0; i < capacity; i++)
	{
		XLogWarmSlot *slot = &XLogWarmQueue->slots[(start + i) % capacity];
		uint32		expected = XLOGWARM_PUBLISHED;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected,
											XLOGWARM_CLAIMED))
			continue;

		pg_atomic_fetch_sub_u32(&XLogWarmQueue->pending, 1);

		/* the state was observed before the payload it advertises */
		pg_read_barrier();

		req->slot = slot;
		req->request_id = slot->request_id;
		req->rlocator = slot->rlocator;
		req->forknum = slot->forknum;
		req->blkno = slot->blkno;

		/*
		 * From here until the slot is finished this worker owns it, and says
		 * so where its exit callback can see it.
		 */
		my_claimed_slot = (start + i) % capacity;
		if (my_worker_id >= 0)
			pg_atomic_write_u32(&XLogWarmQueue->worker_slots[my_worker_id],
								(uint32) my_claimed_slot + 1);

		pg_atomic_fetch_add_u64(&XLogWarmQueue->claimed, 1);
		return true;
	}

	return false;
}

/*
 * Hand the ring over on the way out of the search.
 *
 * A publisher that saw this process searching stayed quiet, so a process
 * that stops searching — to read a page, or for good — has to make sure
 * somebody else is looking if anything is still outstanding.  Otherwise the
 * work it was trusted to find would sit in front of a sleeping pool until
 * the next publication happened to wake somebody.
 *
 * Two things must already be true at the call: this process has left the
 * scanners count, and it is not itself on the wait list — a signal issued
 * while still registered could pick the signaller and leave the others
 * asleep.
 */
static void
XLogWarmHandOff(void)
{
	/*
	 * Pairs with the publisher: it publishes and then reads scanners, this
	 * side leaves scanners and then reads pending, and a full barrier on both
	 * sides is what guarantees at least one of the two sees the other.
	 */
	pg_memory_barrier();

	if (pg_atomic_read_u32(&XLogWarmQueue->pending) > 0 &&
		pg_atomic_read_u32(&XLogWarmQueue->scanners) == 0)
		ConditionVariableSignal(&XLogWarmQueue->cv_work);
}

/*
 * Stop and start searching, as the states above are entered and left.
 */
static void
XLogWarmStopScanning(void)
{
	Assert(my_scanning);
	pg_atomic_fetch_sub_u32(&XLogWarmQueue->scanners, 1);
	my_scanning = false;
	XLogWarmHandOff();
}

static void
XLogWarmStartScanning(void)
{
	Assert(!my_scanning);
	pg_atomic_fetch_add_u32(&XLogWarmQueue->scanners, 1);
	my_scanning = true;
}

/*
 * Serve a claimed request and go back to searching.
 */
static void
XLogWarmServe(XLogWarmRequest * req)
{
	XLogWarmDoOne(req->slot, req->request_id, req->rlocator, req->forknum,
				  req->blkno);
	my_claimed_slot = -1;
	if (my_worker_id >= 0)
		pg_atomic_write_u32(&XLogWarmQueue->worker_slots[my_worker_id], 0);
}

/*
 * Leave the search to serve what this worker just claimed, then rejoin it.
 *
 * The injection point catches a worker in the state the hand-off exists for:
 * holding a request, and still counted as a searcher, so a publication
 * landing now is one the publisher will leave to this process.  It is safe
 * to park here — the worker is not on the pool's wait list at this point, so
 * the waiting the injection point does of its own cannot disturb it.
 */
static void
XLogWarmServeAsScanner(XLogWarmRequest * req)
{
	INJECTION_POINT("replay-warm-claimed", NULL);
	XLogWarmStopScanning();
	XLogWarmServe(req);
	XLogWarmStartScanning();
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
	my_worker_id = worker_id;
	pg_atomic_write_u32(&XLogWarmQueue->worker_pids[worker_id], MyProcPid);
	before_shmem_exit(XLogWarmWorkerExit, Int32GetDatum(worker_id));

	/* this process is searching the ring from here on */
	XLogWarmStartScanning();

	for (;;)
	{
		XLogWarmRequest req;
		int			spins;

		/* the CFI is what turns a pending die() into the FATAL exit */
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (XLogWarmClaimOne(&req))
		{
			XLogWarmServeAsScanner(&req);
			continue;
		}

		/*
		 * Nothing to do this instant, which at replay's publication rate
		 * usually means "not yet" rather than "not at all".  Stay in the
		 * search for a while: a worker that is still counted is a worker the
		 * publisher does not have to wake, and the whole point of the counts
		 * is to keep that system call out of replay's way.  The budget is
		 * small enough that an idle standby settles into sleeping workers
		 * rather than spinning ones.
		 */
		for (spins = XLOGWARM_SPINS; spins > 0; spins--)
		{
			if (pg_atomic_read_u32(&XLogWarmQueue->pending) > 0)
				break;
			pg_spin_delay();
		}

		if (spins > 0 && XLogWarmClaimOne(&req))
		{
			XLogWarmServeAsScanner(&req);
			continue;
		}

		/*
		 * Give up and sleep, without losing a wakeup: join the wait list
		 * first, then leave the searchers, then recheck.  In that order a
		 * publisher that reads no searchers is reading about a process that
		 * is already waiting, and a publisher that reads one is reading about
		 * a process that has yet to look again.
		 */
		ConditionVariablePrepareToSleep(&XLogWarmQueue->cv_work);
		pg_atomic_fetch_sub_u32(&XLogWarmQueue->scanners, 1);
		my_scanning = false;

		if (XLogWarmClaimOne(&req))
		{
			/* leave the wait list before the hand-off can pick this process */
			ConditionVariableCancelSleep();
			XLogWarmHandOff();
			XLogWarmServe(&req);
			XLogWarmStartScanning();
			continue;
		}

		pg_atomic_fetch_add_u32(&XLogWarmQueue->sleepers, 1);
		my_sleeping = true;

		ConditionVariableSleep(&XLogWarmQueue->cv_work,
							   WAIT_EVENT_REPLAY_WARM_MAIN);

		/*
		 * ConditionVariableSleep() puts this process back on the wait list
		 * before it returns, so become a searcher while still registered:
		 * between the two counts there must be no moment where this process
		 * is neither searching nor waiting, or a publisher could look at that
		 * moment and decide the ring needs nobody.
		 */
		pg_atomic_fetch_sub_u32(&XLogWarmQueue->sleepers, 1);
		my_sleeping = false;
		pg_atomic_fetch_add_u32(&XLogWarmQueue->scanners, 1);
		my_scanning = true;
		ConditionVariableCancelSleep();
	}
}
