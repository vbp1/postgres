/*-------------------------------------------------------------------------
 *
 * xlogwarm.h
 *	  Replay prefetch worker pool.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogwarm.h
 *-------------------------------------------------------------------------
 */
#ifndef XLOGWARM_H
#define XLOGWARM_H

#include "access/xlogreader.h"
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/relfilelocator.h"

/* GUCs */
extern PGDLLIMPORT int replay_warm_workers;
extern PGDLLIMPORT int replay_warm_queue_size;

/* a snapshot of the pool's counters */
typedef struct XLogWarmStats
{
	uint64		published;		/* blocks handed to the pool */
	uint64		dropped_full;	/* blocks left unwarmed, no free slot */
	uint64		collected;		/* answers replay picked up */
	uint64		missed;			/* requests with no answer to pick up */
	uint64		stale;			/* answers whose buffer had been evicted */
	uint64		cancelled;		/* requests withdrawn on a prefetcher reset */
	uint64		released;		/* slots handed back by a departing worker */
	uint64		claimed;		/* requests a worker took */
	uint64		reads;			/* pages a worker actually read */
	uint64		hits;			/* pages already resident when claimed */
	uint64		failed;			/* reads that errored out */
	uint64		vanished;		/* relations gone or too short by read time */
	uint64		discarded;		/* results dropped, slot no longer theirs */
}			XLogWarmStats;

extern Size XLogWarmShmemSize(void);
extern void XLogWarmShmemInit(void);
extern void XLogWarmWorkersRegister(void);
pg_noreturn extern void XLogWarmWorkerMain(Datum main_arg);

extern bool XLogWarmPoolActive(void);
extern int	XLogWarmPublish(RelFileLocator rlocator, ForkNumber forknum,
							BlockNumber blkno, uint64 *request_id);
extern Buffer XLogWarmCollect(int slot_no, uint64 request_id);
extern void XLogWarmCancelAll(void);
extern void XLogWarmCountStale(void);
extern void XLogWarmDropBegin(void);
extern void XLogWarmDropEnd(void);
extern bool XLogWarmGetStats(XLogWarmStats * stats);
extern void XLogWarmGetSlotCounts(int *published, int *claimed);
extern int	XLogWarmGetWorkerPids(int *pids);

/* the pool size ceiling, matching the setting's maximum */
#define XLOGWARM_MAX_WORKERS	64

#endif							/* XLOGWARM_H */
