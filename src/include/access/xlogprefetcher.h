/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.h
 *		Declarations for the recovery prefetching module.
 *
 * Portions Copyright (c) 2022-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogprefetcher.h
 *-------------------------------------------------------------------------
 */
#ifndef XLOGPREFETCHER_H
#define XLOGPREFETCHER_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"

/* GUCs */
extern PGDLLIMPORT int recovery_prefetch;

/* Possible values for recovery_prefetch */
typedef enum
{
	RECOVERY_PREFETCH_OFF,
	RECOVERY_PREFETCH_ON,
	RECOVERY_PREFETCH_TRY,
}			RecoveryPrefetchValue;

struct XLogPrefetcher;
typedef struct XLogPrefetcher XLogPrefetcher;


extern void XLogPrefetchReconfigure(void);

extern size_t XLogPrefetchShmemSize(void);
extern void XLogPrefetchShmemInit(void);

extern void XLogPrefetchResetStats(void);

/*
 * Counting from outside the startup process.
 *
 * The replay warm pool decides in its workers what replay used to decide for
 * itself — whether a block was already in a buffer — so the two counters that
 * record that decision are incremented from there.  They keep their meaning;
 * only the process holding the answer has changed.
 */
extern void XLogPrefetchCountHit(void);
extern void XLogPrefetchCountPrefetch(void);

extern XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader);
extern void XLogPrefetcherFree(XLogPrefetcher *prefetcher);

extern XLogReaderState *XLogPrefetcherGetReader(XLogPrefetcher *prefetcher);

extern void XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher,
									XLogRecPtr recPtr);

extern XLogRecord *XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
											char **errmsg);

extern void XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher);

#endif
