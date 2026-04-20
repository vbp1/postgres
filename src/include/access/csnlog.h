/*
 * csnlog.h
 *
 * Stage 1 CSN log storage manager.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/csnlog.h
 */
#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/transam.h"

/*
 * csnlog piggybacks on VarsupShmemCallbacks for shared memory registration in
 * the prototype so that we do not add another built-in callback chain yet.
 */
extern void CSNLOGShmemRequest(void);
extern void CSNLOGShmemInit(void);

extern void BootStrapCSNLOG(void);
extern void StartupCSNLOG(TransactionId oldestActiveXID);
extern void CheckPointCSNLOG(void);
extern void ExtendCSNLOG(TransactionId newestXact);

extern void CSNLogSetSubTransParent(TransactionId xid, TransactionId parentXid);
extern bool CSNLogGetSubTransParent(TransactionId xid, TransactionId *parentXid);
extern void TransactionIdSetCommitSeqNo(TransactionId xid, CommitSeqNo csn);
extern bool TransactionIdGetCommitSeqNoIfAny(TransactionId xid, CommitSeqNo *csn);
extern CommitSeqNo TransactionIdGetCommitSeqNo(TransactionId xid);

#endif							/* CSNLOG_H */
