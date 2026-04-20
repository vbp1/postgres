/*
 * csn_mvcc_vars.h
 *
 * Shared CSN MVCC state wrappers for the Stage 1 prototype.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/csn_mvcc_vars.h
 */
#ifndef CSN_MVCC_VARS_H
#define CSN_MVCC_VARS_H

#include "access/transam.h"

extern void CSNShmemInit(void);

extern CommitSeqNo GetNewCommitSeqNo(void);
extern CommitSeqNo ReadNextCommitSeqNo(void);
extern void AdvanceNextCommitSeqNoPast(CommitSeqNo csn);

extern TransactionId ReadCSNOldestActiveXid(void);
extern void SetCSNOldestActiveXid(TransactionId xid);
extern void AdvanceCSNOldestActiveXid(TransactionId xid);

#endif							/* CSN_MVCC_VARS_H */
