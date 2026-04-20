/*-------------------------------------------------------------------------
 *
 * csn_mvcc_vars.c
 *		Shared CSN MVCC state wrappers for the Stage 1 prototype
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csn_mvcc_vars.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/csn_mvcc_vars.h"
#include "storage/lwlock.h"

static CommitSeqNo
NextCommitSeqNo(CommitSeqNo csn)
{
	if (csn >= MaxNormalCommitSeqNo)
		elog(ERROR, "commit sequence number wraparound");

	CommitSeqNoAdvance(&csn);

	return csn;
}

void
CSNShmemInit(void)
{
	TransamVariables->nextCommitSeqNo = FirstNormalCommitSeqNo;
	TransamVariables->csnOldestActiveXid = InvalidTransactionId;
}

CommitSeqNo
GetNewCommitSeqNo(void)
{
	CommitSeqNo nextCSN;

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	nextCSN = TransamVariables->nextCommitSeqNo;
	Assert(CommitSeqNoIsNormal(nextCSN));
	TransamVariables->nextCommitSeqNo = NextCommitSeqNo(nextCSN);
	LWLockRelease(XidGenLock);

	return nextCSN;
}

CommitSeqNo
ReadNextCommitSeqNo(void)
{
	CommitSeqNo nextCSN;

	LWLockAcquire(XidGenLock, LW_SHARED);
	nextCSN = TransamVariables->nextCommitSeqNo;
	LWLockRelease(XidGenLock);

	return nextCSN;
}

void
AdvanceNextCommitSeqNoPast(CommitSeqNo csn)
{
	CommitSeqNo newNextCSN;

	Assert(CommitSeqNoIsNormal(csn));

	newNextCSN = NextCommitSeqNo(csn);

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	if (CommitSeqNoPrecedesOrEquals(TransamVariables->nextCommitSeqNo, csn))
		TransamVariables->nextCommitSeqNo = newNextCSN;
	LWLockRelease(XidGenLock);
}

TransactionId
ReadCSNOldestActiveXid(void)
{
	TransactionId xid;

	if (LWLockHeldByMeInMode(ProcArrayLock, LW_SHARED) ||
		LWLockHeldByMeInMode(ProcArrayLock, LW_EXCLUSIVE))
		return TransamVariables->csnOldestActiveXid;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	xid = TransamVariables->csnOldestActiveXid;
	LWLockRelease(ProcArrayLock);

	return xid;
}

void
SetCSNOldestActiveXid(TransactionId xid)
{
	Assert(!TransactionIdIsValid(xid) || TransactionIdIsNormal(xid));

	if (LWLockHeldByMeInMode(ProcArrayLock, LW_EXCLUSIVE))
	{
		TransamVariables->csnOldestActiveXid = xid;
		return;
	}

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	TransamVariables->csnOldestActiveXid = xid;
	LWLockRelease(ProcArrayLock);
}

void
SetCSNOldestActiveXidIfEarlier(TransactionId xid)
{
	Assert(TransactionIdIsNormal(xid));

	if (LWLockHeldByMeInMode(ProcArrayLock, LW_EXCLUSIVE))
	{
		if (!TransactionIdIsValid(TransamVariables->csnOldestActiveXid) ||
			TransactionIdPrecedes(xid, TransamVariables->csnOldestActiveXid))
			TransamVariables->csnOldestActiveXid = xid;
		return;
	}

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	if (!TransactionIdIsValid(TransamVariables->csnOldestActiveXid) ||
		TransactionIdPrecedes(xid, TransamVariables->csnOldestActiveXid))
		TransamVariables->csnOldestActiveXid = xid;
	LWLockRelease(ProcArrayLock);
}

void
AdvanceCSNOldestActiveXid(TransactionId xid)
{
	Assert(TransactionIdIsNormal(xid));

	/*
	 * This is only a prototype-owned bookkeeping lower bound.  It is not an
	 * authoritative replacement for procarray, GlobalVis, or nonremovable
	 * horizon tracking.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	if (!TransactionIdIsValid(TransamVariables->csnOldestActiveXid) ||
		TransactionIdPrecedes(TransamVariables->csnOldestActiveXid, xid))
		TransamVariables->csnOldestActiveXid = xid;
	LWLockRelease(ProcArrayLock);
}
