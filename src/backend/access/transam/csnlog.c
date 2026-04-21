/*-------------------------------------------------------------------------
 *
 * csnlog.c
 *		Stage 1 CSN log storage manager
 *
 * This module provides a conservative, non-WAL-backed SLRU skeleton for
 * xid-to-CSN storage.  It does not claim crash-safe semantics.  Runtime
 * truncation uses conservative runtime and legacy horizons, but durable
 * retention reconstruction remains deferred.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csnlog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/csn_mvcc_vars.h"
#include "access/csnlog.h"
#include "access/slru.h"
#include "storage/procarray.h"

/* We store one 64-bit CSN per xid. */
#define CSNLOG_XACTS_PER_PAGE		(BLCKSZ / sizeof(CommitSeqNo))
#define CSNLOG_NBUFFERS			16

/*
 * Although we return an int64 the actual value can't currently exceed
 * 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE.
 */
static inline int64
TransactionIdToCSNPage(TransactionId xid)
{
	return xid / (int64) CSNLOG_XACTS_PER_PAGE;
}

#define TransactionIdToCSNEntry(xid) \
	((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE)

static bool CsnlogPagePrecedes(int64 page1, int64 page2);
static int	csnlog_errdetail_for_io_error(const void *opaque_data);
static bool TransactionIdInCSNLogRange(TransactionId xid);
static int	CSNLogReadPageForWrite(int64 pageno, TransactionId xid);
static TransactionId CSNLogGetRetentionFloor(TransactionId oldestXactToKeep);

static SlruDesc CsnlogSlruDesc;

#define CsnlogCtl (&CsnlogSlruDesc)

void
CSNLOGShmemRequest(void)
{
	SimpleLruRequest(.desc = &CsnlogSlruDesc,
					 .name = "csnlog",
					 .Dir = "pg_csnlog",
					 .long_segment_names = false,
					 .nslots = CSNLOG_NBUFFERS,
					 .sync_handler = SYNC_HANDLER_NONE,
					 .PagePrecedes = CsnlogPagePrecedes,
					 .errdetail_for_io_error = csnlog_errdetail_for_io_error,
		);
}

void
CSNLOGShmemInit(void)
{
	SlruPagePrecedesUnitTests(CsnlogCtl, CSNLOG_XACTS_PER_PAGE);
}

void
BootStrapCSNLOG(void)
{
	SimpleLruZeroAndWritePage(CsnlogCtl, 0);
}

void
StartupCSNLOG(TransactionId oldestActiveXID)
{
	FullTransactionId nextXid;
	int64		startPage;
	int64		endPage;
	LWLock	   *prevlock = NULL;
	LWLock	   *lock;

	if (!TransactionIdIsNormal(oldestActiveXID))
		oldestActiveXID = ReadNextTransactionId();

	startPage = TransactionIdToCSNPage(oldestActiveXID);
	nextXid = TransamVariables->nextXid;
	endPage = TransactionIdToCSNPage(XidFromFullTransactionId(nextXid));

	for (;;)
	{
		lock = SimpleLruGetBankLock(CsnlogCtl, startPage);
		if (prevlock != lock)
		{
			if (prevlock)
				LWLockRelease(prevlock);
			LWLockAcquire(lock, LW_EXCLUSIVE);
			prevlock = lock;
		}

		(void) SimpleLruZeroPage(CsnlogCtl, startPage);
		if (startPage == endPage)
			break;

		startPage++;
		if (startPage > TransactionIdToCSNPage(MaxTransactionId))
			startPage = 0;
	}

	LWLockRelease(lock);

	SetCSNOldestActiveXid(oldestActiveXID);
	SetOldestCSNLogXid(oldestActiveXID);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCSNLOG(void)
{
	TransactionId nextXid;
	int64		pageno;
	int			entryno;
	int			slotno;
	CommitSeqNo *ptr;
	LWLock	   *lock;

	nextXid = XidFromFullTransactionId(TransamVariables->nextXid);
	pageno = TransactionIdToCSNPage(nextXid);
	entryno = TransactionIdToCSNEntry(nextXid);

	/*
	 * Zero out the remainder of the current csnlog page.  This is purely a
	 * restart/recovery hygiene step; later pages remain zeroed on demand.
	 */
	if (entryno == 0)
		return;

	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);
	LWLockAcquire(lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, false, &nextXid);
	ptr = (CommitSeqNo *) CsnlogCtl->shared->page_buffer[slotno];
	ptr += entryno;

	MemSet(ptr, 0, BLCKSZ - entryno * sizeof(CommitSeqNo));

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(lock);
}

void
CheckPointCSNLOG(void)
{
	SimpleLruWriteAll(CsnlogCtl, true);
}

/*
 * Compute the oldest xid that we must still retain in pg_csnlog.
 *
 * The caller's requested floor is the baseline.  We keep that floor from
 * moving right only when that remains conservative with respect to the
 * runtime holders and legacy fallback horizons that still need old xid state.
 */
static TransactionId
CSNLogGetRetentionFloor(TransactionId oldestXactToKeep)
{
	TransactionId runtimeFloor;
	TransactionId clogFloor;

	Assert(TransactionIdIsNormal(oldestXactToKeep));

	runtimeFloor = GetOldestTransactionIdConsideredRunning();
	if (TransactionIdIsValid(runtimeFloor) &&
		TransactionIdPrecedes(runtimeFloor, oldestXactToKeep))
		oldestXactToKeep = runtimeFloor;

	LWLockAcquire(XactTruncationLock, LW_SHARED);
	clogFloor = TransamVariables->oldestClogXid;
	LWLockRelease(XactTruncationLock);

	if (TransactionIdIsValid(clogFloor) &&
		TransactionIdPrecedes(clogFloor, oldestXactToKeep))
		oldestXactToKeep = clogFloor;

	return oldestXactToKeep;
}

/*
 * Truncate old csnlog segments that are no longer needed by either runtime
 * holders or legacy fallback lookups.
 */
void
TruncateCSNLOG(TransactionId oldestXactToKeep)
{
	TransactionId retentionFloor;
	int64		cutoffPage;

	retentionFloor = CSNLogGetRetentionFloor(oldestXactToKeep);
	cutoffPage = TransactionIdToCSNPage(retentionFloor);

	if (!SlruScanDirectory(CsnlogCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;

	LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
	if (!TransactionIdIsValid(TransamVariables->oldestCsnlogXid) ||
		TransactionIdPrecedes(TransamVariables->oldestCsnlogXid, retentionFloor))
		TransamVariables->oldestCsnlogXid = retentionFloor;
	SimpleLruTruncate(CsnlogCtl, cutoffPage);
	LWLockRelease(XactTruncationLock);
}

/*
 * Phase B intentionally omitted runtime truncation wiring.  Truncation is
 * now conservative, but the durable retention contract remains prototype-only.
 */
void
ExtendCSNLOG(TransactionId newestXact)
{
	int64		pageno;
	LWLock	   *lock;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToCSNEntry(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToCSNPage(newestXact);

	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	SimpleLruZeroPage(CsnlogCtl, pageno);
	LWLockRelease(lock);
}

void
TransactionIdSetCommitSeqNo(TransactionId xid, CommitSeqNo csn)
{
	int64		pageno;
	int			entryno;
	int			slotno;
	LWLock	   *lock;
	CommitSeqNo *ptr;

	Assert(TransactionIdIsNormal(xid));
	Assert(CommitSeqNoIsValid(csn));

	pageno = TransactionIdToCSNPage(xid);
	entryno = TransactionIdToCSNEntry(xid);

	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	slotno = CSNLogReadPageForWrite(pageno, xid);
	ptr = (CommitSeqNo *) CsnlogCtl->shared->page_buffer[slotno];
	ptr += entryno;
	*ptr = csn;
	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(lock);
}

void
CSNLogSetSubTransParent(TransactionId xid, TransactionId parentXid)
{
	Assert(TransactionIdIsNormal(xid));
	Assert(TransactionIdIsNormal(parentXid));

	TransactionIdSetCommitSeqNo(xid, CommitSeqNoFromSubTransParent(parentXid));
}

bool
CSNLogGetSubTransParent(TransactionId xid, TransactionId *parentXid)
{
	CommitSeqNo csn;

	Assert(parentXid != NULL);

	*parentXid = InvalidTransactionId;

	if (!TransactionIdGetCommitSeqNoIfAny(xid, &csn))
		return false;
	if (!CommitSeqNoIsSubTransParent(csn))
		return false;

	*parentXid = TransactionIdFromCommitSeqNoParent(csn);

	return true;
}

bool
TransactionIdGetCommitSeqNoIfAny(TransactionId xid, CommitSeqNo *csn)
{
	int64		pageno;
	int			entryno;
	int			slotno;
	CommitSeqNo *ptr;

	Assert(csn != NULL);

	*csn = InvalidCommitSeqNo;

	/*
	 * Keep the published CSN retention floor stable across the range check
	 * and the subsequent SLRU read so VACUUM cannot truncate the backing
	 * segment out from under an in-range lookup.
	 */
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	if (!TransactionIdInCSNLogRange(xid))
	{
		LWLockRelease(XactTruncationLock);
		return false;
	}

	pageno = TransactionIdToCSNPage(xid);
	entryno = TransactionIdToCSNEntry(xid);

	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, &xid);
	ptr = (CommitSeqNo *) CsnlogCtl->shared->page_buffer[slotno];
	ptr += entryno;
	*csn = *ptr;

	LWLockRelease(SimpleLruGetBankLock(CsnlogCtl, pageno));
	LWLockRelease(XactTruncationLock);

	return CommitSeqNoIsValid(*csn);
}

static int
CSNLogReadPageForWrite(int64 pageno, TransactionId xid)
{
	SlruShared	shared = CsnlogCtl->shared;
	int			bankno = pageno % CsnlogCtl->nbanks;
	int			slots_per_bank = shared->num_slots / CsnlogCtl->nbanks;
	int			bankstart = bankno * slots_per_bank;
	int			bankend = bankstart + slots_per_bank;
	int			slotno;

	Assert(LWLockHeldByMeInMode(SimpleLruGetBankLock(CsnlogCtl, pageno),
								LW_EXCLUSIVE));

	/*
	 * A csnlog page can be dirty in shared memory before its backing file
	 * exists on disk. Re-zero only when the page is absent both from the SLRU
	 * buffers and from disk.
	 */
	for (slotno = bankstart; slotno < bankend; slotno++)
	{
		if (shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
			shared->page_number[slotno] == pageno)
			return SimpleLruReadPage(CsnlogCtl, pageno, true, &xid);
	}

	if (!SimpleLruDoesPhysicalPageExist(CsnlogCtl, pageno))
		return SimpleLruZeroPage(CsnlogCtl, pageno);

	return SimpleLruReadPage(CsnlogCtl, pageno, true, &xid);
}

CommitSeqNo
TransactionIdGetCommitSeqNo(TransactionId xid)
{
	CommitSeqNo csn;

	if (!TransactionIdGetCommitSeqNoIfAny(xid, &csn))
		return InvalidCommitSeqNo;

	return csn;
}

static bool
TransactionIdInCSNLogRange(TransactionId xid)
{
	TransactionId oldestActiveXid;
	TransactionId nextXid;

	if (!TransactionIdIsNormal(xid))
		return false;

	/*
	 * oldestCsnlogXid is the monotonic retained-history floor for on-disk
	 * csnlog segments.  It is separate from the runtime-only
	 * csnOldestActiveXid, which can move backwards when old xmin holders
	 * reappear.
	 */
	oldestActiveXid = ReadOldestCSNLogXid();
	if (!TransactionIdIsValid(oldestActiveXid))
		return false;

	nextXid = ReadNextTransactionId();

	if (TransactionIdPrecedes(xid, oldestActiveXid))
		return false;
	if (!TransactionIdPrecedes(xid, nextXid))
		return false;

	return true;
}

static bool
CsnlogPagePrecedes(int64 page1, int64 page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CSNLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId + 1;
	xid2 = ((TransactionId) page2) * CSNLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId + 1;

	return (TransactionIdPrecedes(xid1, xid2) &&
			TransactionIdPrecedes(xid1, xid2 + CSNLOG_XACTS_PER_PAGE - 1));
}

static int
csnlog_errdetail_for_io_error(const void *opaque_data)
{
	TransactionId xid = *(const TransactionId *) opaque_data;

	return errdetail("Could not access CSN status of transaction %u.", xid);
}
