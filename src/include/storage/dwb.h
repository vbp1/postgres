/*-------------------------------------------------------------------------
 *
 * dwb.h
 *	  Short-lived double write buffer (DWB).
 *
 * A DWB slot lives for the duration of one flush batch and is reused only
 * after the data-file fsync covering its page has become durable.  Strict
 * durability order per batch:
 *
 *	  XLogFlush(page LSN)  ->  batch write + fdatasync (leader)  ->
 *	  smgrwrite (kernel cache)  ->  eventual segment fsync  ->  slot reuse
 *
 * See .plan/short-lived-dwb_REL_18.md for the full design.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/dwb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DWB_H
#define DWB_H

#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "common/file_utils.h"
#include "port/pg_crc32c.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"
#include "storage/s_lock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

/*
 * The io_torn_pages_protection GUC values (DWBTornPageProtection) live in
 * catalog/pg_control.h: the mode is recorded in pg_control.
 */

/* GUC: dwb_on_stall (Stage B backpressure behaviour) */
typedef enum
{
	DWB_ON_STALL_WARN,
	DWB_ON_STALL_ERROR,
	DWB_ON_STALL_PANIC,
} DWBOnStall;

/*
 * GUC: dwb_retire_sync_method.  Shares the DataDirSyncMethod values of
 * recovery_init_sync_method (common/file_utils.h): "fsync" retires by
 * fsyncing each touched data-file segment, "syncfs" makes whole file
 * systems durable per retire round.  syncfs is the default where the
 * syscall exists: a shared random workload touches nearly every segment
 * of a large table between rounds, and one syncfs replaces hundreds of
 * per-segment fdatasync calls on the same file system.
 */
#ifdef HAVE_SYNCFS
#define DWB_RETIRE_SYNC_METHOD_DEFAULT DATA_DIR_SYNC_METHOD_SYNCFS
#else
#define DWB_RETIRE_SYNC_METHOD_DEFAULT DATA_DIR_SYNC_METHOD_FSYNC
#endif

/* GUC variables (defined in dwb_ctl.c) */
extern PGDLLIMPORT int io_torn_pages_protection;
extern PGDLLIMPORT int dwb_num_batches;
extern PGDLLIMPORT int dwb_batch_pages;
extern PGDLLIMPORT int dwb_max_segments;
extern PGDLLIMPORT int dwb_retire_workers;
extern PGDLLIMPORT int dwb_cleaner_workers;
extern PGDLLIMPORT int dwb_retire_sync_method;
extern PGDLLIMPORT int dwb_batch_timeout_ms;
extern PGDLLIMPORT int dwb_retire_interval_ms;
extern PGDLLIMPORT bool dwb_writeback;
extern PGDLLIMPORT int dwb_slow_warn_ms;
extern PGDLLIMPORT int dwb_slot_stuck_timeout_ms;
extern PGDLLIMPORT int dwb_write_timeout_ms;
extern PGDLLIMPORT int dwb_on_stall;

#define DWBIsEnabled() (io_torn_pages_protection == DWB_PROTECT_DOUBLE_WRITES)

/*
 * Compile-time capacity limits (GUC maxima).  Statically sized arrays
 * (per-batch shmem arrays, the retire-side segment snapshot) rely on these,
 * so the GUC bounds in guc_tables.c must use them, never bare literals.
 */
#define DWB_BATCH_MAX_PAGES		256
#define DWB_BATCH_MAX_SEGS		DWB_BATCH_MAX_PAGES
#define DWB_BITMAP_WORDS		(DWB_BATCH_MAX_PAGES / 64)
#define DWB_NUM_BATCHES_MAX		1024
/* staging pool: 2 writer classes + 2 in-flight leader writes */
#define DWB_STAGING_BUFFERS		4
/* writer classes (3.6) */
#define DWB_NUM_WCLASSES		2
#define DWB_WCLASS_EVICTION		0
#define DWB_WCLASS_BACKGROUND	1

/*
 * Bin size cap for the vectored background flush (FlushBufferBin, used by
 * the checkpointer's BufferSync, the bgwriter's LRU scan and the cleaner
 * worker pool): the flush holds a pin, a shared content lock and
 * BM_IO_IN_PROGRESS per bin member at once, so the cap must leave
 * MAX_SIMUL_LWLOCKS (200) plenty of headroom.  64 matches the default
 * dwb_batch_pages; larger batch_pages settings seal their batches at
 * bin-sized fills.  Shared here because the cleaner work queue stores
 * bins of this size.
 */
#define DWB_FLUSH_BIN_MAX		64

/*
 * Why a batch was sealed.  Purely diagnostic: per-class seal and page
 * counters in DWCtl attribute batch turnover to its trigger, which is how
 * a half-filled average (batch fsyncs paid for underfilled batches) is
 * told apart from healthy overflow sealing.
 */
typedef enum DWBSealReason
{
	DWB_SEAL_OVERFLOW,			/* a reservation ran past the last slot */
	DWB_SEAL_LONE,				/* solo-stream fast seal: a lone waiter, or
								 * every page when there is no worker pool */
	DWB_SEAL_WAIT_TIMEOUT,		/* a waiting writer hit dwb_batch_timeout_ms */
	DWB_SEAL_WORKER_TIMEOUT,	/* a retire worker force-sealed on age */
	DWB_SEAL_BIN,				/* a background bin flush sealed its batches */
	DWB_SEAL_FORCED,			/* explicit DWBForceSealOpenBatch */
} DWBSealReason;

#define DWB_SEAL_NREASONS		(DWB_SEAL_FORCED + 1)

#define DWB_DIR					"pg_dwb"
#define DWB_CONTROL_FILE		DWB_DIR "/control"

/*
 * On-disk format.
 *
 * pg_dwb/control        - geometry + durable generation, written atomically
 * pg_dwb/batch_NNNN     - meta region (header + slot metas, padded to
 *                         PG_IO_ALIGN_SIZE) followed by a contiguous
 *                         PG_IO_ALIGN_SIZE-aligned stream of BLCKSZ page
 *                         images
 *
 * Slot validity is locally verifiable: meta_crc rejects a torn meta write
 * (including any old/new field mix on slot reuse), image_crc rejects a torn
 * image; an apply-pass candidate must pass meta_crc + generation + image_crc.
 */
#define DWB_CONTROL_MAGIC		0x44574243	/* "DWBC" */
#define DWB_BATCH_MAGIC			0x44574242	/* "DWBB" */
#define DWB_VERSION				1
#define DWB_MIN_VERSION			1

typedef struct DWBControlFileData
{
	uint32		magic;
	uint32		version;
	uint32		min_version;
	uint32		num_batches;
	uint32		batch_pages;
	uint32		flags;			/* DWB_CONTROL_* */
	uint64		generation;		/* apply-pass horizon: bumped durably on every
								 * double_writes start before the ring opens.
								 * Monotonic within one ring incarnation; a
								 * geometry-change recreate restarts it at
								 * zero, which is safe because the wipe leaves
								 * no CRC-valid slot behind */
	pg_crc32c	crc;			/* CRC of all preceding fields */
} DWBControlFileData;

/*
 * DWBControlFileData.flags.  RING_CLEAN certifies that every data-file write
 * covered by an on-disk slot had been fsynced when the server shut down: it
 * is written at the end of a clean shutdown after the ring is fully retired,
 * and cleared by the next double_writes startup before the ring reopens.
 * While it is set, the ring holds no unapplied repairs, so the apply-pass
 * must be skipped (non-double_writes runs in between leave the generation
 * untouched, so old slots would otherwise still match it) and a start under
 * a different io_torn_pages_protection mode is legal.  Any new code path
 * that opens the ring for writes must clear the marker in the same control
 * write that bumps the generation — the marker-only apply decision is
 * sound only while set-marker implies untouched-since-retirement.
 *
 * The flags field occupies what was interior alignment padding in version-1
 * control files; those read back with flags == 0 (the padding was always
 * memset and CRC-covered), which is the safe "not clean" state, so filling
 * the hole needed no DWB_VERSION bump.
 */
#define DWB_CONTROL_RING_CLEAN	0x0001

typedef struct DWBBatchHeader
{
	uint32		magic;
	uint32		version;
	uint64		batch_id;		/* incarnation id, assigned at batch open,
								 * monotonic in open order within one server
								 * run (next_batch_id restarts at 1 with each
								 * start); the apply-pass dedup uses it as an
								 * LSN tie-breaker — see DWBApplyPass */
	uint32		n_slots;		/* capped_slots at seal time */
	pg_crc32c	crc;			/* CRC of all preceding fields */
} DWBBatchHeader;

typedef struct DWSlotMeta
{
	BufferTag	tag;
	XLogRecPtr	page_lsn;
	uint64		generation;		/* ring generation at write time */
	uint16		flags;
	pg_crc32c	image_crc;		/* CRC of the BLCKSZ page image */
	pg_crc32c	meta_crc;		/* CRC of all preceding fields */
} DWSlotMeta;

/* DWSlotMeta.flags */
#define DWB_SLOT_ABORTED		0x0001	/* writer died before publishing */

/*
 * The on-disk layout is pinned: any change to these sizes or offsets is an
 * on-disk format change and requires a DWB_VERSION bump.
 */
StaticAssertDecl(sizeof(DWBControlFileData) == 40,
				 "DWBControlFileData on-disk size changed");
StaticAssertDecl(offsetof(DWBControlFileData, crc) == 32,
				 "DWBControlFileData crc offset changed");
StaticAssertDecl(sizeof(DWBBatchHeader) == 24,
				 "DWBBatchHeader on-disk size changed");
StaticAssertDecl(offsetof(DWBBatchHeader, crc) == 20,
				 "DWBBatchHeader crc offset changed");
StaticAssertDecl(sizeof(DWSlotMeta) == 56,
				 "DWSlotMeta on-disk size changed");
StaticAssertDecl(offsetof(DWSlotMeta, meta_crc) == 48,
				 "DWSlotMeta meta_crc offset changed");

#define DWBMetaRegionSize(batch_pages) \
	TYPEALIGN(PG_IO_ALIGN_SIZE, \
			  sizeof(DWBBatchHeader) + (batch_pages) * sizeof(DWSlotMeta))
#define DWBBatchFileSize(batch_pages) \
	(DWBMetaRegionSize(batch_pages) + (Size) (batch_pages) * BLCKSZ)

/*
 * Batch lifecycle.  A slot is reused only via DWB_FREE.
 *
 * The numeric order of the happy-path states is semantic: the code uses
 * comparisons like "state < DWB_FSYNCED" as progress tests.  Insert new
 * states only in lifecycle order; DWB_OOM_RETIRING is a side fork of
 * DWB_RETIRING and must stay numerically last.
 */
typedef enum DWBatchState
{
	DWB_FREE = 0,
	DWB_ALLOCATED,				/* writers fill slots */
	DWB_SEALED,					/* no new writers; waiting for bitmap
								 * coverage, then the leader writes */
	DWB_WRITTEN,				/* images + meta written, fdatasync pending */
	DWB_FSYNCED,				/* batch durable; writers do smgrwrite */
	DWB_DATA_WRITTEN,			/* all smgrwrite + sync requests done */
	DWB_RETIRING,				/* waiting for fsync of seg_set segments */
	DWB_OOM_RETIRING,			/* publisher retires synchronously after a
								 * DWSegmentHash OOM (3.5) */
} DWBatchState;

StaticAssertDecl(DWB_FREE < DWB_ALLOCATED &&
				 DWB_ALLOCATED < DWB_SEALED &&
				 DWB_SEALED < DWB_WRITTEN &&
				 DWB_WRITTEN < DWB_FSYNCED &&
				 DWB_FSYNCED < DWB_DATA_WRITTEN &&
				 DWB_DATA_WRITTEN < DWB_RETIRING &&
				 DWB_RETIRING < DWB_OOM_RETIRING,
				 "DWBatchState numeric order is semantic (progress tests)");

typedef struct DWSegRef
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	uint32		segno;
} DWSegRef;

/*
 * DWSegRef is a HASH_BLOBS key: hashed and compared as raw bytes, so it must
 * not contain padding (which field-wise construction would leave undefined).
 */
StaticAssertDecl(sizeof(DWSegRef) ==
				 sizeof(RelFileLocator) + sizeof(ForkNumber) + sizeof(uint32),
				 "DWSegRef has padding; unsafe as a HASH_BLOBS key");

/*
 * Segment -> batch back-reference (3.5): one shmem hash entry per segment
 * that at least one RETIRING batch still needs fsynced.  The bitmap is
 * indexed by ring batch index; a bit is set exactly once per batch life, at
 * the DATA_WRITTEN -> RETIRING transition, and cleared by the fsyncer that
 * covered it.  Batch index reuse is disambiguated by snapshotting batch_id
 * before the fsync and re-checking it under the batch's publish_lock before
 * decrementing (the ABA guard of 3.5).
 *
 * The entry size depends on dwb_num_batches, so the bitmap is a flexible
 * array of DWBSegBitmapWords() words; hash lookups/inserts/removals are
 * serialized by DWBSegHashLock.  fsync_in_progress is a best-effort claim
 * that lets concurrent fsyncers skip a segment somebody is already syncing;
 * races on it are benign because the bit-clear + decrement is idempotent.
 */
typedef struct DWSegEntry
{
	DWSegRef	key;
	pg_atomic_uint32 fsync_in_progress;
	pg_atomic_uint64 batch_bitmap[FLEXIBLE_ARRAY_MEMBER];
} DWSegEntry;

#define DWBSegBitmapWords() (((uint32) dwb_num_batches + 63) / 64)

/*
 * Sliced reserves of FREE batches, by the free count F at open time: the
 * bottom slice [1 .. DWB_BG_RESERVE] may be opened only by the background
 * class, the middle slice (.. DWB_BG_RESERVE + DWB_EVICT_RESERVE] only by
 * the eviction class, anything above by both.  The middle slice keeps a
 * checkpoint's BufferSync storm from eating the ring from under user
 * evictions; the bottom slice keeps a crowd of evicting backends from
 * starving the checkpointer outright (each class needs just one open batch,
 * so a non-empty bottom slice is a progress guarantee for the background
 * stream).
 */
#define DWB_EVICT_RESERVE		Max(2, dwb_num_batches / 8)
#define DWB_BG_RESERVE			Max(1, dwb_num_batches / 32)

/*
 * next_slot_idx encoding: 30-bit index + writer-class bit + seal sentinel.
 *
 * The class bit records which writer class opened this incarnation of the
 * batch.  A reservation validates it atomically with the increment (CAS in
 * DWBAcquireSlot), so a stale per-class open pointer can never join a batch
 * that was freed and reopened under the other class: the ring reuses batch
 * indexes, and open_batch_idx[] of an idle class keeps naming its last batch
 * long after that batch was retired.
 */
#define DWB_SEAL_BIT			(1U << 31)
#define DWB_WCLASS_BIT			(1U << 30)
#define DWB_IDX_MASK			(DWB_WCLASS_BIT - 1)

/* one flag bit encodes the opening class: works for exactly two classes */
StaticAssertDecl(DWB_NUM_WCLASSES == 2,
				 "next_slot_idx has a single writer-class bit");

#define DWBWClassBit(wclass) \
	((wclass) == DWB_WCLASS_BACKGROUND ? DWB_WCLASS_BIT : 0)

typedef struct DWBatchCtl
{
	pg_atomic_uint32 state;		/* DWBatchState */
	pg_atomic_uint32 next_slot_idx; /* CAS-incremented while open, see
									 * DWBAcquireSlot() */
	pg_atomic_uint32 capped_slots;	/* fixed by SEAL; leader waits for exactly
									 * this many bitmap bits */
	pg_atomic_uint64 slots_written_bitmap[DWB_BITMAP_WORDS];
	pg_atomic_uint32 ref_count; /* writers holding the batch from slot
								 * reservation to smgrwrite done */
	pg_atomic_uint32 seg_pending_count; /* seg_set entries not yet fsynced;
										 * decremented once per covered
										 * segment, the decrement to zero
										 * frees the batch */
	LWLock		publish_lock;	/* protects n_segs/seg_set (dedup insert),
								 * serializes seg_set publication into
								 * DWSegmentHash and the batch_id-guarded
								 * seg_pending_count decrement (3.5) */
	ConditionVariable cv_state; /* broadcast on state change */
	uint32		n_segs;
	DWSegRef	seg_set[DWB_BATCH_MAX_SEGS];
	BufferTag	pages[DWB_BATCH_MAX_PAGES];
	XLogRecPtr	page_lsns[DWB_BATCH_MAX_PAGES];
	pg_crc32c	image_crcs[DWB_BATCH_MAX_PAGES];	/* computed by writers at
													 * publication */
	uint16		slot_flags[DWB_BATCH_MAX_PAGES];	/* same width as
													 * DWSlotMeta.flags */
	int			staging_idx;	/* staging buffer; held from ALLOCATED until
								 * the leader finishes the image pwrite */
	XLogRecPtr	max_page_lsn;
	uint64		batch_id;		/* monotonic incarnation id.  Written only at
								 * reopen, under DWBRingOpenLock; read under a
								 * held ref (which pins the incarnation) or
								 * racily by the retire side.  The ABA
								 * re-check in DWBSegSnapEnd reads it under
								 * publish_lock, but that lock does not
								 * serialize against the reopen write: safety
								 * comes from the idempotent bitmap re-check
								 * plus id monotonicity (3.5). */
	TimestampTz open_time;		/* FREE -> ALLOCATED instant; drives
								 * force-SEAL via dwb_batch_timeout_ms */
} DWBatchCtl;

typedef struct DWCtl
{
	pg_atomic_uint32 open_batch_idx[DWB_NUM_WCLASSES];	/* current ALLOCATED
														 * batch per writer
														 * class, or
														 * DWB_INVALID_BATCH */
	pg_atomic_uint64 next_batch_id;
	uint64		ring_generation;	/* = control.generation after the startup
									 * bump; constant until restart, stamped
									 * into DWSlotMeta by the leader */
	pg_atomic_uint64 freed_events;	/* monotonic count of batches that reached
									 * FREE; backpressure waiters treat a
									 * change as retire progress */
	pg_atomic_uint64 ring_wait_retries; /* monotonic count of DWBOpenNewBatch
										 * iterations that went to sleep; a
										 * parked waiter on an unchanged ring
										 * must accrue these at the sleep
										 * timeout pace, not spin (see the
										 * silent-probe-release rule in
										 * DWBStagingRelease) */
	/* diagnostic seal accounting: [writer class][DWBSealReason] */
	pg_atomic_uint64 seal_count[DWB_NUM_WCLASSES][DWB_SEAL_NREASONS];
	pg_atomic_uint64 seal_pages[DWB_NUM_WCLASSES][DWB_SEAL_NREASONS];

	/*
	 * TimestampTz of the class's last overflow seal: the "demand is hot"
	 * marker that suppresses the lone-writer fast seal (see
	 * DWBWaitBatchFsynced).  Advisory — read and written without barriers;
	 * a stale value mis-decides at most one seal in either direction.
	 */
	pg_atomic_uint64 last_overflow_seal[DWB_NUM_WCLASSES];
	ConditionVariable cv_want_batch[DWB_NUM_WCLASSES];	/* per-class "want a
														 * batch" queue: both
														 * staging and
														 * ring-space waiters
														 * sleep here; woken by
														 * targeted signals, not
														 * broadcast (3.6) */
	ConditionVariable cv_retire_wake;	/* wakes retire workers */
	slock_t		staging_lock;	/* protects staging_free bitmap */
	uint32		staging_free;	/* bitmap of free staging buffers */
	DWBatchCtl	batches[FLEXIBLE_ARRAY_MEMBER]; /* dwb_num_batches entries */
} DWCtl;

#define DWB_INVALID_BATCH		PG_UINT32_MAX

/* Writer-side handle for one reserved slot */
typedef struct DWBSlotRef
{
	int			batch_idx;
	int			slot_idx;
	uint64		batch_id;
} DWBSlotRef;

/*
 * Work queue between the bgwriter's LRU scan and the cleaner worker pool
 * (dwb_cleaner.c).  An entry is one flush bin: buffer ids the scan
 * classified as cold dirty candidates.  Entries are hints, not
 * obligations — every claim is reclassified under the buffer header lock
 * right before the write (FlushBufferBin's opportunistic mode), so a
 * stale entry is skipped, never wrongly written, and the queue needs no
 * draining on shutdown: whatever it held stays dirty and is covered by
 * the next checkpoint.
 *
 * In error-free operation every accepted page ends up counted as either
 * written or skipped, so enqueued_pages = pool_written_total +
 * skipped_pages once the queue is empty.  A worker error mid-bin
 * abandons the bin's remainder — those pages stay dirty and are simply
 * rescanned later, but they leave the counters short of the identity.
 */
typedef struct DWBCleanerBin
{
	int			nbuf;
	int			buf_ids[DWB_FLUSH_BIN_MAX];
} DWBCleanerBin;

typedef struct DWBCleanerCtl
{
	/* counters are monotonic except pool_written, which bgwriter drains */
	pg_atomic_uint64 enqueued_pages;	/* pages ever accepted into the queue */
	pg_atomic_uint64 pool_written;	/* pages written by cleaners since the
									 * bgwriter last folded them into
									 * buf_written_clean */
	pg_atomic_uint64 pool_written_total;	/* same, never reset (tests,
											 * diagnostics) */
	pg_atomic_uint64 skipped_pages; /* stale claims dropped by
									 * reclassification */
	pg_atomic_uint64 deferred_bins; /* bins refused by a full queue and
									 * carried over by the bgwriter */
	pg_atomic_uint64 pressure_naps; /* checkpointer naps taken only because
									 * the queue was hot */
	pg_atomic_uint32 depth;		/* lock-free mirror of nqueued for the
								 * checkpointer's advisory pressure check */
	ConditionVariable cv_work;	/* one targeted signal per enqueued bin */
	int			capacity;
	/* head/nqueued and the bins are protected by DWBCleanerQueueLock */
	int			head;
	int			nqueued;
	DWBCleanerBin bins[FLEXIBLE_ARRAY_MEMBER];	/* capacity entries */
} DWBCleanerCtl;

extern PGDLLIMPORT DWCtl *DWBCtl;
extern PGDLLIMPORT char *DWBStagingBase;
extern PGDLLIMPORT HTAB *DWSegmentHash;

/* dwb_ctl.c */
extern Size DWBShmemSize(void);
extern void DWBShmemInit(void);

/* dwb.c — write path */
extern void DWBStagePageWrite(const BufferTag *tag, const char *image,
							  XLogRecPtr page_lsn, DWBSlotRef *ref);
extern void DWBStagePageWriteNoWait(const BufferTag *tag, const char *image,
									XLogRecPtr page_lsn, DWBSlotRef *ref);
extern void DWBWaitStagedWrites(const DWBSlotRef *refs, int nrefs);
extern void DWBFinishPageWrite(const DWBSlotRef *ref);
extern bool DWBWritesPaused(void);
extern void DWBAcquireSlot(const BufferTag *tag, int wclass,
						   bool use_resowner, DWBSlotRef *ref);
extern void DWBPublishImage(const DWBSlotRef *ref, const char *image,
							XLogRecPtr page_lsn);
extern void DWBWaitBatchFsynced(const DWBSlotRef *ref);
extern void DWBReleaseSlot(const DWBSlotRef *ref);
extern bool DWBForceSealOpenBatch(int wclass);
extern bool DWBTrySealBatch(int batch_idx, DWBSealReason reason);
extern bool DWBClassIsHot(int wclass);
extern DWBatchState DWBGetBatchState(int batch_idx);

/* internal; exported for test_dwb's stale-open regression test */
extern void DWBOpenNewBatch(int wclass, uint32 old_idx);
extern void DWBWakeRingWaiters(void);

/* dwb_retire.c — segment hash, retirement, worker pool */
struct FileTag;					/* avoid dragging storage/sync.h in here */
extern void DWBPublishBatchSegSet(int batch_idx);
extern void DWBSegmentFsyncBegin(const struct FileTag *ftag);
extern int	DWBSegmentFsyncEnd(bool synced);
extern int	DWBRetireAllSync(void);
extern void DWBRetireWorkersRegister(void);
pg_noreturn extern void DWBRetireWorkerMain(Datum main_arg);

/* dwb_cleaner.c — bgwriter bin queue and the cleaner worker pool */
extern PGDLLIMPORT DWBCleanerCtl *DWBCleanerQueue;
extern PGDLLIMPORT bool DWBAmCleanerWorker;
extern Size DWBCleanerShmemSize(void);
extern void DWBCleanerShmemInit(void);
extern bool DWBCleanersActive(void);
extern bool DWBCleanerEnqueueBin(const int *buf_ids, int nbuf);
extern uint64 DWBCleanerFetchPoolWritten(void);
extern void DWBCleanerCountDeferral(void);
extern bool DWBCleanerQueueHot(void);
extern void DWBCleanerCountPressureNap(void);
extern void DWBCleanerWorkersRegister(void);
pg_noreturn extern void DWBCleanerWorkerMain(Datum main_arg);

/* dwb_file.c */
extern void DWBCreateRing(void);
extern void DWBBatchFilePath(char *path, int batch_idx);
extern bool DWBReadControlFile(DWBControlFileData *control, bool missing_ok,
							   bool *corruptp);
extern void DWBWriteControlFile(const DWBControlFileData *control);
extern int	DWBOpenBatchFile(int batch_idx);
extern void DWBPrepareBatchWrite(int batch_idx);
extern void DWBReadSlotImage(int batch_idx, int slot_idx, char *dst);
extern void DWBWriteBatch(int batch_idx, const DWBBatchHeader *hdr,
						  const DWSlotMeta *metas, const char *images);
extern pg_crc32c DWBImageCrc(const char *image);
extern pg_crc32c DWBSlotMetaCrc(const DWSlotMeta *meta);
extern pg_crc32c DWBControlCrc(const DWBControlFileData *control);
extern pg_crc32c DWBBatchHeaderCrc(const DWBBatchHeader *hdr);

/* dwb_recovery.c */
extern XLogRecPtr DWBStartup(bool restoring_backup);
extern void DWBMarkCleanShutdown(void);

#endif							/* DWB_H */
