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
#include "port/pg_crc32c.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"
#include "storage/s_lock.h"
#include "utils/timestamp.h"

/* GUC: io_torn_pages_protection */
typedef enum
{
	DWB_PROTECT_OFF,
	DWB_PROTECT_FULL_PAGES,
	DWB_PROTECT_DOUBLE_WRITES,
} DWBTornPageProtection;

/* GUC: dwb_on_stall (Stage B backpressure behaviour) */
typedef enum
{
	DWB_ON_STALL_WARN,
	DWB_ON_STALL_ERROR,
	DWB_ON_STALL_PANIC,
} DWBOnStall;

/* GUC variables (defined in dwb_ctl.c) */
extern PGDLLIMPORT int io_torn_pages_protection;
extern PGDLLIMPORT int dwb_num_batches;
extern PGDLLIMPORT int dwb_batch_pages;
extern PGDLLIMPORT int dwb_max_segments;
extern PGDLLIMPORT int dwb_retire_workers;
extern PGDLLIMPORT int dwb_batch_timeout_ms;
extern PGDLLIMPORT int dwb_retire_interval_ms;
extern PGDLLIMPORT bool dwb_writeback;
extern PGDLLIMPORT int dwb_slow_warn_ms;
extern PGDLLIMPORT int dwb_slot_stuck_timeout_ms;
extern PGDLLIMPORT int dwb_write_timeout_ms;
extern PGDLLIMPORT int dwb_on_stall;

#define DWBIsEnabled() (io_torn_pages_protection == DWB_PROTECT_DOUBLE_WRITES)

/*
 * Compile-time capacity limits (GUC maxima).
 */
#define DWB_BATCH_MAX_PAGES		256
#define DWB_BATCH_MAX_SEGS		DWB_BATCH_MAX_PAGES
#define DWB_BITMAP_WORDS		(DWB_BATCH_MAX_PAGES / 64)
/* staging pool: 2 writer classes + 2 in-flight leader writes */
#define DWB_STAGING_BUFFERS		4
/* writer classes (3.6); Stage 1 uses only DWB_WCLASS_EVICTION */
#define DWB_NUM_WCLASSES		2
#define DWB_WCLASS_EVICTION		0
#define DWB_WCLASS_BACKGROUND	1

#define DWB_DIR					"pg_dwb"
#define DWB_CONTROL_FILE		DWB_DIR "/control"

/*
 * On-disk format.
 *
 * pg_dwb/control        - geometry + durable generation, written atomically
 * pg_dwb/batch_NNNN     - meta region (header + slot metas, padded to
 *                         PG_IO_ALIGN_SIZE) followed by a contiguous
 *                         BLCKSZ-aligned page-image stream
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
	uint64		generation;		/* apply-pass horizon: bumped durably on
								 * every start before the ring opens */
	pg_crc32c	crc;			/* CRC of all preceding fields */
} DWBControlFileData;

typedef struct DWBBatchHeader
{
	uint32		magic;
	uint32		version;
	uint64		batch_id;
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

#define DWBMetaRegionSize(batch_pages) \
	TYPEALIGN(PG_IO_ALIGN_SIZE, \
			  sizeof(DWBBatchHeader) + (batch_pages) * sizeof(DWSlotMeta))
#define DWBBatchFileSize(batch_pages) \
	(DWBMetaRegionSize(batch_pages) + (Size) (batch_pages) * BLCKSZ)

/*
 * Batch lifecycle.  A slot is reused only via DWB_FREE.
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
	DWB_OOM_RETIRING,			/* publisher retires synchronously (3.5) */
} DWBatchState;

typedef struct DWSegRef
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	uint32		segno;
} DWSegRef;

/*
 * next_slot_idx encoding: 31-bit index + seal sentinel bit.
 */
#define DWB_SEAL_BIT			(1U << 31)
#define DWB_IDX_MASK			(DWB_SEAL_BIT - 1)

typedef struct DWBatchCtl
{
	pg_atomic_uint32 state;		/* DWBatchState */
	pg_atomic_uint32 next_slot_idx; /* fetch_add on ALLOCATED */
	pg_atomic_uint32 capped_slots;	/* fixed by SEAL; leader waits for
									 * exactly this many bitmap bits */
	pg_atomic_uint64 slots_written_bitmap[DWB_BITMAP_WORDS];
	pg_atomic_uint32 ref_count; /* writers holding the batch from slot
								 * reservation to smgrwrite done */
	pg_atomic_uint32 seg_pending_count; /* seg_set entries not yet fsynced */
	pg_atomic_uint32 orphaned_refs_count;	/* refs whose writer aborted after
											 * publishing the copy but before
											 * smgrwrite; a retire worker
											 * finishes their writes */
	LWLock		publish_lock;	/* serializes seg_set publication and
								 * seg_pending_count decrement (3.5) */
	ConditionVariable cv_state; /* broadcast on state change */
	slock_t		seg_lock;		/* protects n_segs/seg_set dedup insert */
	uint32		n_segs;
	DWSegRef	seg_set[DWB_BATCH_MAX_SEGS];
	BufferTag	pages[DWB_BATCH_MAX_PAGES];
	XLogRecPtr	page_lsns[DWB_BATCH_MAX_PAGES];
	pg_crc32c	image_crcs[DWB_BATCH_MAX_PAGES];	/* computed by writers at
													 * publication */
	uint8		slot_flags[DWB_BATCH_MAX_PAGES];
	int			staging_idx;	/* staging buffer; held from ALLOCATED until
								 * the leader finishes the image pwrite */
	BufferTag	orphan_tags[DWB_BATCH_MAX_PAGES];
	XLogRecPtr	max_page_lsn;
	uint64		batch_id;		/* monotonic, for ordering */
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
	ConditionVariable cv_free_batch;	/* broadcast on retire */
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

extern PGDLLIMPORT DWCtl *DWBCtl;
extern PGDLLIMPORT char *DWBStagingBase;

/* dwb_ctl.c */
extern Size DWBShmemSize(void);
extern void DWBShmemInit(void);

/* dwb.c — write path (Stage 1: driven by tests, not FlushBuffer yet) */
extern void DWBAcquireSlot(const BufferTag *tag, DWBSlotRef *ref);
extern void DWBPublishImage(const DWBSlotRef *ref, const char *image,
							XLogRecPtr page_lsn);
extern void DWBWaitBatchFsynced(const DWBSlotRef *ref);
extern void DWBReleaseSlot(const DWBSlotRef *ref);
extern bool DWBForceSealOpenBatch(int wclass);
extern int	DWBRetireAllSync(void);
extern DWBatchState DWBGetBatchState(int batch_idx);

/* dwb_file.c */
extern void DWBCreateRing(void);
extern bool DWBReadControlFile(DWBControlFileData *control, bool missing_ok);
extern void DWBWriteControlFile(const DWBControlFileData *control);
extern int	DWBOpenBatchFile(int batch_idx);
extern void DWBCloseBatchFiles(void);
extern void DWBWriteBatch(int batch_idx, const DWBBatchHeader *hdr,
						  const DWSlotMeta *metas, const char *images);
extern pg_crc32c DWBImageCrc(const char *image);
extern pg_crc32c DWBSlotMetaCrc(const DWSlotMeta *meta);
extern pg_crc32c DWBControlCrc(const DWBControlFileData *control);
extern pg_crc32c DWBBatchHeaderCrc(const DWBBatchHeader *hdr);

/* dwb_recovery.c */
extern void DWBStartup(void);

#endif							/* DWB_H */
