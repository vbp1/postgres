/*-------------------------------------------------------------------------
 *
 * dwb_recovery.c
 *	  Startup-time handling of the short-lived double write buffer ring.
 *
 * On every start (clean, unclean or cold) the durable generation in
 * pg_dwb/control is bumped BEFORE the ring opens for new writes, so slots
 * left behind by the previous run can never masquerade as current after a
 * future crash.  Order: read G -> (unclean start, Stage 4) apply-pass over
 * generation G + fsync -> durable control.generation := G+1 -> open ring.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/dwb/dwb_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "storage/dwb.h"

/*
 * Called from StartupXLOG before WAL replay.  Creates or validates the
 * ring, enforces data checksums, performs the durable generation bump and
 * publishes ring_generation for the leaders' slot metas.
 */
void
DWBStartup(void)
{
	DWBControlFileData control;

	if (!DWBIsEnabled())
		return;

	/* 3.1.7: a torn page with an intact header must never pass unnoticed */
	if (!DataChecksumsEnabled())
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("io_torn_pages_protection = \"double_writes\" requires data checksums"),
				 errhint("Enable checksums with initdb -k or pg_checksums.")));

	if (!DWBReadControlFile(&control, true))
	{
		/* cold start: no ring yet */
		DWBCreateRing();
		if (!DWBReadControlFile(&control, false))
			pg_unreachable();
	}
	else if (control.num_batches != (uint32) dwb_num_batches ||
			 control.batch_pages != (uint32) dwb_batch_pages)
	{
		/*
		 * Geometry GUCs define the on-disk layout.  Re-creating the ring
		 * under a changed geometry must not skip the apply-pass over the
		 * old ring, so it is deferred to Stage 4; until then, refuse.
		 */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" was created with dwb_num_batches = %u and dwb_batch_pages = %u",
						DWB_DIR, control.num_batches, control.batch_pages),
				 errhint("Restore the previous settings.")));
	}

	/*
	 * Stage 4: on an unclean start the apply-pass over generation
	 * control.generation runs here, before the bump.
	 */

	control.generation++;
	control.crc = DWBControlCrc(&control);
	DWBWriteControlFile(&control);

	DWBCtl->ring_generation = control.generation;

	ereport(LOG,
			(errmsg("double write buffer ring opened: %d batches of %d pages, generation " UINT64_FORMAT,
					dwb_num_batches, dwb_batch_pages, control.generation)));
}
