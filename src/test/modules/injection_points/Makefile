# src/test/modules/injection_points/Makefile

MODULE_big = injection_points
OBJS = \
	$(WIN32RES) \
	injection_points.o \
	regress_injection.o
EXTENSION = injection_points
DATA = injection_points--1.0.sql
PGFILEDESC = "injection_points - facility for injection points"

REGRESS = injection_points hashagg reindex_conc vacuum
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress

ISOLATION = basic \
	    csn_commit_fallback \
	    csn_snapshot_reuse_fallback \
	    csn_snapshot_completion_count_shadow \
	    csn_snapshot_xmax_latest_completed \
	    csn_commit_stable_reads \
	    csn_ordinary_new_tx_state \
	    csn_ordinary_after_procarray_primary \
	    csn_ordinary_after_vxid_clear \
	    csn_ordinary_lock_contention \
	    csn_ordinary_cic_wait \
	    csn_ordinary_delay_chkpt_vxid \
	    csn_ordinary_exit_count \
	    csn_ordinary_completion_vars \
	    csn_ordinary_in_commit_oldest_xid \
	    csn_ordinary_legacy_exit \
	    csn_ordinary_lock_count \
	    csn_ordinary_oldest_active_xid \
	    csn_ordinary_horizons \
	    csn_ordinary_reuse_begin \
	    csn_ordinary_reuse_slot_epoch \
	    csn_ordinary_reuse_completion_count \
	    csn_ordinary_running_xacts \
	    csn_ordinary_snapshot_xmin_state \
	    csn_ordinary_xid_in_progress \
	    csn_ordinary_vxid_lifecycle \
	    csn_commit_published \
	    inplace \
	    repack \
	    repack_toast \
	    syscache-update-pruned \
	    heap_lock_update

# some isolation tests require wal_level=replica
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/injection_points/extra.conf

# The injection points are cluster-wide, so disable installcheck
NO_INSTALLCHECK = 1

export enable_injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/injection_points
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# XXX: This test is conditional on enable_injection_points in the
# parent Makefile, so we should never get here in the first place if
# injection points are not enabled. But the buildfarm 'misc-check'
# step doesn't pay attention to the if-condition in the parent
# Makefile. To work around that, disable running the test here too.
ifeq ($(enable_injection_points),yes)
include $(top_srcdir)/contrib/contrib-global.mk
else
check:
	@echo "injection points are disabled in this build"
endif

endif
