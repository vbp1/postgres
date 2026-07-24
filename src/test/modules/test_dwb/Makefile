# src/test/modules/test_dwb/Makefile

MODULE_big = test_dwb
OBJS = \
	$(WIN32RES) \
	test_dwb.o
PGFILEDESC = "test_dwb - test module for the short-lived double write buffer"

TAP_TESTS = 1

EXTENSION = test_dwb
DATA = test_dwb--1.0.sql

# 003_backpressure.pl uses the injection_points extension
EXTRA_INSTALL = src/test/modules/injection_points
export enable_injection_points

REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_dwb/test_dwb.conf
REGRESS = test_dwb
# Disabled because these tests require io_torn_pages_protection=double_writes,
# which typical installcheck users do not have.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_dwb
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
