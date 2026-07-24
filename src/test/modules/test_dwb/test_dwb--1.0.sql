/* src/test/modules/test_dwb/test_dwb--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_dwb" to load this file. \quit

CREATE FUNCTION test_dwb_cycle(npages int)
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_stress(loops int, npages int)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_ring_slots(current_only bool)
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_ring_rel_slots(relnumber oid)
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_states()
	RETURNS text STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_leak(npages int, do_publish bool)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_force_seal(background bool DEFAULT false)
	RETURNS bool STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_retire()
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_open_stale()
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_fill_ring(background bool DEFAULT false)
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_abort_release(npages int, do_publish bool)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_abort_after_fsync()
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_torn_repair(relnumber oid, blkno int)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_checkpoint_pending(relnumber oid)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_fill_segments(nbatches int)
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;
