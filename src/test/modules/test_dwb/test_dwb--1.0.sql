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

CREATE FUNCTION test_dwb_states()
	RETURNS text STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_leak(npages int, do_publish bool)
	RETURNS void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_force_seal()
	RETURNS bool STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_dwb_retire()
	RETURNS int STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;
