/* src/test/modules/injection_points/injection_points--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION injection_points" to load this file. \quit

--
-- injection_points_attach()
--
-- Attaches the action to the given injection point.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN action text)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_attach()
--
-- Attaches a function to the given injection point, with library name,
-- function name and private data.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN library_name TEXT, IN function_name TEXT, IN private_data BYTEA)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach_func'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_load()
--
-- Load an injection point already attached.
--
CREATE FUNCTION injection_points_load(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_load'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_run()
--
-- Executes the action attached to the injection point.
--
CREATE FUNCTION injection_points_run(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_run'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_cached()
--
-- Executes the action attached to the injection point, from local cache.
--
CREATE FUNCTION injection_points_cached(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_cached'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_wakeup()
--
-- Wakes up a waiting injection point.
--
CREATE FUNCTION injection_points_wakeup(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_wakeup'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_get_count()
--
-- Reads the hit counter for a count-action injection point.
--
CREATE FUNCTION injection_points_get_count(IN point_name TEXT)
RETURNS int4
AS 'MODULE_PATHNAME', 'injection_points_get_count'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_reset_count()
--
-- Resets the hit counter for a count-action injection point.
--
CREATE FUNCTION injection_points_reset_count(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_reset_count'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_set_global_int8()
--
-- Stores a shared int8 value for cross-backend test coordination.
--
CREATE FUNCTION injection_points_set_global_int8(IN slot_name TEXT, IN value int8)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_set_global_int8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_get_global_int8()
--
-- Reads a shared int8 value stored by injection_points_set_global_int8().
--
CREATE FUNCTION injection_points_get_global_int8(IN slot_name TEXT)
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_get_global_int8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_xid_in_progress()
--
-- Exposes TransactionIdIsInProgress() to SQL tests.
--
CREATE FUNCTION injection_points_xid_in_progress(IN fxid xid8)
RETURNS bool
AS 'MODULE_PATHNAME', 'injection_points_xid_in_progress'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_backend_xid()
--
-- Reads the current top-level xid of the backend with the given PID.
--
CREATE FUNCTION injection_points_backend_xid(IN backend_pid int4)
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_backend_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_backend_slot_epoch()
--
-- Reads the current slot epoch of the backend with the given PID.
--
CREATE FUNCTION injection_points_backend_slot_epoch(IN backend_pid int4)
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_backend_slot_epoch'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_oldest_active_xid()
--
-- Exposes GetOldestActiveTransactionId() to SQL tests.
--
CREATE FUNCTION injection_points_oldest_active_xid(
    IN in_commit_only bool,
    IN all_dbs bool)
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_oldest_active_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_csn_oldest_active_xid()
--
-- Exposes ReadCSNOldestActiveXid() to SQL tests.
--
CREATE FUNCTION injection_points_csn_oldest_active_xid()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_csn_oldest_active_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_oldest_considered_running_xid()
--
-- Exposes GetOldestTransactionIdConsideredRunning() to SQL tests.
--
CREATE FUNCTION injection_points_oldest_considered_running_xid()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_oldest_considered_running_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_oldest_nonremovable_xid()
--
-- Exposes GetOldestNonRemovableTransactionId(NULL) to SQL tests.
--
CREATE FUNCTION injection_points_oldest_nonremovable_xid()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_oldest_nonremovable_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_latest_completed_xid()
--
-- Exposes TransamVariables->latestCompletedXid to SQL tests.
--
CREATE FUNCTION injection_points_latest_completed_xid()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_latest_completed_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_latest_completed_xid_shadow()
--
-- Exposes the passive H1-D latestCompletedXid shadow to SQL tests.
--
CREATE FUNCTION injection_points_latest_completed_xid_shadow()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_latest_completed_xid_shadow'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_xact_completion_count()
--
-- Exposes TransamVariables->xactCompletionCount to SQL tests.
--
CREATE FUNCTION injection_points_xact_completion_count()
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_xact_completion_count'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_xact_completion_count_shadow()
--
-- Exposes the passive H1-D xactCompletionCount shadow to SQL tests.
--
CREATE FUNCTION injection_points_xact_completion_count_shadow()
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_xact_completion_count_shadow'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_active_snapshot_xact_completion_count()
--
-- Exposes the active query snapshot's snapXactCompletionCount to SQL tests.
--
CREATE FUNCTION injection_points_active_snapshot_xact_completion_count()
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_active_snapshot_xact_completion_count'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_transaction_snapshot_xact_completion_count()
--
-- Exposes GetTransactionSnapshot()->snapXactCompletionCount to SQL tests.
--
CREATE FUNCTION injection_points_transaction_snapshot_xact_completion_count()
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_transaction_snapshot_xact_completion_count'
LANGUAGE C PARALLEL UNSAFE;

-- injection_points_save_int8()
--
-- Stores a backend-local int8 value for later read-only checks.
--
CREATE FUNCTION injection_points_save_int8(IN value int8)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_save_int8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_get_saved_int8()
--
-- Reads a backend-local int8 value stored by injection_points_save_int8().
--
CREATE FUNCTION injection_points_get_saved_int8()
RETURNS int8
AS 'MODULE_PATHNAME', 'injection_points_get_saved_int8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_save_xid8()
--
-- Stores a backend-local xid8 value for later read-only checks.
--
CREATE FUNCTION injection_points_save_xid8(IN value xid8)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_save_xid8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_get_saved_xid8()
--
-- Reads a backend-local xid8 value stored by injection_points_save_xid8().
--
CREATE FUNCTION injection_points_get_saved_xid8()
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_get_saved_xid8'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_backend_delays_checkpoint()
--
-- Checks whether a backend appears in GetVirtualXIDsDelayingChkpt().
--
CREATE FUNCTION injection_points_backend_delays_checkpoint(
    IN backend_pid int4,
    IN delay_type int4)
RETURNS bool
AS 'MODULE_PATHNAME', 'injection_points_backend_delays_checkpoint'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_running_xacts_include_backend()
--
-- Checks whether a backend's xid is present in GetRunningTransactionData().
--
CREATE FUNCTION injection_points_running_xacts_include_backend(
    IN backend_pid int4,
    IN current_db_only bool)
RETURNS bool
AS 'MODULE_PATHNAME', 'injection_points_running_xacts_include_backend'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_running_xacts_latest_completed_xid()
--
-- Exposes GetRunningTransactionData()->latestCompletedXid to SQL tests.
--
CREATE FUNCTION injection_points_running_xacts_latest_completed_xid(
    IN current_db_only bool)
RETURNS xid8
AS 'MODULE_PATHNAME', 'injection_points_running_xacts_latest_completed_xid'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_set_local()
--
-- Trigger switch to link any future injection points attached to the
-- current process, useful to make SQL tests concurrently-safe.
--
CREATE FUNCTION injection_points_set_local()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_set_local'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_detach()
--
-- Detaches the current action, if any, from the given injection point.
--
CREATE FUNCTION injection_points_detach(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_detach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_list()
--
-- List of all the injection points currently attached.
--
CREATE FUNCTION injection_points_list(OUT point_name text,
   OUT library text,
   OUT function text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'injection_points_list'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

--
-- regress_injection.c functions
--
CREATE FUNCTION removable_cutoff(rel regclass)
RETURNS xid8
AS 'MODULE_PATHNAME'
LANGUAGE C CALLED ON NULL INPUT;
