CREATE EXTENSION injection_points;

SELECT injection_points_latest_completed_xid_shadow() =
       injection_points_latest_completed_xid();
SELECT injection_points_xact_completion_count_shadow() =
       injection_points_xact_completion_count();

\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX
\set regresslib :libdir '/regress' :dlsuffix

CREATE FUNCTION wait_pid(int)
  RETURNS void
  AS :'regresslib'
  LANGUAGE C STRICT;

-- Non-strict checks
SELECT injection_points_run(NULL);
SELECT injection_points_cached(NULL);

SELECT injection_points_attach('TestInjectionBooh', 'booh');
SELECT injection_points_attach('TestInjectionError', 'error');
SELECT injection_points_attach('TestInjectionLog', 'notice');
SELECT injection_points_attach('TestInjectionLog2', 'notice');

SELECT point_name, library, function FROM injection_points_list()
  ORDER BY point_name COLLATE "C";

SELECT injection_points_run('TestInjectionBooh'); -- nothing
SELECT injection_points_run('TestInjectionLog2'); -- notice
SELECT injection_points_run('TestInjectionLog2', NULL); -- notice
SELECT injection_points_run('TestInjectionLog2', 'foobar'); -- notice + arg
SELECT injection_points_run('TestInjectionLog'); -- notice
SELECT injection_points_run('TestInjectionError'); -- error
SELECT injection_points_run('TestInjectionError', NULL); -- error
SELECT injection_points_run('TestInjectionError', 'foobar2'); -- error + arg

-- Re-load cache and run again.
\c
SELECT injection_points_run('TestInjectionLog2'); -- notice
SELECT injection_points_run('TestInjectionLog'); -- notice
SELECT injection_points_run('TestInjectionError'); -- error

-- Remove one entry and check the remaining entries.
SELECT injection_points_detach('TestInjectionError'); -- ok
SELECT injection_points_run('TestInjectionLog'); -- notice
SELECT injection_points_run('TestInjectionError'); -- nothing
-- More entries removed, letting TestInjectionLog2 to check the same
-- callback used in more than one point.
SELECT injection_points_detach('TestInjectionLog'); -- ok
SELECT injection_points_run('TestInjectionLog'); -- nothing
SELECT injection_points_run('TestInjectionError'); -- nothing
SELECT injection_points_run('TestInjectionLog2'); -- notice

SELECT injection_points_detach('TestInjectionLog'); -- fails

SELECT injection_points_run('TestInjectionLog2'); -- notice
SELECT injection_points_detach('TestInjectionLog2');

-- Count action
SELECT injection_points_attach('TestInjectionCount', 'count');
SELECT injection_points_get_count('TestInjectionCount');
SELECT injection_points_run('TestInjectionCount');
SELECT injection_points_get_count('TestInjectionCount');
SELECT injection_points_run('TestInjectionCount', 'ignored');
SELECT injection_points_get_count('TestInjectionCount');
SELECT injection_points_reset_count('TestInjectionCount');
SELECT injection_points_get_count('TestInjectionCount');
SELECT injection_points_detach('TestInjectionCount');

-- Shared int8 coordination
SELECT injection_points_get_global_int8('TestGlobalInt8') IS NULL;
SELECT injection_points_set_global_int8('TestGlobalInt8', 42);
SELECT injection_points_get_global_int8('TestGlobalInt8');

BEGIN;
SELECT pg_current_xact_id();
SELECT injection_points_backend_xid(pg_backend_pid()) IS NOT NULL;
SELECT injection_points_backend_slot_epoch(pg_backend_pid()) > 0;
SELECT injection_points_xid_in_progress(pg_current_xact_id());
SELECT injection_points_oldest_active_xid(false, false) IS NOT NULL;
SELECT injection_points_csn_oldest_active_xid() IS NOT NULL;
SELECT injection_points_oldest_considered_running_xid() IS NOT NULL;
SELECT injection_points_oldest_nonremovable_xid() IS NOT NULL;
SELECT injection_points_latest_completed_xid() IS NOT NULL;
SELECT injection_points_latest_completed_xid_shadow() =
       injection_points_latest_completed_xid();
SELECT injection_points_xact_completion_count() > 0;
SELECT injection_points_xact_completion_count_shadow() > 0;
SELECT injection_points_transaction_snapshot_xact_completion_count() > 0;
SELECT injection_points_save_int8(injection_points_xact_completion_count());
SELECT injection_points_get_saved_int8() > 0;
SELECT injection_points_save_xid8(injection_points_latest_completed_xid());
SELECT injection_points_get_saved_xid8() IS NOT NULL;
SELECT injection_points_running_xacts_include_backend(pg_backend_pid(), true);
SELECT injection_points_running_xacts_latest_completed_xid(true) =
       injection_points_latest_completed_xid_shadow();
ROLLBACK;

-- Loading
SELECT injection_points_cached('TestInjectionLogLoad'); -- nothing in cache
SELECT injection_points_load('TestInjectionLogLoad'); -- nothing
SELECT injection_points_attach('TestInjectionLogLoad', 'notice');
SELECT injection_points_load('TestInjectionLogLoad'); -- nothing happens
SELECT injection_points_cached('TestInjectionLogLoad'); -- runs from cache
SELECT injection_points_cached('TestInjectionLogLoad', NULL); -- runs from cache
SELECT injection_points_cached('TestInjectionLogLoad', 'foobar'); -- runs from cache
SELECT injection_points_run('TestInjectionLogLoad'); -- runs from cache
SELECT injection_points_detach('TestInjectionLogLoad');

-- Runtime conditions
SELECT injection_points_attach('TestConditionError', 'error');
-- Any follow-up injection point attached will be local to this process.
SELECT injection_points_set_local();
SELECT injection_points_attach('TestConditionLocal1', 'error');
SELECT injection_points_attach('TestConditionLocal2', 'notice');
SELECT injection_points_run('TestConditionLocal1'); -- error
SELECT injection_points_run('TestConditionLocal2'); -- notice

SELECT pg_backend_pid() AS oldpid \gset

-- reload, local injection points should be gone.
\c
-- Wait for the previous backend process to exit, ensuring that its local
-- injection points are cleaned up.
SELECT wait_pid(:'oldpid');
SELECT injection_points_run('TestConditionLocal1'); -- nothing
SELECT injection_points_run('TestConditionLocal2'); -- nothing
SELECT injection_points_run('TestConditionError'); -- error
SELECT injection_points_detach('TestConditionError');
-- Attaching injection points that use the same name as one defined locally
-- previously should work.
SELECT injection_points_attach('TestConditionLocal1', 'error');
SELECT injection_points_detach('TestConditionLocal1');

-- Function variant for attach.
SELECT injection_points_attach(repeat('a', 64), 'injection_points',
  'injection_notice', NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', repeat('a', 128),
  'injection_notice', NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', 'injection_points',
  repeat('a', 128), NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', 'injection_points',
  'injection_notice', repeat('a', 1025)::bytea);
SELECT injection_points_attach(NULL, NULL, NULL, NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', NULL, NULL, NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', 'injection_points',
  NULL, NULL);
SELECT injection_points_attach('TestInjectionNoticeFunc', 'injection_points',
  'injection_notice', NULL);
SELECT point_name, library, function FROM injection_points_list()
  ORDER BY point_name COLLATE "C";
SELECT injection_points_run('TestInjectionNoticeFunc', NULL); -- notice
SELECT injection_points_detach('TestInjectionNoticeFunc');
SELECT point_name, library, function FROM injection_points_list()
  ORDER BY point_name COLLATE "C";

DROP EXTENSION injection_points;
DROP FUNCTION wait_pid;
