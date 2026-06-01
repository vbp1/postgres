# Stage 3 H1-B characterization: once a repeatable-read transaction installs
# its first snapshot xmin, the backend keeps both its virtual xid and xmin
# visible to concurrent observers, and the snapshot-install hook fires.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_snapshot_xmin_state_data (id int PRIMARY KEY, val int);
	CREATE TABLE csn_ordinary_snapshot_xmin_state_meta (
		label text PRIMARY KEY,
		value text NOT NULL
	);
	INSERT INTO csn_ordinary_snapshot_xmin_state_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_snapshot_xmin_state_meta;
	DROP TABLE csn_ordinary_snapshot_xmin_state_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_snapshot_xmin_state_meta;
}

session writer
step w_prepare
{
	SELECT injection_points_set_local();
	INSERT INTO csn_ordinary_snapshot_xmin_state_meta VALUES ('pid', pg_backend_pid()::text);
	SELECT injection_points_attach('snapshot-after-install-xmin', 'count');
	SELECT injection_points_get_count('snapshot-after-install-xmin');
}
step w_begin		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step w_take_snapshot
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_ordinary_snapshot_xmin_state_data
	WHERE id = 1;
}
step w_count
{
	SELECT injection_points_get_count('snapshot-after-install-xmin') > 0
		AS snapshot_hook_fired;
}
step w_rollback		{ ROLLBACK; }

session observer
step o_after_xmin
{
	SELECT
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT value::int
				FROM csn_ordinary_snapshot_xmin_state_meta
				WHERE label = 'pid'
			  )
		) AS writer_vxid_visible,
		(
			SELECT backend_xmin IS NOT NULL
			FROM pg_stat_activity
			WHERE pid = (
				SELECT value::int
				FROM csn_ordinary_snapshot_xmin_state_meta
				WHERE label = 'pid'
			)
		) AS backend_xmin_is_set;
}

session ctl
step detach_snapshot	{ SELECT injection_points_detach('snapshot-after-install-xmin'); }

permutation reset w_prepare w_begin w_take_snapshot o_after_xmin w_count detach_snapshot w_rollback
