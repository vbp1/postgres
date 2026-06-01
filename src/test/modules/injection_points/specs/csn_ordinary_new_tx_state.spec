# Stage 3 H1-B characterization: a new top-level transaction publishes a new
# virtual xid before it has installed xmin for its first snapshot.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_new_tx_state_data (id int PRIMARY KEY, val int);
	CREATE TABLE csn_ordinary_new_tx_state_meta (
		label text PRIMARY KEY,
		value text NOT NULL
	);
	INSERT INTO csn_ordinary_new_tx_state_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_new_tx_state_meta;
	DROP TABLE csn_ordinary_new_tx_state_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_new_tx_state_meta;
}

session writer
step w_prepare
{
	SELECT injection_points_set_local();
	INSERT INTO csn_ordinary_new_tx_state_meta VALUES ('pid', pg_backend_pid()::text);
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
}
step w_begin		{ BEGIN; }
step w_rollback		{ ROLLBACK; }

session observer
step o_after_vxid
{
	SELECT
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT value::int
				FROM csn_ordinary_new_tx_state_meta
				WHERE label = 'pid'
			  )
		) AS writer_vxid_visible,
		(
			SELECT backend_xmin IS NULL
			FROM pg_stat_activity
			WHERE pid = (
				SELECT value::int
				FROM csn_ordinary_new_tx_state_meta
				WHERE label = 'pid'
			)
		) AS backend_xmin_is_null;
}

session ctl
step wake_start		{ SELECT injection_points_wakeup('start-after-vxid-publication'); }
step detach_start	{ SELECT injection_points_detach('start-after-vxid-publication'); }

permutation reset w_prepare w_begin o_after_vxid wake_start(w_begin) detach_start w_rollback
