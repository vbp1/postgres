# Stage 3 H1-D characterization: while a writer is suspended in
# DELAY_CHKPT_IN_COMMIT and snapshots are forced onto the legacy non-CSN path,
# a second Read Committed query can still reuse the fallback snapshot payload.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_snapshot_reuse_fallback (id int PRIMARY KEY, val int);
	INSERT INTO csn_snapshot_reuse_fallback VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_snapshot_reuse_fallback;
	DROP EXTENSION injection_points;
}

session writer
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
}
step w_begin	{ BEGIN; }
step w_update	{ UPDATE csn_snapshot_reuse_fallback SET val = 1 WHERE id = 1; }
step w_commit	{ COMMIT; }
step w_noop		{ }

session rc
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('snapshot-reuse-success', 'count');
}
step rc_count_before
{
	SELECT injection_points_get_count('snapshot-reuse-success') AS reuse_count_before;
}
step rc_first
{
	WITH q AS (
		SELECT pg_current_snapshot_uses_csn() AS uses_csn,
			injection_points_get_count('snapshot-reuse-success') AS reuse_count,
			val
		FROM csn_snapshot_reuse_fallback
		WHERE id = 1
	), saved AS (
		SELECT injection_points_save_int8(reuse_count)
		FROM q
	)
	SELECT uses_csn,
		reuse_count > 0 AS reuse_seen,
		val
	FROM q, saved;
}
step rc_second
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		injection_points_get_count('snapshot-reuse-success') > 0 AS reuse_seen,
		injection_points_get_count('snapshot-reuse-success') >
			injection_points_get_saved_int8() AS reuse_advanced,
		val
	FROM csn_snapshot_reuse_fallback
	WHERE id = 1;
}

session ctl
step wake		{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach		{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }

permutation rc_count_before w_begin w_update w_commit rc_first rc_second wake(w_commit) w_noop detach
