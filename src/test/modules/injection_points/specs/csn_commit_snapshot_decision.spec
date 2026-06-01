# H1-E debug characterization: while a writer is suspended at
# commit-after-delay-checkpoint, GetSnapshotData() should see
# DELAY_CHKPT_IN_COMMIT and must not skip the writer as snapshot-safe.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_commit_snapshot_decision (id int PRIMARY KEY, val int);
	INSERT INTO csn_commit_snapshot_decision VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_commit_snapshot_decision;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	SELECT injection_points_reset_count('snapshot-before-skip-safe-to-ignore');
	SELECT injection_points_reset_count('snapshot-saw-delay-chkpt-in-commit');
	SELECT injection_points_set_global_int8('snapshot-decision-writer-pid', 0);
	UPDATE csn_commit_snapshot_decision SET val = 0 WHERE id = 1;
}

session writer
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
}
step w_begin
{
	SELECT injection_points_set_global_int8(
		'snapshot-decision-writer-pid',
		pg_backend_pid()
	);
	BEGIN;
}
step w_update	{ UPDATE csn_commit_snapshot_decision SET val = 1 WHERE id = 1; }
step w_commit	{ COMMIT; }
step w_noop		{ }

session observer
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('snapshot-before-skip-safe-to-ignore', 'count');
	SELECT injection_points_attach('snapshot-saw-delay-chkpt-in-commit', 'count');
}
step o_probe
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		injection_points_backend_xid(
			injection_points_get_global_int8(
				'snapshot-decision-writer-pid'
			)::int4
		) IS NOT NULL AS writer_xid_visible,
		injection_points_backend_delays_checkpoint(
			injection_points_get_global_int8(
				'snapshot-decision-writer-pid'
			)::int4,
			1
		) AS writer_delays_checkpoint,
		injection_points_backend_snapshot_safe_to_ignore(
			injection_points_get_global_int8(
				'snapshot-decision-writer-pid'
			)::int4
		) AS writer_snapshot_safe,
		injection_points_get_count('snapshot-before-skip-safe-to-ignore')
			AS safe_skip_count,
		injection_points_get_count('snapshot-saw-delay-chkpt-in-commit')
			AS delay_seen_count,
		val
	FROM csn_commit_snapshot_decision
	WHERE id = 1;
}

session ctl
step wake		{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach		{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }

permutation reset w_begin w_update w_commit o_probe wake(w_commit) w_noop detach
