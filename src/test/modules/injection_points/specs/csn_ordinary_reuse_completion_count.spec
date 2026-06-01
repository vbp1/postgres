# Stage 3 H1-B/H1-D characterization: once the same backend finishes an
# ordinary top-level transaction and starts the next one, the new transaction
# sees the current completion-generation signal rather than stale metadata from
# the old transaction.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_reuse_completion_count_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_reuse_completion_count_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_reuse_completion_count_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_reuse_completion_count_data SET val = 0 WHERE id = 1;
	SELECT injection_points_set_global_int8('reuse-old-count', 0);
	SELECT injection_points_set_global_int8('reuse-new-count', 0);
}

session writer
step w_prepare_commit
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8(
		'reuse-old-count',
		injection_points_xact_completion_count_shadow()
	);
	UPDATE csn_ordinary_reuse_completion_count_data SET val = 1 WHERE id = 1;
}
step w_prepare_abort
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8(
		'reuse-old-count',
		injection_points_xact_completion_count_shadow()
	);
	UPDATE csn_ordinary_reuse_completion_count_data SET val = 2 WHERE id = 1;
}
step w_commit		{ COMMIT; }
step w_abort		{ ABORT; }
step w_begin_new	{ BEGIN; }
step w_capture_new
{
	SELECT injection_points_set_global_int8(
		'reuse-new-count',
		injection_points_transaction_snapshot_xact_completion_count()
	);
}
step w_rollback		{ ROLLBACK; }

session observer
step o_before_new_xid
{
	SELECT injection_points_xact_completion_count_shadow() >
			injection_points_get_global_int8('reuse-old-count')
			AS completion_count_advanced;
}
step o_new_tx_snapshot
{
	SELECT injection_points_get_global_int8('reuse-new-count') =
			injection_points_xact_completion_count_shadow()
			AS new_tx_matches_shadow,
		injection_points_get_global_int8('reuse-new-count') >
			injection_points_get_global_int8('reuse-old-count')
			AS new_tx_is_fresh;
}

session ctl
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }
step wake_start		{ SELECT injection_points_wakeup('start-after-vxid-publication'); }
step detach_start	{ SELECT injection_points_detach('start-after-vxid-publication'); }

permutation reset w_prepare_commit w_commit wake_after(w_commit) detach_after w_begin_new o_before_new_xid wake_start(w_begin_new) detach_start w_capture_new o_new_tx_snapshot w_rollback
permutation reset w_prepare_abort w_abort wake_after(w_abort) detach_after w_begin_new o_before_new_xid wake_start(w_begin_new) detach_start w_capture_new o_new_tx_snapshot w_rollback
