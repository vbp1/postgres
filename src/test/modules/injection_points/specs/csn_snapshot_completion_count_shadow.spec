# Stage 3 H1-D characterization: the active query snapshot's
# snapXactCompletionCount tracks the passive shadow across the ordinary
# pre-helper and post-helper freeze-points.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_snapshot_completion_count_shadow (id int PRIMARY KEY, val int);
	INSERT INTO csn_snapshot_completion_count_shadow VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_snapshot_completion_count_shadow;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_snapshot_completion_count_shadow SET val = 0 WHERE id = 1;
}

session writer
step wb_begin
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
}
step wa_begin
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
}
step w_update	{ UPDATE csn_snapshot_completion_count_shadow SET val = val + 1 WHERE id = 1; }
step w_update_abort	{ UPDATE csn_snapshot_completion_count_shadow SET val = val + 2 WHERE id = 1; }
step w_commit	{ COMMIT; }
step w_abort	{ ABORT; }

session reader
step r_before
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		injection_points_active_snapshot_xact_completion_count()
			AS snap_xact_completion_count,
		injection_points_transaction_snapshot_xact_completion_count()
			AS txsnap_xact_completion_count,
		injection_points_xact_completion_count_shadow()
			AS shadow_xact_completion_count,
		injection_points_xact_completion_count()
			AS legacy_xact_completion_count
	FROM csn_snapshot_completion_count_shadow
	WHERE id = 1;
}
step r_after
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		injection_points_active_snapshot_xact_completion_count()
			AS snap_xact_completion_count,
		injection_points_transaction_snapshot_xact_completion_count()
			AS txsnap_xact_completion_count,
		injection_points_xact_completion_count_shadow()
			AS shadow_xact_completion_count,
		injection_points_xact_completion_count()
			AS legacy_xact_completion_count
	FROM csn_snapshot_completion_count_shadow
	WHERE id = 1;
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_before	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

permutation reset wb_begin w_update w_commit r_before wake_before(w_commit) detach_before
permutation reset wa_begin w_update w_commit r_after wake_after(w_commit) detach_after
permutation reset wb_begin w_update_abort w_abort r_before wake_before(w_abort) detach_before
permutation reset wa_begin w_update_abort w_abort r_after wake_after(w_abort) detach_after
