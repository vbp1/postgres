# Stage 3 H1-E characterization: snapshot xmax remains tied to the
# shadow-backed latestCompletedXid + 1 across the ordinary pre-helper and
# post-helper freeze-points.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_snapshot_xmax_latest_completed (id int PRIMARY KEY, val int);
	INSERT INTO csn_snapshot_xmax_latest_completed VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_snapshot_xmax_latest_completed;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_snapshot_xmax_latest_completed SET val = 0 WHERE id = 1;
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
step w_update	{ UPDATE csn_snapshot_xmax_latest_completed SET val = val + 1 WHERE id = 1; }
step w_commit	{ COMMIT; }

session reader
step r_before
{
	WITH snap AS (
		SELECT pg_current_snapshot() AS snap
	)
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		pg_snapshot_xmax(snap)::text::int8 =
			injection_points_latest_completed_xid_shadow()::text::int8 + 1
			AS xmax_matches_shadow
	FROM snap;
}
step r_after
{
	WITH snap AS (
		SELECT pg_current_snapshot() AS snap
	)
	SELECT pg_current_snapshot_uses_csn() AS uses_csn,
		pg_snapshot_xmax(snap)::text::int8 =
			injection_points_latest_completed_xid_shadow()::text::int8 + 1
			AS xmax_matches_shadow
	FROM snap;
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_before	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

permutation reset wb_begin w_update w_commit r_before wake_before(w_commit) detach_before
permutation reset wa_begin w_update w_commit r_after wake_after(w_commit) detach_after
