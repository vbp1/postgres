# Stage 3 CSN baseline: a backend suspended in DELAY_CHKPT_IN_COMMIT currently
# forces concurrent snapshots onto the legacy path.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_commit_fallback (id int PRIMARY KEY, val int);
	INSERT INTO csn_commit_fallback VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_commit_fallback;
	DROP EXTENSION injection_points;
}

session writer
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
}
step w_begin	{ BEGIN; }
step w_update	{ UPDATE csn_commit_fallback SET val = 1 WHERE id = 1; }
step w_commit	{ COMMIT; }
step w_noop		{ }

session rc
step rc_before
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_fallback
	WHERE id = 1;
}
step rc_during
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_fallback
	WHERE id = 1;
}
step rc_after
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_fallback
	WHERE id = 1;
}

session rr
step rr_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_during
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_fallback
	WHERE id = 1;
}
step rr_after
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_fallback
	WHERE id = 1;
}
step rr_commit	{ COMMIT; }

session ctl
step wake		{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach		{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }

# Read Committed falls back while the concurrent commit is suspended, then
# returns to a CSN snapshot after the writer finishes.
permutation rc_before w_begin w_update w_commit rc_during wake(w_commit) w_noop detach rc_after

# Repeatable Read takes its first snapshot while the commit is suspended and
# keeps that legacy fallback view for the life of the transaction.
permutation w_begin w_update w_commit rr_begin rr_during wake(w_commit) w_noop detach rr_after rr_commit
