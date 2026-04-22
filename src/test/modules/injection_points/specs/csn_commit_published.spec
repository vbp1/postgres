# Stage 3 C1 characterization: once the commit outcome is published strongly
# enough for CSN snapshots, a backend that still has legacy ProcArray state
# should no longer force concurrent snapshots off snapshot_csn.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_commit_published (id int PRIMARY KEY, val int);
	INSERT INTO csn_commit_published VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_commit_published;
	DROP EXTENSION injection_points;
}

session writer
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-csn-publication', 'wait');
}
step w_begin	{ BEGIN; }
step w_update	{ UPDATE csn_commit_published SET val = 1 WHERE id = 1; }
step w_commit	{ COMMIT; }
step w_noop		{ }

session rc
step rc_before
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_published
	WHERE id = 1;
}
step rc_during
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_published
	WHERE id = 1;
}
step rc_after
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_published
	WHERE id = 1;
}

session rr
step rr_begin	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_during
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_published
	WHERE id = 1;
}
step rr_after
{
	SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
	FROM csn_commit_published
	WHERE id = 1;
}
step rr_commit	{ COMMIT; }

session ctl
step wake		{ SELECT injection_points_wakeup('commit-after-csn-publication'); }
step detach		{ SELECT injection_points_detach('commit-after-csn-publication'); }

# Read Committed keeps using snapshot_csn once the concurrent commit has
# published its CSN-visible outcome, even before legacy ProcArray cleanup.
permutation rc_before w_begin w_update w_commit rc_during wake(w_commit) w_noop detach rc_after

# Repeatable Read can take its first snapshot during the published-but-not-yet-
# cleaned window and keep that CSN-aware view for the rest of the transaction.
permutation w_begin w_update w_commit rr_begin rr_during wake(w_commit) w_noop detach rr_after rr_commit
