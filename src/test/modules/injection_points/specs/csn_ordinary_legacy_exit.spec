# Stage 3 H1-B characterization: ordinary top-level COMMIT/ABORT still enter
# the legacy ordinary ProcArray cleanup helper before final backend-local
# cleanup finishes.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_legacy_exit (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_legacy_exit VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_legacy_exit;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_legacy_exit SET val = 0 WHERE id = 1;
}

session writer
step w_begin
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
}
step w_update_commit	{ UPDATE csn_ordinary_legacy_exit SET val = 1 WHERE id = 1; }
step w_update_abort	{ UPDATE csn_ordinary_legacy_exit SET val = 2 WHERE id = 1; }
step w_commit		{ COMMIT; }
step w_abort		{ ABORT; }

session reader
step r_during_commit
{
	SELECT val
	FROM csn_ordinary_legacy_exit
	WHERE id = 1;
}
step r_after_commit
{
	SELECT val
	FROM csn_ordinary_legacy_exit
	WHERE id = 1;
}
step r_during_abort
{
	SELECT val
	FROM csn_ordinary_legacy_exit
	WHERE id = 1;
}
step r_after_abort
{
	SELECT val
	FROM csn_ordinary_legacy_exit
	WHERE id = 1;
}

session ctl
step wake		{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach		{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }

# The commit outcome is already visible to readers while the writer is blocked
# at the legacy ordinary ProcArray cleanup helper.
permutation reset w_begin w_update_commit w_commit r_during_commit wake(w_commit) detach r_after_commit

# The abort outcome is already visible to readers while the writer is blocked
# at the legacy ordinary ProcArray cleanup helper.
permutation reset w_begin w_update_abort w_abort r_during_abort wake(w_abort) detach r_after_abort
