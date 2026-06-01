# Stage 3 H1-B characterization: after the ordinary ProcArray cleanup helper
# returns, the transaction outcome is already authoritative even though
# backend-local cleanup still has not finished.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_after_procarray_primary (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_after_procarray_primary VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_after_procarray_primary;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_after_procarray_primary SET val = 0 WHERE id = 1;
}

session writer
step w_begin
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
}
step w_update_commit	{ UPDATE csn_ordinary_after_procarray_primary SET val = 1 WHERE id = 1; }
step w_update_abort	{ UPDATE csn_ordinary_after_procarray_primary SET val = 2 WHERE id = 1; }
step w_commit		{ COMMIT; }
step w_abort		{ ABORT; }

session reader
step r_during_commit
{
	SELECT val
	FROM csn_ordinary_after_procarray_primary
	WHERE id = 1;
}
step r_after_commit
{
	SELECT val
	FROM csn_ordinary_after_procarray_primary
	WHERE id = 1;
}
step r_during_abort
{
	SELECT val
	FROM csn_ordinary_after_procarray_primary
	WHERE id = 1;
}
step r_after_abort
{
	SELECT val
	FROM csn_ordinary_after_procarray_primary
	WHERE id = 1;
}

session ctl
step wake		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach		{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

# Once the ordinary ProcArray cleanup helper has returned, the committed value
# is already stable for readers even though backend-local cleanup still lags.
permutation reset w_begin w_update_commit w_commit r_during_commit wake(w_commit) detach r_after_commit

# Once the ordinary ProcArray cleanup helper has returned, the aborted value
# remains invisible to readers even though backend-local cleanup still lags.
permutation reset w_begin w_update_abort w_abort r_during_abort wake(w_abort) detach r_after_abort
