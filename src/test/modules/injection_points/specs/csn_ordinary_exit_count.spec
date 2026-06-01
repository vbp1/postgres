# Stage 3 H1-B characterization: ordinary top-level COMMIT/ABORT still hit
# the legacy ordinary ProcArray cleanup entrypoint exactly once on the current
# tree.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_exit_count (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_exit_count VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_exit_count;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_exit_count SET val = 0 WHERE id = 1;
}

session writer
step w_begin_commit
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'count');
	SELECT injection_points_get_count('ordinary-before-procarray-primary');
	UPDATE csn_ordinary_exit_count SET val = 1 WHERE id = 1;
}
step w_commit	{ COMMIT; }
step w_begin_abort
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'count');
	SELECT injection_points_get_count('ordinary-before-procarray-primary');
	UPDATE csn_ordinary_exit_count SET val = 2 WHERE id = 1;
}
step w_abort	{ ABORT; }

session reader
step r_count
{
	SELECT injection_points_get_count('ordinary-before-procarray-primary');
}
step r_val
{
	SELECT val
	FROM csn_ordinary_exit_count
	WHERE id = 1;
}

session ctl
step detach
{
	SELECT injection_points_detach('ordinary-before-procarray-primary');
}

permutation reset w_begin_commit w_commit r_count r_val detach
permutation reset w_begin_abort w_abort r_count r_val detach
