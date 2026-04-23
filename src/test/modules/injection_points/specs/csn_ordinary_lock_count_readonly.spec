# Stage 3 H1-E negative witness: xmin-only ordinary COMMIT/ABORT no longer
# reach the explicit ProcArrayLock acquisition point.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_lock_count_readonly (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_lock_count_readonly VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_lock_count_readonly;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	SELECT injection_points_reset_count('ordinary-before-procarray-lock');
}

session writer
step w_begin_commit
{
	BEGIN;
	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-lock', 'count');
	SELECT injection_points_get_count('ordinary-before-procarray-lock');
	SELECT val FROM csn_ordinary_lock_count_readonly WHERE id = 1;
}
step w_commit	{ COMMIT; }
step w_begin_abort
{
	BEGIN;
	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-lock', 'count');
	SELECT injection_points_get_count('ordinary-before-procarray-lock');
	SELECT val FROM csn_ordinary_lock_count_readonly WHERE id = 1;
}
step w_abort	{ ABORT; }

session reader
step r_count
{
	SELECT injection_points_get_count('ordinary-before-procarray-lock');
}

session ctl
step detach
{
	SELECT injection_points_detach('ordinary-before-procarray-lock');
}

permutation reset w_begin_commit w_commit r_count detach
permutation reset w_begin_abort w_abort r_count detach
