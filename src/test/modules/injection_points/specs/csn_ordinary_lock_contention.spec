# Stage 3 H1-E negative witness: concurrent ordinary writers complete without
# reaching the legacy ProcArrayLock boundary.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_lock_contention (
		id int PRIMARY KEY,
		val int
	);
	INSERT INTO csn_ordinary_lock_contention VALUES (1, 0), (2, 0);
}
teardown
{
	DROP TABLE csn_ordinary_lock_contention;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_lock_contention SET val = 0;
	SELECT injection_points_reset_count('ordinary-before-procarray-lock');
}

session writer1
step w1_prepare
{
	BEGIN;
	UPDATE csn_ordinary_lock_contention SET val = 1 WHERE id = 1;
}
step w1_commit		{ COMMIT; }

session writer2
step w2_prepare
{
	BEGIN;
	UPDATE csn_ordinary_lock_contention SET val = 1 WHERE id = 2;
}
step w2_commit		{ COMMIT; }

session observer
step o_count
{
	SELECT injection_points_get_count('ordinary-before-procarray-lock') = 0
		AS no_writers_reached_lock_boundary;
}
step o_vals
{
	SELECT count(*) FILTER (WHERE val = 1) = 2 AS both_commits_visible
	FROM csn_ordinary_lock_contention;
}

session ctl
step attach_count
{
	SELECT injection_points_attach('ordinary-before-procarray-lock', 'count');
}
step detach_count
{
	SELECT injection_points_detach('ordinary-before-procarray-lock');
}

permutation reset attach_count w1_prepare w2_prepare w1_commit w2_commit o_count o_vals detach_count
