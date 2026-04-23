# Stage 3 H1-B characterization: multiple ordinary writers can be gathered at
# the explicit ProcArrayLock boundary, and each still hits the legacy lock-path
# count witness on the current tree.

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
	SELECT injection_points_get_count('ordinary-before-procarray-lock') = 2
		AS two_writers_reached_lock_boundary;
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
step attach_wait
{
	SELECT injection_points_attach('ordinary-before-procarray-lock-wait', 'wait');
}
step wake1
{
	SELECT injection_points_wakeup('ordinary-before-procarray-lock-wait');
}
step wake2
{
	SELECT injection_points_wakeup('ordinary-before-procarray-lock-wait');
}
step detach_wait
{
	SELECT injection_points_detach('ordinary-before-procarray-lock-wait');
}
step detach_count
{
	SELECT injection_points_detach('ordinary-before-procarray-lock');
}

permutation reset attach_count attach_wait w1_prepare w2_prepare w1_commit w2_commit o_count wake1 wake2 detach_wait o_vals detach_count
