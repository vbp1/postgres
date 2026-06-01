# Stage 3 H1-B characterization: CREATE INDEX CONCURRENTLY still waits for an
# ordinary snapshot holder blocked before ordinary completion publication, and
# still waits while compatibility cleanup is pending after publication.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_cic_wait (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_cic_wait
	SELECT g, g
	FROM generate_series(1, 10) AS g;
}
teardown
{
	DROP TABLE csn_ordinary_cic_wait;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	DROP INDEX IF EXISTS csn_ordinary_cic_wait_before_idx;
	DROP INDEX IF EXISTS csn_ordinary_cic_wait_after_idx;
}

session holder_before
step hb_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT count(*) FROM csn_ordinary_cic_wait;
}
step hb_commit	{ COMMIT; }

session holder_after
step ha_begin
{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT count(*) FROM csn_ordinary_cic_wait;
}
step ha_commit	{ COMMIT; }

session cic
step cic_before
{
	CREATE INDEX CONCURRENTLY csn_ordinary_cic_wait_before_idx
		ON csn_ordinary_cic_wait (id);
}
step cic_after
{
	CREATE INDEX CONCURRENTLY csn_ordinary_cic_wait_after_idx
		ON csn_ordinary_cic_wait (id);
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_before	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

# WaitForOlderSnapshots still sees the repeatable-read backend while it is
# blocked before ordinary completion publication.
permutation reset hb_begin hb_commit cic_before(*) wake_before(hb_commit) detach_before

# After publication but before backend-local compatibility cleanup, CREATE
# INDEX CONCURRENTLY still waits for the same backend.
permutation reset ha_begin ha_commit cic_after(*) wake_after(ha_commit) detach_after
