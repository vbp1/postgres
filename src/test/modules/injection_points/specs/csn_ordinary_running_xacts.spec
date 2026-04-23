# Stage 3 H1-C characterization: GetRunningTransactionData() still includes
# the ordinary xid while the writer is blocked before the legacy helper, but
# no longer includes it once the writer is blocked after the helper.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_running_xacts_state (
		label text PRIMARY KEY,
		pid int NOT NULL
	);
	CREATE TABLE csn_ordinary_running_xacts_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_running_xacts_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_running_xacts_data;
	DROP TABLE csn_ordinary_running_xacts_state;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_running_xacts_state;
	UPDATE csn_ordinary_running_xacts_data SET val = 0 WHERE id = 1;
}

session writer_before
step wb_seed
{
	INSERT INTO csn_ordinary_running_xacts_state(label, pid)
	VALUES ('before', pg_backend_pid());
}
step wb_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	UPDATE csn_ordinary_running_xacts_data SET val = 1 WHERE id = 1;
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
}
step wb_commit		{ COMMIT; }

session writer_after
step wa_seed
{
	INSERT INTO csn_ordinary_running_xacts_state(label, pid)
	VALUES ('after', pg_backend_pid());
}
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	UPDATE csn_ordinary_running_xacts_data SET val = 2 WHERE id = 1;
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
}
step wa_commit		{ COMMIT; }

session observer
step o_before_visible
{
	SELECT injection_points_running_xacts_include_backend(
		(SELECT pid
		 FROM csn_ordinary_running_xacts_state
		 WHERE label = 'before'),
		true
	) AS before_seen_by_running_xacts;
	SELECT injection_points_running_xacts_latest_completed_xid(true) =
		injection_points_latest_completed_xid_shadow()
		AS before_running_xacts_latest_completed_uses_shadow;
}
step o_after_not_visible
{
	SELECT injection_points_running_xacts_include_backend(
		(SELECT pid
		 FROM csn_ordinary_running_xacts_state
		 WHERE label = 'after'),
		true
	) = false AS after_not_seen_by_running_xacts;
	SELECT injection_points_running_xacts_latest_completed_xid(true) =
		injection_points_latest_completed_xid_shadow()
		AS after_running_xacts_latest_completed_uses_shadow;
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_before	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

permutation reset wb_seed wb_prepare wb_commit o_before_visible wake_before(wb_commit) detach_before
permutation reset wa_seed wa_prepare wa_commit o_after_not_visible wake_after(wa_commit) detach_after
