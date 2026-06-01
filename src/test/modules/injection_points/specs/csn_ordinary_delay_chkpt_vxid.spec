# Stage 3 H1-C characterization: the checkpoint wait-list reader still sees
# the ordinary backend while the writer is blocked with DELAY_CHKPT_IN_COMMIT
# set, but no longer sees it once the writer reaches the ordinary legacy
# helper after the delay flag is cleared.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_delay_chkpt_vxid_state (
		label text PRIMARY KEY,
		pid int NOT NULL
	);
	CREATE TABLE csn_ordinary_delay_chkpt_vxid_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_delay_chkpt_vxid_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_delay_chkpt_vxid_data;
	DROP TABLE csn_ordinary_delay_chkpt_vxid_state;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_delay_chkpt_vxid_state;
	UPDATE csn_ordinary_delay_chkpt_vxid_data SET val = 0 WHERE id = 1;
}

session writer_before
step wb_seed
{
	INSERT INTO csn_ordinary_delay_chkpt_vxid_state(label, pid)
	VALUES ('before', pg_backend_pid());
}
step wb_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
	UPDATE csn_ordinary_delay_chkpt_vxid_data SET val = 1 WHERE id = 1;
}
step wb_commit		{ COMMIT; }

session writer_after
step wa_seed
{
	INSERT INTO csn_ordinary_delay_chkpt_vxid_state(label, pid)
	VALUES ('after', pg_backend_pid());
}
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	UPDATE csn_ordinary_delay_chkpt_vxid_data SET val = 2 WHERE id = 1;
}
step wa_commit		{ COMMIT; }

session observer
step o_before_visible
{
	SELECT injection_points_backend_delays_checkpoint(
		(SELECT pid
		 FROM csn_ordinary_delay_chkpt_vxid_state
		 WHERE label = 'before'),
		1
	) AS before_seen_by_delay_chkpt_reader;
}
step o_after_not_visible
{
	SELECT injection_points_backend_delays_checkpoint(
		(SELECT pid
		 FROM csn_ordinary_delay_chkpt_vxid_state
		 WHERE label = 'after'),
		1
	) = false AS after_not_seen_by_delay_chkpt_reader;
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach_before	{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }

permutation reset wb_seed wb_prepare wb_commit o_before_visible wake_before(wb_commit) detach_before
permutation reset wa_seed wa_prepare wa_commit o_after_not_visible wake_after(wa_commit) detach_after
