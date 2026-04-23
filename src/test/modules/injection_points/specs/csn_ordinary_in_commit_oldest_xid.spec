# Stage 3 H1-C characterization: checkpoint-facing commit-only readers still
# see the ordinary xid while the writer is blocked with
# DELAY_CHKPT_IN_COMMIT set, but no longer see that xid once the writer has
# reached the ordinary legacy helper after the delay flag is cleared.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_in_commit_oldest_xid_state (
		label text PRIMARY KEY,
		pid int NOT NULL,
		fxid xid8
	);
	CREATE TABLE csn_ordinary_in_commit_oldest_xid_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_in_commit_oldest_xid_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_in_commit_oldest_xid_data;
	DROP TABLE csn_ordinary_in_commit_oldest_xid_state;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_in_commit_oldest_xid_state;
	UPDATE csn_ordinary_in_commit_oldest_xid_data SET val = 0 WHERE id = 1;
}

session writer_before
step wb_seed
{
	INSERT INTO csn_ordinary_in_commit_oldest_xid_state(label, pid, fxid)
	VALUES ('before', pg_backend_pid(), NULL);
}
step wb_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_in_commit_oldest_xid_data SET val = 1 WHERE id = 1;
}
step wb_commit		{ COMMIT; }
step wb_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_in_commit_oldest_xid_state
		 WHERE label = 'before')
	);
}

session writer_after
step wa_seed
{
	INSERT INTO csn_ordinary_in_commit_oldest_xid_state(label, pid, fxid)
	VALUES ('after', pg_backend_pid(), NULL);
}
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_in_commit_oldest_xid_data SET val = 2 WHERE id = 1;
}
step wa_commit		{ COMMIT; }
step wa_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_in_commit_oldest_xid_state
		 WHERE label = 'after')
	);
}

session observer
step o_before_capture
{
	UPDATE csn_ordinary_in_commit_oldest_xid_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_in_commit_oldest_xid_state
			WHERE label = 'before'
		  )
	)
	WHERE label = 'before';
	SELECT fxid IS NOT NULL AS before_fxid_captured
	FROM csn_ordinary_in_commit_oldest_xid_state
	WHERE label = 'before';
}
step o_before_visible
{
	SELECT injection_points_oldest_active_xid(true, false) =
		(SELECT fxid
		 FROM csn_ordinary_in_commit_oldest_xid_state
		 WHERE label = 'before') AS before_seen_by_commit_reader;
}
step o_after_capture
{
	UPDATE csn_ordinary_in_commit_oldest_xid_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_in_commit_oldest_xid_state
			WHERE label = 'after'
		  )
	)
	WHERE label = 'after';
	SELECT fxid IS NOT NULL AS after_fxid_captured
	FROM csn_ordinary_in_commit_oldest_xid_state
	WHERE label = 'after';
}
step o_after_not_visible
{
	SELECT injection_points_oldest_active_xid(true, false) <>
		(SELECT fxid
		 FROM csn_ordinary_in_commit_oldest_xid_state
		 WHERE label = 'after') AS after_not_seen_by_commit_reader;
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach_before	{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }

permutation reset wb_seed wb_prepare o_before_capture wb_commit o_before_visible wake_before(wb_commit) detach_before wb_unlock
permutation reset wa_seed wa_prepare o_after_capture wa_commit o_after_not_visible wake_after(wa_commit) detach_after wa_unlock
