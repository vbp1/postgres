# Stage 3 H1-E characterization: ordinary completion metadata is published
# through the shadow-backed contract at ordinary finish publication.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_completion_vars_state (
		label text PRIMARY KEY,
		pid int NOT NULL,
		fxid xid8
	);
	CREATE TABLE csn_ordinary_completion_vars_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_completion_vars_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_completion_vars_data;
	DROP TABLE csn_ordinary_completion_vars_state;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_completion_vars_state;
	UPDATE csn_ordinary_completion_vars_data SET val = 0 WHERE id = 1;
}

session writer_commit_before
step wcb_seed
{
	INSERT INTO csn_ordinary_completion_vars_state(label, pid, fxid)
	VALUES (
		'commit_before',
		pg_backend_pid(),
		NULL
	);
}
step wcb_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_completion_vars_data SET val = 1 WHERE id = 1;
}
step wcb_commit		{ COMMIT; }
step wcb_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_completion_vars_state
		 WHERE label = 'commit_before')
	);
}

session writer_commit_after
step wca_seed
{
	INSERT INTO csn_ordinary_completion_vars_state(label, pid, fxid)
	VALUES (
		'commit_after',
		pg_backend_pid(),
		NULL
	);
}
step wca_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_completion_vars_data SET val = 2 WHERE id = 1;
}
step wca_commit		{ COMMIT; }
step wca_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_completion_vars_state
		 WHERE label = 'commit_after')
	);
}

session writer_abort_before
step wab_seed
{
	INSERT INTO csn_ordinary_completion_vars_state(label, pid, fxid)
	VALUES (
		'abort_before',
		pg_backend_pid(),
		NULL
	);
}
step wab_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_completion_vars_data SET val = 3 WHERE id = 1;
}
step wab_abort		{ ABORT; }
step wab_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_completion_vars_state
		 WHERE label = 'abort_before')
	);
}

session writer_abort_after
step waa_seed
{
	INSERT INTO csn_ordinary_completion_vars_state(label, pid, fxid)
	VALUES (
		'abort_after',
		pg_backend_pid(),
		NULL
	);
}
step waa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_ordinary_completion_vars_data SET val = 4 WHERE id = 1;
}
step waa_abort		{ ABORT; }
step waa_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_ordinary_completion_vars_state
		 WHERE label = 'abort_after')
	);
}

session observer
step o_capture_commit_before
{
	UPDATE csn_ordinary_completion_vars_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_completion_vars_state
			WHERE label = 'commit_before'
		  )
	)
	WHERE label = 'commit_before';
	SELECT fxid IS NOT NULL AS commit_before_fxid_captured
	FROM csn_ordinary_completion_vars_state
	WHERE label = 'commit_before';
}
step o_commit_before_state
{
	SELECT injection_points_latest_completed_xid_shadow() =
		injection_points_get_saved_xid8() AS commit_before_shadow_latest_stable,
		injection_points_xact_completion_count_shadow() =
		injection_points_get_saved_int8() AS commit_before_shadow_count_unchanged;
}
step o_capture_commit_after
{
	UPDATE csn_ordinary_completion_vars_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_completion_vars_state
			WHERE label = 'commit_after'
		  )
	)
	WHERE label = 'commit_after';
	SELECT fxid IS NOT NULL AS commit_after_fxid_captured
	FROM csn_ordinary_completion_vars_state
	WHERE label = 'commit_after';
}
step o_commit_after_state
{
	SELECT injection_points_latest_completed_xid_shadow() =
		injection_points_get_saved_xid8() AS commit_after_shadow_latest_stable,
		injection_points_xact_completion_count_shadow() =
		injection_points_get_saved_int8() + 1 AS commit_after_shadow_count_advanced;
}
step o_capture_abort_before
{
	UPDATE csn_ordinary_completion_vars_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_completion_vars_state
			WHERE label = 'abort_before'
		  )
	)
	WHERE label = 'abort_before';
	SELECT fxid IS NOT NULL AS abort_before_fxid_captured
	FROM csn_ordinary_completion_vars_state
	WHERE label = 'abort_before';
}
step o_abort_before_state
{
	SELECT injection_points_latest_completed_xid_shadow() =
		injection_points_get_saved_xid8() AS abort_before_shadow_latest_stable,
		injection_points_xact_completion_count_shadow() =
		injection_points_get_saved_int8() AS abort_before_shadow_count_unchanged;
}
step o_capture_abort_after
{
	UPDATE csn_ordinary_completion_vars_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_completion_vars_state
			WHERE label = 'abort_after'
		  )
	)
	WHERE label = 'abort_after';
	SELECT fxid IS NOT NULL AS abort_after_fxid_captured
	FROM csn_ordinary_completion_vars_state
	WHERE label = 'abort_after';
}
step o_abort_after_state
{
	SELECT injection_points_latest_completed_xid_shadow() =
		injection_points_get_saved_xid8() AS abort_after_shadow_latest_stable,
		injection_points_xact_completion_count_shadow() =
		injection_points_get_saved_int8() + 1 AS abort_after_shadow_count_advanced;
}

step o_save_count
{
	SELECT injection_points_latest_completed_xid_shadow() IS NOT NULL
		AS saved_shadow_latest_present;
	SELECT injection_points_xact_completion_count_shadow() > 0
		AS saved_shadow_count_present;
	SELECT injection_points_save_int8(injection_points_xact_completion_count_shadow());
	SELECT injection_points_save_xid8(injection_points_latest_completed_xid_shadow());
}

session ctl
step wake_before	{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach_before	{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

permutation reset wcb_seed wcb_prepare o_capture_commit_before o_save_count wcb_commit o_commit_before_state wake_before(wcb_commit) detach_before wcb_unlock
permutation reset wca_seed wca_prepare o_capture_commit_after o_save_count wca_commit o_commit_after_state wake_after(wca_commit) detach_after wca_unlock
permutation reset wab_seed wab_prepare o_capture_abort_before o_save_count wab_abort o_abort_before_state wake_before(wab_abort) detach_before wab_unlock
permutation reset waa_seed waa_prepare o_capture_abort_after o_save_count waa_abort o_abort_after_state wake_after(waa_abort) detach_after waa_unlock
