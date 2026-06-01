# Stage 3 H1-C characterization: for ordinary primary transactions,
# TransactionIdIsInProgress() reports the xid as running before the top-level
# finish starts, but already reports it as not running once the writer reaches
# the legacy ordinary ProcArray helper.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_xid_in_progress_state (
		label text PRIMARY KEY,
		pid int NOT NULL,
		fxid xid8
	);
}
teardown
{
	DROP TABLE csn_ordinary_xid_in_progress_state;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_xid_in_progress_state;
}

session writer_commit
step wc_seed
{
	INSERT INTO csn_ordinary_xid_in_progress_state(label, pid, fxid)
	VALUES ('commit', pg_backend_pid(), NULL);
}
step wc_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
}
step wc_finish		{ COMMIT; }
step wc_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4 FROM csn_ordinary_xid_in_progress_state
		 WHERE label = 'commit')
	);
}

session writer_abort
step wa_seed
{
	INSERT INTO csn_ordinary_xid_in_progress_state(label, pid, fxid)
	VALUES ('abort', pg_backend_pid(), NULL);
}
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-before-procarray-primary', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
}
step wa_finish		{ ROLLBACK; }
step wa_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4 FROM csn_ordinary_xid_in_progress_state
		 WHERE label = 'abort')
	);
}

session observer
step o_commit_running
{
	SELECT count(*) = 1 AS advisory_lock_visible
	FROM pg_locks
	WHERE locktype = 'advisory'
	  AND classid = 1
	  AND pid = (
		SELECT pid
		FROM csn_ordinary_xid_in_progress_state
		WHERE label = 'commit'
	  );
	UPDATE csn_ordinary_xid_in_progress_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_xid_in_progress_state
			WHERE label = 'commit'
		  )
	);
	SELECT fxid IS NOT NULL AS commit_fxid_captured
	FROM csn_ordinary_xid_in_progress_state
	WHERE label = 'commit';
	SELECT injection_points_xid_in_progress(
		(SELECT fxid FROM csn_ordinary_xid_in_progress_state WHERE label = 'commit')
	) AS commit_xid_running;
}
step o_commit_finished
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid FROM csn_ordinary_xid_in_progress_state WHERE label = 'commit')
	) AS commit_xid_running;
}
step o_abort_running
{
	SELECT count(*) = 1 AS advisory_lock_visible
	FROM pg_locks
	WHERE locktype = 'advisory'
	  AND classid = 1
	  AND pid = (
		SELECT pid
		FROM csn_ordinary_xid_in_progress_state
		WHERE label = 'abort'
	  );
	UPDATE csn_ordinary_xid_in_progress_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_ordinary_xid_in_progress_state
			WHERE label = 'abort'
		  )
	);
	SELECT fxid IS NOT NULL AS abort_fxid_captured
	FROM csn_ordinary_xid_in_progress_state
	WHERE label = 'abort';
	SELECT injection_points_xid_in_progress(
		(SELECT fxid FROM csn_ordinary_xid_in_progress_state WHERE label = 'abort')
	) AS abort_xid_running;
}
step o_abort_finished
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid FROM csn_ordinary_xid_in_progress_state WHERE label = 'abort')
	) AS abort_xid_running;
}

session ctl
step wake		{ SELECT injection_points_wakeup('ordinary-before-procarray-primary'); }
step detach		{ SELECT injection_points_detach('ordinary-before-procarray-primary'); }

permutation reset wc_seed wc_prepare o_commit_running wc_finish o_commit_finished wake(wc_finish) detach wc_unlock
permutation reset wa_seed wa_prepare o_abort_running wa_finish o_abort_finished wake(wa_finish) detach wa_unlock
