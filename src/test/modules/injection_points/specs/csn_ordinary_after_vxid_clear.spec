# Stage 3 H1-B characterization: once the writer reaches the post-vxid-clear
# hook, the old xid is retired, the old virtual xid is gone, and the backend
# already exposes a different current virtual xid while still carrying no xid.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_after_vxid_clear_data (id int PRIMARY KEY, val int);
	CREATE TABLE csn_ordinary_after_vxid_clear_state (
		label text PRIMARY KEY,
		pid int NOT NULL,
		fxid xid8,
		vxid text
	);
	INSERT INTO csn_ordinary_after_vxid_clear_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_after_vxid_clear_state;
	DROP TABLE csn_ordinary_after_vxid_clear_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_after_vxid_clear_state;
	UPDATE csn_ordinary_after_vxid_clear_data SET val = 0 WHERE id = 1;
}

session writer
step wc_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-vxid-clear', 'wait');
	INSERT INTO csn_ordinary_after_vxid_clear_state(label, pid, fxid, vxid)
	VALUES (
		'writer',
		pg_backend_pid(),
		pg_current_xact_id(),
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		)
	);
	UPDATE csn_ordinary_after_vxid_clear_data SET val = 1 WHERE id = 1;
}
step wc_finish		{ COMMIT; }
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-vxid-clear', 'wait');
	INSERT INTO csn_ordinary_after_vxid_clear_state(label, pid, fxid, vxid)
	VALUES (
		'abort',
		pg_backend_pid(),
		pg_current_xact_id(),
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		)
	);
	UPDATE csn_ordinary_after_vxid_clear_data SET val = 2 WHERE id = 1;
}
step wa_finish		{ ABORT; }

session observer
step o_commit_after_clear_state
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_ordinary_after_vxid_clear_state
		 WHERE label = 'writer')
	) = false AS old_xid_retired,
		injection_points_backend_xid(
			(SELECT pid
			 FROM csn_ordinary_after_vxid_clear_state
			 WHERE label = 'writer')
		) IS NULL AS backend_xid_cleared,
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND virtualtransaction = (
				SELECT vxid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'writer'
			  )
		) = false AS old_vxid_gone,
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT pid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'writer'
			  )
		) AS current_vxid_visible,
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT pid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'writer'
			  )
		) <>
		(
			SELECT vxid
			FROM csn_ordinary_after_vxid_clear_state
			WHERE label = 'writer'
		) AS current_vxid_changed;
}
step o_abort_after_clear_state
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_ordinary_after_vxid_clear_state
		 WHERE label = 'abort')
	) = false AS old_xid_retired,
		injection_points_backend_xid(
			(SELECT pid
			 FROM csn_ordinary_after_vxid_clear_state
			 WHERE label = 'abort')
		) IS NULL AS backend_xid_cleared,
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND virtualtransaction = (
				SELECT vxid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'abort'
			  )
		) = false AS old_vxid_gone,
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT pid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'abort'
			  )
		) AS current_vxid_visible,
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT pid
				FROM csn_ordinary_after_vxid_clear_state
				WHERE label = 'abort'
			  )
		) <>
		(
			SELECT vxid
			FROM csn_ordinary_after_vxid_clear_state
			WHERE label = 'abort'
		) AS current_vxid_changed;
}

session ctl
step wake		{ SELECT injection_points_wakeup('ordinary-after-vxid-clear'); }
step detach		{ SELECT injection_points_detach('ordinary-after-vxid-clear'); }

permutation reset wc_prepare wc_finish o_commit_after_clear_state wake(wc_finish) detach
permutation reset wa_prepare wa_finish o_abort_after_clear_state wake(wa_finish) detach
