# Stage 3 H1-B characterization: once the same backend has completed the old
# commit and started the next top-level transaction, the old xid is already
# retired while the new transaction still has only a virtual xid.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_reuse_begin_data (id int PRIMARY KEY, val int);
	CREATE TABLE csn_ordinary_reuse_begin_state (
		label text PRIMARY KEY,
		pid int,
		fxid xid8
	);
	INSERT INTO csn_ordinary_reuse_begin_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_reuse_begin_state;
	DROP TABLE csn_ordinary_reuse_begin_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_reuse_begin_state;
	UPDATE csn_ordinary_reuse_begin_data SET val = 0 WHERE id = 1;
}

session writer
step w_prepare_old
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	INSERT INTO csn_ordinary_reuse_begin_state(label, pid, fxid)
	VALUES ('writer', pg_backend_pid(), pg_current_xact_id());
	UPDATE csn_ordinary_reuse_begin_data SET val = 1 WHERE id = 1;
}
step w_commit		{ COMMIT; }
step w_begin_new	{ BEGIN; }
step w_rollback		{ ROLLBACK; }

session observer
step o_reuse_state
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_ordinary_reuse_begin_state
		 WHERE label = 'writer')
	) = false AS old_xid_retired,
		injection_points_backend_xid(
			(SELECT pid
			 FROM csn_ordinary_reuse_begin_state
			 WHERE label = 'writer')
		) IS NULL AS new_xid_not_assigned,
		EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = (
				SELECT pid
				FROM csn_ordinary_reuse_begin_state
				WHERE label = 'writer'
			  )
		) AS new_vxid_visible,
		injection_points_oldest_considered_running_xid() <>
			(SELECT fxid
			 FROM csn_ordinary_reuse_begin_state
			 WHERE label = 'writer') AS old_xid_not_in_oldest_considered_running,
		injection_points_oldest_nonremovable_xid() <>
			(SELECT fxid
			 FROM csn_ordinary_reuse_begin_state
			 WHERE label = 'writer') AS old_xid_not_in_oldest_nonremovable;
}

session ctl
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }
step wake_start		{ SELECT injection_points_wakeup('start-after-vxid-publication'); }
step detach_start	{ SELECT injection_points_detach('start-after-vxid-publication'); }

permutation reset w_prepare_old w_commit wake_after(w_commit) detach_after w_begin_new o_reuse_state wake_start(w_begin_new) detach_start w_rollback
