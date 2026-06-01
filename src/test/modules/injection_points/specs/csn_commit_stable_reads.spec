# Stage 3 H1-B characterization: repeated TransactionIdIsInProgress() reads
# stay stable while commit publication is still in progress, and stay stable
# again once completion has become visible.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_commit_stable_reads (
		id int PRIMARY KEY,
		val int
	);
	CREATE TABLE csn_commit_stable_reads_state (
		label text PRIMARY KEY,
		pid int NOT NULL,
		fxid xid8
	);
	INSERT INTO csn_commit_stable_reads VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_commit_stable_reads_state;
	DROP TABLE csn_commit_stable_reads;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_commit_stable_reads_state;
	UPDATE csn_commit_stable_reads SET val = 0 WHERE id = 1;
}

session writer_prepub
step wp_seed
{
	INSERT INTO csn_commit_stable_reads_state(label, pid, fxid)
	VALUES ('prepub', pg_backend_pid(), NULL);
}
step wp_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_commit_stable_reads SET val = 1 WHERE id = 1;
}
step wp_commit		{ COMMIT; }
step wp_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'prepub')
	);
}

session writer_postpub
step wa_seed
{
	INSERT INTO csn_commit_stable_reads_state(label, pid, fxid)
	VALUES ('postpub', pg_backend_pid(), NULL);
}
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-after-csn-publication', 'wait');
	SELECT pg_current_xact_id() IS NOT NULL AS writer_fxid_assigned;
	SELECT pg_advisory_lock_shared(1, pg_current_xact_id()::text::int4);
	UPDATE csn_commit_stable_reads SET val = 2 WHERE id = 1;
}
step wa_commit		{ COMMIT; }
step wa_unlock
{
	SELECT pg_advisory_unlock_shared(
		1,
		(SELECT fxid::text::int4
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'postpub')
	);
}

session observer
step o_capture_prepub_fxid
{
	UPDATE csn_commit_stable_reads_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_commit_stable_reads_state
			WHERE label = 'prepub'
		  )
	)
	WHERE label = 'prepub';
	SELECT fxid IS NOT NULL AS writer_fxid_captured
	FROM csn_commit_stable_reads_state
	WHERE label = 'prepub';
}
step o_before_publication
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'prepub')
	) AS before_publication_read_1;
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'prepub')
	) AS before_publication_read_2;
}
step o_capture_postpub_fxid
{
	UPDATE csn_commit_stable_reads_state
	SET fxid = (
		SELECT objid::text::xid8
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = 1
		  AND pid = (
			SELECT pid
			FROM csn_commit_stable_reads_state
			WHERE label = 'postpub'
		  )
	)
	WHERE label = 'postpub';
	SELECT fxid IS NOT NULL AS writer_fxid_captured
	FROM csn_commit_stable_reads_state
	WHERE label = 'postpub';
}
step o_after_publication
{
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'postpub')
	) AS after_publication_read_1;
	SELECT injection_points_xid_in_progress(
		(SELECT fxid
		 FROM csn_commit_stable_reads_state
		 WHERE label = 'postpub')
	) AS after_publication_read_2;
}

session ctl
step wake_prepub	{ SELECT injection_points_wakeup('commit-after-delay-checkpoint'); }
step detach_prepub	{ SELECT injection_points_detach('commit-after-delay-checkpoint'); }
step wake_postpub	{ SELECT injection_points_wakeup('commit-after-csn-publication'); }
step detach_postpub	{ SELECT injection_points_detach('commit-after-csn-publication'); }

permutation reset wp_seed wp_prepare o_capture_prepub_fxid wp_commit o_before_publication wake_prepub(wp_commit) detach_prepub wp_unlock
permutation reset wa_seed wa_prepare o_capture_postpub_fxid wa_commit o_after_publication wake_postpub(wa_commit) detach_postpub wa_unlock
