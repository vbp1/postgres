# Stage 3 H1-B characterization: while a backend is blocked after
# ProcArrayEndTransactionPrimary(), its old virtual xid is still visible; only
# after the commit returns can the same backend start a new transaction with a
# new local vxid component.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_vxid_lifecycle_data (id int PRIMARY KEY, val int);
	CREATE TABLE csn_ordinary_vxid_lifecycle_state (
		label text PRIMARY KEY,
		vxid text NOT NULL
	);
	INSERT INTO csn_ordinary_vxid_lifecycle_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_vxid_lifecycle_state;
	DROP TABLE csn_ordinary_vxid_lifecycle_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	TRUNCATE csn_ordinary_vxid_lifecycle_state;
	UPDATE csn_ordinary_vxid_lifecycle_data SET val = 0 WHERE id = 1;
}

session writer
step w_begin_old
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	INSERT INTO csn_ordinary_vxid_lifecycle_state
	VALUES (
		'pid',
		pg_backend_pid()::text
	);
	INSERT INTO csn_ordinary_vxid_lifecycle_state
	VALUES (
		'old',
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		)
	);
	UPDATE csn_ordinary_vxid_lifecycle_data SET val = 1 WHERE id = 1;
}
step w_commit	{ COMMIT; }
step w_begin_new
{
	BEGIN;
}
step w_compare_new
{
	SELECT
		split_part(
			(SELECT vxid FROM csn_ordinary_vxid_lifecycle_state WHERE label = 'old'),
			'/',
			1
		) =
		split_part(
			(
				SELECT virtualtransaction
				FROM pg_locks
				WHERE pid = pg_backend_pid()
				  AND locktype = 'virtualxid'
			),
			'/',
			1
		) AS same_proc,
		(SELECT vxid FROM csn_ordinary_vxid_lifecycle_state WHERE label = 'old') <>
		(
			SELECT virtualtransaction
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		) AS changed_vxid,
		split_part(
			(
				SELECT virtualtransaction
				FROM pg_locks
				WHERE pid = pg_backend_pid()
				  AND locktype = 'virtualxid'
			),
			'/',
			2
		)::int >
		split_part(
			(SELECT vxid FROM csn_ordinary_vxid_lifecycle_state WHERE label = 'old'),
			'/',
			2
		)::int AS advanced_lxid;
}
step w_rollback	{ ROLLBACK; }

session observer
step o_old_visible
{
	SELECT EXISTS (
		SELECT 1
		FROM pg_locks
		WHERE locktype = 'virtualxid'
		  AND virtualtransaction = (
			SELECT vxid
			FROM csn_ordinary_vxid_lifecycle_state
			WHERE label = 'old'
		  )
	) AS old_vxid_visible;
}
step o_old_gone_new_visible
{
	SELECT EXISTS (
		SELECT 1
		FROM pg_locks
		WHERE locktype = 'virtualxid'
		  AND virtualtransaction = (
			SELECT vxid
			FROM csn_ordinary_vxid_lifecycle_state
			WHERE label = 'old'
		  )
	) AS old_vxid_visible,
	EXISTS (
		SELECT 1
		FROM pg_locks
		WHERE locktype = 'virtualxid'
		  AND pid = (
			SELECT vxid::int
			FROM csn_ordinary_vxid_lifecycle_state
			WHERE label = 'pid'
		  )
	) AS writer_vxid_visible;
}

session ctl
step wake		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach		{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }

permutation reset w_begin_old w_commit o_old_visible wake(w_commit) detach w_begin_new o_old_gone_new_visible w_compare_new w_rollback
