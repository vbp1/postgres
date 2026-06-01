# Stage 3 H1-B/H1-D characterization: before the same backend advertises the
# next top-level xid, it already exposes a strictly newer slot epoch in the
# same proc slot. This is a passive scaffold for the future generation-tagged
# ordinary completion protocol.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_reuse_slot_epoch_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_reuse_slot_epoch_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_reuse_slot_epoch_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_reuse_slot_epoch_data SET val = 0 WHERE id = 1;
	SELECT injection_points_set_global_int8('slot-epoch-old-pid', 0);
	SELECT injection_points_set_global_int8('slot-epoch-old-fxid', 0);
	SELECT injection_points_set_global_int8('slot-epoch-old-proc', 0);
	SELECT injection_points_set_global_int8('slot-epoch-old-epoch', 0);
}

session writer
step wc_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8('slot-epoch-old-pid', pg_backend_pid());
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-fxid',
		pg_current_xact_id()::text::int8
	);
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-proc',
		(
			SELECT split_part(virtualtransaction, '/', 1)::int8
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		)
	);
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-epoch',
		injection_points_backend_slot_epoch(pg_backend_pid())
	);
	UPDATE csn_ordinary_reuse_slot_epoch_data SET val = 1 WHERE id = 1;
}
step wc_finish		{ COMMIT; }
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8('slot-epoch-old-pid', pg_backend_pid());
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-fxid',
		pg_current_xact_id()::text::int8
	);
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-proc',
		(
			SELECT split_part(virtualtransaction, '/', 1)::int8
			FROM pg_locks
			WHERE pid = pg_backend_pid()
			  AND locktype = 'virtualxid'
		)
	);
	SELECT injection_points_set_global_int8(
		'slot-epoch-old-epoch',
		injection_points_backend_slot_epoch(pg_backend_pid())
	);
	UPDATE csn_ordinary_reuse_slot_epoch_data SET val = 2 WHERE id = 1;
}
step wa_finish		{ ABORT; }
step w_begin_new	{ BEGIN; }
step w_rollback		{ ROLLBACK; }

session observer
step o_reuse_epoch
{
	SELECT injection_points_xid_in_progress(
		injection_points_get_global_int8('slot-epoch-old-fxid')::text::xid8
	) = false AS old_xid_retired,
		injection_points_backend_xid(
			injection_points_get_global_int8('slot-epoch-old-pid')::int4
		) IS NULL AS new_xid_not_assigned,
		(
			SELECT split_part(virtualtransaction, '/', 1)::int8
			FROM pg_locks
			WHERE locktype = 'virtualxid'
			  AND pid = injection_points_get_global_int8('slot-epoch-old-pid')::int4
		) =
		injection_points_get_global_int8('slot-epoch-old-proc') AS same_proc_slot,
		injection_points_backend_slot_epoch(
			injection_points_get_global_int8('slot-epoch-old-pid')::int4
		) >
		injection_points_get_global_int8('slot-epoch-old-epoch') AS slot_epoch_advanced;
}

session ctl
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }
step wake_start		{ SELECT injection_points_wakeup('start-after-vxid-publication'); }
step detach_start	{ SELECT injection_points_detach('start-after-vxid-publication'); }

permutation reset wc_prepare wc_finish wake_after(wc_finish) detach_after w_begin_new o_reuse_epoch wake_start(w_begin_new) detach_start w_rollback
permutation reset wa_prepare wa_finish wake_after(wa_finish) detach_after w_begin_new o_reuse_epoch wake_start(w_begin_new) detach_start w_rollback
