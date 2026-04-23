# Stage 3 H1-E characterization: the current ordinary writer path keeps the
# published ordinary mirror epoch aligned with the current slot epoch through
# the post-helper freeze point, and the next top-level transaction starts a
# strictly newer slot epoch before publishing a new virtual xid.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE csn_ordinary_mirror_epoch_data (id int PRIMARY KEY, val int);
	INSERT INTO csn_ordinary_mirror_epoch_data VALUES (1, 0);
}
teardown
{
	DROP TABLE csn_ordinary_mirror_epoch_data;
	DROP EXTENSION injection_points;
}

session seed
step reset
{
	UPDATE csn_ordinary_mirror_epoch_data SET val = 0 WHERE id = 1;
	SELECT injection_points_set_global_int8('mirror-pid', 0);
	SELECT injection_points_set_global_int8('mirror-old-slot-epoch', 0);
}

session writer
step wc_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8('mirror-pid', pg_backend_pid());
	SELECT injection_points_set_global_int8(
		'mirror-old-slot-epoch',
		injection_points_backend_slot_epoch(pg_backend_pid())
	);
	UPDATE csn_ordinary_mirror_epoch_data SET val = 1 WHERE id = 1;
}
step wc_finish		{ COMMIT; }
step wa_prepare
{
	BEGIN;
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ordinary-after-procarray-primary', 'wait');
	SELECT injection_points_attach('start-after-vxid-publication', 'wait');
	SELECT injection_points_set_global_int8('mirror-pid', pg_backend_pid());
	SELECT injection_points_set_global_int8(
		'mirror-old-slot-epoch',
		injection_points_backend_slot_epoch(pg_backend_pid())
	);
	UPDATE csn_ordinary_mirror_epoch_data SET val = 2 WHERE id = 1;
}
step wa_finish		{ ABORT; }
step w_begin_new	{ BEGIN; }
step w_rollback		{ ROLLBACK; }

session observer
step o_after_finish
{
	SELECT injection_points_backend_ordinary_finished(
			injection_points_get_global_int8('mirror-pid')::int4
		) AS ordinary_finished,
		injection_points_backend_published_mirror_epoch(
			injection_points_get_global_int8('mirror-pid')::int4
		) =
		injection_points_backend_slot_epoch(
			injection_points_get_global_int8('mirror-pid')::int4
		) AS mirror_matches_current_epoch;
}
step o_new_epoch
{
	SELECT injection_points_backend_ordinary_finished(
			injection_points_get_global_int8('mirror-pid')::int4
		) = false AS finished_cleared,
		injection_points_backend_slot_epoch(
			injection_points_get_global_int8('mirror-pid')::int4
		) >
		injection_points_get_global_int8('mirror-old-slot-epoch') AS slot_epoch_advanced,
		injection_points_backend_published_mirror_epoch(
			injection_points_get_global_int8('mirror-pid')::int4
		) <
		injection_points_backend_slot_epoch(
			injection_points_get_global_int8('mirror-pid')::int4
		) AS mirror_epoch_now_stale;
}

session ctl
step wake_after		{ SELECT injection_points_wakeup('ordinary-after-procarray-primary'); }
step detach_after	{ SELECT injection_points_detach('ordinary-after-procarray-primary'); }
step wake_start		{ SELECT injection_points_wakeup('start-after-vxid-publication'); }
step detach_start	{ SELECT injection_points_detach('start-after-vxid-publication'); }

permutation reset wc_prepare wc_finish o_after_finish wake_after(wc_finish) detach_after w_begin_new o_new_epoch wake_start(w_begin_new) detach_start w_rollback
permutation reset wa_prepare wa_finish o_after_finish wake_after(wa_finish) detach_after w_begin_new o_new_epoch wake_start(w_begin_new) detach_start w_rollback
