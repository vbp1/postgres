
# Copyright (c) 2025, PostgreSQL Global Development Group

# Hot standby under io_torn_pages_protection = double_writes: the standby
# runs its own ring while replaying, both sides survive crashes, promotion
# drains a replay backlog through the ring, the minRecoveryPoint contract
# holds for replay-driven flushes, and a base backup initiated on the
# standby is refused loudly.

use strict;
use warnings FATAL => 'all';
use FindBin;
use lib $FindBin::RealBin;
use DWBTest;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('dwb_primary');
$primary->init(allows_streaming => 1);
# The workload table (~3.4 MB) exceeds shared_buffers, so replay on the
# standby must evict through its ring; fsync stays ON so the restartpoint
# ProcessSyncRequests path runs its DWB wrap for real.
$primary->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_batch_timeout_ms = 20
shared_buffers = 2MB
autovacuum = off
fsync = on
));
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# --- a standby provisioned from a base backup cold-starts its ring -------

$primary->backup('bkp');
my $standby = PostgreSQL::Test::Cluster->new('dwb_standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
my $standby_log_offset = (-s $standby->logfile) // 0;
$standby->start;

# pg_dwb/ is excluded from the backup, so the standby must open a fresh
# ring rather than inherit the primary's.
ok( $standby->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$standby_log_offset),
	'standby cold-started a fresh ring from the base backup');

# --- the retire worker pool runs during recovery -------------------------

$standby->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'dwb retire worker'"
) or die 'timed out waiting for the standby retire worker to start';
pass('retire worker is running on the standby during recovery');

# --- replay traffic flows through the standby ring -----------------------

$primary->safe_psql('postgres', q(
	CREATE TABLE dwb_t AS
		SELECT g AS id, repeat('x', 300) AS filler
		FROM generate_series(1, 10000) g;
	UPDATE dwb_t SET filler = repeat('y', 300) WHERE id % 10 = 0;
));
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);

# The workload exceeds the standby's shared_buffers, so the startup
# process itself must have evicted dirty pages through the standby's own
# DWB write path.  Startup flushes its stats when it replays the
# XLOG_RUNNING_XACTS record the primary's CHECKPOINT above emitted, but
# that is asynchronous to wait_for_catchup — hence the poll.
$standby->poll_query_until('postgres',
	"SELECT COALESCE(sum(writes), 0) > 0 FROM pg_stat_io "
	  . "WHERE object = 'dwb' AND backend_type = 'startup'")
  or die 'timed out waiting for startup-process DWB writes on the standby';
pass('replay evictions flowed through the standby ring');

is( $standby->safe_psql('postgres',
		"SELECT count(*) FROM dwb_t WHERE filler = repeat('y', 300)"),
	'1000', 'replayed page contents are correct');

# --- FlushBuffer on the standby advances minRecoveryPoint ----------------

# Take the baseline right after a restartpoint, then push replay-eviction
# traffic with NO further checkpoint or restartpoint anywhere: any advance
# past the baseline can then come only from buffer flushes — XLogFlush in
# recovery does not fsync WAL, it calls UpdateMinRecoveryPoint instead
# (see 3.9 of the design plan).
$standby->safe_psql('postgres', 'CHECKPOINT');
my $mrp_before = $standby->safe_psql('postgres',
	'SELECT min_recovery_end_lsn FROM pg_control_recovery()');
$primary->safe_psql('postgres',
	"UPDATE dwb_t SET filler = repeat('m', 300) WHERE id % 9 = 0");
$primary->wait_for_catchup($standby);
$standby->poll_query_until('postgres',
	"SELECT min_recovery_end_lsn > '$mrp_before'::pg_lsn FROM pg_control_recovery()")
  or die 'minRecoveryPoint did not advance from replay-driven flushes alone';
pass('replay-driven flushes advanced minRecoveryPoint without a restartpoint');

# and the ring keeps circulating: the worker drains it back to all-free
$standby->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the standby ring to drain';
pass('standby ring drained back to all-free');

# --- a base backup initiated on the standby is refused loudly ------------

# The replayed WAL carries no page images and the primary's ring cannot
# substitute for them, so the vanilla do_pg_backup_start guard must refuse
# with a hint that names the real knob.
my $refused_path = $primary->backup_dir . '/standby_backup';
my ($out, $err) = run_command(
	[
		'pg_basebackup', '--no-sync',
		'--pgdata' => $refused_path,
		'--host' => $standby->host,
		'--port' => $standby->port,
		'--checkpoint' => 'fast'
	]);
ok(!-f "$refused_path/PG_VERSION",
	'base backup from the standby is refused');
like(
	$err,
	qr/WAL generated without full page images was replayed/,
	'... loudly');
like(
	$err,
	qr/io_torn_pages_protection/,
	'... with a hint naming the real knob');

# --- the standby survives its own crash ----------------------------------

$standby->stop('immediate');
$standby_log_offset = -s $standby->logfile;
$standby->start;
ok( $standby->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 2\b/,
		$standby_log_offset),
	'crashed standby reopened its ring under a bumped generation');

$primary->safe_psql('postgres',
	"INSERT INTO dwb_t VALUES (100001, 'after standby crash')");
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'10001', 'replication resumed after the standby crash');

# --- the primary survives its own crash ----------------------------------

$primary->stop('immediate');
$primary->start;
$primary->safe_psql('postgres',
	"INSERT INTO dwb_t VALUES (100002, 'after primary crash')");
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'10002', 'replication resumed after the primary crash');

# --- a torn page on the standby is repaired by its own apply-pass ---------

# The apply-pass on a crashed standby runs against pg_control state
# DB_IN_ARCHIVE_RECOVERY — the branch that may raise minRecoveryPoint —
# and must repair from the standby's OWN ring: the replayed WAL carries no
# page images that could do it instead.
$primary->safe_psql('postgres', q(
	CREATE TABLE ts_repair AS SELECT g AS id FROM generate_series(1, 100) g;
));
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);
my $ts_path = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('ts_repair')");
my $ts_relnum = $primary->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'ts_repair'");

# a restartpoint flushes the replayed pages through the standby's ring;
# crash right after, while the table's slot is still on disk
$standby->append_conf('postgresql.conf', 'log_min_messages = debug1');
$standby->safe_psql('postgres', 'CHECKPOINT');
$standby->stop('immediate');

my $ts_file = $standby->data_dir . '/' . $ts_path;
write_block($ts_file, 0,
	substr(read_block($ts_file, 0), 0, 4096) . ("\0" x 4096));

$standby_log_offset = -s $standby->logfile;
$standby->start;
ok( $standby->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$ts_relnum fork 0!,
		$standby_log_offset),
	'the crashed standby repaired its torn page from its own ring');
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM ts_repair'),
	'100', 'the repaired standby page reads whole');

# --- promotion with a replay backlog -------------------------------------

# Pause replay, pile up a burst, make sure it is flushed to the standby's
# local WAL, and promote with the pause still in effect: promotion breaks
# the pause (recoveryPausesHere exits on the standby trigger), so the
# whole backlog demonstrably replays through the standby's DWB write path
# before the timeline switch.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$standby->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'timed out waiting for replay to pause';
$primary->safe_psql('postgres', q(
	UPDATE dwb_t SET filler = repeat('p', 300) WHERE id % 3 = 0;
	INSERT INTO dwb_t VALUES (100003, 'burst tail');
));
$primary->wait_for_catchup($standby, 'flush', $primary->lsn('write'));
is( $standby->safe_psql('postgres',
		'SELECT pg_last_wal_replay_lsn() < pg_last_wal_receive_lsn()'),
	't', 'a real replay backlog exists at promotion time');
$standby->promote;

is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'10003', 'promoted standby replayed the whole backlog');
is( $standby->safe_psql('postgres',
		"SELECT count(*) FROM dwb_t WHERE filler = repeat('p', 300)"),
	'3334', 'backlog page contents are correct');
is( $standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'standby left recovery');

# --- the promoted node is a full DWB primary -----------------------------

my $tl2_start = $standby->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$standby->safe_psql('postgres', q(
	UPDATE dwb_t SET filler = repeat('q', 300) WHERE id % 5 = 0;
	INSERT INTO dwb_t VALUES (100004, 'after promotion');
));
my $tl2_end = $standby->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$standby->safe_psql('postgres', 'CHECKPOINT');
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'10004', 'promoted node accepts writes');
$standby->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the promoted ring to drain';
pass('promoted ring drained back to all-free');

# the new timeline still carries no page images
my ($waldump, $walerr) = run_command(
	[
		'pg_waldump', '--path' => $standby->data_dir . '/pg_wal',
		'--timeline' => 2,
		'--start' => $tl2_start, '--end' => $tl2_end
	]);
is($walerr, '', 'pg_waldump read the post-promotion window cleanly');
like($waldump, qr/Heap/, 'the window covers the post-promotion update');
unlike($waldump, qr/\bFPW\b/, 'no full-page images after promotion');

done_testing();
