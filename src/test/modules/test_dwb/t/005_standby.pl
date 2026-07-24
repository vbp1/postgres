
# Copyright (c) 2025, PostgreSQL Global Development Group

# Hot standby under io_torn_pages_protection = double_writes: the standby
# runs its own ring while replaying, both sides survive crashes, promotion
# works with a replay backlog, and the minRecoveryPoint contract holds for
# pages the standby flushes through the DWB.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('dwb_primary');
$primary->init(allows_streaming => 1);
# A tiny buffer pool forces replay evictions through the standby's ring;
# fsync must be ON so restartpoint ProcessSyncRequests retires batches.
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
my $standby_log_offset = -s $standby->logfile;
$standby->start;

# pg_dwb/ is excluded from the backup, so the standby must open a fresh
# ring rather than inherit the primary's.
ok( $standby->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1/,
		$standby_log_offset),
	'standby cold-started a fresh ring from the base backup');

# --- the retire worker pool runs during recovery -------------------------

$standby->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'dwb retire worker'"
) or die 'timed out waiting for the standby retire worker to start';
pass('retire worker is running on the standby during recovery');

# --- replay traffic flows through the standby ring -----------------------

my $mrp_before = $standby->safe_psql('postgres',
	'SELECT min_recovery_end_lsn FROM pg_control_recovery()');

$primary->safe_psql('postgres', q(
	CREATE TABLE dwb_t AS
		SELECT g AS id, repeat('x', 300) AS filler
		FROM generate_series(1, 50000) g;
	UPDATE dwb_t SET filler = repeat('y', 300) WHERE id % 10 = 0;
));
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);

# The workload far exceeds the standby's shared_buffers, so replay must
# have evicted dirty pages through the standby's own DWB write path.
$standby->safe_psql('postgres', 'CHECKPOINT');
is( $standby->safe_psql(
		'postgres',
		"SELECT sum(writes) > 0 FROM pg_stat_io WHERE object = 'dwb'"),
	't', 'standby replay flushed pages through its own ring');

# The restartpoint moved the minRecoveryPoint contract forward: FlushBuffer
# on the standby cannot fsync WAL itself, it advances minRecoveryPoint
# through XLogFlush instead (see 3.9 of the design plan).
is( $standby->safe_psql(
		'postgres',
		"SELECT min_recovery_end_lsn > '$mrp_before'::pg_lsn FROM pg_control_recovery()"),
	't', 'minRecoveryPoint advanced past the replayed flushes');

# and the ring keeps circulating: the worker drains it back to all-free
$standby->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the standby ring to drain';
pass('standby ring drained back to all-free');

# --- the standby survives its own crash ----------------------------------

$standby->stop('immediate');
$standby_log_offset = -s $standby->logfile;
$standby->start;
ok( $standby->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 2/,
		$standby_log_offset),
	'crashed standby reopened its ring under a bumped generation');

$primary->safe_psql('postgres', 'INSERT INTO dwb_t VALUES (100001, \'after standby crash\')');
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50001', 'replication resumed after the standby crash');

# --- the primary survives its own crash ----------------------------------

$primary->stop('immediate');
$primary->start;
$primary->safe_psql('postgres', 'INSERT INTO dwb_t VALUES (100002, \'after primary crash\')');
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50002', 'replication resumed after the primary crash');

# --- promotion with a replay backlog -------------------------------------

# Pause replay, pile up a burst, make sure it is flushed to the standby's
# local WAL, then resume and promote: the promotion completes only after
# the backlog has replayed through the standby's DWB write path.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$primary->safe_psql('postgres', q(
	UPDATE dwb_t SET filler = repeat('p', 300) WHERE id % 3 = 0;
	INSERT INTO dwb_t VALUES (100003, 'burst tail');
));
$primary->wait_for_catchup($standby, 'flush', $primary->lsn('write'));
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$standby->promote;

is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50003', 'promoted standby replayed the whole backlog');
is( $standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	'f', 'standby left recovery');

# the promoted node keeps writing through its ring as a primary
$standby->safe_psql('postgres', q(
	UPDATE dwb_t SET filler = repeat('q', 300) WHERE id % 5 = 0;
	INSERT INTO dwb_t VALUES (100004, 'after promotion');
));
$standby->safe_psql('postgres', 'CHECKPOINT');
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50004', 'promoted node accepts writes through the DWB path');
$standby->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the promoted ring to drain';
pass('promoted ring drained back to all-free');

done_testing();
