
# Copyright (c) 2025, PostgreSQL Global Development Group

# Retirement fallback paths: the checkpointer's ProcessSyncRequests as the
# only retire point, and the synchronous OOM retire when DWSegmentHash
# overflows.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_retire');
$node->init;
# The default 64x64 geometry lets RETIRING batches pin more distinct
# segments (64 x 64 = 4096) than the smallest segment hash can hold; no
# workers and no background flushers keep retirement fully under the test's
# control.  fsync must be ON: with fsync = off ProcessSyncRequests skips
# its whole per-file block, including the DWBSegmentFsyncBegin/End wrap
# this test exists to exercise.
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 64
dwb_batch_pages = 64
dwb_max_segments = 1024
dwb_retire_workers = 0
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
autovacuum = off
fsync = on
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# --- a CHECKPOINT alone retires a batch (ProcessSyncRequests path) -------

$node->safe_psql('postgres', q(
	CREATE TABLE dwb_ckpt AS
		SELECT g AS id, repeat('c', 64) AS pad FROM generate_series(1, 100) g;
));
my $filenode =
  $node->safe_psql('postgres', "SELECT pg_relation_filenode('dwb_ckpt')");

# Warmup: run the same statements once and CHECKPOINT, so every catalog
# hint bit they dirty is flushed now.  The real run below then leaves no
# dirty buffer behind, the final CHECKPOINT has nothing to feed through the
# DWB write path (whose no-pool finish would retire our batch as a side
# effect), and only the DWBSegmentFsyncBegin/End wrap of
# ProcessSyncRequests can free the parked batch.
$node->safe_psql('postgres', "SELECT test_dwb_checkpoint_pending($filenode)");
$node->safe_psql('postgres', 'SELECT test_dwb_states()');
$node->safe_psql('postgres', 'CHECKPOINT');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/^free=64 /, 'warmup batch retired');

$node->safe_psql('postgres', "SELECT test_dwb_checkpoint_pending($filenode)");
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/retiring=1/, 'one batch parked in RETIRING with a pending sync request');
$node->safe_psql('postgres', 'CHECKPOINT');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/^free=64 /, 'CHECKPOINT alone retired the parked batch');

# --- segment hash overflow degrades to synchronous retire ----------------

my $log_offset = -s $node->logfile;
my ($rc, $out, $err) =
  $node->psql('postgres', 'SELECT test_dwb_fill_segments(30)');
is($rc, 0, 'segment fill survived the hash overflow');
is($out, 30 * 64, 'thirty batches of unique segments published');
like(
	$err,
	qr/double write buffer segment hash is full/,
	'hash overflow warning reached the publisher');
ok( $node->log_contains('double write buffer segment hash is full',
		$log_offset),
	'hash overflow logged');

# The batches parked in RETIRING drain through the normal sweep (dropped
# fake segments count as covered); the OOM-retired ones are already free.
$node->poll_query_until('postgres',
	"SELECT CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=64 %' END")
  or die 'timed out waiting for the ring to drain after the hash overflow';
pass('ring drained after the hash overflow');

done_testing();
