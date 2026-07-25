
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
	qr/retiring=1$/, 'one batch parked in RETIRING with a pending sync request');
$node->safe_psql('postgres', 'CHECKPOINT');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/^free=64 /, 'CHECKPOINT alone retired the parked batch');

# --- a checkpoint tolerates a live ALLOCATED batch ------------------------

# Checkpoints take no DWB barrier.  With no retire workers nothing seals
# behind our back, so the holder's open batch must stay ALLOCATED across a
# CHECKPOINT; the test seals and retires it explicitly once the holder is
# gone.
#
# This doubles as the regression test for cross-class open-pointer
# aliasing: the checkpoints above left open_batch_idx[BACKGROUND] naming a
# long-freed batch index, the holder's EVICTION-class open reuses exactly
# that index (lowest FREE), and the CHECKPOINT below makes the checkpointer
# flush the holder's login hint bits through the DWB.  Without the writer
# class stamp in next_slot_idx the checkpointer would join the holder's
# batch and, on the no-pool path, seal it.
my $holder = $node->background_psql('postgres');
$holder->query_safe('SELECT test_dwb_leak(1, true)');
my $one_open =
  'free=63 allocated=1 sealed=0 written=0 fsynced=0 data_written=0 retiring=0';
is( $node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	$one_open, 'an open ALLOCATED batch is live before the checkpoint');
$node->safe_psql('postgres', 'CHECKPOINT');
is( $node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	$one_open, 'CHECKPOINT completed and left the open batch alone');
$holder->quit;
$node->poll_query_until('postgres',
	"SELECT CASE WHEN test_dwb_force_seal() IS NOT NULL THEN "
	  . "CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=64 %' END END")
  or die 'timed out waiting for the open batch to drain after the holder quit';
pass('abandoned open batch drained');

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

# --- a leftover fsync snapshot must not be consumed by a foreign End -----

# test_dwb_stale_snapshot replays the checkpointer hazard: Begin for the
# parked segment without the matching End (the state an fsync ERROR under
# data_sync_retry = on leaves behind), then a successful Begin/End of an
# unrelated non-MD entry.  The parked batch must still be RETIRING — a
# consumed stale snapshot would have freed it without durability.
$node->safe_psql('postgres', 'SELECT test_dwb_park(98000)');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/retiring=1$/, 'batch parked for the stale-snapshot scenario');
$node->safe_psql('postgres', 'SELECT test_dwb_stale_snapshot(98000)');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/retiring=1$/, 'stale snapshot dropped, parked batch still RETIRING');
$node->poll_query_until('postgres',
	"SELECT CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=64 %' END")
  or die 'timed out waiting for the stale-snapshot batch to drain';

# --- a soft retire-fsync failure keeps the batch and releases the claim --

# With data_sync_retry = on a failed segment fsync must not throw: the
# batch stays RETIRING for a later retry and the advisory claim is
# released.  A directory planted at the fake segment's path makes the
# fsync fail deterministically (EISDIR); removing it lets the next sweep
# cover the segment — which only works if the failed attempt released the
# claim.
$node->append_conf('postgresql.conf', 'data_sync_retry = on');
$node->restart;

my $segdir = $node->data_dir . '/base/1/99000';
mkdir $segdir or die "mkdir $segdir: $!";

$node->safe_psql('postgres', 'SELECT test_dwb_park(99000)');
($rc, $out, $err) = $node->psql('postgres', 'SELECT test_dwb_retire()');
is($rc, 0, 'retire sweep survives the failing segment fsync');
is($out, '0', 'no batch freed while the segment fsync fails');
like(
	$err,
	qr/could not fsync file/,
	'soft fsync failure reported as a WARNING');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/retiring=1$/, 'batch stays RETIRING after the soft fsync failure');

rmdir $segdir or die "rmdir $segdir: $!";
is( $node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'released claim lets the next sweep cover the segment');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/^free=64 /, 'ring drained after the soft-failure scenario');

done_testing();
