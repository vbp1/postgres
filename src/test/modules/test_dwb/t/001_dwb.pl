
# Copyright (c) 2025, PostgreSQL Global Development Group

# Concurrency, restart (generation) and enforcement tests for the
# short-lived double write buffer.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb');
$node->init;
# dwb_retire_workers = 0 keeps batch sealing and retirement fully under the
# test's control (writers retire synchronously); the quiescing settings keep
# background flushes from opening batches between the state assertions.
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
autovacuum = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# --- single-session cycles ---------------------------------------------

is($node->safe_psql('postgres', 'SELECT test_dwb_cycle(40)'),
	'3', 'overflow-sealed cycle retires three batches');
# eager retirement reuses batch file 0 within the cycle: its final content
# is the 8-slot tail write, plus 16 slots in batch file 1
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'24', 'all surviving slots validate (meta_crc, generation, image_crc)');

# --- torn copies are rejected by the on-disk CRCs ----------------------

# Batch file layout with dwb_batch_pages = 16: a 24-byte header, 16 slot
# metas of 56 bytes each (sizes pinned by StaticAssertDecl in dwb.h), the
# meta region padded to 4096; page images follow at 4096 + slot * 8192.
my $bfile = $node->data_dir . '/pg_dwb/batch_0001';

sub flip_byte
{
	my ($file, $offset) = @_;
	open my $bf, '+<:raw', $file or die "open $file: $!";
	sysseek($bf, $offset, 0) or die "seek: $!";
	die "read: $!" unless sysread($bf, my $byte, 1) == 1;
	sysseek($bf, $offset, 0) or die "seek: $!";
	die "write: $!" unless syswrite($bf, chr(ord($byte) ^ 0xFF), 1) == 1;
	close $bf;
	return;
}

# a torn image: one flipped byte inside slot 0's page image
flip_byte($bfile, 4096 + 100);
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'23', 'image_crc rejects a torn page image');

# a torn meta: one flipped byte inside slot 1's meta (offset 24 + 56)
flip_byte($bfile, 24 + 56);
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'22', 'meta_crc rejects a torn slot meta');

# --- concurrent writers over a small ring ------------------------------

# 3 clients x 30 transactions x 40 pages through a 16x16 ring
my $script = $node->basedir . '/stress.sql';
open my $fh, '>', $script or die $!;
print $fh "SELECT test_dwb_stress(1, 40);\n";
close $fh;
$node->command_ok(
	[
		'pgbench', '-n', '-c', '3', '-j', '3',
		'-t', '30', '-f', $script, 'postgres'
	],
	'concurrent stress over a small ring');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16 allocated=0 sealed=0 written=0 fsynced=0 data_written=0 retiring=0/,
	'ring fully retired after concurrent stress');
my $valid = $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)');
cmp_ok($valid, '>', 0,
	'ring holds valid current-generation slots after stress');
cmp_ok($valid, '<=', 16 * 16, 'slot count bounded by the ring capacity');

# --- restart bumps the durable generation ------------------------------

# The shutdown checkpoint itself streams pages through the ring, so exact
# slot counts cannot survive a restart; the invariants that must hold are
# that CRC-valid slots exist and that none of them belongs to the new
# generation.
my $stale = $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)');
cmp_ok($stale, '>', 0, 'ring holds slots before the restart check');
$node->restart;
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'0', 'no slot belongs to the new generation after restart');
cmp_ok($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)'),
	'>', 0, 'stale slots still CRC-valid, only the generation gates them');

# --- process exit cleanup ----------------------------------------------

# A backend dies holding unpublished slots: DWBProcExit poisons them, the
# next seal's coverage wait is satisfied by the poison bits, the batch
# completes and the aborted slots never validate.
my $bg = $node->background_psql('postgres');
$bg->query_safe('SELECT test_dwb_leak(3, false)');
$bg->quit;
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/allocated=1/, 'abandoned batch stays open');
is($node->safe_psql('postgres', 'SELECT test_dwb_force_seal()'),
	't', 'abandoned batch seals');
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE '%retiring=1%'")
  or die 'timed out waiting for the abandoned batch to reach RETIRING';
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'abandoned batch retires');
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'0', 'aborted slots are not apply candidates');

# A backend dies after publishing: the copies are still written out and
# validate; the leader's own pin hands the batch over to retirement even
# though no writer is left alive.
$bg = $node->background_psql('postgres');
$bg->query_safe('SELECT test_dwb_leak(3, true)');
$bg->quit;
is($node->safe_psql('postgres', 'SELECT test_dwb_force_seal()'),
	't', 'orphaned batch seals');
# the dead backend's ProcExit may still be releasing its refs: wait for
# the FSYNCED -> RETIRING hand-off instead of assuming it already happened
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE '%retiring=1%'")
  or die 'timed out waiting for the orphaned batch to reach RETIRING';
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'orphaned batch retires');
is($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'3', 'published slots of a dead backend survive and validate');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16/, 'ring fully idle after the orphan hand-off');

# A backend dies holding the LAST ref of an already-durable batch: the exit
# backstop itself performs the FSYNCED -> RETIRING hand-off (seg_set
# publication under LWLocks), which is only legal because it runs as a
# before_shmem_exit callback while the PGPROC is still alive.
$bg = $node->background_psql('postgres');
$bg->query_safe('SELECT test_dwb_leak_fsynced()');
$bg->quit;
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE '%retiring=1%'")
  or die 'timed out waiting for the exit-time publication';
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'batch published from the exit backstop retires');

# --- transaction abort releases refs (ResourceOwner path) ---------------

# An ERROR with unpublished refs: the abort poisons the slots, and the
# batch seals and completes later exactly like the dead-backend case —
# without a process exit.
my ($rc, $out, $err) =
  $node->psql('postgres', 'SELECT test_dwb_abort_release(3, false)');
isnt($rc, 0, 'deliberate abort with pending refs reported');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/allocated=1/, 'batch of the aborted transaction stays open');
is($node->safe_psql('postgres', 'SELECT test_dwb_force_seal()'),
	't', 'batch of the aborted transaction seals');
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'batch of the aborted transaction retires');

# An ERROR after the batch is durable: the abort cleanup goes through the
# abandoned-slot ref hand-off (the fake relation exits via the
# dropped-relation branch) and must still hand the batch over to retirement.
($rc, $out, $err) =
  $node->psql('postgres', 'SELECT test_dwb_abort_after_fsync()');
isnt($rc, 0, 'deliberate abort after batch fsync reported');
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'batch of the post-fsync abort retires');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16/, 'ring idle after the abort scenarios');

# --- torn data page repaired from the batch copy on abort ----------------

# A REAL relation this time: test_dwb_torn_repair stages the pristine
# on-disk image of block 0 into the DWB, tears the block on disk, and
# aborts.  The ResourceOwner release must rewrite the block from the
# durable batch copy (DWBRewriteAbandonedSlot).  The restart proves the
# repair reached the data file: the buffer cache is dropped, and with data
# checksums a block left torn would make the read below fail.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_repair AS
		SELECT g AS id, repeat('r', 64) AS pad FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
my $filenode =
  $node->safe_psql('postgres', "SELECT pg_relation_filenode('dwb_repair')");
($rc, $out, $err) =
  $node->psql('postgres', "SELECT test_dwb_torn_repair($filenode, 0)");
isnt($rc, 0, 'deliberate abort after tearing the data page reported');
like($err, qr/deliberate abort after tearing/, 'the tear scenario ran');
is($node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'batch of the torn-page scenario retires');
$node->restart;
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_repair'),
	'100', 'torn block repaired from the batch copy (checksum-clean read)');

# --- the sliced reserves shape who may open what --------------------------

# On this geometry DWB_BG_RESERVE = Max(1, 16/32) = 1 and DWB_EVICT_RESERVE
# = Max(2, 16/8) = 2: a background-class writer filling a fresh ring stops
# above the middle eviction slice, an eviction-class writer consumes
# everything but the bottom background slice, and the background class can
# still open that last batch — the starvation-proof lane of the
# checkpointer.
$bg = $node->background_psql('postgres');
my $bg_taken = $bg->query_safe('SELECT test_dwb_fill_ring(true)');
cmp_ok($bg_taken, '>', 0, 'background class filled the ring');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=3 /, 'background fill stops above the eviction slice');
my $ev_taken = $bg->query_safe('SELECT test_dwb_fill_ring(false)');
cmp_ok($ev_taken, '>', 0, 'eviction class still opens batches');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=1 /, 'eviction leaves the bottom background slice');
my $lane_taken = $bg->query_safe('SELECT test_dwb_fill_ring(true)');
cmp_ok($lane_taken, '>', 0, 'background class opens its reserved lane');
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=0 /, '... consuming the ring fully');
$bg->quit;
$node->poll_query_until('postgres',
		"SELECT CASE WHEN test_dwb_force_seal(false) IS NOT NULL THEN "
	  . "CASE WHEN test_dwb_force_seal(true) IS NOT NULL THEN "
	  . "CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=16 %' END END END")
  or die 'timed out waiting for the ring to drain after the reserve scenario';

# --- stale open must not hijack a reopened index ------------------------

($rc, $out, $err) = $node->psql('postgres', 'SELECT test_dwb_open_stale()');
is($rc, 0, 'stale open leaves the live reopened batch alone')
  or diag($err);
like($node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16/, 'ring idle after the stale-open scenario');

# --- a geometry change recreates the ring ------------------------------

# The on-disk layout follows the geometry GUCs, so a change rebuilds the
# ring from scratch (after applying the old one if needed — exercised in
# t/011_geometry_recovery.pl); the generation restarts with the fresh
# control file.
$node->stop;
my $log_offset = -s $node->logfile;
$node->append_conf('postgresql.conf', 'dwb_num_batches = 32');
$node->start;
ok( $node->log_contains(
		'recreating double write buffer ring: geometry changed from 16 batches of 16 pages to 32 batches of 16 pages',
		$log_offset),
	'geometry change recreates the ring');
ok( $node->log_contains(
		qr/ring opened: 32 batches of 16 pages, generation 1\b/, $log_offset),
	'recreated ring opens with a fresh generation');
$node->stop;
$log_offset = -s $node->logfile;
$node->append_conf(
	'postgresql.conf',
	'dwb_num_batches = 16
dwb_batch_pages = 32');
$node->start;
ok( $node->log_contains(
		'geometry changed from 32 batches of 16 pages to 16 batches of 32 pages',
		$log_offset),
	'batch_pages change recreates the ring too');
$node->stop;
$node->append_conf('postgresql.conf', 'dwb_batch_pages = 16');
$node->start;
$node->stop;

# --- the retire worker pool must actually fit into the worker slots ------

# The logical replication launcher takes a slot before the pool registers;
# RegisterBackgroundWorker itself only LOGs on overflow, so the pool checks
# the remaining capacity and refuses to start a silently smaller pool.
$log_offset = -s $node->logfile;
$node->append_conf(
	'postgresql.conf', qq(
max_worker_processes = 1
dwb_retire_workers = 1
));
my $ret = $node->start(fail_ok => 1);
is($ret, 0, 'start refused when the pool does not fit into worker slots');
ok( $node->log_contains(
		'needs more "max_worker_processes" slots than remain free',
		$log_offset),
	'worker slot shortage reported');
$node->append_conf(
	'postgresql.conf', qq(
max_worker_processes = 8
dwb_retire_workers = 0
));
$node->start;
$node->stop;

# --- double_writes requires data checksums -----------------------------

my $node2 = PostgreSQL::Test::Cluster->new('dwb_nochecksums');
$node2->init(extra => ['--no-data-checksums']);
$node2->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$ret = $node2->start(fail_ok => 1);
is($ret, 0, 'start refused without data checksums');
ok($node2->log_contains('requires data checksums'),
	'checksum requirement reported');

done_testing();
