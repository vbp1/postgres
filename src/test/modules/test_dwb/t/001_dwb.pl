
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
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# --- single-session cycles ---------------------------------------------

is( $node->safe_psql('postgres', 'SELECT test_dwb_cycle(40)'),
	'3', 'overflow-sealed cycle retires three batches');
# eager retirement reuses batch file 0 within the cycle: its final content
# is the 8-slot tail write, plus 16 slots in batch file 1
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
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
	sysseek($bf, $offset, 0) // die "seek: $!";
	die "read: $!" unless sysread($bf, my $byte, 1) == 1;
	sysseek($bf, $offset, 0) // die "seek: $!";
	die "write: $!" unless syswrite($bf, chr(ord($byte) ^ 0xFF), 1) == 1;
	close $bf;
	return;
}

# a torn image: one flipped byte inside slot 0's page image
flip_byte($bfile, 4096 + 100);
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'23', 'image_crc rejects a torn page image');

# a torn meta: one flipped byte inside slot 1's meta (offset 24 + 56)
flip_byte($bfile, 24 + 56);
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'22', 'meta_crc rejects a torn slot meta');

# --- concurrent writers over a small ring ------------------------------

# 3 clients x 30 transactions x 40 pages through a 16x16 ring
my $script = $node->basedir . '/stress.sql';
open my $fh, '>', $script or die $!;
print $fh "SELECT test_dwb_stress(1, 40);\n";
close $fh;
$node->command_ok(
	[ 'pgbench', '-n', '-c', '3', '-j', '3', '-t', '30', '-f', $script,
		'postgres' ],
	'concurrent stress over a small ring');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16 allocated=0 sealed=0 written=0 fsynced=0 data_written=0 retiring=0/,
	'ring fully retired after concurrent stress');
my $valid = $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)');
cmp_ok($valid, '>', 0, 'ring holds valid current-generation slots after stress');
cmp_ok($valid, '<=', 16 * 16, 'slot count bounded by the ring capacity');

# --- restart bumps the durable generation ------------------------------

my $stale = $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)');
cmp_ok($stale, '>', 0, 'ring holds slots before the restart check');
$node->restart;
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'0', 'no slot belongs to the new generation after restart');
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)'),
	$stale, 'stale slots still CRC-valid, only the generation gates them');

# --- process exit cleanup ----------------------------------------------

# A backend dies holding unpublished slots: DWBProcExit poisons them, the
# next seal's coverage wait is satisfied by the poison bits, the batch
# completes and the aborted slots never validate.
my $bg = $node->background_psql('postgres');
$bg->query_safe('SELECT test_dwb_leak(3, false)');
$bg->quit;
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/allocated=1/, 'abandoned batch stays open');
is( $node->safe_psql('postgres', 'SELECT test_dwb_force_seal()'),
	't', 'abandoned batch seals');
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE '%retiring=1%'")
  or die 'timed out waiting for the abandoned batch to reach RETIRING';
is( $node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'abandoned batch retires');
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'0', 'aborted slots are not apply candidates');

# A backend dies after publishing: the copies are still written out and
# validate; the leader's own pin hands the batch over to retirement even
# though no writer is left alive.
$bg = $node->background_psql('postgres');
$bg->query_safe('SELECT test_dwb_leak(3, true)');
$bg->quit;
is( $node->safe_psql('postgres', 'SELECT test_dwb_force_seal()'),
	't', 'orphaned batch seals');
# the dead backend's ProcExit may still be releasing its refs: wait for
# the FSYNCED -> RETIRING hand-off instead of assuming it already happened
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE '%retiring=1%'")
  or die 'timed out waiting for the orphaned batch to reach RETIRING';
is( $node->safe_psql('postgres', 'SELECT test_dwb_retire()'),
	'1', 'orphaned batch retires');
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'3', 'published slots of a dead backend survive and validate');
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16/, 'ring fully idle after the orphan hand-off');

# --- stale open must not hijack a reopened index ------------------------

my ($rc, $out, $err) =
  $node->psql('postgres', 'SELECT test_dwb_open_stale()');
is($rc, 0, 'stale open leaves the live reopened batch alone')
  or diag($err);
like(
	$node->safe_psql('postgres', 'SELECT test_dwb_states()'),
	qr/free=16/, 'ring idle after the stale-open scenario');

# --- geometry is fixed by the on-disk control file ---------------------

$node->stop;
$node->append_conf('postgresql.conf', 'dwb_num_batches = 32');
my $ret = $node->start(fail_ok => 1);
is($ret, 0, 'start refused after geometry change');
ok( $node->log_contains('was created with dwb_num_batches = 16'),
	'geometry mismatch reported');
$node->append_conf('postgresql.conf', 'dwb_num_batches = 16');
$node->start;
$node->stop;

# the second geometry GUC is enforced independently
my $log_offset = -s $node->logfile;
$node->append_conf('postgresql.conf', 'dwb_batch_pages = 32');
$ret = $node->start(fail_ok => 1);
is($ret, 0, 'start refused after batch_pages change');
ok( $node->log_contains('was created with dwb_num_batches = 16 and dwb_batch_pages = 16',
		$log_offset),
	'batch_pages mismatch reported');
$node->append_conf('postgresql.conf', 'dwb_batch_pages = 16');
$node->start;
$node->stop;

# --- double_writes requires data checksums -----------------------------

my $node2 = PostgreSQL::Test::Cluster->new('dwb_nochecksums');
$node2->init(extra => ['--no-data-checksums']);
$node2->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$ret = $node2->start(fail_ok => 1);
is($ret, 0, 'start refused without data checksums');
ok( $node2->log_contains('requires data checksums'),
	'checksum requirement reported');

done_testing();
