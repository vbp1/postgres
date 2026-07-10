
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
cmp_ok($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'>', 0, 'ring holds valid current-generation slots after stress');

# --- restart bumps the durable generation ------------------------------

my $stale = $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)');
$node->restart;
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'0', 'no slot belongs to the new generation after restart');
is( $node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(false)'),
	$stale, 'stale slots still CRC-valid, only the generation gates them');

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
