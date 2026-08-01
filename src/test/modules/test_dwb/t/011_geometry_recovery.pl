
# Copyright (c) 2025, PostgreSQL Global Development Group

# A geometry change over a ring that was not cleanly closed: the apply-pass
# must run with the OLD geometry recorded in pg_dwb/control — not the new
# GUCs — before the ring is recreated.  A regression that read the GUCs
# instead would compute a wrong meta-region size, fail every slot CRC,
# report nothing to restore and leave the damage in place, silently: only
# the combination "page repaired AND ring recreated in one start" pins the
# ordering.
#
# The damage technique follows t/010_recovery.pl: dwb_retire_workers = 0
# retires every batch on the spot, so the last page flushed before the
# crash — the user table's, sorted last in BufferSync by relfilenode — is
# the one slot still on disk.

use strict;
use warnings FATAL => 'all';
use FindBin;
use lib $FindBin::RealBin;
use DWBTest;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_geometry_recovery');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_batch_timeout_ms = 20
autovacuum = off
bgwriter_lru_maxpages = 0
));
$node->start;

$node->safe_psql(
	'postgres', q(
	CREATE TABLE tgeo AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
my $tgeo_file =
	$node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('tgeo')");

# stale-page damage: put the pre-update image back after the crash, so the
# repair can only come from the ring copy's higher LSN
my $tgeo_v1 = read_block($tgeo_file, 0);
$node->safe_psql('postgres', 'UPDATE tgeo SET id = id + 1000 WHERE id <= 50');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop('immediate');

write_block($tgeo_file, 0, $tgeo_v1);

$node->append_conf('postgresql.conf', 'dwb_batch_pages = 32');
my $log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 1 of \d+ candidate pages restored/,
		$log_offset),
	'the crashed ring is applied with its recorded geometry');
ok( $node->log_contains(
		qr/recreating double write buffer ring: geometry changed from 16 batches of 16 pages to 16 batches of 32 pages/,
		$log_offset),
	'... and only then recreated under the new GUCs');
ok( $node->log_contains(
		qr/ring opened: 16 batches of 32 pages, generation 1\b/, $log_offset),
	'... with a fresh generation');
is( $node->safe_psql('postgres', 'SELECT count(*) FROM tgeo WHERE id > 1000'),
	'50',
	'the stale page carries the update again');

done_testing();
