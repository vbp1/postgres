
# Copyright (c) 2025, PostgreSQL Global Development Group

# The apply-pass dedup comparator, exercised through crafted batch files.
# The runtime write path cannot produce two surviving copies of one page —
# with dwb_retire_workers = 0 a sequential writer keeps reusing the lowest
# ring index — so test_dwb_craft_batch() writes the competing candidates
# directly: same page, two batches, chosen LSNs and batch_ids.  The DEBUG1
# "restoring ... from batch N" line names the winning slot exactly.
#
# The same helper drives the standby scenario: a crafted slot whose LSN lies
# beyond the standby's minRecoveryPoint must make the startup apply-pass
# raise it before consistency can be declared.

use strict;
use warnings FATAL => 'all';
use FindBin;
use lib $FindBin::RealBin;
use DWBTest;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('dwb_craft');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_batch_timeout_ms = 20
autovacuum = off
bgwriter_lru_maxpages = 0
log_min_messages = debug1
));
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

$primary->safe_psql('postgres', q(
	CREATE TABLE tlsn AS SELECT g AS id FROM generate_series(1, 100) g;
	CREATE TABLE ttie AS SELECT g AS id FROM generate_series(1, 100) g;
));
$primary->safe_psql('postgres', 'CHECKPOINT');

my $tlsn_file =
  $primary->data_dir . '/'
  . $primary->safe_psql('postgres', "SELECT pg_relation_filepath('tlsn')");
my $ttie_file =
  $primary->data_dir . '/'
  . $primary->safe_psql('postgres', "SELECT pg_relation_filepath('ttie')");
my $tlsn_relnum = $primary->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'tlsn'");
my $ttie_relnum = $primary->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'ttie'");

# --- higher LSN wins the dedup; on a tie the higher batch_id does ---------

# All crafted LSNs must stay at or below the real WAL insert position (they
# become page LSNs of live pages) and above the blocks' current disk LSNs:
# pad the WAL a little and take LSNs from just below the insert position.
$primary->safe_psql('postgres',
	"SELECT pg_logical_emit_message(false, 'dwb', repeat('x', 256))")
  for (1 .. 2);
my $insert_lsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
my ($lsn_a, $lsn_b) = split /\|/,
  $primary->safe_psql('postgres',
	"SELECT '$insert_lsn'::pg_lsn - 16, '$insert_lsn'::pg_lsn - 8");

# The LSN pair goes into batches 14/15.  The tie pair goes into 12/13 with
# the HIGHER batch_id in the LOWER batch index, so a comparator that merely
# kept the later-scanned candidate would pick the wrong slot.
$primary->safe_psql('postgres', qq(
	SELECT test_dwb_craft_batch(14, 501, $tlsn_relnum, 0, '$lsn_a', 'DWBLSNLOSER');
	SELECT test_dwb_craft_batch(15, 502, $tlsn_relnum, 0, '$lsn_b', 'DWBLSNWINNER');
	SELECT test_dwb_craft_batch(12, 601, $ttie_relnum, 0, '$insert_lsn', 'DWBTIEWINNER');
	SELECT test_dwb_craft_batch(13, 600, $ttie_relnum, 0, '$insert_lsn', 'DWBTIELOSER');
));
$primary->stop('immediate');

my $log_offset = -s $primary->logfile;
$primary->start;
ok( $primary->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$tlsn_relnum fork 0 from batch 15!,
		$log_offset),
	'the higher-LSN copy won the dedup');
ok( $primary->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$ttie_relnum fork 0 from batch 12!,
		$log_offset),
	'on equal LSNs the higher batch_id won, against scan order');

like(read_block($tlsn_file, 0), qr/DWBLSNWINNER/,
	'winning image is on disk');
unlike(read_block($tlsn_file, 0), qr/DWBLSNLOSER/,
	'... and the losing image is not');
like(read_block($ttie_file, 0), qr/DWBTIEWINNER/,
	'winning tie image is on disk');
unlike(read_block($ttie_file, 0), qr/DWBTIELOSER/,
	'... and the losing tie image is not');

is( $primary->safe_psql(
		'postgres', 'SELECT count(*) FROM tlsn UNION ALL SELECT count(*) FROM ttie'),
	"100\n100", 'both repaired pages read back fine');

# --- a crafted slot beyond minRecoveryPoint raises it on the standby ------

$primary->backup('bkp');
my $standby = PostgreSQL::Test::Cluster->new('dwb_craft_standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres', q(
	CREATE TABLE tmrp AS SELECT g AS id FROM generate_series(1, 100) g;
));
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);
# the restartpoint flushes tmrp's block to the standby's disk
$standby->safe_psql('postgres', 'CHECKPOINT');
my $tmrp_relnum = $standby->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'tmrp'");
my $tmrp_file =
  $standby->data_dir . '/'
  . $standby->safe_psql('postgres', "SELECT pg_relation_filepath('tmrp')");

# Hold replay while the primary moves ahead: the standby then holds
# received-but-unreplayed WAL, and any LSN inside it lies beyond the
# standby's minRecoveryPoint yet within the WAL it can replay to.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$primary->safe_psql('postgres',
	"SELECT pg_logical_emit_message(false, 'dwb', repeat('x', 1024))");
my $mrp_lsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$primary->wait_for_catchup($standby, 'flush', $mrp_lsn);

$standby->safe_psql('postgres',
	"SELECT test_dwb_craft_batch(15, 700, $tmrp_relnum, 0, '$mrp_lsn', 'DWBMRPMARK')");
$standby->stop('immediate');

$log_offset = -s $standby->logfile;
$standby->start;
ok( $standby->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$tmrp_relnum fork 0 from batch 15!,
		$log_offset),
	'crafted slot applied on the standby');
my $mrp_re = quotemeta($mrp_lsn);
ok( $standby->log_contains(
		qr/raising minimum recovery point to $mrp_re to cover pages repaired from the double write buffer/,
		$log_offset),
	'minimum recovery point raised to the applied LSN');
like(read_block($tmrp_file, 0), qr/DWBMRPMARK/,
	'crafted image is on the standby disk');

$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM tmrp'),
	'100', 'standby reads the repaired page fine');

done_testing();
