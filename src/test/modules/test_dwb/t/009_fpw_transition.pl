
# Copyright (c) 2025, PostgreSQL Global Development Group

# A primary restarted into io_torn_pages_protection = double_writes emits
# no XLOG_FPW_CHANGE (UpdateFullPageWrites at startup runs before recovery
# is marked done, and the checkpointer's later call sees no remaining
# change), so the checkpoints written after the restart are the only
# replayed evidence that page images stopped.  Crossing that transition
# must fail pg_backup_stop() for an online backup opened on a standby,
# and a standby without a double write buffer of its own must warn once
# per startup.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('dwb_fpw_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = full_pages
autovacuum = off
));
$primary->start;

$primary->backup('bkp');
my $standby = PostgreSQL::Test::Cluster->new('dwb_fpw_standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres',
	'CREATE TABLE dwb_fpw AS SELECT g AS id FROM generate_series(1, 1000) g');
$primary->wait_for_catchup($standby);

# --- an online backup opened on the standby while page images flow -------

# on_error_stop off: the session must survive the expected pg_backup_stop()
# error below
my $backer = $standby->background_psql('postgres', on_error_stop => 0);
$backer->query_safe("SELECT pg_backup_start('dwb_fpw_transition')");

# --- the primary crosses into double_writes via a restart ----------------

my $warn_offset = -s $standby->logfile;
$primary->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$primary->restart;

$primary->safe_psql('postgres',
	'INSERT INTO dwb_fpw SELECT g FROM generate_series(1001, 2000) g');
$primary->safe_psql('postgres', 'CHECKPOINT');
# a second image-less checkpoint proves the warning below does not repeat
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);

# --- the DWB-less standby warns exactly once -----------------------------

my $log = slurp_file($standby->logfile, $warn_offset);
my @warnings = $log =~
  /(replaying WAL generated without full page images, but this server does not use the double write buffer)/g;
is(scalar(@warnings), 1,
	'standby without a ring of its own warns exactly once per startup');

# --- the open backup cannot be closed cleanly ----------------------------

my $stop_offset = -s $standby->logfile;
my ($out, $errored) = $backer->query('SELECT * FROM pg_backup_stop()');
ok($errored, 'pg_backup_stop() on the standby fails across the transition');
ok( $standby->log_contains(
		qr/WAL generated without full page images was replayed during online backup/,
		$stop_offset),
	'... naming the replayed image-less WAL');
$backer->quit;

done_testing();
