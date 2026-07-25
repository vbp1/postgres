
# Copyright (c) 2025, PostgreSQL Global Development Group

# Two ways a primary can stop writing page images, and how a full_pages
# standby reacts to each.  Disabling the legacy full_page_writes GUC (the
# primary staying in full_pages mode) is the vanilla situation: the standby
# keeps replaying, but an online backup opened on it cannot be closed.  The
# transition is carried by checkpoint records only — a restart emits no
# XLOG_FPW_CHANGE (UpdateFullPageWrites at startup runs before recovery is
# marked done, and the checkpointer's later call sees no remaining change).
# Switching the primary to io_torn_pages_protection = double_writes is a
# protocol change recorded in pg_control and XLOG_PARAMETER_CHANGE, and a
# standby still expecting full-page protection is refused outright: its own
# crash would leave torn pages nothing can repair.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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

# --- the legacy GUC is disabled under full_pages -------------------------

$primary->append_conf('postgresql.conf', 'full_page_writes = off');
$primary->restart;

$primary->safe_psql('postgres',
	'INSERT INTO dwb_fpw SELECT g FROM generate_series(1001, 2000) g');
# the post-restart checkpoint records are the only replayed evidence
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);

# --- the open backup cannot be closed cleanly ----------------------------

my $stop_offset = -s $standby->logfile;
my ($out, $errored) = $backer->query('SELECT * FROM pg_backup_stop()');
ok($errored, 'pg_backup_stop() on the standby fails across the transition');
ok( $standby->log_contains(
		qr/WAL generated without full page images was replayed during online backup/,
		$stop_offset),
	'... naming the replayed image-less WAL');
ok( $standby->log_contains(
		qr/the primary has "full_page_writes" disabled/, $stop_offset),
	'... and blaming the legacy GUC, not the mode');
$backer->quit;

# --- the primary switches to double_writes -------------------------------

# The standby still runs full_pages: replaying the XLOG_PARAMETER_CHANGE
# that announces the mode must be fatal, and the whole standby exits.
my $fatal_offset = -s $standby->logfile;
$primary->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$primary->restart;
$primary->safe_psql('postgres',
	'INSERT INTO dwb_fpw SELECT g FROM generate_series(2001, 3000) g');

foreach my $i (1 .. 300)
{
	last unless -f $standby->data_dir . '/postmaster.pid';
	usleep(100_000);
}
ok(!-f $standby->data_dir . '/postmaster.pid',
	'full_pages standby dies replaying the double_writes transition');
# the node died on its own; let the harness notice before restarting it
$standby->stop('fast', fail_ok => 1);
ok( $standby->log_contains(
		qr/FATAL: .* WAL was generated with "io_torn_pages_protection=double_writes", cannot continue recovering with "io_torn_pages_protection=full_pages"/,
		$fatal_offset),
	'... with the incompatibility spelled out');

# The refusal is durable: the mode is in the standby's pg_control now, so a
# restart is refused up front, before any replay.
$fatal_offset = -s $standby->logfile;
my $ret = $standby->start(fail_ok => 1);
is($ret, 0, 'restarting the full_pages standby is refused up front');
ok( $standby->log_contains(
		qr/WAL was generated with "io_torn_pages_protection=double_writes"/,
		$fatal_offset),
	'... for the same reason');

# --- a double_writes standby follows the same primary --------------------

$standby->append_conf('postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
));
my $ring_offset = -s $standby->logfile;
$standby->start;
ok( $standby->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$ring_offset),
	'reconfigured standby cold-starts a ring of its own');
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM dwb_fpw'),
	'3000', 'and replays the image-less WAL');

done_testing();
