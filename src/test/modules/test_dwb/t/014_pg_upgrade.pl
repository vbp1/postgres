
# Copyright (c) 2025, PostgreSQL Global Development Group

# pg_upgrade and the ring: the new cluster is a fresh initdb, so nothing of
# pg_dwb/ ever transfers — the old ring stays with the old cluster, and the
# upgraded cluster cold-starts a ring of its own on its first double_writes
# start.  A same-version upgrade exercises the whole path, including
# pg_upgrade's own starts of the cleanly-stopped double_writes old cluster.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dwb_conf = qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_batch_timeout_ms = 20
autovacuum = off
);

my $old = PostgreSQL::Test::Cluster->new('dwb_upgrade_old');
$old->init;
$old->append_conf('postgresql.conf', $dwb_conf);
$old->start;
$old->safe_psql('postgres', q(
	CREATE TABLE dwb_up AS SELECT g AS id FROM generate_series(1, 100) g;
));
$old->safe_psql('postgres', 'CHECKPOINT');
$old->stop;
ok(-f $old->data_dir . '/pg_dwb/control',
	'the old cluster leaves a ring behind');

my $new = PostgreSQL::Test::Cluster->new('dwb_upgrade_new');
$new->init;

my $bindir = $new->config_data('--bindir');
command_ok(
	[
		'pg_upgrade', '--no-sync',
		'--old-datadir' => $old->data_dir,
		'--new-datadir' => $new->data_dir,
		'--old-bindir' => $bindir,
		'--new-bindir' => $bindir,
		'--socketdir' => $new->host,
		'--old-port' => $old->port,
		'--new-port' => $new->port,
	],
	'pg_upgrade from a double_writes cluster succeeds');

ok(-f $old->data_dir . '/pg_dwb/control',
	'the old ring stays with the old cluster');
ok(!-d $new->data_dir . '/pg_dwb',
	'nothing of the ring was shipped into the new cluster');

# the upgraded cluster starts its double_writes life cold
$new->append_conf('postgresql.conf', $dwb_conf);
my $log_offset = -s $new->logfile;
$new->start;
ok( $new->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$log_offset),
	'the upgraded cluster cold-starts a fresh ring');
is( $new->safe_psql('postgres', 'SELECT count(*) FROM dwb_up'),
	'100', 'the upgraded data is intact');

done_testing();
