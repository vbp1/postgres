
# Copyright (c) 2025, PostgreSQL Global Development Group

# pg_rewind on a double_writes pair: a live source is refused (its WAL has
# no full-page images to repair pages read mid-write), a stopped source
# works, the target's own ring is wiped by the rewind, and the rewound
# node cold-starts a fresh ring and follows the promoted primary.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_a = PostgreSQL::Test::Cluster->new('dwb_rewind_a');
$node_a->init(allows_streaming => 1);
$node_a->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_batch_timeout_ms = 20
autovacuum = off
wal_keep_size = 64MB
));
$node_a->start;
$node_a->safe_psql('postgres', q(
	CREATE TABLE dwb_r AS SELECT g AS id FROM generate_series(1, 100) g;
));

$node_a->backup('bkp');
my $node_b = PostgreSQL::Test::Cluster->new('dwb_rewind_b');
$node_b->init_from_backup($node_a, 'bkp', has_streaming => 1);
$node_b->start;
$node_a->wait_for_catchup($node_b);

# --- diverge the timelines -----------------------------------------------

$node_b->promote;
$node_b->safe_psql('postgres', "INSERT INTO dwb_r VALUES (100001)");
# A keeps running as the old primary and diverges past the fork point
$node_a->safe_psql('postgres', "INSERT INTO dwb_r VALUES (200001)");
$node_a->stop('fast');

# --- a live double_writes source is refused ------------------------------

command_fails_like(
	[
		'pg_rewind',
		'--target-pgdata' => $node_a->data_dir,
		'--source-server' => $node_b->connstr('postgres')
	],
	qr/"io_torn_pages_protection" must be "full_pages" in the source server/,
	'pg_rewind refuses a live source that writes no full-page images');

# --- a stopped source works and wipes the target ring --------------------

# leave proof on the target that the rewind, not a later cold start,
# removed the ring files
ok(-f $node_a->data_dir . '/pg_dwb/control',
	'the target has ring files before the rewind');

$node_b->stop('fast');
command_ok(
	[
		'pg_rewind',
		'--target-pgdata' => $node_a->data_dir,
		'--source-pgdata' => $node_b->data_dir
	],
	'pg_rewind from a stopped source succeeds');

ok(-d $node_a->data_dir . '/pg_dwb', 'the target still has a pg_dwb directory');
{
	opendir(my $dh, $node_a->data_dir . '/pg_dwb') or die "opendir: $!";
	my @entries = grep { !/^\.\.?$/ } readdir($dh);
	closedir($dh);
	is(scalar(@entries), 0, 'the rewind wiped the target ring');
}

# --- the rewound node cold-starts a ring and follows the new primary -----

$node_b->start;
# the rewind copied the source's configuration; restore this node's port
$node_a->append_conf('postgresql.conf', 'port = ' . $node_a->port);
$node_a->enable_streaming($node_b);
my $a_log_offset = -s $node_a->logfile;
$node_a->start;
ok( $node_a->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$a_log_offset),
	'rewound node cold-started a fresh ring');

$node_b->wait_for_catchup($node_a);
is( $node_a->safe_psql('postgres', 'SELECT count(*) FROM dwb_r'),
	'101', 'rewound node converged on the new primary timeline');
is( $node_a->safe_psql('postgres',
		'SELECT count(*) FROM dwb_r WHERE id = 200001'),
	'0', 'the divergent row is gone');
is( $node_a->safe_psql('postgres',
		'SELECT count(*) FROM dwb_r WHERE id = 100001'),
	'1', "the new primary's row is present");

done_testing();
