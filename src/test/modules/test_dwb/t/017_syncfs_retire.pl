
# Copyright (c) 2025, PostgreSQL Global Development Group

# dwb_retire_sync_method = syncfs: one syncfs() per retire round replaces
# the per-segment fdatasync protocol, and every batch that was RETIRING
# when the round began is freed wholesale.  fsync stays ON so the rounds
# issue real syncfs() calls; the crash at the end proves the durability
# chain end to end.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_syncfs');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
fsync = on
shared_buffers = 2MB
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
autovacuum = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# syncfs is a build-time option (Linux); skip where the GUC cannot take it
if ($node->safe_psql('postgres',
			"SELECT 'syncfs' = ANY(enumvals) FROM pg_settings "
		  . "WHERE name = 'dwb_retire_sync_method'") ne 't')
{
	plan skip_all => 'syncfs not supported by this build';
}

$node->safe_psql('postgres',
	'ALTER SYSTEM SET dwb_retire_sync_method = syncfs');
$node->reload;
$node->poll_query_until('postgres',
	"SELECT current_setting('dwb_retire_sync_method') = 'syncfs'")
  or die 'timed out waiting for the syncfs retire method to apply';

# A workload well past shared_buffers streams evictions through the ring;
# the worker's syncfs rounds must keep freeing batches for it to finish.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_syncfs_t AS
		SELECT g AS id, repeat('s', 300) AS filler
		FROM generate_series(1, 20000) g;
	UPDATE dwb_syncfs_t SET filler = repeat('f', 300) WHERE id % 5 = 0;
));
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_syncfs_t'),
	'20000', 'workload survived the syncfs retire path');

# The worker alone must drain the ring: the poll only nudges the tail
# batch closed, all the freeing is the worker's wholesale rounds.
$node->poll_query_until('postgres',
		"SELECT CASE WHEN test_dwb_force_seal() IS NOT NULL THEN "
	  . "test_dwb_states() LIKE 'free=16 %' END")
  or die 'timed out waiting for syncfs rounds to drain the ring';
pass('the worker drained the ring through syncfs rounds');

# Crash recovery on top of syncfs-retired data: intact.
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_syncfs_t'),
	'20000', 'data intact after crash recovery');

done_testing();
