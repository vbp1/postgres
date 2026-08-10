
# Copyright (c) 2025, PostgreSQL Global Development Group

# dwb_retire_sync_method = syncfs: one syncfs() per retire round replaces
# the per-segment fdatasync protocol, and every batch that was RETIRING
# when the round began is freed wholesale.  Two workers prove the round
# gate (DWBSyncfsRoundLock) admits one process at a time; fsync stays ON
# so the rounds issue real syncfs() calls; the crash at the end proves
# the durability chain end to end.

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
dwb_retire_workers = 2
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

$node->poll_query_until('postgres',
	"SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'dwb retire worker'"
) or die 'timed out waiting for the retire workers to start';

# --- the round gate admits exactly one process ---------------------------

# The dwb-syncfs-round point sits INSIDE the trylock-guarded round, so a
# process can only park there after winning DWBSyncfsRoundLock.  While a
# winner is parked there, ALL wholesale retirement is frozen, so this
# scenario must stay ring-quiet: one parked batch via test_dwb_park() and
# pg_stat_activity polls, no eviction workload.
SKIP:
{
	skip 'injection points not supported by this build', 3
	  unless defined $ENV{enable_injection_points}
	  && $ENV{enable_injection_points} eq 'yes';

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('dwb-syncfs-round', 'wait')");

	# One RETIRING batch gives the winner's resumed round something to
	# free; the client never touches the gate (park does not retire).
	$node->safe_psql('postgres', 'SELECT test_dwb_park(99001)');

	# The workers race the gate on their own interval clock: one parks at
	# the point, the loser's tries all fail and put it back to sleep.
	$node->wait_for_event('dwb retire worker', 'dwb-syncfs-round');
	pass('a worker won the gate and parked at the point');

	$node->poll_query_until(
		'postgres', q(
		SELECT count(*) FILTER (WHERE wait_event = 'dwb-syncfs-round') = 1
		   AND count(*) FILTER (WHERE wait_event = 'DwbRetireMain') = 1
		FROM pg_stat_activity WHERE backend_type = 'dwb retire worker'
	))
	  or die 'timed out waiting for the gate loser to sleep in its main loop';
	pass('exactly one process is inside the syncfs round gate');

	# Detach BEFORE waking: the winner re-enters the round on its next
	# cycle, and with the point still attached it would park again with no
	# wakeup left to release it.
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('dwb-syncfs-round')");
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('dwb-syncfs-round')");

	# The resumed round collects and frees the parked batch.
	$node->poll_query_until('postgres',
		"SELECT test_dwb_states() LIKE 'free=16 %'")
	  or die 'timed out waiting for the resumed round to free the batch';
	pass('the resumed round freed the parked batch');
}

# --- syncfs rounds drain a real workload ---------------------------------

# A workload well past shared_buffers streams evictions through the ring;
# the workers' syncfs rounds must keep freeing batches for it to finish.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_syncfs_t AS
		SELECT g AS id, repeat('s', 300) AS filler
		FROM generate_series(1, 20000) g;
	UPDATE dwb_syncfs_t SET filler = repeat('f', 300) WHERE id % 5 = 0;
));
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_syncfs_t'),
	'20000', 'workload survived the syncfs retire path');

# The workers alone must drain the ring: the poll only nudges the tail
# batch closed, all the freeing is the pool's wholesale rounds.
$node->poll_query_until('postgres',
		"SELECT CASE WHEN test_dwb_force_seal() IS NOT NULL THEN "
	  . "test_dwb_states() LIKE 'free=16 %' END")
  or die 'timed out waiting for syncfs rounds to drain the ring';
pass('the workers drained the ring through syncfs rounds');

# Crash recovery on top of syncfs-retired data: intact.
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_syncfs_t'),
	'20000', 'data intact after crash recovery');

done_testing();
