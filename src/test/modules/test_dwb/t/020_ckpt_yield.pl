
# Copyright (c) 2025, PostgreSQL Global Development Group

# Pressure-aware checkpoint pacing: while the cleaner pool's bin queue
# is hot, the checkpointer keeps napping slightly past its schedule,
# inside a bounded slice of the completion-target slack.  The scenario
# parks the pool at an injection point, fills the queue past the hot
# threshold, and lets a timed (non-immediate — SQL CHECKPOINT would set
# CHECKPOINT_IMMEDIATE and bypass the branch) checkpoint pace itself
# over a large dirty set: stock pacing oscillates around the schedule
# boundary, writing roughly one page per nap, so with the queue hot a
# stream of pressure naps must register — and the checkpoint must still
# complete while the pressure persists, proving the yield budget is
# bounded rather than a stall.  A second timed checkpoint with the pool
# released and the queue drained must add no pressure naps.
#
# The file needs real timed-checkpoint cycles (checkpoint_timeout has a
# 30 s floor), so it runs for a bit over a minute by construction.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (!defined $ENV{enable_injection_points}
	|| $ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('dwb_ckpt_yield');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_cleaner_workers = 2
shared_buffers = 32MB
bgwriter_lru_maxpages = 0
checkpoint_timeout = 30s
checkpoint_completion_target = 0.5
log_checkpoints = on
autovacuum = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

$node->poll_query_until('postgres',
	"SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'dwb cleaner'"
) or die 'timed out waiting for the cleaner workers to start';

sub counter
{
	my ($name) = @_;
	return $node->safe_psql('postgres',
		"SELECT $name FROM test_dwb_cleaner_counters()");
}

sub ckpts_done
{
	return $node->safe_psql('postgres',
		'SELECT num_done FROM pg_stat_checkpointer');
}

# Park both workers before any dequeue: idle workers sleep on the queue
# condition variable and pass the loop top — where the point sits —
# only when woken, one targeted signal per enqueued claim.
$node->safe_psql('postgres',
	q(CREATE TABLE t_park AS SELECT 1 AS id, repeat('p', 100) AS filler));
$node->safe_psql('postgres',
	"SELECT injection_points_attach('dwb-cleaner-loop', 'wait')");
$node->safe_psql('postgres', "SELECT test_dwb_enqueue_block('t_park', 0)");
$node->safe_psql('postgres', "SELECT test_dwb_enqueue_block('t_park', 0)");
$node->poll_query_until(
	'postgres', q(
	SELECT count(*) FILTER (WHERE wait_event = 'dwb-cleaner-loop') = 2
	FROM pg_stat_activity WHERE backend_type = 'dwb cleaner'
)) or die 'timed out waiting for both cleaners to park at the point';
pass('both cleaners parked at the injection point');

# Fill the queue to capacity and keep it there: the scan is disabled
# and the pool is parked, so nothing drains it below the hot threshold
# (half of the 64-bin capacity) for the rest of the pressure phase.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_fill AS
		SELECT g AS id, repeat('x', 800) AS filler
		FROM generate_series(1, 800) g;
));
for my $blk (0 .. 66)
{
	$node->psql('postgres', "SELECT test_dwb_enqueue_block('t_fill', $blk)");
}
cmp_ok(counter('queued'), '>=', 32, 'the bin queue is hot');

# The dirty set the checkpoint paces over; it stays in shared buffers
# (no LRU scan, pool parked).  It must be much larger than the pacing
# can absorb at one page per nap (completion window / nap quantum ≈ 150
# slots), or the checkpointer never falls behind schedule at an
# evaluation point and the pressure branch is never reached — ~3000
# pages against 150 slots keeps it behind for most of the window.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_dirt AS
		SELECT g AS id, repeat('d', 800) AS filler
		FROM generate_series(1, 24000) g;
));

my $naps_before = counter('pressure_naps');
my $done_before = ckpts_done();

# The next timed checkpoint runs with the queue hot the whole way (the
# workers stay parked through the assertion).  Completing under
# sustained pressure is itself the boundedness proof.
$node->poll_query_until('postgres',
	"SELECT num_done > $done_before FROM pg_stat_checkpointer")
  or die 'timed out waiting for the timed checkpoint under pressure';
pass('the timed checkpoint completed under sustained queue pressure');

cmp_ok(counter('pressure_naps'),
	'>', $naps_before,
	'the checkpointer took pressure naps while the queue was hot');

# Release the pool: detach BEFORE waking (a woken worker loops back to
# the point and would re-park with no wakeup left), then nudge until
# both are off the point — each wakeup releases one waiter.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('dwb-cleaner-loop')");
my $deadline = time() + 30;
while (time() < $deadline)
{
	last
	  if $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'dwb-cleaner-loop'"
	  ) == 0;
	$node->psql('postgres',
		"SELECT injection_points_wakeup('dwb-cleaner-loop')");
}
$node->poll_query_until('postgres',
	'SELECT queued = 0 FROM test_dwb_cleaner_counters()')
  or die 'timed out waiting for the released pool to drain the queue';
pass('the released pool drained the queue');

# Control: a comparable dirty set, the queue empty — the next timed
# (again non-immediate) checkpoint must add no pressure naps.
$node->safe_psql('postgres',
	"UPDATE t_dirt SET filler = repeat('e', 800) WHERE id % 2 = 0");
my $naps_quiet = counter('pressure_naps');
my $done_quiet = ckpts_done();
$node->poll_query_until('postgres',
	"SELECT num_done > $done_quiet FROM pg_stat_checkpointer")
  or die 'timed out waiting for the control timed checkpoint';
is(counter('pressure_naps'),
	$naps_quiet, 'no pressure naps without queue pressure');

done_testing();
