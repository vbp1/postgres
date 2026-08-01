
# Copyright (c) 2025, PostgreSQL Global Development Group

# Stage B backpressure policy tests: with the ring exhausted, a stalled
# non-critical writer gets an ERROR (dwb_on_stall = error) and the cluster
# stays up, while a stalled checkpointer always PANICs by role policy and
# the cluster crash-recovers.  The dwb-force-stall injection point makes
# the current wait escalate immediately instead of after
# dwb_write_timeout_ms; everything else is the production code path.

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

my $node = PostgreSQL::Test::Cluster->new('dwb_stall');
$node->init;
# No retire workers and no background flushers: the ring stays exactly as
# exhausted as test_dwb_fill_ring() leaves it.  shared_buffers is sized so
# that ONLY the deliberately oversized victim workload evicts dirty pages —
# incidental sessions (attach/detach, liveness probes) must never touch the
# exhausted ring, or they would stall in its place.
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_on_stall = error
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
autovacuum = off
shared_buffers = 16MB
restart_after_crash = on
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Dirty pages for the checkpointer scenario, created while the ring is
# still healthy and small enough to stay in shared_buffers.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_dirty AS
		SELECT g AS id, repeat('d', 300) AS filler
		FROM generate_series(1, 1000) g;
));

# --- ERROR in a non-critical writer keeps the cluster alive --------------

# Attach while the ring is still healthy; the point only fires for a
# process already stuck waiting for ring space.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('dwb-force-stall', 'notice')");

my $filler = $node->background_psql('postgres');
my $taken = $filler->query_safe('SELECT test_dwb_fill_ring()');
cmp_ok($taken, '>', 0, 'ring exhausted by leaked refs');

# The victim outgrows shared_buffers, so it must evict its own dirty pages
# through the exhausted ring.  Its rollback drops its buffers unwritten,
# leaving the pool clean for the sessions that follow.
my ($rc, $out, $err) = $node->psql(
	'postgres', q(
	CREATE TABLE dwb_victim AS
		SELECT g AS id, repeat('v', 300) AS filler
		FROM generate_series(1, 80000) g;
));
isnt($rc, 0, 'stalled eviction fails instead of hanging');
like(
	$err,
	qr/double write buffer retirement made no progress/,
	'stall ERROR reported to the writer');

is($node->safe_psql('postgres', 'SELECT 1'),
	'1', 'cluster alive after the writer ERROR');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('dwb-force-stall')");

# Releasing the leaked refs lets the abandoned batches finish.  With no
# worker pool, drive sealing and retirement from the poll itself (nested
# CASEs order the side effects before the state probe).
$filler->quit;
$node->poll_query_until('postgres',
		"SELECT CASE WHEN test_dwb_force_seal() IS NOT NULL THEN "
	  . "CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=16 %' END END")
  or die 'timed out waiting for the ring to drain after the ERROR scenario';

# --- a stalled checkpointer PANICs by role policy -------------------------

# Dirty the pages for BufferSync while the ring is still healthy, THEN
# exhaust it: the UPDATE itself must not stall.
$node->safe_psql('postgres',
	"UPDATE dwb_dirty SET filler = repeat('e', 300) WHERE id % 2 = 0");

$filler = $node->background_psql('postgres');
$taken = $filler->query_safe('SELECT test_dwb_fill_ring()');
cmp_ok($taken, '>', 0, 'ring exhausted again for the checkpointer scenario');

# An eviction fill stops at the bottom background slice, which is exactly
# the checkpointer's guaranteed lane — consume it too, or the checkpoint
# below would simply proceed through it instead of stalling.
$taken = $filler->query_safe('SELECT test_dwb_fill_ring(true)');
cmp_ok($taken, '>', 0, 'the background lane is consumed as well');

$node->safe_psql('postgres',
	"SELECT injection_points_attach('dwb-force-stall', 'notice')");

my $log_offset = -s $node->logfile;
($rc, $out, $err) = $node->psql('postgres', 'CHECKPOINT');
isnt($rc, 0, 'CHECKPOINT fails when the checkpointer PANICs');

# probing with psql during the restart window trips over dying sockets;
# wait for the crash-recovery cycle in the log instead
$node->wait_for_log(qr/database system is ready to accept connections/,
	$log_offset);
pass('cluster restarted after the checkpointer PANIC');
ok( $node->log_contains(
		'double write buffer retirement made no progress', $log_offset),
	'checkpointer stall escalated to the role-policy PANIC');

is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_dirty'),
	'1000', 'data intact after crash recovery');

# --- Stage A warning fires on the real clock ------------------------------

# No injection point this time: shrink the real thresholds and let a victim
# writer walk through Stage A (WARNING after dwb_slow_warn_ms) into Stage B
# (ERROR after dwb_write_timeout_ms, dwb_on_stall = error).  The bgwriter
# pause of Stage A has no SQL-visible probe and stays untested here.
$node->append_conf(
	'postgresql.conf', qq(
dwb_slow_warn_ms = 100
dwb_write_timeout_ms = 1000
));
$node->reload;

$filler = $node->background_psql('postgres');
$taken = $filler->query_safe('SELECT test_dwb_fill_ring()');
cmp_ok($taken, '>', 0, 'ring exhausted for the slow-warn scenario');

my $retries0 =
  $node->safe_psql('postgres', 'SELECT test_dwb_ring_wait_retries()');

($rc, $out, $err) = $node->psql('postgres', 'SELECT test_dwb_cycle(1)');
isnt($rc, 0, 'victim writer errors out on the real stall clock');

# Anti-spin regression: nothing woke the victim during its ~1s of waiting
# (no retire, no leader write), so its wait iterations must be paced by the
# 1s sleep timeout — a handful, not the thousands a self-waking rotation of
# the probe-released staging buffer would produce.
my $retries1 =
  $node->safe_psql('postgres', 'SELECT test_dwb_ring_wait_retries()');
cmp_ok($retries1 - $retries0, '>=', 1,
	'the stalled victim slept in the wait');
cmp_ok($retries1 - $retries0,
	'<=', 10, 'ring wait paced by the sleep timeout, not a busy rotation');
like(
	$err,
	qr/double write buffer has no free batch after/,
	'Stage A warning reached the writer');
like(
	$err,
	qr/double write buffer retirement made no progress/,
	'Stage B error reached the writer');

$filler->quit;
$node->poll_query_until('postgres',
		"SELECT CASE WHEN test_dwb_force_seal() IS NOT NULL THEN "
	  . "CASE WHEN test_dwb_retire() >= 0 THEN "
	  . "test_dwb_states() LIKE 'free=16 %' END END")
  or die
  'timed out waiting for the ring to drain after the slow-warn scenario';

done_testing();
