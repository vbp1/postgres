
# Copyright (c) 2025, PostgreSQL Global Development Group

# The cleaner worker pool: the bgwriter's LRU scan hands its flush bins
# to a shared-memory queue and dwb_cleaner_workers background workers
# execute them.  Queue entries are hints — every claim is reclassified
# right before the write — so the scenarios here drive the queue with
# deterministic one-page claims: a cold dirty page is written, a pinned
# page and an already-clean page are skipped, a full queue makes the
# bgwriter defer its bin and pause the scan instead of writing.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_cleaners');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_cleaner_workers = 2
shared_buffers = 2MB
bgwriter_delay = 10ms
bgwriter_lru_maxpages = 1000
bgwriter_lru_multiplier = 10
checkpoint_timeout = 1h
autovacuum = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

$node->poll_query_until('postgres',
	"SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'dwb cleaner'"
) or die 'timed out waiting for the cleaner workers to start';
pass('two cleaner workers are visible in pg_stat_activity');

sub counter
{
	my ($name) = @_;
	return $node->safe_psql('postgres',
		"SELECT $name FROM test_dwb_cleaner_counters()");
}

# A quiet baseline: nothing dirty, the bgwriter has nothing to scan.
$node->safe_psql('postgres', 'CHECKPOINT');

# --- a stale-claim queue entry naming a cold dirty page is written -------

# enqueue_block cools the page as it queues the claim (production pages
# go cold when the clock hand sweeps past); the page is dirty and nobody
# holds it, so this claim must be written.  The system is quiet after
# the checkpoint, so background-class DWB pages can only come from the
# cleaner executing this claim — pinned by the class-page growth with
# the deferral counter standing still (the queue was never full, so the
# bgwriter had nothing to defer and writes nothing itself anyway).
my $written_before = counter('written');
my $deferred_before = counter('deferred');
my $bg_pages_before = $node->safe_psql('postgres',
	"SELECT sum(pages) FROM test_dwb_seal_stats() WHERE wclass = 'background'"
);
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_cold (id int);
	INSERT INTO t_cold VALUES (1);
));
is( $node->safe_psql(
		'postgres', "SELECT test_dwb_enqueue_block('t_cold', 0)"),
	't',
	'the queue accepted a claim on a cold dirty page');
$node->poll_query_until('postgres',
	"SELECT written > $written_before FROM test_dwb_cleaner_counters()")
  or die 'timed out waiting for a cleaner to write the cold dirty page';
pass('a cleaner wrote the queued cold dirty page');
$node->poll_query_until(
	'postgres', qq(
	SELECT sum(pages) > $bg_pages_before FROM test_dwb_seal_stats()
	WHERE wclass = 'background'
)) or die 'timed out waiting for the background-class DWB batch';
is(counter('deferred'), $deferred_before,
	'the background-class write was the cleaner, with no deferrals');

# --- a claim on a page that became hot is skipped ------------------------

# The pin is held across statements by an open transaction in a second
# session; the enqueue then races nothing — the claim is stale from the
# start and the reclassification must drop it without waiting.
my $skipped_before = counter('skipped');
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_hot (id int);
	INSERT INTO t_hot VALUES (1);
));
my $pinner = $node->background_psql('postgres');
$pinner->query_safe('BEGIN');
$pinner->query_safe("SELECT test_dwb_pin_block('t_hot', 0)");

is( $node->safe_psql('postgres', "SELECT test_dwb_enqueue_block('t_hot', 0)"),
	't',
	'the queue accepted a claim on a pinned page');
$node->poll_query_until('postgres',
	"SELECT skipped > $skipped_before FROM test_dwb_cleaner_counters()")
  or die 'timed out waiting for the pinned page claim to be skipped';
pass('the claim on the pinned page was skipped, not written');

$pinner->query_safe('SELECT test_dwb_unpin_block()');
$pinner->query_safe('COMMIT');

# A rollback releases the pin through the resource owner; the helpers
# must notice and be usable again in the next transaction.
$pinner->query_safe('BEGIN');
$pinner->query_safe("SELECT test_dwb_pin_block('t_hot', 0)");
$pinner->query_safe('ROLLBACK');
$pinner->query_safe('BEGIN');
$pinner->query_safe("SELECT test_dwb_pin_block('t_hot', 0)");
$pinner->query_safe('SELECT test_dwb_unpin_block()');
$pinner->query_safe('COMMIT');
$pinner->quit;
pass('the pin helpers survive a rollback and pin again');

# --- a duplicate claim finds the page clean and is skipped ---------------

# t_cold's page was written by the first scenario; a second claim on the
# same block must resolve as a skip (clean already), never a rewrite.
$skipped_before = counter('skipped');
is( $node->safe_psql(
		'postgres', "SELECT test_dwb_enqueue_block('t_cold', 0)"),
	't',
	'the queue accepted a duplicate claim');
$node->poll_query_until('postgres',
	"SELECT skipped > $skipped_before FROM test_dwb_cleaner_counters()")
  or die 'timed out waiting for the duplicate claim to be skipped';
pass('the duplicate claim on the clean page was skipped');

# --- the production path: scan feeds the queue, the pool executes --------

# A workload well past shared_buffers leaves plenty of cold dirty pages
# behind; the bgwriter's scan bins them into the queue and the pool
# must execute them (the class and statistics attribution have their
# own quiet-window scenarios).
my $organic_before = counter('written');
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_organic AS
		SELECT g AS id, repeat('o', 300) AS filler
		FROM generate_series(1, 20000) g;
	UPDATE t_organic SET filler = repeat('p', 300) WHERE id % 5 = 0;
));
$node->poll_query_until('postgres',
	"SELECT written >= $organic_before + 16 FROM test_dwb_cleaner_counters()")
  or die 'timed out waiting for the pool to execute scan-produced bins';
pass('the pool executed at least one full scan-produced bin');
# pg_stat_io aggregates by process type, so the pool shows up under
# 'background worker' (bgw_type granularity exists only in
# pg_stat_activity); nothing else of that type writes DWB batches here.
$node->poll_query_until(
	'postgres', q(
	SELECT sum(writes) > 0 FROM pg_stat_io
	WHERE object = 'dwb' AND backend_type = 'background worker'
)) or die 'timed out waiting for the cleaner pg_stat_io dwb rows';
pass('the cleaners show their DWB writes in pg_stat_io');

# --- a full queue makes the bgwriter defer bins, never write them --------

SKIP:
{
	skip 'injection points not supported by this build', 8
	  unless defined $ENV{enable_injection_points}
	  && $ENV{enable_injection_points} eq 'yes';

	my $bgw_io_before = $node->safe_psql('postgres',
		"SELECT coalesce(sum(writes), 0) FROM pg_stat_io WHERE object = 'dwb' AND backend_type = 'background writer'"
	);

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('dwb-cleaner-loop', 'wait')");

	# Idle workers sleep on the queue's condition variable and only pass
	# the loop top — where the point sits — when woken; one targeted
	# signal per enqueued claim wakes them one by one.  The two wake-up
	# claims park both workers BEFORE any dequeue, so they stay queued.
	$node->safe_psql('postgres',
		"SELECT test_dwb_enqueue_block('t_cold', 0)");
	$node->safe_psql('postgres',
		"SELECT test_dwb_enqueue_block('t_cold', 0)");
	$node->poll_query_until(
		'postgres', q(
		SELECT count(*) FILTER (WHERE wait_event = 'dwb-cleaner-loop') = 2
		FROM pg_stat_activity WHERE backend_type = 'dwb cleaner'
	)) or die 'timed out waiting for both cleaners to park at the point';
	pass('both cleaners parked at the injection point');

	# Capacity is a fixed 64 bins, two of which the wake-up claims hold;
	# 67 claims into a parked queue must overflow it (the bgwriter may
	# race bins in as well, so the accepted count is bounded, not exact).
	# A refused test claim defers nothing — the deferral counter is the
	# bgwriter's own bookkeeping, checked right below.
	$node->safe_psql(
		'postgres', q(
		CREATE TABLE t_fill AS
			SELECT g AS id, repeat('x', 800) AS filler
			FROM generate_series(1, 800) g;
	));
	my $accepted = 0;
	for my $blk (0 .. 66)
	{
		$accepted++
		  if $node->safe_psql('postgres',
			"SELECT test_dwb_enqueue_block('t_fill', $blk)") eq 't';
	}
	cmp_ok($accepted, '<=', 62,
		'the queue turned the overflow claims away at its capacity');

	# With the pool parked and the queue full, the bgwriter defers: each
	# refused bin bumps the counter, the bin is carried over, nothing is
	# written by the bgwriter itself (checked once the dust settles).
	my $deferred_solo = counter('deferred');
	$node->safe_psql(
		'postgres', q(
		CREATE TABLE t_solo AS
			SELECT g AS id, repeat('y', 800) AS filler
			FROM generate_series(1, 2000) g;
		UPDATE t_solo SET filler = repeat('z', 800) WHERE id % 3 = 0;
	));
	$node->poll_query_until('postgres',
		"SELECT deferred > $deferred_solo FROM test_dwb_cleaner_counters()")
	  or die 'timed out waiting for the bgwriter to defer bins';
	pass('the bgwriter deferred bins while the pool was parked');

	# Disabling the LRU scan must stop the deferral stream: the carried
	# bin is dropped, the queue is no longer polled.  The workers are
	# still parked, so nothing else can move the counter.
	$node->append_conf('postgresql.conf', 'bgwriter_lru_maxpages = 0');
	$node->reload;
	my ($def_prev, $def_now) = (-1, -2);
	my $deadline = time() + 30;
	while (time() < $deadline)
	{
		$def_now = counter('deferred');
		last if $def_now == $def_prev;
		$def_prev = $def_now;
		sleep 1;
	}
	is(counter('deferred'), $def_now,
		'the deferral stream stopped once the scan was disabled');

	# Detach BEFORE waking: a woken worker loops back to the point, and
	# with it still attached it would park again with no wakeup left.
	# Each wakeup releases ONE waiter (the first waiter slot matching the
	# point name), so keep nudging until both workers are off the point —
	# a worker left parked here would sleep on the injection DSM forever
	# and its eventual FATAL exit would touch the detached segment.
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('dwb-cleaner-loop')");
	$deadline = time() + 30;
	while (time() < $deadline)
	{
		last
		  if $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'dwb-cleaner-loop'"
		  ) == 0;
		$node->psql('postgres',
			"SELECT injection_points_wakeup('dwb-cleaner-loop')");
	}
	is( $node->safe_psql(
			'postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'dwb-cleaner-loop'"
		),
		'0',
		'both cleaners left the injection point');

	# The released pool drains the queue, but the disabled scan feeds it
	# nothing: enqueued freezes even under a dirty workload (the dropped
	# carry-over never lands either — it would show up right here).
	$node->poll_query_until('postgres',
		'SELECT queued = 0 FROM test_dwb_cleaner_counters()')
	  or die 'timed out waiting for the released pool to drain the queue';
	my $enq_frozen = counter('enqueued');
	$node->safe_psql('postgres',
		"UPDATE t_solo SET filler = repeat('w', 800) WHERE id % 4 = 0");
	sleep 2;
	is(counter('enqueued'), $enq_frozen,
		'a disabled scan feeds the pool nothing');

	# By now seconds have passed since the deferral workload, well past
	# the statistics flush interval: had the bgwriter written any bin
	# itself, its pg_stat_io row would show it.
	is( $node->safe_psql(
			'postgres',
			"SELECT coalesce(sum(writes), 0) FROM pg_stat_io WHERE object = 'dwb' AND backend_type = 'background writer'"
		),
		$bgw_io_before,
		'the bgwriter wrote no bins itself throughout');

	# Re-enabling the scan resumes the feed.
	$node->append_conf('postgresql.conf', 'bgwriter_lru_maxpages = 1000');
	$node->reload;
	$node->safe_psql('postgres',
		"UPDATE t_solo SET filler = repeat('v', 800) WHERE id % 5 = 0");
	$node->poll_query_until('postgres',
		"SELECT enqueued > $enq_frozen FROM test_dwb_cleaner_counters()")
	  or die 'timed out waiting for the re-enabled scan to feed the pool';
	pass('the re-enabled scan resumed feeding the pool');
}

# --- the queue drains and the counters reconcile -------------------------

# Every accepted claim ends as written or skipped, nothing else: once the
# queue is empty, enqueued = written + skipped exactly.
$node->poll_query_until(
	'postgres', q(
	SELECT queued = 0 AND enqueued = written + skipped
	FROM test_dwb_cleaner_counters()
)) or die 'timed out waiting for the queue to drain and reconcile';
pass('the drained queue reconciles: enqueued = written + skipped');

# --- one quiet claim moves buffers_clean and republishes pg_stat_io ------

# With the queue drained and the system quiet, let buffers_clean settle
# (pending folds of the workload above trickle in with the bgwriter's
# reporting), then drive exactly one pool write and pin the attribution:
# buffers_clean grows while the deferral counter stands still, and the
# worker's pg_stat_io row reflects the new write too (that the report is
# forced, not merely allowed by the stats interval, has its own
# injection-point scenario below).
my ($bclean_base, $bclean_prev) = (0, -1);
my $deadline = time() + 60;
while (time() < $deadline)
{
	$bclean_base = $node->safe_psql('postgres',
		'SELECT buffers_clean FROM pg_stat_bgwriter');
	last if $bclean_base == $bclean_prev;
	$bclean_prev = $bclean_base;
	sleep 1;
}
my $deferred_base = counter('deferred');
my $io_base = $node->safe_psql('postgres',
	"SELECT sum(writes) FROM pg_stat_io WHERE object = 'dwb' AND backend_type = 'background worker'"
);
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_fold (id int);
	INSERT INTO t_fold VALUES (1);
));
is( $node->safe_psql(
		'postgres', "SELECT test_dwb_enqueue_block('t_fold', 0)"),
	't',
	'the queue accepted the attribution claim');
$node->poll_query_until(
	'postgres', qq(
	SELECT buffers_clean > $bclean_base FROM pg_stat_bgwriter
)) or die 'timed out waiting for the pool write to reach buffers_clean';
is(counter('deferred'), $deferred_base,
	'the buffers_clean growth came through the pool fold alone');
$node->poll_query_until(
	'postgres', qq(
	SELECT sum(writes) > $io_base FROM pg_stat_io
	WHERE object = 'dwb' AND backend_type = 'background worker'
)) or die 'timed out waiting for the pg_stat_io row to grow again';
pass('the attribution write reached pg_stat_io as well');

# --- a terminated worker is restarted, the pool keeps its size -----------

# The workers exit FATAL on SIGTERM (exit code 1): a zero exit would
# unregister the worker for good and one stray terminate would shrink
# the pool permanently.
my $victim = $node->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE backend_type = 'dwb cleaner' LIMIT 1"
);
$node->safe_psql('postgres', "SELECT pg_terminate_backend($victim)");
$node->poll_query_until(
	'postgres', qq(
	SELECT count(*) = 2 AND count(*) FILTER (WHERE pid = $victim) = 0
	FROM pg_stat_activity WHERE backend_type = 'dwb cleaner'
)) or die 'timed out waiting for the terminated cleaner to be replaced';
pass('a terminated cleaner was restarted and the pool is back to size');

# --- crash recovery on top of pool-cleaned data --------------------------

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_cold'),
	'1', 'data intact after crash recovery');

# --- the idle-time report is forced, not deferred ------------------------

# The dwb-cleaner-reported point sits right AFTER pgstat_report_stat:
# a parked worker has published everything it wrote so far.  The next
# claim is executed milliseconds after the wakeup, so the worker's next
# report attempt lands well inside PGSTAT_MIN_INTERVAL of its previous
# one — a non-forced report would be suppressed there, and since the
# worker then sleeps indefinitely, the write would never surface.  The
# delta becoming visible is therefore the forced report and nothing
# else.  One worker, so no second cleaner can publish the delta on its
# own schedule.
SKIP:
{
	skip 'injection points not supported by this build', 2
	  unless defined $ENV{enable_injection_points}
	  && $ENV{enable_injection_points} eq 'yes';

	$node->stop;
	$node->append_conf('postgresql.conf', 'dwb_cleaner_workers = 1');
	$node->start;

	$node->safe_psql(
		'postgres', q(
		CREATE TABLE t_pub (id int);
		INSERT INTO t_pub VALUES (1);
	));
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('dwb-cleaner-reported', 'wait')");

	# The waker claim: the sleeping worker only reaches the point after
	# processing something and going idle again.
	$node->safe_psql('postgres', "SELECT test_dwb_enqueue_block('t_pub', 0)");
	$node->wait_for_event('dwb cleaner', 'dwb-cleaner-reported');
	pass('the worker parked right after publishing its statistics');

	my $io_pub = $node->safe_psql('postgres',
		"SELECT sum(writes) FROM pg_stat_io WHERE object = 'dwb' AND backend_type = 'background worker'"
	);
	$node->safe_psql(
		'postgres', q(
		INSERT INTO t_pub VALUES (2);
	));
	$node->safe_psql('postgres', "SELECT test_dwb_enqueue_block('t_pub', 0)");

	$node->safe_psql('postgres',
		"SELECT injection_points_detach('dwb-cleaner-reported')");
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('dwb-cleaner-reported')");

	$node->poll_query_until(
		'postgres', qq(
		SELECT sum(writes) > $io_pub FROM pg_stat_io
		WHERE object = 'dwb' AND backend_type = 'background worker'
	)) or die 'timed out waiting for the forced back-to-back publication';
	pass('the report published a delta inside the minimum stats interval');
}

# --- a pool larger than the free worker slots refuses to start -----------

$node->stop;
$node->append_conf(
	'postgresql.conf', qq(
max_worker_processes = 2
dwb_cleaner_workers = 8
));
my $ret = $node->start(fail_ok => 1);
is($ret, 0, 'start with an oversized cleaner pool fails');
ok( $node->log_contains(
		qr/"dwb_cleaner_workers" \(8\) needs more "max_worker_processes" slots/
	),
	'the refusal names the pool size and the slot shortage');

done_testing();
