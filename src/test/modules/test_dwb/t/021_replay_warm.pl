
# Copyright (c) 2025, PostgreSQL Global Development Group

# The replay warm pool: with no full-page images in the stream, a standby
# has to fetch every page it replays, and this pool does that fetching in
# background workers instead of in the startup process.  The scenarios
# here check that the pool is actually used, that over one and the same
# stretch of WAL it takes reads off the startup process, that a request a
# worker is holding survives the relation being dropped, that a worker
# killed while holding a request gives the slot back, that the pool works
# on its own with the kernel-advice prefetcher turned off, and that
# promotion with requests still outstanding is clean.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $injection_points = defined $ENV{enable_injection_points}
  && $ENV{enable_injection_points} eq 'yes';

# The workload must not fit in the standby's buffer cache, or replay would
# find every page resident and the pool would have nothing to do.
my $primary = PostgreSQL::Test::Cluster->new('warm_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
shared_buffers = 2MB
autovacuum = off
fsync = off
wal_keep_size = 256MB
));
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# The standbys park their workers at an injection point, so the extension
# has to exist in their catalogs; a standby cannot create it itself.
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;

# --- a standby with the pool enabled ------------------------------------

$primary->backup('bkp');

sub make_standby
{
	my ($name, $warm_workers, %extra) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init_from_backup($primary, 'bkp', has_streaming => 1);
	$node->append_conf(
		'postgresql.conf', qq(
shared_buffers = 2MB
max_worker_processes = 16
replay_warm_workers = $warm_workers
replay_warm_queue_size = 64
));
	# The workers hold no database connection, so a lazily loaded injection
	# point would palloc outside a transaction; preloading gives them the
	# library from the postmaster instead.
	$node->append_conf('postgresql.conf',
		"shared_preload_libraries = 'injection_points'")
	  if $injection_points;
	$node->append_conf('postgresql.conf', $extra{conf}) if $extra{conf};
	return $node;
}

my $standby = make_standby('warm_standby', 2);
$standby->start;

# The workers hold no database connection, so they have no pg_stat_activity
# row; they advertise themselves in shared memory instead.
$standby->poll_query_until('postgres',
	'SELECT count(*) = 2 FROM test_dwb_warm_worker_pids()')
  or die 'timed out waiting for the warm workers to start';
pass('both warm workers are running');

# a table several times the buffer cache, so replay must fetch pages
$primary->safe_psql(
	'postgres', q(
	CREATE TABLE t AS
		SELECT g AS id, repeat('x', 200) AS filler
		FROM generate_series(1, 40000) g;
	CHECKPOINT;
));
$primary->wait_for_catchup($standby, 'replay');
$primary->safe_psql('postgres',
	"UPDATE t SET filler = repeat('y', 200) WHERE id % 3 = 0");
$primary->wait_for_catchup($standby, 'replay');

sub warm_counters
{
	my ($node) = @_;
	my %c;
	@c{
		qw(published dropped_full collected missed stale cancelled
		  released claimed reads hits failed discarded vanished)
	  }
	  = split /\|/,
	  $node->safe_psql('postgres', 'SELECT * FROM test_dwb_warm_counters()');
	return \%c;
}

my $warm = warm_counters($standby);

cmp_ok($warm->{published}, '>', 0, 'blocks were published to the warm pool');
cmp_ok($warm->{claimed}, '>', 0, 'workers claimed published blocks');
cmp_ok($warm->{reads}, '>', 0, 'workers read pages for replay');
cmp_ok($warm->{collected}, '>', 0,
	'replay collected pages the workers had read');
is($warm->{discarded}, 0, 'no worker lost its slot under a healthy run');

# --- the same stretch of WAL, with the pool and without -----------------

# One wakeup releases one waiter, and a waiter clears its registration only
# once it runs, so waking a parked pool takes as many wakeups as it takes:
# keep at it until no worker holds a request any more.  Detach the point
# first, or a woken worker parks again on its next request.
sub wake_parked_workers
{
	my ($node) = @_;

	foreach my $attempt (1 .. 300)
	{
		return
		  if $node->safe_psql('postgres',
			'SELECT claimed FROM test_dwb_warm_slot_states()') == 0;

		# not safe_psql: with the last waiter already gone this errors out
		$node->psql('postgres',
			"SELECT injection_points_wakeup('replay-warm-before-read')");
		usleep(100_000);
	}
	die 'the parked workers would not wake up';
}

sub startup_reads
{
	my ($node) = @_;
	return $node->safe_psql(
		'postgres',
		q(SELECT coalesce(sum(reads), 0) FROM pg_stat_io
		  WHERE backend_type = 'startup' AND object = 'relation'));
}

# Both standbys are caught up to the same point before the measured
# statement, and both are measured by their own increment over it, so the
# two numbers cover one and the same WAL.
#
# A startup process reports its statistics when it replays a running-xacts
# record, which a checkpoint emits; without one on each side of the
# measured statement the numbers would be whatever happened to have been
# reported by then.
my $plain = make_standby('plain_standby', 0);
$plain->start;
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($plain, 'replay');
$primary->wait_for_catchup($standby, 'replay');

my $warm_reads_before = startup_reads($standby);
my $plain_reads_before = startup_reads($plain);

$primary->safe_psql('postgres',
	"UPDATE t SET filler = repeat('z', 200) WHERE id % 3 = 1");
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($plain, 'replay');
$primary->wait_for_catchup($standby, 'replay');

$plain->poll_query_until(
	'postgres',
	"SELECT coalesce(sum(reads), 0) > $plain_reads_before FROM pg_stat_io
	 WHERE backend_type = 'startup' AND object = 'relation'"
) or die 'timed out waiting for the unaided standby to report its reads';

my $warm_delta = startup_reads($standby) - $warm_reads_before;
my $plain_delta = startup_reads($plain) - $plain_reads_before;

cmp_ok($plain_delta, '>', 0,
	'an unaided startup process reads pages over this WAL');
cmp_ok($warm_delta, '<', $plain_delta,
	'the warm pool keeps reads off the startup process');
$plain->stop;

# --- a relation dropped while a worker holds a request for it -----------

SKIP:
{
	skip 'injection points not supported by this build', 8
	  unless $injection_points;

	# The table exists on both sides before the pool is parked, so the only
	# WAL left in flight — and so the only thing the parked workers can be
	# holding — is the update of the table about to be dropped.
	$primary->safe_psql(
		'postgres', q(
		CREATE TABLE doomed AS
			SELECT g AS id, repeat('d', 200) AS filler
			FROM generate_series(1, 20000) g;
		CHECKPOINT;
	));
	$primary->wait_for_catchup($standby, 'replay');

	# First make the workers read this table for real, so each of them holds
	# open segments and a cached size for it.  Creating the table would not
	# have done that: its pages arrive as initialised pages, which replay
	# never needs read.  A worker still carrying that state into the drop is
	# what the scenario is about.
	my $reads_before_doomed = warm_counters($standby)->{reads};
	$primary->safe_psql('postgres',
		"UPDATE doomed SET filler = repeat('e', 200) WHERE id % 2 = 0");
	$primary->wait_for_catchup($standby, 'replay');
	cmp_ok(warm_counters($standby)->{reads},
		'>', $reads_before_doomed,
		'the workers read the table before it is doomed');

	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-before-read', 'wait')");

	$primary->safe_psql('postgres',
		"UPDATE doomed SET filler = repeat('f', 200) WHERE id % 2 = 1");

	$standby->poll_query_until('postgres',
		'SELECT claimed = 2 FROM test_dwb_warm_slot_states()')
	  or die 'timed out waiting for the workers to hold requests';
	pass('both workers are parked holding a request for the doomed table');

	my $doomed_file = $primary->safe_psql('postgres',
		"SELECT relfilenode FROM pg_class WHERE relname = 'doomed'");
	my $vanished_before = warm_counters($standby)->{vanished};

	# Replay never waits for the pool, so it drops the relation out from
	# under the parked request and moves on.
	$primary->safe_psql('postgres', 'DROP TABLE doomed');
	$primary->wait_for_catchup($standby, 'replay');
	pass('replay went past the drop while a request was held');

	$standby->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-before-read')");
	wake_parked_workers($standby);

	# Each woken worker starts its read after the drop has finished, which is
	# the interesting order: the interlock makes it find the relation gone
	# rather than read a page through a descriptor that still reaches the
	# unlinked file.
	$standby->poll_query_until('postgres',
		"SELECT vanished > $vanished_before FROM test_dwb_warm_counters()")
	  or die 'timed out waiting for the held request to fail on the drop';
	pass('a read starting after the drop finds the relation gone');

	$standby->poll_query_until('postgres',
		'SELECT claimed = 0 FROM test_dwb_warm_slot_states()')
	  or die 'timed out waiting for the workers to let their slots go';
	pass('the workers gave their slots back');

	is( $standby->safe_psql(
			'postgres', "SELECT test_dwb_count_rel_buffers($doomed_file)"),
		0,
		'no buffer was left behind for the dropped relation');

	# a pool that only ever fails would be silently useless
	my $reads_before = warm_counters($standby)->{reads};
	$primary->safe_psql('postgres',
		"UPDATE t SET filler = repeat('k', 200) WHERE id % 3 = 2");
	$primary->wait_for_catchup($standby, 'replay');
	cmp_ok(warm_counters($standby)->{reads},
		'>', $reads_before, 'the pool kept reading after the drop');

	is(warm_counters($standby)->{discarded},
		0, 'no result was written into a slot its worker had lost');
}

# --- a relation truncated under a request for a block past its new end --

SKIP:
{
	skip 'injection points not supported by this build', 4
	  unless $injection_points;

	$primary->safe_psql(
		'postgres', q(
		CREATE TABLE shrunk AS
			SELECT g AS id, repeat('s', 200) AS filler
			FROM generate_series(1, 20000) g;
		CHECKPOINT;
	));
	$primary->wait_for_catchup($standby, 'replay');

	# Warm the workers on the full-length relation, so each of them is
	# carrying its size from before the truncation.
	my $reads_before = warm_counters($standby)->{reads};
	$primary->safe_psql('postgres',
		"UPDATE shrunk SET filler = repeat('u', 200) WHERE id % 2 = 0");
	$primary->wait_for_catchup($standby, 'replay');
	cmp_ok(warm_counters($standby)->{reads},
		'>', $reads_before,
		'the workers read the relation at its full length');

	# Park them on blocks near the end — the part about to be cut off.
	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-before-read', 'wait')");
	$primary->safe_psql('postgres',
		"UPDATE shrunk SET filler = repeat('v', 200) WHERE id > 18000");
	$standby->poll_query_until('postgres',
		'SELECT claimed = 2 FROM test_dwb_warm_slot_states()')
	  or die 'timed out waiting for the workers to hold requests';

	my $vanished_before = warm_counters($standby)->{vanished};
	my $failed_before = warm_counters($standby)->{failed};
	# Vacuum gives back the empty tail.  Every row goes, so the file ends up
	# empty and every parked request points past its end — with rows left
	# behind, the tail could still hold live versions of them and the
	# truncation would stop short of the blocks the workers are holding.
	$primary->safe_psql('postgres', 'DELETE FROM shrunk');
	$primary->safe_psql('postgres', 'VACUUM shrunk');
	$primary->wait_for_catchup($standby, 'replay');

	is( $standby->safe_psql('postgres', "SELECT pg_relation_size('shrunk')"),
		0,
		'replay truncated the relation away while requests were held');

	$standby->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-before-read')");
	wake_parked_workers($standby);

	# The size each worker remembers is from before the truncation, and
	# nothing tells a process without a database connection to forget it —
	# except the pool itself, which is what this asserts: the block is
	# recognised as past the end instead of being read against a stale size.
	$standby->poll_query_until('postgres',
		"SELECT vanished > $vanished_before FROM test_dwb_warm_counters()")
	  or die
	  'timed out waiting for the held requests to notice the truncation'
	  . '; counters: '
	  . $standby->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_counters()')
	  . " (vanished was $vanished_before)";
	pass('a request for a block past the new end is dropped, not read');

	# wake_parked_workers() returned only once no worker held a request, so
	# every parked request has been dealt with by now.  A worker that carried
	# its old size past the check would have gone on to read past the end of
	# the file and errored out, which lands in a different counter; that this
	# one did not move is what says the size was refreshed rather than the
	# read merely failing somewhere else.
	is(warm_counters($standby)->{failed},
		$failed_before, 'no worker read against the size it remembered');
}

# --- a worker killed while it holds a request ---------------------------

SKIP:
{
	skip 'injection points not supported by this build', 3
	  unless $injection_points;

	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-before-read', 'wait')");

	$primary->safe_psql('postgres',
		"UPDATE t SET filler = repeat('m', 200) WHERE id % 5 = 0");
	$standby->poll_query_until('postgres',
		'SELECT claimed = 2 FROM test_dwb_warm_slot_states()')
	  or die 'timed out waiting for the workers to hold requests';

	my $released_before = warm_counters($standby)->{released};
	my $victim = $standby->safe_psql('postgres',
		'SELECT pid FROM test_dwb_warm_worker_pids() ORDER BY worker LIMIT 1'
	);
	kill 'TERM', $victim;

	$standby->poll_query_until('postgres',
		"SELECT released > $released_before FROM test_dwb_warm_counters()")
	  or die 'the killed worker did not give its slot back';
	pass('a worker killed mid-request gives its slot back');

	# The killed worker never got to clear its registration among the point's
	# waiters, and a wakeup goes to the first registration under that name —
	# so it would keep going to a process that no longer exists.  Nothing
	# below needs the surviving worker to move: replay does not wait for the
	# pool, and the pool's return is the restarted worker's doing.
	$standby->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-before-read')");

	$primary->wait_for_catchup($standby, 'replay');
	pass('replay continued across a warm worker that died');

	$standby->poll_query_until('postgres',
		'SELECT count(*) = 2 FROM test_dwb_warm_worker_pids()')
	  or die 'the pool did not come back after losing a worker';
	pass('the pool restored its worker');
}

# --- the wakeup protocol ------------------------------------------------

# Replay publishes on the order of a hundred thousand blocks a second, and
# waking a worker for each of them costs a system call each time.  So the
# publisher stays quiet while somebody is searching the ring, and a worker
# that stops searching hands the ring on in its place.  Driving that from
# replay would mean driving it thousands of requests at a time; these
# scenarios hand the pool one request at a time instead.

my $proto = make_standby('warm_proto', 2);
$proto->start;
$proto->poll_query_until('postgres',
	'SELECT count(*) = 2 FROM test_dwb_warm_worker_pids()')
  or die 'timed out waiting for the protocol standby to start its workers';
$primary->wait_for_catchup($proto, 'replay');

sub pool_state
{
	my ($node) = @_;
	my %s;
	@s{qw(published claimed scanners pending sleepers)} = split /\|/,
	  $node->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	return \%s;
}

# With nothing being replayed the pool has nothing to search for, and the
# spin that keeps a busy worker out of the wait list runs out.
$proto->poll_query_until('postgres',
	'SELECT sleepers = 2 AND scanners = 0 FROM test_dwb_warm_slot_states()')
  or die 'the idle pool never settled into sleeping workers';
pass('an idle pool settles into sleeping workers');

# Quiesced, the count and the ring agree.  Under load they need not: a
# publication raises the count before its slot becomes visible, so the count
# leads by whatever is in flight, and the ring walk is not a snapshot anyway.
my $quiet = pool_state($proto);
is($quiet->{pending}, $quiet->{published},
	'the pending count matches the published slots with the pool quiesced');

# The edge a lost wakeup would show at: a request arriving at a pool where
# nobody is searching has to be one the publisher wakes somebody for.
my $idle_before = warm_counters($proto);
my $pf_before = $proto->safe_psql('postgres',
	q{SELECT prefetch || ' ' || hit FROM pg_stat_recovery_prefetch});
my ($pf_prefetch, $pf_hit) = split / /, $pf_before;

my $slot =
  $proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 0)");
cmp_ok($slot, '>=', 0, 'the published request got a slot');
$proto->poll_query_until('postgres',
	"SELECT claimed > $idle_before->{claimed} FROM test_dwb_warm_counters()")
  or die 'a request published to a sleeping pool was never claimed';
pass('a request published to a sleeping pool is served');

# Whether a block was already in a buffer is decided in the pool now, and
# pg_stat_recovery_prefetch is where that decision has always been counted.
# The block above was not resident, so it was read; asking for the same block
# again is the other answer.
$proto->poll_query_until('postgres',
	"SELECT prefetch > $pf_prefetch FROM pg_stat_recovery_prefetch")
  or die 'the view did not count the page the pool read';
pass('the view counts a page the pool read');

$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 0)");
$proto->poll_query_until('postgres',
	"SELECT hit > $pf_hit FROM pg_stat_recovery_prefetch")
  or die 'the view did not count the page the pool found resident';
pass('the view counts a page the pool found already in a buffer');

# A worker can be signalled away while it sleeps, and the count it is part of
# has to go with it: the decrement that follows the sleep never runs in that
# case, and a count left standing says the pool has a sleeper it does not
# have — which is the very fact the scenarios above wait on.
$proto->poll_query_until('postgres',
	'SELECT sleepers = 2 FROM test_dwb_warm_slot_states()')
  or die 'the pool did not settle before the worker was killed; state: '
  . $proto->safe_psql('postgres', 'SELECT * FROM test_dwb_warm_slot_states()')
  . ' workers: '
  . $proto->safe_psql(
	'postgres',
	q{SELECT string_agg(worker || ':' || pid || ':' || holding, ',')
	  FROM test_dwb_warm_worker_pids()});

my $doomed = $proto->safe_psql('postgres',
	'SELECT pid FROM test_dwb_warm_worker_pids() ORDER BY worker LIMIT 1');
kill 'TERM', $doomed;

$proto->poll_query_until('postgres',
	'SELECT count(*) = 2 FROM test_dwb_warm_worker_pids()')
  or die 'the protocol standby did not restore its worker';
$proto->poll_query_until('postgres',
	'SELECT sleepers = 2 FROM test_dwb_warm_slot_states()')
  or die 'a worker killed in its sleep left its count behind; state: '
  . $proto->safe_psql('postgres',
	'SELECT * FROM test_dwb_warm_slot_states()');
pass('a worker killed in its sleep leaves no count behind');

# The pool still works afterwards, which is what the counts are for.
my $after_kill = warm_counters($proto);
$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 4)");
$proto->poll_query_until('postgres',
	"SELECT claimed > $after_kill->{claimed} FROM test_dwb_warm_counters()")
  or die 'the pool stopped serving after losing a sleeping worker';
pass('the pool serves again after losing a sleeping worker');

SKIP:
{
	skip 'injection points not supported by this build', 2
	  unless $injection_points;

	# This scenario holds a process still with SIGSTOP, which Windows has no
	# equivalent of.
	skip 'stopping and continuing a process is not portable to Windows', 2
	  if $PostgreSQL::Test::Utils::windows_os;

	# The worse version of the same thing: the worker that dies is the one
	# the publisher has just woken.  A signal is delivered to the head of the
	# wait list and takes it off that list, so if the head is on its way out,
	# the request it was woken for has been told to nobody.  Handing the ring
	# on from the exit path is what covers that, and it is the only thing
	# that can.
	#
	# Freezing the process is what makes the race observable.  A stopped
	# worker stays on the wait list and stays asleep, so the wakeup is spent
	# on it and the rest of the pool hears nothing.  An injection point could
	# not stand in for this: preparing to sleep on the point's own variable
	# cancels the registration on the pool's — see
	# ConditionVariablePrepareToSleep — and that registration is the whole
	# subject.
	$proto->poll_query_until('postgres',
		'SELECT sleepers = 2 AND pending = 0 FROM test_dwb_warm_slot_states()'
	) or die 'the pool did not settle before the head-of-queue scenario';

	my @pids = split /\n/,
	  $proto->safe_psql('postgres',
		'SELECT pid FROM test_dwb_warm_worker_pids() ORDER BY worker');

	# The list is joined at the tail and served from the head, so replacing
	# one of the two workers leaves the other at the head for certain:
	# whatever the order was, the replacement can only have joined behind it.
	kill 'TERM', $pids[1];
	$proto->poll_query_until(
		'postgres',
		"SELECT count(*) = 2 AND count(*) FILTER (WHERE pid = $pids[1]) = 0
		 FROM test_dwb_warm_worker_pids()"
	) or die 'the protocol standby did not replace the worker';
	$proto->poll_query_until('postgres',
		'SELECT sleepers = 2 FROM test_dwb_warm_slot_states()')
	  or die 'the pool did not settle after the worker was replaced';

	my $head = $pids[0];
	my $rest = $proto->safe_psql('postgres',
		"SELECT pid FROM test_dwb_warm_worker_pids() WHERE pid <> $head");

	# The dying worker is replaced within a second, and a replacement is free
	# to walk the ring and take whatever it finds — which would drain the
	# request whether or not anybody was ever told about it.  So the point
	# stays attached: the worker that claims parks while still holding the
	# slot, and the pool then says which pid that is.  A served request is
	# not the evidence here; who served it is.
	$proto->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-claimed', 'wait')");

	kill 'STOP', $head;

	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 6)");
	is( $proto->safe_psql(
			'postgres', 'SELECT pending FROM test_dwb_warm_slot_states()'),
		1,
		'the one wakeup goes to the worker at the head of the wait list');

	# That worker never gets to serve it: it is signalled away before it runs
	# again, so the request now depends entirely on what its exit path does.
	kill 'TERM', $head;
	kill 'CONT', $head;

	$proto->poll_query_until('postgres',
		'SELECT count(*) = 1 FROM test_dwb_warm_worker_pids() WHERE holding >= 0'
	  )
	  or die 'nobody took the request the dying worker was woken for; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	is( $proto->safe_psql(
			'postgres',
			'SELECT pid FROM test_dwb_warm_worker_pids() WHERE holding >= 0'),
		$rest,
		'a worker that dies holding the wakeup hands the ring to the sleeper'
	);

	# Let the parked worker out and put the pool back to sleep, the same way
	# the scenarios below do: a detached point can still be reached by a
	# worker that looked it up a moment earlier.
	$proto->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-claimed')");
	foreach my $attempt (1 .. 600)
	{
		last
		  if $proto->safe_psql(
			'postgres',
			'SELECT claimed = 0 AND sleepers = 2
			 FROM test_dwb_warm_slot_states()') eq 't';
		$proto->psql('postgres',
			"SELECT injection_points_wakeup('replay-warm-claimed')");
		usleep(100_000);
	}
	$proto->poll_query_until('postgres',
		'SELECT claimed = 0 AND sleepers = 2 FROM test_dwb_warm_slot_states()'
	  )
	  or die
	  'the pool stayed parked after the head-of-queue scenario; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
}

SKIP:
{
	skip 'injection points not supported by this build', 3
	  unless $injection_points;

	# Park a worker where it holds a request and still counts as a searcher.
	# Everything published while it sits there is something the publisher
	# leaves to it, so the only way the rest of the ring gets served is if
	# that worker hands the ring on when it stops searching.
	$proto->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-claimed', 'wait')");
	$proto->poll_query_until('postgres',
		'SELECT sleepers = 2 FROM test_dwb_warm_slot_states()')
	  or die 'the pool did not go back to sleep before the hand-off scenario';

	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 1)");
	$proto->poll_query_until(
		'postgres',
		'SELECT scanners = 1 AND sleepers = 1 AND claimed = 1
		 FROM test_dwb_warm_slot_states()')
	  or die 'no worker parked holding a request; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	pass('a worker parks holding a request, still counted as a searcher');

	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 2)");
	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 3)");

	# Two requests, one searcher the publisher trusted, and a worker asleep
	# that was told nothing.
	is( $proto->safe_psql(
			'postgres', 'SELECT pending FROM test_dwb_warm_slot_states()'),
		2,
		'requests wait while the publisher leaves them to the searcher');

	# Let the parked worker go, but leave the point attached: it takes one
	# of the two waiting requests and parks again.  The other one can only
	# be served by the worker that is asleep, and nothing has woken it but
	# the hand-off.
	$proto->safe_psql('postgres',
		"SELECT injection_points_wakeup('replay-warm-claimed')");
	$proto->poll_query_until('postgres',
		'SELECT pending = 0 FROM test_dwb_warm_slot_states()')
	  or die 'the worker leaving the search did not hand the ring on; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	pass('a worker leaving the search hands the ring to a sleeping one');

	# Let the parked workers out and put the pool back to sleep before the
	# next scenario builds its own state.
	$proto->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-claimed')");
	# "Nobody is holding a request" is not the same as "nobody is parked":
	# a detached point can still be reached by a worker that looked it up a
	# moment earlier, so a loop that stops at the first idle instant can
	# leave the next claim parked with nothing left to wake it.  Waiting for
	# the pool to be asleep is the state that cannot be a gap between two
	# claims.
	foreach my $attempt (1 .. 600)
	{
		last
		  if $proto->safe_psql(
			'postgres',
			'SELECT claimed = 0 AND sleepers = 2
			 FROM test_dwb_warm_slot_states()') eq 't';
		$proto->psql('postgres',
			"SELECT injection_points_wakeup('replay-warm-claimed')");
		usleep(100_000);
	}
	$proto->poll_query_until('postgres',
		'SELECT claimed = 0 AND sleepers = 2 FROM test_dwb_warm_slot_states()'
	  )
	  or die 'workers stayed parked after the point was detached; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	$proto->poll_query_until('postgres',
		'SELECT sleepers = 2 AND pending = 0 FROM test_dwb_warm_slot_states()'
	) or die 'the pool did not settle before the dying-searcher scenario';

	# The same hand-off from the other side: the searcher the publisher
	# trusted does not stop searching, it dies.  Whatever it was trusted to
	# find has to be picked up by the worker asleep beside it, and the only
	# thing that can tell that worker is the exit path.
	#
	# The point stays attached on purpose.  The dying worker is replaced
	# within a second, and a replacement free to drain the ring would hide a
	# missing hand-off: it parks on its first claim instead, so of the two
	# requests waiting it can take only one, and the other is left where
	# nothing but the hand-off reaches it.
	$proto->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-claimed', 'wait')");
	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 5)");
	$proto->poll_query_until('postgres',
		'SELECT scanners = 1 AND claimed = 1 FROM test_dwb_warm_slot_states()'
	) or die 'no worker parked for the dying-searcher scenario';

	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 6)");
	$proto->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', 7)");

	# The workers advertise which slot each of them holds, so the one to end
	# is the one parked rather than the one asleep.
	my $victim = $proto->safe_psql('postgres',
		'SELECT pid FROM test_dwb_warm_worker_pids() WHERE holding >= 0');
	like($victim, qr/^\d+$/, 'exactly one worker is holding a request');
	kill 'TERM', $victim;

	$proto->poll_query_until('postgres',
		'SELECT pending = 0 FROM test_dwb_warm_slot_states()')
	  or die 'a searcher that died did not hand the ring on; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	pass('a searcher that dies hands the ring to a sleeping worker');

	# Nobody is woken from here on, and nothing below needs a worker to
	# move.  The worker killed above was waiting at the point, and a waiter
	# that dies leaves its registration behind: a wakeup goes to the first
	# registration under that name, so it would keep going to a process that
	# no longer exists.  This node is finished after the check below.
	$proto->safe_psql('postgres',
		"SELECT injection_points_detach('replay-warm-claimed')");

	# A worker died holding a request, which is one of the two ways a
	# published request leaves the ring without a claim behind it.
	$proto->poll_query_until('postgres',
		'SELECT count(*) = 2 FROM test_dwb_warm_worker_pids()')
	  or die 'the pool did not come back after the dying-searcher scenario';
	$proto->poll_query_until('postgres',
		'SELECT pending = published FROM test_dwb_warm_slot_states()')
	  or die 'the pending count and the ring disagree after a worker died '
	  . 'holding a request; state: '
	  . $proto->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()');
	pass('the pending count survives a worker dying with a request');
}

$proto->stop;

# --- the pool on its own, with kernel advice turned off -----------------

my $noadvice =
  make_standby('noadvice_standby', 2, conf => "recovery_prefetch = off\n");
$noadvice->start;
$primary->wait_for_catchup($noadvice, 'replay');

$primary->safe_psql('postgres',
	"UPDATE t SET filler = repeat('n', 200) WHERE id % 7 = 0");
$primary->wait_for_catchup($noadvice, 'replay');

my $na = warm_counters($noadvice);
cmp_ok($na->{published}, '>', 0,
	'blocks reach the pool with the advice prefetcher off');
cmp_ok($na->{collected}, '>', 0,
	'replay collects the pool answers with the advice prefetcher off');
$noadvice->stop;

# --- a relation that grew after a worker learned its size ---------------

# The size a worker goes by is the one the storage manager recorded when it
# last asked, and nothing tells a worker that replay has extended a
# relation.  A request past the end it knows must therefore make it ask
# again rather than refuse on what it remembers.
#
# The remembered size belongs to the process that asked, so this runs with
# a single worker: with more of them the second half could land in a
# process that never saw the first and would pass without asking anything.
my $onework = make_standby('warm_onework', 1);
$onework->start;
$onework->poll_query_until('postgres',
	'SELECT count(*) = 1 FROM test_dwb_warm_worker_pids()')
  or die 'timed out waiting for the single warm worker to start';
$primary->wait_for_catchup($onework, 'replay');

my $past = $onework->safe_psql('postgres',
	q{SELECT (pg_relation_size('t') / current_setting('block_size')::int)::int}
);

my $before = warm_counters($onework);
$onework->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', $past)");
$onework->poll_query_until('postgres',
	"SELECT vanished > $before->{vanished} FROM test_dwb_warm_counters()")
  or die 'a request past the end of a relation was not refused';
pass('a block past the end of a relation is refused');

$primary->safe_psql('postgres',
	q{INSERT INTO t SELECT g, repeat('z', 200) FROM generate_series(40001, 60000) g}
);
$primary->wait_for_catchup($onework, 'replay');

cmp_ok(
	$onework->safe_psql(
		'postgres',
		q{SELECT (pg_relation_size('t') / current_setting('block_size')::int)::int}
	),
	'>', $past,
	'the relation grew past the refused block');

$before = warm_counters($onework);
$onework->safe_psql('postgres', "SELECT test_dwb_warm_publish('t', $past)");
$onework->poll_query_until(
	'postgres',
	"SELECT hits + reads > @{[ $before->{hits} + $before->{reads} ]}
	 FROM test_dwb_warm_counters()")
  or die 'the worker went by the size it learned before the relation grew; '
  . 'counters: '
  . $onework->safe_psql('postgres', 'SELECT * FROM test_dwb_warm_counters()');
pass('a worker asks again for a block past the size it knows');
$onework->stop;

# --- promotion with requests still outstanding --------------------------

SKIP:
{
	skip 'injection points not supported by this build', 1
	  unless $injection_points;

	# Counted against what the queue already holds: a worker left parked by
	# the scenario above still holds its slot, and that one is not what this
	# is about.
	my $held_before = $standby->safe_psql('postgres',
		'SELECT published + claimed FROM test_dwb_warm_slot_states()');

	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('replay-warm-before-read', 'wait')");
	$primary->safe_psql('postgres',
		"UPDATE t SET filler = repeat('p', 200) WHERE id % 11 = 0");
	$standby->poll_query_until('postgres',
		"SELECT published + claimed > $held_before FROM test_dwb_warm_slot_states()"
	  )
	  or die 'timed out waiting for the queue to hold requests; slots: '
	  . $standby->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_slot_states()')
	  . ' counters: '
	  . $standby->safe_psql('postgres',
		'SELECT * FROM test_dwb_warm_counters()')
	  . ' pids: '
	  . $standby->safe_psql('postgres',
		'SELECT count(*) FROM test_dwb_warm_worker_pids()')
	  . ' received/replayed: '
	  . $standby->safe_psql(
		'postgres',
		q{SELECT pg_last_wal_receive_lsn() || ' ' || pg_last_wal_replay_lsn()}
	  );
	pass('the queue holds requests going into promotion');
}

$standby->promote;
$standby->safe_psql('postgres', 'SELECT 1');
pass('the standby promoted with warm requests outstanding');

cmp_ok(
	$standby->safe_psql(
		'postgres', "SELECT count(*) FROM t WHERE filler LIKE 'y%'"),
	'>', 0,
	'the promoted node has the replayed data');

# The end of recovery withdraws every request nobody claimed, which is the
# other way the count of outstanding requests goes down without a claim
# behind it.  A withdrawal that forgot the count would leave the pool
# claiming to owe work it has thrown away.
$standby->poll_query_until('postgres',
	'SELECT pending = published FROM test_dwb_warm_slot_states()')
  or die 'the pending count and the ring disagree after promotion withdrew '
  . 'the outstanding requests; state: '
  . $standby->safe_psql('postgres',
	'SELECT * FROM test_dwb_warm_slot_states()');
pass('the pending count comes down with the requests promotion withdraws');

$standby->stop;
$primary->stop;

done_testing();
