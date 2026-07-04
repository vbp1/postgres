# Copyright (c) 2026, PostgreSQL Global Development Group

# Test concurrent commits while limiting backend processes that may flush WAL.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;

my $use_injection_points = ($ENV{enable_injection_points} // '') eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('wal_flush_backend_flushers');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
wal_flush_backend_flushers = 1
});
$node->start;

is($node->safe_psql('postgres', 'SHOW wal_flush_backend_flushers'),
	'1', 'wal_flush_backend_flushers is set');

$node->safe_psql('postgres',
	'CREATE TABLE wal_flush_backend_flushers_probe (id bigserial)');
$node->safe_psql('postgres',
	'CREATE TABLE wal_flush_backend_flushers_test (id bigserial)');

if ($use_injection_points && $node->check_extension('injection_points'))
{
	# The wait-mode injection points used below fire inside critical
	# sections, where the injection_points module must not allocate its
	# lazily-initialized local state.  Preloading the module sets that
	# state up at backend start instead (compare test_slru/t/001_multixact.pl,
	# which waits inside a critical section the same way).  The availability
	# check above ran without the preload, so a build without the module
	# still skips this section gracefully instead of failing to start.
	$node->append_conf(
		'postgresql.conf', q{
shared_preload_libraries = 'injection_points'
});
	$node->restart;

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	$node->safe_psql(
		'postgres', q{
SELECT injection_points_attach('wal-backend-flush-after-acquire', 'wait');
SELECT injection_points_attach('wal-backend-flush-wait', 'wait');
});

	# The injection points fire during the commit's XLogFlush, which runs
	# inside the caller's critical section where the points cannot be loaded
	# into the local cache anymore; pre-load them in each session first.
	my $preload_points = q{
SELECT injection_points_load('wal-backend-flush-after-acquire');
SELECT injection_points_load('wal-backend-flush-wait');
};

	# The flusher acquires the single flusher slot and then blocks at the
	# injection point, holding the slot.
	my $flusher = $node->background_psql('postgres', on_error_stop => 1);
	$flusher->query_safe($preload_points);
	$flusher->query_until(
		qr/flusher_started/,
		q{
\echo flusher_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo flusher_done
});
	$node->wait_for_event('client backend',
		'wal-backend-flush-after-acquire');

	# With the only slot taken, a second backend fails to acquire one and
	# reaches the wait path.  Both injection points sit in code reachable
	# only by client backends, so no background process can consume them.
	my $waiter = $node->background_psql('postgres', on_error_stop => 1);
	$waiter->query_safe($preload_points);
	$waiter->query_until(
		qr/waiter_started/,
		q{
\echo waiter_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo waiter_done
});
	$node->wait_for_event('client backend', 'wal-backend-flush-wait');

	pass('a backend waited in the limited WAL flush queue');

	# Let the waiter proceed into its condition-variable sleep, then let the
	# flusher finish: it flushes the waiter's WAL too, releases the slot and
	# wakes the waiter.  Each backend passes its injection point exactly
	# once, so a single wakeup per point suffices.
	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-wait')});
	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-after-acquire')});

	$flusher->query_until(qr/flusher_done/, '');
	$waiter->query_until(qr/waiter_done/, '');
	pass('waiter completed after the flusher released the slot');

	$flusher->quit;
	$waiter->quit;

	# Second scenario: the slot is released and the flushed position
	# advances while the waiter is registered but not yet sleeping on the
	# condition variable (parked at the injection point placed before
	# ConditionVariablePrepareToSleep), so both the handoff signal and the
	# broadcast find an empty wait queue.  The waiter must not rely on them:
	# rechecking the flushed position after preparing to sleep has to let it
	# complete.
	my $flusher2 = $node->background_psql('postgres', on_error_stop => 1);
	$flusher2->query_safe($preload_points);
	$flusher2->query_until(
		qr/flusher2_started/,
		q{
\echo flusher2_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo flusher2_done
});
	$node->wait_for_event('client backend',
		'wal-backend-flush-after-acquire');

	my $waiter2 = $node->background_psql('postgres', on_error_stop => 1);
	$waiter2->query_safe($preload_points);
	$waiter2->query_until(
		qr/waiter2_started/,
		q{
\echo waiter2_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo waiter2_done
});
	$node->wait_for_event('client backend', 'wal-backend-flush-wait');

	# Let the flusher finish first: its group flush covers the waiter's
	# already-published LSN, and its slot release signals an empty queue.
	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-after-acquire')});
	$flusher2->query_until(qr/flusher2_done/, '');

	# Only now release the waiter; it must notice that its WAL has been
	# flushed and complete without any wakeup arriving.
	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-wait')});
	$waiter2->query_until(qr/waiter2_done/, '');
	pass('waiter completed although wakeups preceded its sleep');

	$flusher2->quit;
	$waiter2->quit;

	$node->safe_psql(
		'postgres', q{
SELECT injection_points_detach('wal-backend-flush-after-acquire');
SELECT injection_points_detach('wal-backend-flush-wait');
});
}
elsif ($use_injection_points)
{
	note(
		'extension injection_points not installed; skipping wait/wakeup probe'
	);
}
else
{
	note(
		'injection points not supported by this build; skipping wait/wakeup probe'
	);
}

$node->pgbench(
	'--no-vacuum --client=8 --jobs=4 --transactions=200',
	0,
	[qr/number of failed transactions: 0/],
	[],
	'concurrent commits with one backend allowed to flush WAL',
	{
		'wal_flush_backend_flushers.pgb' =>
		  'INSERT INTO wal_flush_backend_flushers_test DEFAULT VALUES'
	});

is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM wal_flush_backend_flushers_test'),
	'1600',
	'all concurrent WAL-writing transactions committed');

# The limit can be changed with a reload; make sure commits still work after
# disabling it on the fly.
$node->adjust_conf('postgresql.conf', 'wal_flush_backend_flushers', '0');
$node->reload;
$node->safe_psql('postgres',
	'INSERT INTO wal_flush_backend_flushers_test DEFAULT VALUES');
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM wal_flush_backend_flushers_test'),
	'1601',
	'commits work after disabling the limit via reload');

# Stress the reload path: keep sessions committing while the limit bounces
# between off, high and one.  Any acquire/release imbalance across the
# transitions would permanently wedge the flusher slots (with the limit back
# at one, a leaked slot blocks every commit), which shows up here as a hang.
my @churners =
  map { $node->background_psql('postgres', on_error_stop => 1) } (1 .. 3);
my @limits = ('3', '1', '0', '1');
my $churn_rows = 0;
foreach my $limit (@limits)
{
	$node->adjust_conf('postgresql.conf', 'wal_flush_backend_flushers',
		$limit);
	$node->reload;
	for my $i (1 .. 5)
	{
		foreach my $session (@churners)
		{
			$session->query(
				'INSERT INTO wal_flush_backend_flushers_test DEFAULT VALUES');
			$churn_rows++;
		}
	}
}
$_->quit foreach @churners;
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM wal_flush_backend_flushers_test'),
	1601 + $churn_rows,
	'commits survive limit changes between off, high and one');

# The published flush request is clamped to the end position of inserted
# WAL; a wrong clamp (e.g. the start-position conversion, which lands past
# the page header on page boundaries) surfaces as this complaint from the
# flushing backend.  The trigger needs an unlucky insert position, so a
# clean pass is only probabilistic insurance, but the message must never
# appear with a correct clamp.
ok( !$node->log_contains('request to flush past end of generated WAL'),
	'no flush-past-end complaints from limited backend flushing');

# All the commits above went through the limited flush path with
# synchronous_commit on, so they must survive a crash.
$node->stop('immediate');
$node->start;
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM wal_flush_backend_flushers_test'),
	1601 + $churn_rows,
	'limited-path commits are durable across a crash');

done_testing();
