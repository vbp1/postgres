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
if ($use_injection_points)
{
	$node->append_conf(
		'postgresql.conf', q{
shared_preload_libraries = 'injection_points'
});
}
$node->start;

is($node->safe_psql('postgres', 'SHOW wal_flush_backend_flushers'),
	'1', 'wal_flush_backend_flushers can be set at server start');

$node->safe_psql('postgres',
	'CREATE TABLE wal_flush_backend_flushers_probe (id bigserial)');
$node->safe_psql('postgres',
	'CREATE TABLE wal_flush_backend_flushers_test (id bigserial)');

if ($use_injection_points && $node->check_extension('injection_points'))
{
	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	$node->safe_psql(
		'postgres', q{
SELECT injection_points_attach('wal-backend-flush-after-acquire', 'wait');
SELECT injection_points_attach('wal-backend-flush-wait', 'wait');
SELECT injection_points_attach('wal-backend-flush-wakeup', 'wait');
});

	my $flusher = $node->background_psql('postgres', on_error_stop => 1);
	$flusher->query_until(
		qr/flusher_started/,
		q{
\echo flusher_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo flusher_done
});
	$node->wait_for_event('client backend',
		'wal-backend-flush-after-acquire');

	my $waiter = $node->background_psql('postgres', on_error_stop => 1);
	$waiter->query_until(
		qr/waiter_started/,
		q{
\echo waiter_started
INSERT INTO wal_flush_backend_flushers_probe DEFAULT VALUES;
\echo waiter_done
});
	$node->wait_for_event('client backend', 'wal-backend-flush-wait');

	pass('a backend waited in the limited WAL flush queue');

	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-after-acquire')});
	$node->wait_for_event('client backend', 'wal-backend-flush-wakeup');
	pass('WAL flush wakeup path reached a queued backend');

	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-wakeup')});
	$flusher->query_until(qr/flusher_done/, '');

	$node->safe_psql('postgres',
		q{SELECT injection_points_wakeup('wal-backend-flush-wait')});
	$waiter->query_until(qr/waiter_done/, '');

	$flusher->quit;
	$waiter->quit;

	$node->safe_psql(
		'postgres', q{
SELECT injection_points_detach('wal-backend-flush-after-acquire');
SELECT injection_points_detach('wal-backend-flush-wait');
SELECT injection_points_detach('wal-backend-flush-wakeup');
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

done_testing();
