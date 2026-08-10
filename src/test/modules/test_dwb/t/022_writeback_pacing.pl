# Copyright (c) 2025, PostgreSQL Global Development Group

# The double write buffer asks the kernel to start writing a staged page back
# before the batch retires, so that the retiring fsync is a cheap barrier
# rather than a full flush.  dwb_writeback_after says how many pages a process
# accumulates before it hands them over.  pg_stat_io's writebacks column
# counts the pages handed over, which is what this test reads back.
#
# The threshold is visible in the counter's arithmetic: a process hands its
# pages over in whole batches of dwb_writeback_after, so what a run adds up to
# is a multiple of whatever the parameter says, which is what tells the
# thresholds apart.  At a threshold of one, and whenever the batch retires
# inline (dwb_retire_workers = 0, where waiting for the array to fill would
# mean handing the page over after its own sync), every staged write is handed
# over as it happens and the two counters meet.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_writeback');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_writeback_after = 32
autovacuum = off
shared_buffers = 1MB
bgwriter_lru_maxpages = 0
backend_flush_after = 0
checkpoint_timeout = 1h
# ScheduleBufferTagForWriteback is a no-op without it, so the counters this
# test reads would all be zero on the usual TAP setting.
fsync = on
));
$node->start;

# Client-backend writebacks of relation pages: what the parameter paces.
# Every page of a logged table is staged, so the writes of the same rows are
# what the hand-over is measured against.  Both counters have to come out of
# one query: opening a session evicts pages of its own, so reading them one
# after another would compare two different moments.
my $counters = q(
	SELECT COALESCE(sum(writebacks), 0) || ' ' || COALESCE(sum(writes), 0)
	FROM pg_stat_io
	WHERE backend_type = 'client backend' AND object = 'relation');

# A table well past shared_buffers, so an UPDATE pass has to evict.  Returns
# the pages handed over and the pages written, in that order.
sub churn
{
	my ($table, $mark) = @_;
	$node->safe_psql('postgres',
		"UPDATE $table SET pad = repeat('$mark', 400)");
	# statistics reach the collector when the session ends
	$node->safe_psql('postgres', 'SELECT 1');
	return split(/ /, $node->safe_psql('postgres', $counters));
}

$node->safe_psql(
	'postgres', q(
	CREATE TABLE wb_logged (id int, pad text);
	INSERT INTO wb_logged SELECT g, repeat('a', 400) FROM generate_series(1, 20000) g;
));

# --- bounds -----------------------------------------------------------------

is( $node->safe_psql(
		'postgres', q(
		SELECT unit = (current_setting('block_size')::int / 1024) || 'kB'
		FROM pg_settings WHERE name = 'dwb_writeback_after')),
	't',
	'dwb_writeback_after is measured in blocks');

my ($rc, $stdout, $stderr) =
  $node->psql('postgres', 'SET dwb_writeback_after = 1');
like(
	$stderr,
	qr/cannot be changed now/,
	'dwb_writeback_after cannot be set from a session');

# pg_settings.setting is the raw block count; current_setting() would render
# it with a unit, which is not what the bounds are expressed in.
sub setting
{
	return $node->safe_psql('postgres',
		"SELECT setting FROM pg_settings WHERE name = 'dwb_writeback_after'");
}

sub set_to
{
	my ($value) = @_;

	$node->adjust_conf('postgresql.conf', 'dwb_writeback_after', $value);
	$node->reload;
	$node->poll_query_until('postgres',
			"SELECT setting = '$value' FROM pg_settings"
		  . " WHERE name = 'dwb_writeback_after'")
	  or die "dwb_writeback_after did not reach $value";
}

for my $ok (0, 1, 256)
{
	set_to($ok);
	is(setting(), "$ok", "dwb_writeback_after accepts $ok");
}

my $offset = -s $node->logfile;
$node->adjust_conf('postgresql.conf', 'dwb_writeback_after', 257);
$node->reload;
$node->wait_for_log(
	qr/is outside the valid range for parameter "dwb_writeback_after"/,
	$offset);
is(setting(), '256',
	'a value past the maximum is refused and the old one kept');

# --- the parameter's effect across a reload ---------------------------------

set_to(0);
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
my ($handed, $written) = churn('wb_logged', 'b');
is($handed, '0', 'no page is handed over while the parameter is zero');
cmp_ok($written, '>', 1000, 'though the pass did evict');

# Each threshold leaves its own signature in the total, so an implementation
# that ignored the parameter and used a fixed size would fail all but one.
my $mark = 'c';
for my $n (32, 256, 1)
{
	set_to($n);
	$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
	($handed, $written) = churn('wb_logged', $mark++);
	cmp_ok($handed, '>', 0, "pages are handed over at a threshold of $n");
	is($handed % $n, 0, "and in whole batches of $n");
	if ($n == 1)
	{
		is($handed, $written,
			'at a threshold of one, every staged write as it happens');
	}
	else
	{
		cmp_ok($handed, '<=', $written,
			"and never more than what was written");
	}
}

# --- scope: only what the double write buffer stages -------------------------

# Built here and dropped again, so that no page of it is left in the pool to
# be evicted by a later pass and counted as a write nobody staged.
$node->safe_psql(
	'postgres', q(
	CREATE UNLOGGED TABLE wb_unlogged (id int, pad text);
	INSERT INTO wb_unlogged SELECT g, repeat('a', 400) FROM generate_series(1, 20000) g;
));
set_to(32);
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
my ($unlogged, $writes) = churn('wb_unlogged', 'd');
cmp_ok($writes, '>', 1000, 'the unlogged pass did evict');
cmp_ok($unlogged, '<', $writes / 10,
	'pages the double write buffer does not stage are left to backend_flush_after'
);
$node->safe_psql('postgres', 'DROP TABLE wb_unlogged');

# --- synchronous retirement --------------------------------------------------

$node->adjust_conf('postgresql.conf', 'dwb_retire_workers', '0');
$node->restart;
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
($handed, $written) = churn('wb_logged', 'e');
cmp_ok($handed, '>', 0,
	'pages are handed over when the batch retires inline');
is($handed, $written,
	'every one of them, at a threshold that would otherwise hold them back');

# The counters above cannot tell "issued before the sync" from "issued after
# it": both leave the same totals.  This can.  The watch runs inside whichever
# process is writing, is handed the very context that process queued into, and
# reports through the server log — so it covers the bin-gather path in the
# checkpointer as well as the eviction path in a backend, neither of which can
# be read out of the test session's memory.
SKIP:
{
	skip 'injection points not supported by this build', 3
	  unless $ENV{enable_injection_points}
	  && $ENV{enable_injection_points} eq 'yes';

	$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');
	my $offset = -s $node->logfile;
	$node->safe_psql('postgres', 'SELECT test_dwb_watch_inline_retire()');

	# a backend evicting, then the checkpointer flushing in bins
	churn('wb_logged', 'g');
	$node->safe_psql('postgres', 'CHECKPOINT');

	$node->wait_for_log(qr/dwb-inline-retire watch armed in client backend/,
		$offset);
	ok(1, 'the watch ran on the eviction path');
	$node->wait_for_log(qr/dwb-inline-retire watch armed in checkpointer/,
		$offset);
	ok(1, 'and on the bin-gather path');
	ok( !$node->log_contains(qr/writebacks still queued/, $offset),
		'nothing was still queued when a batch was made durable');

	$node->safe_psql('postgres', 'SELECT test_dwb_unwatch_inline_retire()');
}

# --- the threshold, one page at a time ---------------------------------------

# Everything above reads totals, which a fixed internal size could match by
# luck.  This evicts named buffers one by one in a session of its own: one
# short of the threshold nothing has been handed over, and the page that
# reaches it hands over the whole array.
$node->adjust_conf('postgresql.conf', 'dwb_retire_workers', '1');
$node->adjust_conf('postgresql.conf', 'shared_buffers', '16MB');
$node->restart;
set_to(8);

$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION pg_buffercache;
	CREATE TABLE wb_exact (id int, pad text);
	INSERT INTO wb_exact SELECT g, repeat('a', 400) FROM generate_series(1, 400) g;
	CHECKPOINT;));

my $evict = q{
	SELECT count(*) FROM (
		SELECT pg_buffercache_evict(bufferid) FROM pg_buffercache
		WHERE relfilenode = pg_relation_filenode('wb_exact') AND isdirty
		LIMIT };

my $bg = $node->background_psql('postgres');
$bg->query("UPDATE wb_exact SET pad = repeat('h', 400)");
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");

$bg->query($evict . '7) x');
$node->poll_query_until('postgres', "SELECT ($counters) = '0 7'")
  or die 'the first seven evictions did not settle';
ok(1, 'one page short of the threshold, nothing has been handed over');

$bg->query($evict . '1) x');
$node->poll_query_until('postgres', "SELECT ($counters) = '8 8'")
  or die 'the eighth eviction did not settle';
ok(1, 'the page that reaches it hands over the whole array');
$bg->quit;

$node->stop;
done_testing();
