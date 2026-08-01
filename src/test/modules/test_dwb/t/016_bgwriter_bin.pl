
# Copyright (c) 2025, PostgreSQL Global Development Group

# The bgwriter's LRU scan flushes through the double write buffer in bins
# (FlushBufferBin), like the checkpointer in 015: without the bins every
# scattered singleton write pays a full batch fdatasync through the
# lone-writer seal.  pg_stat_io proves the batching: one dwb "write" is one
# batch, so write_bytes/writes is the average batch size.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('dwb_bgwriter');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
autovacuum = off
shared_buffers = 2MB
bgwriter_delay = 10ms
bgwriter_lru_maxpages = 1000
bgwriter_lru_multiplier = 10
checkpoint_timeout = 1h
));
$node->start;

# A table several times larger than shared_buffers: every UPDATE pass
# streams allocations through the small pool, which is what makes the
# bgwriter clean dirty buffers ahead of the clock sweep.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_bgw (id int, pad text) WITH (fillfactor = 50);
	INSERT INTO dwb_bgw SELECT g, repeat('b', 500) FROM generate_series(1, 20000) g;
));

# Only the bgwriter's own writes may enter the average: drop what the
# initial data load accumulated.
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");

# The bgwriter's write volume per round depends on its allocation estimator,
# and its statistics reach the collector asynchronously: drive passes of
# fresh allocations and poll between them until the dwb row carries the
# proof.  writes >= 10 skips the noise of the first few partial bins.
my $binned = 0;
OUTER: for my $pass (1 .. 8)
{
	$node->safe_psql('postgres',
		"UPDATE dwb_bgw SET pad = repeat(chr(96 + $pass), 500)");
	for my $probe (1 .. 25)
	{
		$binned = $node->safe_psql(
			'postgres', q(
			SELECT COALESCE(bool_or(
					writes >= 10
					AND (write_bytes::numeric / writes - 4096) / 8192 >= 4), false)
			FROM pg_stat_io
			WHERE backend_type = 'background writer' AND object = 'dwb'
				AND context = 'normal'
		));
		last OUTER if $binned eq 't';
		usleep(200_000);
	}
}
is($binned, 't', 'bgwriter dwb batches average >= 4 slots, not one per page');

done_testing();
