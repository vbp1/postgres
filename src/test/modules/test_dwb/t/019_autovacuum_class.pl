
# Copyright (c) 2025, PostgreSQL Global Development Group

# Autovacuum workers write the ring through the BACKGROUND class: their
# private ring strategy makes them scheduled sequential writers, not
# latency-critical evictors.  The node is configured so that background
# class traffic can come from nothing else — no cleaner pool, no LRU
# scan, checkpoints an hour away — and the vacuum buffer ring is shrunk
# so autovacuum must flush the pages it dirties.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_av_class');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_cleaner_workers = 0
shared_buffers = 64MB
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
autovacuum_naptime = 1s
vacuum_buffer_usage_limit = 128kB
log_autovacuum_min_duration = 0
# vacuum's ring only reuses a dirty buffer whose WAL is already flushed
# (StrategyRejectBuffer); keep the flushed LSN hard on the ring's heels
# so the reject path stays cold and the flushes really happen
wal_writer_delay = 1ms
wal_writer_flush_after = 0
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# Several hundred pages, well past the 16-buffer vacuum ring.  The ring
# strategy only kicks in on reads, and pruning must be what dirties the
# pages, so the workload is staged cold: delete with autovacuum held
# off, then a clean restart empties shared buffers (and flushes the
# delete's dirt through the shutdown checkpoint).  The autovacuum pass
# then reads every page through its ring, prunes it dirty, and the ring
# wrap forces the per-page DWB flushes under test.
$node->safe_psql(
	'postgres', q(
	CREATE TABLE t_av (id int, filler text)
		WITH (autovacuum_enabled = off,
			  autovacuum_vacuum_threshold = 1,
			  autovacuum_vacuum_scale_factor = 0);
	INSERT INTO t_av SELECT g, repeat('a', 100) FROM generate_series(1, 20000) g;
	DELETE FROM t_av WHERE id % 2 = 0;
));
$node->restart;

my $bg_before = $node->safe_psql('postgres',
	"SELECT coalesce(sum(pages), 0) FROM test_dwb_seal_stats() WHERE wclass = 'background'"
);
my $ev_before = $node->safe_psql('postgres',
	"SELECT coalesce(sum(pages), 0) FROM test_dwb_seal_stats() WHERE wclass = 'eviction'"
);

$node->safe_psql('postgres',
	'ALTER TABLE t_av SET (autovacuum_enabled = on)');

$node->poll_query_until(
	'postgres', qq(
	SELECT coalesce(sum(pages), 0) > $bg_before FROM test_dwb_seal_stats()
	WHERE wclass = 'background'
)) or die 'timed out waiting for autovacuum to write the background class';
pass('autovacuum flushed its ring through the background class');

is( $node->safe_psql(
		'postgres',
		"SELECT coalesce(sum(pages), 0) FROM test_dwb_seal_stats() WHERE wclass = 'eviction'"
	),
	$ev_before,
	'the eviction class saw none of the autovacuum writes');

$node->poll_query_until(
	'postgres', q(
	SELECT coalesce(sum(writes), 0) > 0 FROM pg_stat_io
	WHERE object = 'dwb' AND backend_type = 'autovacuum worker'
)) or die 'timed out waiting for the autovacuum pg_stat_io dwb row';
pass('the autovacuum worker reports its DWB batches in pg_stat_io');

done_testing();
