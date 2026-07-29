
# Copyright (c) 2025, PostgreSQL Global Development Group

# End-to-end tests of the FlushBuffer integration: real pages flow through
# the double write buffer, the retire worker pool frees the ring, pg_stat_io
# accounts the batch IO, and the cluster survives a crash.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_flush');
$node->init;
# A tiny buffer pool forces evictions through the DWB on a modest workload;
# the short batch timeout keeps the worker pool sealing and retiring briskly.
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 2
dwb_batch_timeout_ms = 20
shared_buffers = 2MB
autovacuum = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_dwb');

# --- the worker pool is running -----------------------------------------

# the workers start asynchronously once the server is up
$node->poll_query_until('postgres',
	"SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'dwb retire worker'"
) or die 'timed out waiting for the retire workers to start';
pass('both retire workers are running');

# --- a real workload flows through the ring ------------------------------

$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_t AS
		SELECT g AS id, repeat('x', 300) AS filler
		FROM generate_series(1, 50000) g;
	UPDATE dwb_t SET filler = repeat('y', 300) WHERE id % 10 = 0;
));
$node->safe_psql('postgres', 'CHECKPOINT');

is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50000', 'workload survived the DWB write path');

# The workload far exceeds shared_buffers, so evictions must have staged
# real pages into the ring under the current generation.
cmp_ok($node->safe_psql('postgres', 'SELECT test_dwb_ring_slots(true)'),
	'>', 0, 'real pages were staged into the ring');

# --- the worker pool retires everything ----------------------------------

$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the retire workers to free the ring';
pass('retire workers returned the ring to all-free');

# --- pg_stat_io accounts the batch writes and fdatasyncs -----------------

is( $node->safe_psql(
		'postgres',
		"SELECT sum(writes) > 0 AND sum(fsyncs) > 0 FROM pg_stat_io WHERE object = 'dwb'"
	),
	't',
	'pg_stat_io shows double write buffer writes and fsyncs');

# --- crash recovery: data intact, generation bumped ----------------------

# Until the Stage 4 apply-pass lands this is a WAL-replay smoke test: it
# asserts that the DWB write path corrupts nothing and replay still works,
# not that torn pages get repaired from the ring.
$node->safe_psql('postgres',
	"UPDATE dwb_t SET filler = repeat('z', 300) WHERE id % 7 = 0");
$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50000', 'data intact after crash recovery');

# --- unlogged relations bypass the double write buffer -------------------

# A normal CHECKPOINT skips unlogged buffers entirely (BufferSync writes
# only BM_PERMANENT ones), so the flush that exercises the FlushBuffer gate
# is the SHUTDOWN checkpoint of a clean restart, which writes all dirty
# buffers.  The assertion reads the ring itself: a broken gate would leave
# dwb_ul's tags in batch files, and no stray permanent-page flush can fake
# that.
$node->safe_psql(
	'postgres', q(
	CREATE UNLOGGED TABLE dwb_ul AS
		SELECT g AS id, repeat('u', 300) AS filler
		FROM generate_series(1, 1000) g;
	UPDATE dwb_ul SET filler = repeat('w', 300);
));
my $ul_filenode =
  $node->safe_psql('postgres', "SELECT pg_relation_filenode('dwb_ul')");
my $t_filenode =
  $node->safe_psql('postgres', "SELECT pg_relation_filenode('dwb_t')");
my $rel_pre = $node->safe_psql('postgres',
		"SELECT COALESCE(sum(writes), 0) FROM pg_stat_io "
	  . "WHERE object = 'relation' AND backend_type = 'checkpointer'");
$node->restart;
cmp_ok(
	$node->safe_psql(
		'postgres',
		"SELECT COALESCE(sum(writes), 0) FROM pg_stat_io "
		  . "WHERE object = 'relation' AND backend_type = 'checkpointer'"),
	'>', $rel_pre,
	'shutdown checkpoint flushed the unlogged pages');
is( $node->safe_psql(
		'postgres', "SELECT test_dwb_ring_rel_slots($ul_filenode)"),
	'0',
	'no unlogged page ever entered the ring');
cmp_ok(
	$node->safe_psql(
		'postgres', "SELECT test_dwb_ring_rel_slots($t_filenode)"),
	'>', 0,
	'permanent pages did enter the ring (control)');

# --- the write path is self-sufficient without the worker pool -----------

# dwb_retire_workers = 0: writers seal immediately and retire synchronously
# in DWBFinishPageWrite; a real eviction workload must keep circulating and
# the ring must drain to all-free without any worker.
$node->append_conf('postgresql.conf', 'dwb_retire_workers = 0');
$node->restart;
$node->safe_psql('postgres',
	"UPDATE dwb_t SET filler = repeat('n', 300) WHERE id % 5 = 0");
$node->safe_psql('postgres', 'CHECKPOINT');
is($node->safe_psql('postgres', 'SELECT count(*) FROM dwb_t'),
	'50000', 'workload survived the no-pool write path');
$node->poll_query_until('postgres',
	"SELECT test_dwb_states() LIKE 'free=16 %'")
  or die 'timed out waiting for the ring to drain without a worker pool';
pass('ring drained to all-free without a worker pool');

done_testing();
