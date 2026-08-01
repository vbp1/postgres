
# Copyright (c) 2025, PostgreSQL Global Development Group

# The vectored background flush: a checkpoint writes its buffers through
# the double write buffer in bins — one batch write and one fdatasync per
# bin instead of one per page.  pg_stat_io proves the batching (the dwb
# "writes" counter is per batch, so write_bytes/writes is the batch size),
# and a torn page written by the vectored path is restored by the
# apply-pass like any other.

use strict;
use warnings FATAL => 'all';
use FindBin;
use lib $FindBin::RealBin;
use DWBTest;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_vectored');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
autovacuum = off
bgwriter_lru_maxpages = 0
log_min_messages = debug1
));
$node->start;

# ~200 heap pages, all dirty: enough for a dozen full bins
$node->safe_psql(
	'postgres', q(
	CREATE TABLE dwb_vec (id int, pad text) WITH (fillfactor = 10);
	INSERT INTO dwb_vec SELECT g, repeat('v', 256) FROM generate_series(1, 1000) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', "UPDATE dwb_vec SET pad = repeat('w', 256)");
my $npages =
  $node->safe_psql('postgres', "SELECT pg_relation_size('dwb_vec') / 8192");
cmp_ok($npages, '>', 150, 'the table spans enough pages for full bins');

$node->safe_psql('postgres', 'CHECKPOINT');

# --- the checkpointer writes multi-slot batches ---------------------------

# One dwb "write" is one batch write by its leader, so write_bytes/writes
# is the average batch size: the meta region (4096 bytes on this geometry)
# plus one 8 KB image per slot.  The per-page protocol pins this ratio at
# exactly one slot for the checkpointer's sequential stream; the vectored
# flush must push it to bin-sized fills.  Checkpointer stats reach the
# collector with a delay, so poll.
$node->poll_query_until(
	'postgres', q(
	SELECT writes > 0 FROM pg_stat_io
	WHERE backend_type = 'checkpointer' AND object = 'dwb'
		AND context = 'normal'
)) or die 'timed out waiting for checkpointer dwb stats';

my $avg_slots = $node->safe_psql(
	'postgres', q(
	SELECT round((write_bytes::numeric / writes - 4096) / 8192, 1)
	FROM pg_stat_io
	WHERE backend_type = 'checkpointer' AND object = 'dwb'
		AND context = 'normal'
));
cmp_ok($avg_slots, '>=', 4,
	"checkpointer batches average $avg_slots slots, not one per page");

# --- a torn page of the vectored path is repaired -------------------------

# The last heap block sorts last in BufferSync, so it lands in the final
# bin and its slot survives any batch-index reuse by earlier bins.
my $vec_file =
	$node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('dwb_vec')");
my $vec_relnum = $node->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'dwb_vec'");
my $sum_before = $node->safe_psql('postgres', 'SELECT sum(id) FROM dwb_vec');
my $last_block = $npages - 1;

$node->stop('immediate');
write_block($vec_file, $last_block,
	substr(read_block($vec_file, $last_block), 0, 4096) . ("\0" x 4096));

my $log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 1 of \d+ candidate pages restored/,
		$log_offset),
	'apply-pass restored the page torn under the vectored flush');
ok( $node->log_contains(
		qr!restoring page $last_block of relation \d+/\d+/$vec_relnum fork 0!,
		$log_offset),
	'... and it was the torn heap page');
is($node->safe_psql('postgres', 'SELECT sum(id) FROM dwb_vec'),
	$sum_before, 'data intact after the repair');

done_testing();
