
# Copyright (c) 2025, PostgreSQL Global Development Group

# The startup apply-pass: torn data pages are repaired from the ring before
# WAL replay.  The scenarios pick pages replay itself can never fix — a
# hint-bit-only page is logged as XLOG_FPI_FOR_HINT without an image and
# redo does not even read it, and the other damaged pages predate the last
# checkpoint — so any repair observed here came from the ring.  Also pins
# the boundaries of the pass: a second pass over the same ring is a no-op
# (idempotence after a crash mid-pass), a clean start skips the pass, and a
# slot of a past generation is never applied, even to a page that fails
# verification.
#
# With dwb_retire_workers = 0 every publish retires its batch on the spot,
# so a sequential writer keeps reusing the lowest ring index and only the
# LAST page written before a crash still has its slot on disk.  Each damage
# scenario therefore ends its run with the target page's flush:
# BufferSync sorts a checkpoint's writes by relfilenode, and the user
# tables here sort after every catalog page a session may have hint-dirtied.

use strict;
use warnings FATAL => 'all';
use File::Path qw(rmtree);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_recovery');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_batch_timeout_ms = 20
autovacuum = off
bgwriter_lru_maxpages = 0
log_min_messages = debug1
));
$node->start;

sub read_block
{
	my ($file, $blkno) = @_;
	my $buf;

	open my $fh, '<:raw', $file or die "could not open $file: $!";
	sysseek($fh, $blkno * 8192, 0) or die "could not seek $file: $!";
	sysread($fh, $buf, 8192) == 8192 or die "short read from $file: $!";
	close $fh;
	return $buf;
}

sub write_block
{
	my ($file, $blkno, $buf) = @_;

	open my $fh, '+<:raw', $file or die "could not open $file: $!";
	sysseek($fh, $blkno * 8192, 0) or die "could not seek $file: $!";
	syswrite($fh, $buf) == length($buf) or die "short write to $file: $!";
	close $fh;
	return;
}

$node->safe_psql('postgres', q(
	CREATE TABLE thint AS SELECT g AS id FROM generate_series(1, 100) g;
	CREATE TABLE told AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');

my $thint_file =
  $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('thint')");
my $told_file =
  $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('told')");
my $thint_relnum = $node->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'thint'");
my $told_relnum = $node->safe_psql('postgres',
	"SELECT relfilenode FROM pg_class WHERE relname = 'told'");

# --- a torn hint-bit-only page is repaired before replay -----------------

# With checksums on and page images off, setting hint bits logs
# XLOG_FPI_FOR_HINT without an image and still advances the page LSN, and
# redo of such a record never reads the page — the original silent-loss
# hole.  Dirty thint's page with hint bits only, flush it through the ring,
# crash, and tear the on-disk page as if that flush had been cut short.
$node->safe_psql('postgres', 'SELECT count(*) FROM thint');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop('immediate');

write_block($thint_file, 0,
	substr(read_block($thint_file, 0), 0, 4096) . ("\0" x 4096));

my $log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 1 of 1 candidate pages restored/,
		$log_offset),
	'apply-pass restored the torn page');
ok( $node->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$thint_relnum fork 0!,
		$log_offset),
	'... and it was the hint-bit page');
ok( $node->log_contains(
		qr/ring opened: 16 batches of 16 pages, generation 2\b/, $log_offset),
	'generation bumped after the pass');
is( $node->safe_psql('postgres', 'SELECT count(*) FROM thint'),
	'100', 'torn hint page is whole again');

# --- a checksum-valid but stale page is repaired by its LSN --------------

# Put the pre-update page image back after the crash: bytewise it verifies
# fine, so only the LSN comparison can see that the ring copy is newer.
my $told_v1 = read_block($told_file, 0);
$node->safe_psql('postgres', 'UPDATE told SET id = id + 1000 WHERE id <= 50');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->stop('immediate');

write_block($told_file, 0, $told_v1);

# keep a copy of the whole ring for the re-apply scenario below
my $ring_stash = $node->basedir . '/ring_stash';
PostgreSQL::Test::RecursiveCopy::copypath($node->data_dir . '/pg_dwb',
	$ring_stash);

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 1 of 1 candidate pages restored/,
		$log_offset),
	'apply-pass restored the stale page');
ok( $node->log_contains(
		qr!restoring page 0 of relation \d+/\d+/$told_relnum fork 0!,
		$log_offset),
	'... by its LSN — the page verified fine');
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM told WHERE id > 1000'),
	'50', 'stale page carries the update again');

# --- a repeated pass over the same ring is a no-op -----------------------

# Equivalent to a crash in the middle of the pass: the ring is untouched
# and the generation not yet bumped, so the next start sees the very same
# candidate — now against a repaired, newer-or-equal page.
$node->stop('immediate');
rmtree($node->data_dir . '/pg_dwb');
PostgreSQL::Test::RecursiveCopy::copypath($ring_stash,
	$node->data_dir . '/pg_dwb');

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 0 of 1 candidate pages restored/,
		$log_offset),
	're-applied pass sees the same candidate and rewrites nothing');
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM told WHERE id > 1000'),
	'50', 'data intact after the repeated pass');

# --- a clean start skips the pass, but still bumps the generation --------

$node->safe_psql('postgres', q(
	CREATE TABLE tstale AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
my $tstale_file =
  $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('tstale')");
$node->stop;

my $tstale_good = read_block($tstale_file, 0);

$log_offset = -s $node->logfile;
$node->start;
ok( !$node->log_contains(qr/double write buffer recovery:/, $log_offset),
	'clean start runs no apply-pass');
ok( $node->log_contains(qr/ring opened: .* generation 4\b/, $log_offset),
	'... yet the generation still moves, expiring the old slots');

# --- a slot of a past generation is never applied ------------------------

# tstale's only slot is one generation behind now.  Corrupt its page so
# that it fails verification — the branch where an LSN comparison cannot
# veto a repair — and crash: the pass must leave the page alone anyway.
$node->stop('immediate');
write_block($tstale_file, 0, chr(0xAB) x 8192);

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 0 of 0 candidate pages restored/,
		$log_offset),
	'no current-generation candidates after the idle crash');
is( read_block($tstale_file, 0), chr(0xAB) x 8192,
	'the stale slot was not applied to the corrupted page');

# put the good page back so the cluster winds down healthy
write_block($tstale_file, 0, $tstale_good);
is( $node->safe_psql('postgres', 'SELECT count(*) FROM tstale'),
	'100', 'page manually restored, cluster consistent');

# --- the marker alone triggers the pass, not the pg_control state --------

# Leave a crashed ring behind a CLEAN pg_control: stash the ring right
# after a crash, run a clean stop cycle, then put the crashed ring back.
# Only the unset RING_CLEAN marker knows this ring was never retired — a
# standby whose shutdown restartpoint was skipped leaves exactly this
# combination, and the pass must key on the marker, not on pg_control.
$node->safe_psql('postgres', q(
	CREATE TABLE tmark AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
my $tmark_file =
  $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('tmark')");
$node->stop('immediate');

my $mark_stash = $node->basedir . '/mark_stash';
PostgreSQL::Test::RecursiveCopy::copypath($node->data_dir . '/pg_dwb',
	$mark_stash);

$node->start;
$node->stop;

rmtree($node->data_dir . '/pg_dwb');
PostgreSQL::Test::RecursiveCopy::copypath($mark_stash,
	$node->data_dir . '/pg_dwb');
write_block($tmark_file, 0,
	substr(read_block($tmark_file, 0), 0, 4096) . ("\0" x 4096));

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 1 of 1 candidate pages restored/,
		$log_offset),
	'unretired ring is applied despite a clean pg_control');
is( $node->safe_psql('postgres', 'SELECT count(*) FROM tmark'),
	'100', 'torn page behind a clean shutdown is whole again');

# --- a slot for a dropped relation is skipped ----------------------------

# The relation's file may survive as an empty tombstone until the next
# checkpoint, or be gone entirely; either way there is nothing to repair
# and the pass must not trip over it.
$node->safe_psql('postgres', q(
	CREATE TABLE tdrop AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', 'DROP TABLE tdrop');
$node->stop('immediate');

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 0 of 1 candidate pages restored/,
		$log_offset),
	'a candidate for a dropped relation is counted but skipped');

# --- an all-zero on-disk page is never repaired --------------------------

# Zero the whole block: an empty header means replay recreates the page
# from its init record without reading it, and a stale slot must not
# resurrect on it — the zeroed page's LSN 0 would lose the LSN comparison
# that this skip protects.
$node->safe_psql('postgres', q(
	CREATE TABLE tzero AS SELECT g AS id FROM generate_series(1, 100) g;
));
$node->safe_psql('postgres', 'CHECKPOINT');
my $tzero_file =
  $node->data_dir . '/'
  . $node->safe_psql('postgres', "SELECT pg_relation_filepath('tzero')");
$node->stop('immediate');

my $tzero_good = read_block($tzero_file, 0);
write_block($tzero_file, 0, "\0" x 8192);

$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(
		qr/double write buffer recovery: 0 of 1 candidate pages restored/,
		$log_offset),
	'a zeroed page is not repaired from its slot');
is( read_block($tzero_file, 0), "\0" x 8192,
	'... and stays zero for replay to drive');

# put the good page back so the cluster winds down healthy
write_block($tzero_file, 0, $tzero_good);
is( $node->safe_psql('postgres', 'SELECT count(*) FROM tzero'),
	'100', 'page manually restored, cluster consistent');

done_testing();
