
# Copyright (c) 2025, PostgreSQL Global Development Group

# The io_torn_pages_protection modes that do not use the ring: "off"
# forces full-page images off no matter what the legacy GUC says (and is
# loud about it), "full_pages" defers to the legacy full_page_writes GUC,
# and under "double_writes" a SIGHUP of the legacy GUC is a no-op.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_modes');
$node->init;

# A tiny helper: pg_waldump over the WAL the given statement generated.
sub wal_window
{
	my ($stmt) = @_;
	my $lsn0 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', $stmt);
	my $lsn1 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	my ($out, $err) = run_command(
		[
			'pg_waldump', '--path' => $node->data_dir . '/pg_wal',
			'--start' => $lsn0, '--end' => $lsn1
		]);
	is($err, '', "pg_waldump read the window of: $stmt");
	return $out;
}

# --- mode "off": no page images regardless of the legacy GUC -------------

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = off');
my $log_offset = (-s $node->logfile) // 0;
$node->start;
ok( $node->log_contains(
		qr/torn page protection is disabled/, $log_offset),
	'mode "off" announces itself');
ok(!-d $node->data_dir . '/pg_dwb', 'mode "off" creates no ring');
is( $node->safe_psql('postgres', 'SHOW full_page_writes'),
	'on', 'the legacy GUC still reads on...');

$node->safe_psql('postgres',
	'CREATE TABLE dwb_m AS SELECT g AS id FROM generate_series(1, 100) g');
$node->safe_psql('postgres', 'CHECKPOINT');
my $dump = wal_window('UPDATE dwb_m SET id = id WHERE id = 1');
like($dump, qr/Heap/, '...the window covers the update...');
unlike($dump, qr/\bFPW\b/, '...but no page image is written');

# --- mode "full_pages": the legacy GUC keeps its vanilla meaning ---------

$node->append_conf('postgresql.conf', qq(
io_torn_pages_protection = full_pages
full_page_writes = off
));
$node->restart;
$node->safe_psql('postgres', 'CHECKPOINT');
$dump = wal_window('UPDATE dwb_m SET id = id WHERE id = 2');
like($dump, qr/Heap/, 'full_pages + legacy off: window covers the update');
unlike($dump, qr/\bFPW\b/, 'full_pages + legacy off: no page image');

$node->append_conf('postgresql.conf', 'full_page_writes = on');
$node->restart;
$node->safe_psql('postgres', 'CHECKPOINT');
$dump = wal_window('UPDATE dwb_m SET id = id WHERE id = 3');
like($dump, qr/\bFPW\b/, 'full_pages + legacy on: page image written');

# --- mode "double_writes": a SIGHUP of the legacy GUC is a no-op ---------

$node->append_conf('postgresql.conf', qq(
io_torn_pages_protection = double_writes
full_page_writes = on
));
$node->restart;
$node->safe_psql('postgres', 'CHECKPOINT');
$dump = wal_window('UPDATE dwb_m SET id = id WHERE id = 4');
unlike($dump, qr/\bFPW\b/, 'double_writes ignores the legacy on');

my $reload_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->append_conf('postgresql.conf', 'full_page_writes = off');
$node->reload;
# a CHECKPOINT forces a checkpointer cycle, which processes the pending
# SIGHUP (and would emit XLOG_FPW_CHANGE if the reload were not a no-op)
$node->safe_psql('postgres', 'CHECKPOINT');
$dump = wal_window('UPDATE dwb_m SET id = id WHERE id = 5');
unlike($dump, qr/\bFPW\b/, 'reloading the legacy GUC changes nothing');

my $lsn_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my ($out, $err) = run_command(
	[
		'pg_waldump', '--path' => $node->data_dir . '/pg_wal',
		'--start' => $reload_lsn, '--end' => $lsn_end
	]);
is($err, '', 'pg_waldump read the reload window cleanly');
unlike($out, qr/FPW_CHANGE/,
	'the no-op reload emitted no XLOG_FPW_CHANGE record');

done_testing();
