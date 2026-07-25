
# Copyright (c) 2025, PostgreSQL Global Development Group

# The io_torn_pages_protection modes that do not use the ring: "off"
# forces full-page images off no matter what the legacy GUC says (and is
# loud about it), "full_pages" defers to the legacy full_page_writes GUC,
# and under "double_writes" a SIGHUP of the legacy GUC is a no-op.

use strict;
use warnings FATAL => 'all';
use File::Path qw(rmtree);
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

# --- leaving double_writes takes one clean shutdown ----------------------

# After a crash the ring may hold repairs only a double_writes start can
# apply, so a start in any other mode is refused until the ring has been
# closed cleanly once.
$node->stop('immediate');

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = full_pages');
my $ret = $node->start(fail_ok => 1);
is($ret, 0, 'crashed ring refuses a full_pages start');
ok( $node->log_contains(
		qr/FATAL: .* the double write buffer ring was not cleanly shut down, cannot start with "io_torn_pages_protection=full_pages"/
	),
	'... naming the mode change as the problem');

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = off');
$ret = $node->start(fail_ok => 1);
is($ret, 0, 'crashed ring refuses an off start too');

# one double_writes start applies the ring, and a clean stop releases it
$node->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$log_offset = -s $node->logfile;
$node->start;
ok( $node->log_contains(qr/double write buffer recovery:/, $log_offset),
	'the double_writes start runs the apply-pass');
$node->stop;

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = full_pages');
$log_offset = -s $node->logfile;
$node->start;
is( $node->safe_psql('postgres', 'SHOW io_torn_pages_protection'),
	'full_pages', 'after a clean stop the mode change is legal');
ok( !$node->log_contains(qr/ring opened/, $log_offset),
	'... and the leftover ring stays closed');

# --- reopening the ring re-arms the guard --------------------------------

# A double_writes start clears RING_CLEAN before the ring reopens, so a
# crash of that run leaves the marker unset and the guard must fire again:
# the marker certifies one clean closure, not a permanent state.
$node->stop;
$node->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$node->start;
$node->stop('immediate');

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = full_pages');
$ret = $node->start(fail_ok => 1);
is($ret, 0, 'a crash after reopening the ring re-arms the guard');

# close it cleanly once more
$node->append_conf('postgresql.conf',
	'io_torn_pages_protection = double_writes');
$node->start;
$node->stop;

# --- a corrupt ring control is refused, with a way out -------------------

# Modes that never touch the ring must still refuse an unreadable control
# (the ring may hold unapplied repairs), but with a message naming the
# removal recipe instead of a bare low-level read error.
my $control = $node->data_dir . '/pg_dwb/control';
open my $fh, '>', $control or die "open $control: $!";
binmode $fh;
print $fh "\x00" x 16;
close $fh;

$node->append_conf('postgresql.conf', 'io_torn_pages_protection = full_pages');
$log_offset = -s $node->logfile;
$ret = $node->start(fail_ok => 1);
is($ret, 0, 'a corrupt ring control refuses a full_pages start');
ok( $node->log_contains(
		qr/FATAL: .* the double write buffer ring state could not be validated, cannot start with "io_torn_pages_protection=full_pages"/,
		$log_offset),
	'... naming the ring state as the problem');

# the hint's recipe: removing pg_dwb unblocks the start
rmtree($node->data_dir . '/pg_dwb');
$node->start;
is( $node->safe_psql('postgres', 'SHOW io_torn_pages_protection'),
	'full_pages', 'removing pg_dwb unblocks the non-ring mode');

done_testing();
