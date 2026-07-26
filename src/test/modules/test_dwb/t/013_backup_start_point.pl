
# Copyright (c) 2025, PostgreSQL Global Development Group

# The second arm of the restored-backup detection: backup_label is renamed
# away early in the first recovery attempt, so a crash mid-backup-recovery
# leaves only pg_control's backupStartPoint to say "this is still a restored
# backup".  A low-level backup copied without the backup-end WAL fails its
# recovery deterministically ("WAL ends before end of online backup"), which
# yields both states in turn: the first start sees the label, the second
# start sees no label but a set backupStartPoint — and both must discard
# the ring instead of applying it.

use strict;
use warnings FATAL => 'all';
use FindBin;
use lib $FindBin::RealBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_bsp');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 0
dwb_batch_timeout_ms = 20
autovacuum = off
));
$node->start;

$node->safe_psql('postgres', q(
	CREATE TABLE dwb_bsp AS SELECT g AS id FROM generate_series(1, 100) g;
));

# A low-level backup: open the window, OS-copy the whole data directory,
# close the window.  The copy predates pg_backup_stop, so it cannot contain
# the backup-end WAL record — recovery from it must run out of WAL inside
# the backup window.
my $bk = $node->background_psql('postgres');
$bk->query_safe('SET client_min_messages = warning');
$bk->query_safe("SELECT pg_backup_start('dwb_bsp_wide', true)");
$node->safe_psql('postgres',
	'INSERT INTO dwb_bsp SELECT g FROM generate_series(101, 200) g');

my $backup_path = $node->backup_dir . '/wide';
PostgreSQL::Test::RecursiveCopy::copypath($node->data_dir, $backup_path);
unlink("$backup_path/postmaster.pid", "$backup_path/postmaster.opts");

my $label = $bk->query_safe('SELECT labelfile FROM pg_backup_stop()');
$bk->quit;
like($label, qr/^START WAL LOCATION/, 'pg_backup_stop returned the label');
open my $lf, '>', "$backup_path/backup_label" or die "backup_label: $!";
print $lf $label;
print $lf "\n" unless $label =~ /\n$/;
close $lf;

my $restored = PostgreSQL::Test::Cluster->new('dwb_bsp_restored');
$restored->init_from_backup($node, 'wide');
my $pgdata = $restored->data_dir;

# --- first start: the label arm ------------------------------------------

my $log_offset = -s $restored->logfile;
my $ret = $restored->start(fail_ok => 1);
is($ret, 0, 'recovery without the backup-end WAL fails');
ok( $restored->log_contains(
		qr/FATAL: .* WAL ends before end of online backup/, $log_offset),
	'... for the expected reason');
ok( $restored->log_contains(
		qr/discarding double write buffer ring contents restored from a base backup/,
		$log_offset),
	'the label start discarded the copied ring');
ok( !$restored->log_contains(
		qr/double write buffer recovery:/, $log_offset),
	'... and ran no apply-pass');

ok(!-f "$pgdata/backup_label", 'the failed recovery consumed backup_label');
ok(-f "$pgdata/backup_label.old", '... renaming it out of the way');
my ($cd, $cderr) = run_command([ 'pg_controldata', $pgdata ]);
like($cd, qr/Backup start location:\s+(?!0\/0)\S/,
	'pg_control still carries backupStartPoint');

# --- second start: the backupStartPoint arm ------------------------------

# No label anymore; only the control flag says this recovery still belongs
# to a restored backup.  The ring recreated by the first start must be
# discarded again, not applied.
$log_offset = -s $restored->logfile;
$ret = $restored->start(fail_ok => 1);
is($ret, 0, 'the second recovery attempt fails the same way');
ok( $restored->log_contains(
		qr/discarding double write buffer ring contents restored from a base backup/,
		$log_offset),
	'backupStartPoint alone still discards the ring');
ok( !$restored->log_contains(
		qr/double write buffer recovery:/, $log_offset),
	'... and no apply-pass ran on the second start either');

done_testing();
