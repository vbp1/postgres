
# Copyright (c) 2025, PostgreSQL Global Development Group

# Online backup under io_torn_pages_protection = double_writes: WAL carries
# no full-page images in normal running, but an active backup forces them
# back on (the ring only repairs local torn writes; a backup copied
# mid-write can hold a torn page that only WAL replay with FPIs can
# repair).  The backup itself excludes pg_dwb/ contents, tolerates pg_dwb
# being a symlink, and a cluster restored from it cold-starts a fresh ring.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dwb_backup');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_batch_timeout_ms = 20
autovacuum = off
));
$node->start;

$node->safe_psql('postgres', q(
	CREATE TABLE dwb_fpi AS
		SELECT g AS id, repeat('f', 64) AS pad FROM generate_series(1, 100) g;
));
my $fpi_filenode =
  $node->safe_psql('postgres', "SELECT pg_relation_filenode('dwb_fpi')");

# --- normal running writes no full-page images ---------------------------

# The first touch of a page after a checkpoint is exactly where an FPI
# would go; under double_writes none may appear.  Hint-bit records
# (FPI_FOR_HINT) can still show up in the window, but without doPageWrites
# they carry no page image and thus no FPW block flag — the probe below
# stays meaningful.
$node->safe_psql('postgres', 'CHECKPOINT');
my $lsn0 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', "UPDATE dwb_fpi SET pad = repeat('a', 64) WHERE id = 1");
my $lsn1 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

my ($waldump, $walerr) = run_command(
	[
		'pg_waldump', '--path' => $node->data_dir . '/pg_wal',
		'--start' => $lsn0, '--end' => $lsn1
	]);
is($walerr, '', 'pg_waldump read the no-backup window cleanly');
like($waldump, qr/Heap/, 'the WAL window covers the update');
unlike($waldump, qr/\bFPW\b/, 'no full-page image outside a backup');

# --- an active backup forces full-page images back on ---------------------

# pg_backup_start checkpoints and raises runningBackups; the first touch
# of any page after that must carry an FPI (doPageWrites =
# Insert->fullPageWrites || runningBackups > 0).
my $bk = $node->background_psql('postgres');
$bk->query_safe('SET client_min_messages = warning');
$bk->query_safe("SELECT pg_backup_start('dwb_fpi_probe', true)");

my $lsn2 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', "UPDATE dwb_fpi SET pad = repeat('b', 64) WHERE id = 2");
my $lsn3 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

($waldump, $walerr) = run_command(
	[
		'pg_waldump', '--path' => $node->data_dir . '/pg_wal',
		'--start' => $lsn2, '--end' => $lsn3
	]);
is($walerr, '', 'pg_waldump read the backup window cleanly');
like(
	$waldump,
	qr!rel \d+/\d+/$fpi_filenode .* FPW!,
	'an active backup forces a full-page image of the touched page');

$bk->query_safe('SELECT pg_backup_stop()');
$bk->quit;

# --- the backup keeps pg_dwb as an empty directory ------------------------

# guard against a vacuous emptiness assert: the source ring is non-empty
ok(-f $node->data_dir . '/pg_dwb/control',
	'the source cluster has ring files to exclude');

my $backup_path = $node->backup_dir . '/content_check';
my ($out, $err) = run_command(
	[
		'pg_basebackup', '--no-sync',
		'--pgdata' => $backup_path,
		'--host' => $node->host,
		'--port' => $node->port,
		'--checkpoint' => 'fast'
	]);
ok(-f "$backup_path/PG_VERSION", 'backup completed');
unlike($err, qr/WARNING|skipping special file/,
	'pg_basebackup issued no warnings');
ok(-d "$backup_path/pg_dwb", 'backup contains a pg_dwb directory');
{
	opendir(my $dh, "$backup_path/pg_dwb") or die "opendir: $!";
	my @entries = grep { !/^\.\.?$/ } readdir($dh);
	closedir($dh);
	is(scalar(@entries), 0, 'the pg_dwb directory in the backup is empty');
}

# --- a cluster restored from the backup cold-starts its ring --------------

my $restored = PostgreSQL::Test::Cluster->new('dwb_restored');
$restored->init_from_backup($node, 'content_check');
my $restored_log_offset = -s $restored->logfile;
$restored->start;
ok( $restored->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$restored_log_offset),
	'restored cluster opened a fresh ring');
is( $restored->safe_psql('postgres', 'SELECT count(*) FROM dwb_fpi'),
	'100', 'restored data is intact');
$restored->stop;

# --- a ring shipped into a restore by a foreign tool is discarded ---------

# pg_basebackup excludes the ring, but a third-party backup tool may ship
# pg_dwb/ contents into the restore.  Neither staleness defence works
# there — the slots carry the restored control's own generation, and the
# restored data files are legitimately older than the slot copies — so a
# start from a base backup (backup_label present) must not apply the ring:
# it is wiped and recreated cold.  The planted control file is short
# garbage; reading it would be a fatal "read 16 of 40" error, so this also
# pins that the wipe comes before any ring-state read.
my $planted = PostgreSQL::Test::Cluster->new('dwb_planted');
$planted->init_from_backup($node, 'content_check');
ok(-f $planted->data_dir . '/backup_label',
	'the restore still carries backup_label');
append_to_file($planted->data_dir . '/pg_dwb/control', 'torn by the tool');
append_to_file($planted->data_dir . '/pg_dwb/batch_9999', 'foreign slots');

my $planted_log_offset = -s $planted->logfile;
$planted->start;
ok( $planted->log_contains(
		qr/discarding double write buffer ring contents restored from a base backup/,
		$planted_log_offset),
	'the restored ring is discarded');
ok( !$planted->log_contains(
		qr/double write buffer recovery:/, $planted_log_offset),
	'... without an apply-pass over it');
ok( $planted->log_contains(
		qr/ring opened: 16 batches of 16 pages, generation 1\b/,
		$planted_log_offset),
	'... and a fresh ring is created cold');
ok(!-f $planted->data_dir . '/pg_dwb/batch_9999',
	'the foreign ring files are gone');
is( $planted->safe_psql('postgres', 'SELECT count(*) FROM dwb_fpi'),
	'100', 'restored data is intact');

# once the backup recovery is over the guard is gone: an ordinary crash
# of this cluster is served by the apply-pass again
$planted->stop('immediate');
$planted_log_offset = -s $planted->logfile;
$planted->start;
ok( $planted->log_contains(
		qr/double write buffer recovery: \d+ of \d+ candidate pages restored/,
		$planted_log_offset),
	'a later crash of the restored cluster applies the ring normally');
$planted->stop;

# --- the wipe also covers restores in the non-ring modes ------------------

# A planted ring is dangerous even to a cluster restored under
# "full_pages": left dormant, it would greet a much later switch to
# double_writes with a plausible control file.  The restore start must
# discard it — before any ring-state read, and without tripping the
# downgrade guard on it.
my $planted_fp = PostgreSQL::Test::Cluster->new('dwb_planted_fp');
$planted_fp->init_from_backup($node, 'content_check');
$planted_fp->append_conf('postgresql.conf',
	'io_torn_pages_protection = full_pages');
append_to_file($planted_fp->data_dir . '/pg_dwb/control', 'torn by the tool');
append_to_file($planted_fp->data_dir . '/pg_dwb/batch_9999', 'foreign slots');

my $planted_fp_log_offset = -s $planted_fp->logfile;
$planted_fp->start;
ok( $planted_fp->log_contains(
		qr/discarding double write buffer ring contents restored from a base backup/,
		$planted_fp_log_offset),
	'a full_pages restore discards the planted ring too');
ok(!-f $planted_fp->data_dir . '/pg_dwb/batch_9999',
	'... removing the foreign files');
ok( !$planted_fp->log_contains(qr/ring opened/, $planted_fp_log_offset),
	'... without creating a ring it will not use');
is( $planted_fp->safe_psql('postgres', 'SELECT count(*) FROM dwb_fpi'),
	'100', 'restored data is intact under full_pages');
$planted_fp->stop;

# --- pg_dwb as a symlink backs up as an empty real directory --------------

SKIP:
{
	skip 'symlinks are not portable to Windows', 4 if $windows_os;

	$node->stop;
	my $dwb_store = $node->basedir . '/dwb_store';
	rename($node->data_dir . '/pg_dwb', $dwb_store)
	  or die "rename pg_dwb: $!";
	symlink($dwb_store, $node->data_dir . '/pg_dwb')
	  or die "symlink pg_dwb: $!";
	$node->start;

	my $link_backup = $node->backup_dir . '/symlink_check';
	($out, $err) = run_command(
		[
			'pg_basebackup', '--no-sync',
			'--pgdata' => $link_backup,
			'--host' => $node->host,
			'--port' => $node->port,
			'--checkpoint' => 'fast'
		]);
	ok(-f "$link_backup/PG_VERSION", 'backup of the symlinked ring completed');
	unlike($err, qr/WARNING|skipping special file/,
		'no warnings for the symlinked pg_dwb');
	ok(-d "$link_backup/pg_dwb" && !-l "$link_backup/pg_dwb",
		'symlinked pg_dwb became a real directory in the backup');
	opendir(my $dh, "$link_backup/pg_dwb") or die "opendir: $!";
	my @entries = grep { !/^\.\.?$/ } readdir($dh);
	closedir($dh);
	is(scalar(@entries), 0, 'the symlinked pg_dwb backed up empty');
}

done_testing();
