
# Copyright (c) 2025, PostgreSQL Global Development Group

# pg_rewind on a double_writes pair: a live source is refused (its WAL has
# no full-page images to repair pages read mid-write), a stopped source
# works, the target's own ring is wiped by the rewind — also when pg_dwb
# is a symlink — and the rewound node cold-starts a fresh ring and follows
# the promoted primary.  A regular file in place of pg_dwb is refused, and
# a broken ring symlink on the source is ignored.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_a = PostgreSQL::Test::Cluster->new('dwb_rewind_a');
$node_a->init(allows_streaming => 1);
$node_a->append_conf(
	'postgresql.conf', qq(
io_torn_pages_protection = double_writes
dwb_num_batches = 16
dwb_batch_pages = 16
dwb_retire_workers = 1
dwb_batch_timeout_ms = 20
autovacuum = off
wal_keep_size = 64MB
));
$node_a->start;
$node_a->safe_psql('postgres', q(
	CREATE TABLE dwb_r AS SELECT g AS id FROM generate_series(1, 100) g;
));

$node_a->backup('bkp');
my $node_b = PostgreSQL::Test::Cluster->new('dwb_rewind_b');
$node_b->init_from_backup($node_a, 'bkp', has_streaming => 1);
$node_b->start;
$node_a->wait_for_catchup($node_b);

# --- diverge the timelines -----------------------------------------------

$node_b->promote;
$node_b->safe_psql('postgres', "INSERT INTO dwb_r VALUES (100001)");
# A keeps running as the old primary and diverges past the fork point.
# Crash it: the refusal scenarios below must fire before pg_rewind's
# single-user recovery of the target gets a chance to run.
$node_a->safe_psql('postgres', "INSERT INTO dwb_r VALUES (200001)");
$node_a->stop('immediate');

# --- relocate the target ring behind a symlink ---------------------------

# Like pg_wal, pg_dwb may be a symlink to a directory on other storage;
# the rewind must wipe the ring through the link and leave the link
# itself in place.  Creating symlinks requires a privilege on Windows,
# so the plain-directory layout is exercised there instead.
my $dwb_is_symlinked = 0;
unless ($windows_os)
{
	my $ring_home = PostgreSQL::Test::Utils::tempdir('dwb_ring');
	my $dwb_path = $node_a->data_dir . '/pg_dwb';
	rename($dwb_path, "$ring_home/pg_dwb")
	  or BAIL_OUT("could not move $dwb_path: $!");
	symlink("$ring_home/pg_dwb", $dwb_path)
	  or BAIL_OUT("could not symlink $dwb_path: $!");
	$dwb_is_symlinked = 1;
}

# --- a live double_writes source is refused ------------------------------

command_fails_like(
	[
		'pg_rewind',
		'--target-pgdata' => $node_a->data_dir,
		'--source-server' => $node_b->connstr('postgres')
	],
	qr/"io_torn_pages_protection" must be "full_pages" in the source server/,
	'pg_rewind refuses a live source that writes no full-page images');

# --- a stopped source works and wipes the target ring --------------------

# leave proof on the target that the rewind, not a later cold start,
# removed the ring files
ok(-f $node_a->data_dir . '/pg_dwb/control',
	'the target has ring files before the rewind');

$node_b->stop('fast');

# --- a garbage entry in place of pg_dwb is refused -----------------------

# Anything but a directory, a symlink or nothing at all would survive the
# rewind only to fail the next double_writes startup, so the rewind must
# reject it before touching the target.
{
	my $dwb_path = $node_a->data_dir . '/pg_dwb';
	my $stash = $node_a->basedir . '/pg_dwb_stash';
	rename($dwb_path, $stash) or BAIL_OUT("could not move $dwb_path: $!");
	append_to_file($dwb_path, "not a ring\n");
	command_fails_like(
		[
			'pg_rewind',
			'--target-pgdata' => $node_a->data_dir,
			'--source-pgdata' => $node_b->data_dir
		],
		qr/"pg_dwb" in target is not a directory or symbolic link/,
		'pg_rewind refuses a regular file in place of pg_dwb');
	unlink($dwb_path) or BAIL_OUT("could not remove $dwb_path: $!");

	unless ($windows_os)
	{
		require POSIX;

		# a FIFO never even reaches the file map, so it takes the up-front
		# check
		POSIX::mkfifo($dwb_path, 0700)
		  or BAIL_OUT("could not create FIFO $dwb_path: $!");
		command_fails_like(
			[
				'pg_rewind',
				'--target-pgdata' => $node_a->data_dir,
				'--source-pgdata' => $node_b->data_dir
			],
			qr/"pg_dwb" in target is not a directory or symbolic link/,
			'pg_rewind refuses a FIFO in place of pg_dwb');
		unlink($dwb_path) or BAIL_OUT("could not remove $dwb_path: $!");

		# a symlink is only as good as what it points to
		symlink('/nonexistent/dwb_ring_target', $dwb_path)
		  or BAIL_OUT("could not symlink $dwb_path: $!");
		command_fails_like(
			[
				'pg_rewind',
				'--target-pgdata' => $node_a->data_dir,
				'--source-pgdata' => $node_b->data_dir
			],
			qr/"pg_dwb" in target is a symbolic link that does not point to a directory/,
			'pg_rewind refuses a broken pg_dwb symlink');
		unlink($dwb_path) or BAIL_OUT("could not remove $dwb_path: $!");

		append_to_file("$stash.file", "not a ring\n");
		symlink("$stash.file", $dwb_path)
		  or BAIL_OUT("could not symlink $dwb_path: $!");
		command_fails_like(
			[
				'pg_rewind',
				'--target-pgdata' => $node_a->data_dir,
				'--source-pgdata' => $node_b->data_dir
			],
			qr/"pg_dwb" in target is a symbolic link that does not point to a directory/,
			'pg_rewind refuses a pg_dwb symlink to a regular file');
		unlink($dwb_path) or BAIL_OUT("could not remove $dwb_path: $!");
		unlink("$stash.file") or BAIL_OUT("could not remove $stash.file: $!");

		# garbage inside the ring is refused too: it would silently survive
		# the wipe
		mkdir($dwb_path) or BAIL_OUT("could not create $dwb_path: $!");
		POSIX::mkfifo("$dwb_path/control", 0700)
		  or BAIL_OUT("could not create FIFO $dwb_path/control: $!");
		command_fails_like(
			[
				'pg_rewind',
				'--target-pgdata' => $node_a->data_dir,
				'--source-pgdata' => $node_b->data_dir
			],
			qr!"pg_dwb/control" in target is not a regular file!,
			'pg_rewind refuses a FIFO inside pg_dwb');
		unlink("$dwb_path/control")
		  or BAIL_OUT("could not remove $dwb_path/control: $!");
		rmdir($dwb_path) or BAIL_OUT("could not remove $dwb_path: $!");
	}

	# the crashed target was left alone throughout: every refusal came
	# before the single-user recovery run
	command_like(
		[ 'pg_controldata', $node_a->data_dir ],
		qr/Database cluster state:\s+in production/,
		'the refusals precede the single-user recovery of the target');

	rename($stash, $dwb_path) or BAIL_OUT("could not restore $dwb_path: $!");
}

# return the target to a clean shutdown for the rewind proper; recovery
# runs with the ring already behind the symlink
$node_a->start;
$node_a->stop('fast');

# --- a broken ring symlink on the source is harmless ---------------------

# The source's ring is never used, so only the target's pg_dwb link may be
# entered.  Point the source's at nowhere and let the rewind below prove
# it: if the traversal followed the link, it would fail outright.
my $b_dwb_stash;
unless ($windows_os)
{
	my $b_dwb = $node_b->data_dir . '/pg_dwb';
	$b_dwb_stash = $node_b->basedir . '/pg_dwb_stash';
	rename($b_dwb, $b_dwb_stash) or BAIL_OUT("could not move $b_dwb: $!");
	symlink('/nonexistent/dwb_ring', $b_dwb)
	  or BAIL_OUT("could not symlink $b_dwb: $!");
}

command_ok(
	[
		'pg_rewind',
		'--target-pgdata' => $node_a->data_dir,
		'--source-pgdata' => $node_b->data_dir
	],
	'pg_rewind from a stopped source succeeds');

ok(-d $node_a->data_dir . '/pg_dwb', 'the target still has a pg_dwb directory');
{
	opendir(my $dh, $node_a->data_dir . '/pg_dwb') or die "opendir: $!";
	my @entries = grep { !/^\.\.?$/ } readdir($dh);
	closedir($dh);
	is(scalar(@entries), 0, 'the rewind wiped the target ring');
}
ok(-l $node_a->data_dir . '/pg_dwb', 'the pg_dwb symlink survived the rewind')
  if $dwb_is_symlinked;

# --- the rewound node cold-starts a ring and follows the new primary -----

if (defined $b_dwb_stash)
{
	my $b_dwb = $node_b->data_dir . '/pg_dwb';
	unlink($b_dwb) or BAIL_OUT("could not remove $b_dwb: $!");
	rename($b_dwb_stash, $b_dwb) or BAIL_OUT("could not restore $b_dwb: $!");
}
$node_b->start;
# the rewind copied the source's configuration; restore this node's port
$node_a->append_conf('postgresql.conf', 'port = ' . $node_a->port);
$node_a->enable_streaming($node_b);
my $a_log_offset = -s $node_a->logfile;
$node_a->start;
ok( $node_a->log_contains(
		qr/double write buffer ring opened: 16 batches of 16 pages, generation 1\b/,
		$a_log_offset),
	'rewound node cold-started a fresh ring');

$node_b->wait_for_catchup($node_a);
is( $node_a->safe_psql('postgres', 'SELECT count(*) FROM dwb_r'),
	'101', 'rewound node converged on the new primary timeline');
is( $node_a->safe_psql('postgres',
		'SELECT count(*) FROM dwb_r WHERE id = 200001'),
	'0', 'the divergent row is gone');
is( $node_a->safe_psql('postgres',
		'SELECT count(*) FROM dwb_r WHERE id = 100001'),
	'1', "the new primary's row is present");

done_testing();
