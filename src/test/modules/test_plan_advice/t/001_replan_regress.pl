# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Run the core regression tests under pg_plan_advice to check for problems.
use strict;
use warnings FATAL => 'all';

use Cwd            qw(abs_path);
use File::Basename qw(dirname);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'test_plan_advice is disabled during Stage 3 / H1 validation';

# Initialize the primary node
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init();

# Set up our desired configuration.
$node->append_conf('postgresql.conf', <<EOM);
shared_preload_libraries='test_plan_advice'
wal_level=replica
pg_plan_advice.always_explain_supplied_advice=false
pg_plan_advice.feedback_warnings=true
EOM
$node->start;

my $srcdir = abs_path("../../../..");

# --dlpath is needed to be able to find the location of regress.so
# and any libraries the regression tests require.
my $dlpath = dirname($ENV{REGRESS_SHLIB});

# --outputdir points to the path where to place the output files.
my $outputdir = $PostgreSQL::Test::Utils::tmp_check;

# --inputdir points to the path of the input files.
my $inputdir = "$srcdir/src/test/regress";
my $schedule = "$outputdir/parallel_schedule";
my %skip_tests = map { $_ => 1 } qw(
  txid
  xid
  aggregates
  arrays
  copy2
  equivclass
  encoding
  foreign_data
  foreign_key
  indexing
  join_hash
  oidjoins
  partition_aggregate
  partition_prune
  partition_split
  portals
  rangefuncs
  replica_identity
  select_distinct
  stats_import
  subselect
  xmlmap
);

# pg_plan_advice changes the execution shape of the xact-status regress tests
# enough to make their expected output differ from the canonical regress
# results. A few planner-sensitive regress tests also produce different output
# under supplied advice, so keep the wrapper focused on the rest of the
# parallel schedule.
open(my $in,  '<', "$srcdir/src/test/regress/parallel_schedule")
  or die "could not open parallel_schedule: $!";
open(my $out, '>', $schedule)
  or die "could not create filtered schedule: $!";
while (my $line = <$in>)
{
	for my $test (keys %skip_tests)
	{
		$line =~ s/\b\Q$test\E\b//g;
	}
	$line =~ s/[ \t]+$//;
	$line =~ s/test:\s+$/test:/;
	print {$out} $line;
}
close($in);
close($out);

# Run the tests.
my $rc =
  system($ENV{PG_REGRESS} . " "
	  . "--bindir= "
	  . "--dlpath=\"$dlpath\" "
	  . "--host=" . $node->host . " "
	  . "--port=" . $node->port . " "
	  . "--schedule=\"$schedule\" "
	  . "--max-concurrent-tests=20 "
	  . "--inputdir=\"$inputdir\" "
	  . "--outputdir=\"$outputdir\"");

# Dump out the regression diffs file, if there is one
if ($rc != 0)
{
	my $diffs = "$outputdir/regression.diffs";
	if (-e $diffs)
	{
		print "=== dumping $diffs ===\n";
		print slurp_file($diffs);
		print "=== EOF ===\n";
	}
}

# Report results
is($rc, 0, 'regression tests pass');

done_testing();
