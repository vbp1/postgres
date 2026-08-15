# Copyright (c) 2026, PostgreSQL Global Development Group

# A walsender coalesces the releases a drained batch of standby replies
# asks for into one pass at the end of the drain.  The drain has early
# exits, the standby's goodbye being the common one, and a release owed
# by a reply processed in the same drain must survive them: the positions are
# in shared memory already, and a committer acknowledged by that reply has
# nothing else to wake it.  What this file proves is that a commit whose
# ack arrives in the same drain as the standby's goodbye comes back.
#
# The choreography makes that coincidence certain instead of likely.  A
# paused standby holds a remote_apply committer in the queue while the
# flush acks flow; the walsender is then held with SIGSTOP, the standby
# is resumed, allowed to apply past the commit, and shut down, so its
# final reply, the one carrying the apply position the committer waits
# for, lands in the walsender's socket right next to the goodbye.  The
# walsender, released, drains both in one pass.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep time);
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('rel_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q(
autovacuum = off
checkpoint_timeout = 1h
));
$primary->start;
$primary->safe_psql('postgres', 'CREATE TABLE t (id int)');
$primary->backup('bkp');

# A node streaming from a backup reports its own name as its
# application_name, which is what the synchronous set goes by.
my $standby = PostgreSQL::Test::Cluster->new('rel_standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
$standby->start;
$primary->wait_for_catchup($standby, 'replay');

$primary->safe_psql('postgres',
	"ALTER SYSTEM SET synchronous_standby_names = 'rel_standby'");
$primary->reload;
$primary->poll_query_until('postgres',
	"SELECT sync_state = 'sync' FROM pg_stat_replication WHERE application_name = 'rel_standby'"
) or die "standby never became synchronous";

# Hold replay: the flush acks keep flowing, the apply position does not,
# so a remote_apply commit queues and stays queued.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');

my $committer = $primary->background_psql('postgres');
$committer->query_until(
	qr/inserting/, q(
\echo inserting
SET synchronous_commit = remote_apply;
INSERT INTO t VALUES (1);
));

$primary->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_stat_activity WHERE wait_event = 'SyncRep'")
  or die "committer never reached the sync-rep queue";
my $commit_lsn =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

# Hold the walsender, so everything the standby says from here on is
# drained in one pass.
my $walsender = $primary->safe_psql('postgres',
	"SELECT pid FROM pg_stat_replication WHERE application_name = 'rel_standby'"
);
die "no walsender pid" unless $walsender =~ /^\d+$/;
kill 'STOP', $walsender or die "SIGSTOP walsender: $!";

# Let the standby apply past the commit, then say goodbye.
$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
my $deadline = time() + 30;
while (time() < $deadline)
{
	my $replayed = $standby->safe_psql('postgres',
		"SELECT pg_last_wal_replay_lsn() >= '$commit_lsn'::pg_lsn");
	last if $replayed eq 't';
	usleep(100_000);
}
$standby->stop('fast');

# The final reply and the goodbye are now side by side in the held
# walsender's socket.  Release it: the drain must not drop the release
# the reply asks for on its way out.
kill 'CONT', $walsender or die "SIGCONT walsender: $!";

$deadline = time() + 10;
my $released = 0;
while (time() < $deadline)
{
	my $waiting = $primary->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'");
	if ($waiting eq '0')
	{
		$released = 1;
		last;
	}
	usleep(200_000);
}
ok($released,
	'a commit acknowledged in the drain the standby left in is released');

# Free the committer session whatever state it is in.
$primary->safe_psql('postgres',
	"SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'"
) if !$released;
$committer->quit;

# The other way out of the drain: an error thrown while parsing a later
# message in the same pass.  A reply processed just before it has already
# asked for a release, and the walsender's error cleanup must run that
# release on the way out.  The connection is lost either way; the
# committers the reply acknowledged are not.  An injection point right
# after a drained reply stands in for the torn message.
#
# The reply the error lands on has to be the one that carries the apply
# position the committer waits for, and the injection point fires on
# every drained reply, so no reply may reach the walsender between arming
# it and the apply position passing the commit.  Holding the
# walreceiver with SIGSTOP is what guarantees that: replay proceeds from
# WAL already on standby disk, and the receiver's first words on release
# are the positions as they stand then.
if (($ENV{enable_injection_points} // 'no') eq 'yes')
{
	$standby->start;
	$primary->wait_for_catchup($standby, 'replay');
	$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points');

	$standby->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');

	my $committer2 = $primary->background_psql('postgres');
	$committer2->query_until(
		qr/inserting2/, q(
\echo inserting2
SET synchronous_commit = remote_apply;
INSERT INTO t VALUES (3);
));
	$primary->poll_query_until('postgres',
		"SELECT count(*) > 0 FROM pg_stat_activity WHERE wait_event = 'SyncRep'"
	) or die "second committer never reached the sync-rep queue";
	my $lsn2 = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	# The commit's WAL must be on standby disk before the receiver is
	# held, or replay below has nothing to apply.
	$standby->poll_query_until('postgres',
		"SELECT pg_last_wal_receive_lsn() >= '$lsn2'::pg_lsn")
	  or die "standby never flushed the commit's WAL";

	my $walreceiver =
	  $standby->safe_psql('postgres', 'SELECT pid FROM pg_stat_wal_receiver');
	die "no walreceiver pid" unless $walreceiver =~ /^\d+$/;
	kill 'STOP', $walreceiver or die "SIGSTOP walreceiver: $!";

	# A reply already in flight when the receiver stopped is drained --
	# and released, long before the injection point is armed.
	usleep(300_000);
	$primary->safe_psql('postgres',
		"SELECT injection_points_attach('walsender-reply-drained', 'error')");

	$standby->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$deadline = time() + 30;
	while (time() < $deadline)
	{
		my $replayed = $standby->safe_psql('postgres',
			"SELECT pg_last_wal_replay_lsn() >= '$lsn2'::pg_lsn");
		last if $replayed eq 't';
		usleep(100_000);
	}

	# The receiver's first reply now carries an apply position past the
	# commit, and the injection point tears the drain right after it.
	kill 'CONT', $walreceiver or die "SIGCONT walreceiver: $!";

	$deadline = time() + 15;
	my $released2 = 0;
	while (time() < $deadline)
	{
		my $waiting = $primary->safe_psql('postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'"
		);
		if ($waiting eq '0')
		{
			$released2 = 1;
			last;
		}
		usleep(200_000);
	}
	ok($released2,
		'a commit acknowledged right before a torn message is released');

	$primary->safe_psql('postgres',
		"SELECT injection_points_detach('walsender-reply-drained')");
	$primary->safe_psql('postgres',
		"SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'"
	) if !$released2;
	$committer2->quit;
}

done_testing();
