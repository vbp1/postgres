
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test for point-in-time recovery (PITR) with prepared transactions
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Compare;

# Initialize and start primary node with WAL archiving
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(has_archiving => 1, allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq{
max_prepared_transactions = 10});
$node_primary->start;

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Initialize node for PITR targeting a very specific restore point, just
# after a PREPARE TRANSACTION is issued so as we finish with a promoted
# node where this 2PC transaction needs an explicit COMMIT PREPARED.
my $node_pitr = PostgreSQL::Test::Cluster->new('node_pitr');
$node_pitr->init_from_backup(
	$node_primary, $backup_name,
	standby => 0,
	has_restoring => 1);
$node_pitr->append_conf(
	'postgresql.conf', qq{
recovery_target_name = 'rp'
recovery_target_action = 'promote'});

# Workload with prepared transactions and the target restore point.
$node_primary->psql(
	'postgres', qq{
CREATE TABLE foo(tag text PRIMARY KEY);
BEGIN;
INSERT INTO foo VALUES('commit_top');
SAVEPOINT s1;
INSERT INTO foo VALUES('commit_sub');
PREPARE TRANSACTION 'foocommit';
BEGIN;
INSERT INTO foo VALUES('abort_top');
SAVEPOINT s1;
INSERT INTO foo VALUES('abort_sub');
PREPARE TRANSACTION 'fooabort';
SELECT pg_create_restore_point('rp');
INSERT INTO foo VALUES('after_rp');
COMMIT;
});

# Find next WAL segment to be archived
my $walfile_to_be_archived = $node_primary->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn());");

# Make WAL segment eligible for archival
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');

# Wait until the WAL segment has been archived.
my $archive_wait_query =
  "SELECT '$walfile_to_be_archived' <= last_archived_wal FROM pg_stat_archiver;";
$node_primary->poll_query_until('postgres', $archive_wait_query)
  or die "Timed out while waiting for WAL segment to be archived";
my $last_archived_wal_file = $walfile_to_be_archived;

# Now start the PITR node.
$node_pitr->start;

# Wait until the PITR node exits recovery.
$node_pitr->poll_query_until('postgres', "SELECT pg_is_in_recovery() = 'f';")
  or die "Timed out while waiting for PITR promotion";

# Hold a CSN snapshot on the promoted primary before finishing either prepared
# transaction. Prepared rows stay invisible until finish-prepared publishes a
# final outcome, and an old snapshot must keep that decision stable.
my $snapshot_session = $node_pitr->background_psql('postgres',
	on_error_stop => 1);

my $result;

$snapshot_session->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ;");
pass('open repeatable read snapshot on promoted primary');

$result = $snapshot_session->query_safe(
	"SELECT pg_current_snapshot_uses_csn(), count(*) FROM foo;");
is($result, 't|0',
	'prepared rows stay invisible to a CSN snapshot before finish');

$node_pitr->psql('postgres', qq{
COMMIT PREPARED 'foocommit';
ROLLBACK PREPARED 'fooabort';
});

$result = $snapshot_session->query_safe(
	"SELECT pg_current_snapshot_uses_csn(), count(*) FROM foo;");
is($result, 't|0',
	'old CSN snapshot keeps prepared rows invisible after finish');

$snapshot_session->query_safe("COMMIT;");

$result = $node_pitr->safe_psql('postgres',
	"SELECT string_agg(tag, ',' ORDER BY tag) FROM foo;");
is($result, 'commit_sub,commit_top',
	'new snapshot sees only committed prepared rows after PITR finish');

$snapshot_session->quit;

# Insert more data and do a checkpoint.  These should be generated on the
# timeline chosen after the PITR promotion.
$node_pitr->psql(
	'postgres', qq{
INSERT INTO foo VALUES('post_pitr');
CHECKPOINT;
});

# Enforce recovery, the checkpoint record generated previously should
# still be found.
$node_pitr->stop('immediate');
$node_pitr->start;

done_testing();
