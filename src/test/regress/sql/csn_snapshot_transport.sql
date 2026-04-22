-- CSN snapshot transport hardening.
--
-- SQL export/import stays text-only and rejects CSN-sensitive snapshots.
-- Internal snapshot transport must preserve snapshot_csn for parallel workers.

DO $$
DECLARE
  snapshot_path text := current_setting('data_directory') || '/pg_snapshots/DEADBEEF-CAFEBABE-1';
BEGIN
  EXECUTE format($copy$
    COPY (
      VALUES
        ('vxid:1/1'),
        ('pid:1'),
        ('dbid:1'),
        ('iso:2'),
        ('ro:0'),
        ('xmin:1'),
        ('xmax:2'),
        ('xcnt:0'),
        ('sof:0'),
        ('sxcnt:0'),
        ('rec:0'),
        ('csn:1')
    ) TO %L
  $copy$, snapshot_path);
END;
$$;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT 'DEADBEEF-CAFEBABE-1';
ROLLBACK;

CREATE TABLE csn_snapshot_transport AS
SELECT i AS id
FROM generate_series(1, 100000) AS g(i);

ALTER TABLE csn_snapshot_transport SET (parallel_workers = 4);
ANALYZE csn_snapshot_transport;

CREATE FUNCTION csn_snapshot_visible(int) RETURNS boolean
LANGUAGE plpgsql STABLE PARALLEL SAFE
AS $$
BEGIN
  RETURN pg_current_snapshot_uses_csn();
END;
$$;

SELECT pg_stat_force_next_flush();
SELECT parallel_workers_launched AS parallel_workers_launched_before
FROM pg_stat_database
WHERE datname = current_database() \gset

BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL debug_parallel_query = on;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL max_parallel_workers_per_gather = 4;
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM csn_snapshot_transport
WHERE csn_snapshot_visible(id);
SELECT pg_current_snapshot_uses_csn() AS uses_csn,
       count(*) AS visible_rows
FROM csn_snapshot_transport
WHERE csn_snapshot_visible(id);
COMMIT;

SELECT pg_stat_force_next_flush();
SELECT parallel_workers_launched > :'parallel_workers_launched_before' AS workers_launched
FROM pg_stat_database
WHERE datname = current_database();

DROP FUNCTION csn_snapshot_visible(int);
DROP TABLE csn_snapshot_transport;
