setup
{
  CREATE TABLE csn_stage1 (
    id int PRIMARY KEY,
    val text
  );

  INSERT INTO csn_stage1 VALUES (1, 'seed');
}

teardown
{
  DROP TABLE csn_stage1;
}

session rr
step rr_b1         { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_c1         { COMMIT; }
step rr_b2         { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_c2         { COMMIT; }
step rr_new_before  { SELECT count(*) AS visible_new_rows FROM csn_stage1 WHERE id > 1; }
step rr_new_after   { SELECT count(*) AS visible_new_rows FROM csn_stage1 WHERE id > 1; }
step rr_new_fresh   { SELECT count(*) AS visible_new_rows FROM csn_stage1 WHERE id > 1; }

session rc
step rc_b1          { BEGIN ISOLATION LEVEL READ COMMITTED; }
step rc_c1          { COMMIT; }
step rc_seed_before { SELECT val FROM csn_stage1 WHERE id = 1; }
step rc_seed_after   { SELECT val FROM csn_stage1 WHERE id = 1; }
step rc_row2_before  { SELECT count(*) AS row2_visible FROM csn_stage1 WHERE id = 2; }
step rc_row2_after   { SELECT count(*) AS row2_visible FROM csn_stage1 WHERE id = 2; }

session w1
step w1_b1   { BEGIN; }
step w1_c1   { COMMIT; }
step w1_r1   { ROLLBACK; }
step w1_ins2 { INSERT INTO csn_stage1 VALUES (2, 'writer1'); }
step w1_upd1 { UPDATE csn_stage1 SET val = 'updated' WHERE id = 1; }

session w2
step w2_b1   { BEGIN; }
step w2_c1   { COMMIT; }
step w2_ins3 { INSERT INTO csn_stage1 VALUES (3, 'writer2'); }

# Snapshot before commit, snapshot after commit, and concurrent writers.
permutation rr_b1 rr_new_before w1_b1 w1_ins2 w2_b1 w2_ins3 w1_c1 w2_c1 rr_new_after rr_c1 rr_b2 rr_new_fresh rr_c2

# READ COMMITTED sees the committed xmax change on the next statement.
permutation rc_b1 w1_b1 w1_upd1 rc_seed_before w1_c1 rc_seed_after rc_c1

# Rollback keeps the row invisible to other sessions.
permutation rc_b1 w1_b1 w1_ins2 rc_row2_before w1_r1 rc_row2_after rc_c1
