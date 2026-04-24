# CSN subxid contract
#
# This test covers the supported primary MVCC subxid path and the explicit
# overflow fallback path.

setup
{
DROP TABLE IF EXISTS subxid_csn_contract;
DROP TABLE IF EXISTS subxid_csn_sink;
CREATE TABLE subxid_csn_contract (id integer PRIMARY KEY, val integer);
CREATE TABLE subxid_csn_sink (id integer PRIMARY KEY, val integer);
}

teardown
{
 DROP TABLE subxid_csn_sink;
 DROP TABLE subxid_csn_contract;
}

session seed
step reset
{
  TRUNCATE subxid_csn_contract, subxid_csn_sink;
  INSERT INTO subxid_csn_contract VALUES (1, 0);
}

session writer
step nonov_ins
{
  BEGIN;
  SAVEPOINT s;
  INSERT INTO subxid_csn_contract VALUES (2, 0);
}
step nonov_upd
{
  BEGIN;
  SAVEPOINT s;
  UPDATE subxid_csn_contract SET val = 1 WHERE id = 1;
}
step ov_begin
{
  BEGIN;
  SAVEPOINT s01; INSERT INTO subxid_csn_sink VALUES (1, 1);
  SAVEPOINT s02; INSERT INTO subxid_csn_sink VALUES (2, 1);
  SAVEPOINT s03; INSERT INTO subxid_csn_sink VALUES (3, 1);
  SAVEPOINT s04; INSERT INTO subxid_csn_sink VALUES (4, 1);
  SAVEPOINT s05; INSERT INTO subxid_csn_sink VALUES (5, 1);
  SAVEPOINT s06; INSERT INTO subxid_csn_sink VALUES (6, 1);
  SAVEPOINT s07; INSERT INTO subxid_csn_sink VALUES (7, 1);
  SAVEPOINT s08; INSERT INTO subxid_csn_sink VALUES (8, 1);
  SAVEPOINT s09; INSERT INTO subxid_csn_sink VALUES (9, 1);
  SAVEPOINT s10; INSERT INTO subxid_csn_sink VALUES (10, 1);
  SAVEPOINT s11; INSERT INTO subxid_csn_sink VALUES (11, 1);
  SAVEPOINT s12; INSERT INTO subxid_csn_sink VALUES (12, 1);
  SAVEPOINT s13; INSERT INTO subxid_csn_sink VALUES (13, 1);
  SAVEPOINT s14; INSERT INTO subxid_csn_sink VALUES (14, 1);
  SAVEPOINT s15; INSERT INTO subxid_csn_sink VALUES (15, 1);
  SAVEPOINT s16; INSERT INTO subxid_csn_sink VALUES (16, 1);
  SAVEPOINT s17; INSERT INTO subxid_csn_sink VALUES (17, 1);
  SAVEPOINT s18; INSERT INTO subxid_csn_sink VALUES (18, 1);
  SAVEPOINT s19; INSERT INTO subxid_csn_sink VALUES (19, 1);
  SAVEPOINT s20; INSERT INTO subxid_csn_sink VALUES (20, 1);
  SAVEPOINT s21; INSERT INTO subxid_csn_sink VALUES (21, 1);
  SAVEPOINT s22; INSERT INTO subxid_csn_sink VALUES (22, 1);
  SAVEPOINT s23; INSERT INTO subxid_csn_sink VALUES (23, 1);
  SAVEPOINT s24; INSERT INTO subxid_csn_sink VALUES (24, 1);
  SAVEPOINT s25; INSERT INTO subxid_csn_sink VALUES (25, 1);
  SAVEPOINT s26; INSERT INTO subxid_csn_sink VALUES (26, 1);
  SAVEPOINT s27; INSERT INTO subxid_csn_sink VALUES (27, 1);
  SAVEPOINT s28; INSERT INTO subxid_csn_sink VALUES (28, 1);
  SAVEPOINT s29; INSERT INTO subxid_csn_sink VALUES (29, 1);
  SAVEPOINT s30; INSERT INTO subxid_csn_sink VALUES (30, 1);
  SAVEPOINT s31; INSERT INTO subxid_csn_sink VALUES (31, 1);
  SAVEPOINT s32; INSERT INTO subxid_csn_sink VALUES (32, 1);
  SAVEPOINT s33; INSERT INTO subxid_csn_sink VALUES (33, 1);
  SAVEPOINT s34; INSERT INTO subxid_csn_sink VALUES (34, 1);
  SAVEPOINT s35; INSERT INTO subxid_csn_sink VALUES (35, 1);
  SAVEPOINT s36; INSERT INTO subxid_csn_sink VALUES (36, 1);
  SAVEPOINT s37; INSERT INTO subxid_csn_sink VALUES (37, 1);
  SAVEPOINT s38; INSERT INTO subxid_csn_sink VALUES (38, 1);
  SAVEPOINT s39; INSERT INTO subxid_csn_sink VALUES (39, 1);
  SAVEPOINT s40; INSERT INTO subxid_csn_sink VALUES (40, 1);
  SAVEPOINT s41; INSERT INTO subxid_csn_sink VALUES (41, 1);
  SAVEPOINT s42; INSERT INTO subxid_csn_sink VALUES (42, 1);
  SAVEPOINT s43; INSERT INTO subxid_csn_sink VALUES (43, 1);
  SAVEPOINT s44; INSERT INTO subxid_csn_sink VALUES (44, 1);
  SAVEPOINT s45; INSERT INTO subxid_csn_sink VALUES (45, 1);
  SAVEPOINT s46; INSERT INTO subxid_csn_sink VALUES (46, 1);
  SAVEPOINT s47; INSERT INTO subxid_csn_sink VALUES (47, 1);
  SAVEPOINT s48; INSERT INTO subxid_csn_sink VALUES (48, 1);
  SAVEPOINT s49; INSERT INTO subxid_csn_sink VALUES (49, 1);
  SAVEPOINT s50; INSERT INTO subxid_csn_sink VALUES (50, 1);
  SAVEPOINT s51; INSERT INTO subxid_csn_sink VALUES (51, 1);
  SAVEPOINT s52; INSERT INTO subxid_csn_sink VALUES (52, 1);
  SAVEPOINT s53; INSERT INTO subxid_csn_sink VALUES (53, 1);
  SAVEPOINT s54; INSERT INTO subxid_csn_sink VALUES (54, 1);
  SAVEPOINT s55; INSERT INTO subxid_csn_sink VALUES (55, 1);
  SAVEPOINT s56; INSERT INTO subxid_csn_sink VALUES (56, 1);
  SAVEPOINT s57; INSERT INTO subxid_csn_sink VALUES (57, 1);
  SAVEPOINT s58; INSERT INTO subxid_csn_sink VALUES (58, 1);
  SAVEPOINT s59; INSERT INTO subxid_csn_sink VALUES (59, 1);
  SAVEPOINT s60; INSERT INTO subxid_csn_sink VALUES (60, 1);
  SAVEPOINT s61; INSERT INTO subxid_csn_sink VALUES (61, 1);
  SAVEPOINT s62; INSERT INTO subxid_csn_sink VALUES (62, 1);
  SAVEPOINT s63; INSERT INTO subxid_csn_sink VALUES (63, 1);
  SAVEPOINT s64; INSERT INTO subxid_csn_sink VALUES (64, 1);
  SAVEPOINT s65; INSERT INTO subxid_csn_sink VALUES (65, 1);
  SAVEPOINT s66; INSERT INTO subxid_csn_sink VALUES (66, 1);
  SELECT count(*) FROM subxid_csn_sink;
}
step ov_upd
{
  SAVEPOINT s67;
  UPDATE subxid_csn_contract SET val = 2 WHERE id = 1;
}
step wcommit    { COMMIT; }

session rc
step rc_begin   { BEGIN ISOLATION LEVEL READ COMMITTED; }
step rc_cnt_csn
{
  SELECT pg_current_snapshot_uses_csn() AS uses_csn,
         count(*) AS visible_new_rows
  FROM subxid_csn_contract
  WHERE id = 2;
}
step rc_cnt_post { SELECT count(*) AS visible_new_rows FROM subxid_csn_contract WHERE id = 2; }
step rc_val_csn
{
  SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
  FROM subxid_csn_contract
  WHERE id = 1;
}
step rc_val_post { SELECT val FROM subxid_csn_contract WHERE id = 1; }
step rc_commit  { COMMIT; }

session rr
step rr_begin   { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step rr_cnt_csn
{
  SELECT pg_current_snapshot_uses_csn() AS uses_csn,
         count(*) AS visible_new_rows
  FROM subxid_csn_contract
  WHERE id = 2;
}
step rr_val_csn
{
  SELECT pg_current_snapshot_uses_csn() AS uses_csn, val
  FROM subxid_csn_contract
  WHERE id = 1;
}
step rr_commit  { COMMIT; }

# Non-overflow subxid insert path: the reader stays on the CSN path before
# commit and sees the row on the next statement after commit.
permutation reset nonov_ins rc_begin rc_cnt_csn wcommit rc_cnt_post rc_commit

# Non-overflow subxid insert path: RR keeps the row invisible after commit.
permutation reset nonov_ins rr_begin rr_cnt_csn wcommit rr_cnt_csn rr_commit

# Non-overflow subxid update path: RC sees the committed update on the next statement.
permutation reset nonov_upd rc_begin rc_val_csn wcommit rc_val_post rc_commit

# Overflowed subxid tree: the reader begins after enough savepoints to force
# the legacy fallback path and therefore does not use `snapshot_csn`.
permutation reset ov_begin ov_upd rc_begin rc_val_csn wcommit rc_val_post rc_commit

# Overflowed subxid tree: RR starts after overflow has already been observed
# and must keep the same snapshot semantics after commit.
permutation reset ov_begin ov_upd rr_begin rr_val_csn wcommit rr_val_csn rr_commit
