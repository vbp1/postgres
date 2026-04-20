-- Stage 1 CSN visibility smoke test.
--
-- Stay within the supported Stage 1 contract: top-level xacts only.
-- There is no SQL-visible CSN counter yet, so monotonic allocation is
-- exercised indirectly through visibility ordering in the isolation tests.
-- The read-only xmin-without-xid retention case is covered separately by the
-- isolation test csn-stage1; this file is only a single-session DML smoke
-- test for supported RC/RR tuple visibility.

CREATE TABLE csn_visibility (
	id int PRIMARY KEY,
	val text
);

INSERT INTO csn_visibility VALUES (1, 'seed');

BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT * FROM csn_visibility ORDER BY id;
INSERT INTO csn_visibility VALUES (2, 'rc-insert');
SELECT * FROM csn_visibility ORDER BY id;
UPDATE csn_visibility SET val = 'rc-update' WHERE id = 1;
SELECT * FROM csn_visibility ORDER BY id;
DELETE FROM csn_visibility WHERE id = 2;
SELECT * FROM csn_visibility ORDER BY id;
ROLLBACK;

SELECT * FROM csn_visibility ORDER BY id;

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM csn_visibility ORDER BY id;
INSERT INTO csn_visibility VALUES (2, 'rr-insert');
SELECT * FROM csn_visibility ORDER BY id;
UPDATE csn_visibility SET val = 'rr-update' WHERE id = 1;
SELECT * FROM csn_visibility ORDER BY id;
DELETE FROM csn_visibility WHERE id = 2;
SELECT * FROM csn_visibility ORDER BY id;
ROLLBACK;

SELECT * FROM csn_visibility ORDER BY id;

DROP TABLE csn_visibility;
