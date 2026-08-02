CREATE EXTENSION test_dwb;

-- ring is idle after startup
SELECT test_dwb_states();

-- a partial batch, sealed by force (ring: 16 batches x 16 pages)
SELECT test_dwb_cycle(5);

-- overflow-sealed batches: 40 pages = 16 + 16 + 8 (tail force-sealed)
SELECT test_dwb_cycle(40);

-- every surviving slot validates against meta_crc, generation and
-- image_crc: eager retirement lets the cycles reuse batch file 0, so its
-- final content is the 8-slot tail write, plus 16 slots in batch file 1
SELECT test_dwb_ring_slots(true);

-- and the ring is fully retired again
SELECT test_dwb_states();

-- the cycles sealed deterministically: two overflow seals from the 40-page
-- run (2 x 16 slots) and two forced tail seals (5 + 8 slots)
SELECT wclass, reason, seals, pages FROM test_dwb_seal_stats()
 WHERE seals > 0 ORDER BY wclass, reason;

-- lone-writer fast seal in a QUIET class: get clear of the cycles' overflow
-- stamps first, then a single staged page must seal immediately as "lone"
SELECT pg_sleep(0.3);
CREATE TEMP TABLE seal_before_quiet AS SELECT * FROM test_dwb_seal_stats();
SELECT test_dwb_stage_lone_wait();
SELECT s.wclass, s.reason, s.seals - b.seals AS dseals, s.pages - b.pages AS dpages
  FROM test_dwb_seal_stats() s JOIN seal_before_quiet b USING (wclass, reason)
 WHERE s.seals <> b.seals ORDER BY s.wclass, s.reason;
SELECT test_dwb_retire() >= 0 AS drained;

-- HOT class: an overflow seal microseconds before the lone attempt must
-- suppress the fast seal into the waiter's timeout seal
CREATE TEMP TABLE seal_before_hot AS SELECT * FROM test_dwb_seal_stats();
SELECT test_dwb_overflow_lone_wait();
SELECT s.wclass, s.reason, s.seals - b.seals AS dseals, s.pages - b.pages AS dpages
  FROM test_dwb_seal_stats() s JOIN seal_before_hot b USING (wclass, reason)
 WHERE s.seals <> b.seals ORDER BY s.wclass, s.reason;
SELECT test_dwb_retire() >= 0 AS drained;

-- the hot test reads the stamp through the same helper: a fresh stamp is
-- hot, a stamp from the FUTURE (a backward clock step) must read as quiet
-- and keep the fast seal immediate
SELECT test_dwb_set_overflow_stamp(0) AS hot_now;
SELECT test_dwb_set_overflow_stamp(60000) AS hot_future;
CREATE TEMP TABLE seal_before_future AS SELECT * FROM test_dwb_seal_stats();
SELECT test_dwb_stage_lone_wait();
SELECT s.wclass, s.reason, s.seals - b.seals AS dseals, s.pages - b.pages AS dpages
  FROM test_dwb_seal_stats() s JOIN seal_before_future b USING (wclass, reason)
 WHERE s.seals <> b.seals ORDER BY s.wclass, s.reason;
SELECT test_dwb_retire() >= 0 AS drained;

-- and the ring is idle again
SELECT test_dwb_states();
