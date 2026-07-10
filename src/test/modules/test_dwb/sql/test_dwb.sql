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
