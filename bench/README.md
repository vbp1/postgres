# Benchmark charts

Assets referenced by the benchmark comments of the short-lived double
write buffer pull request.  Generated from the pgbench series described
there (104-thread NVMe stand, update-heavy pgbench, 1.5 TB cluster);
`vanilla` is the same tree with `io_torn_pages_protection = full_pages`
and data checksums enabled, converted from the same reference cluster.

* `users-tps.png`, `users-lat.png` — TPS and average latency vs
  connection count at `checkpoint_timeout = 300s`.
* `time-tps.png`, `time-lat.png` — TPS and average latency over the
  900 s run at 2700 connections (10 s pgbench samples, 30 s step).
* `repl-tps-wal.png`, `repl-lag.png` — the primary/synchronous-standby
  series: throughput and shipped WAL per client count, and the standby's
  replay backlog over the run.  Both instances share the stand, split by
  socket, with a netem-emulated 10 GbE hop between them.
