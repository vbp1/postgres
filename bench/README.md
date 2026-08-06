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
* `repl-time-tps.png`, `repl-time-lat.png` — TPS and average latency over
  the run at 750 connections against a synchronous standby, vanilla
  against DWB with the replay warm pool (`replay_warm_workers = 12`).
  The vanilla run is 900 s, the pool run 600 s.
* `repl2-tps-sockets.png`, `repl2-time-tps.png`, `repl2-time-lat.png` —
  the two-host grid: the primary owns 1, 2 or 4 whole NUMA sockets of a
  240-thread host, the synchronous standby has a second host to itself,
  and the link between them is a real 100 GbE hop (RTT 0.126 ms).  The
  bar chart is throughput per socket count; the two line charts follow
  the two-socket point over its 600 s run.
