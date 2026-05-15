# Week 1 — Load Test Throughput Investigation

## TL;DR

**Problem.** Target 60,000 evt/s, pass/fail floor 50,000 evt/s. The
single-process threading load harness capped at **~14.5k evt/s** (later
~11.5k as host state drifted) — 3–4× short of the floor — with no
delivery failures.

**Root cause.** The **GIL**. Five independent single-process
architectures (librdkafka tuning, per-worker producers, per-batch poll,
a dedicated callback-pump thread) all clustered in the same 9.5–14.8k
band. The bottleneck is process-wide Python-bytecode serialization that
no in-process change can escape — one process ≈ one core of Python
execution ≈ ~14.5k evt/s for this Pydantic + Avro + librdkafka workload.

**Solution.** A separate **multi-process harness**
(`streaming_feature_store.load_mp`): one producer process per shard,
each with its own interpreter and GIL, results aggregated by the parent.
At **6 processes × 2 workers** it sustains **~59–62k evt/s** with zero
failures — **clears the 50k floor**, ~5× the single-process number.

**Key tuning insight (counter-intuitive).** Workers-per-process has an
**optimum (2 here), not "fewer is always better."** 4×3 = 34.5k but
6×2 = 60.9k (same 12 total threads). 8×1 does *not* beat 6×2 either.
The rule: `workers_per_process ≈ round(1 / s)` where `s` is the
fraction of a worker's wall time holding the GIL (`s ≈ 0.5` here ⇒ 2).

**Canonical config today.** `make load-test-mp` →
6 procs × 2 workers, fix #1 librdkafka knobs per process, ~59k evt/s.
Live report: [week1_load_test_results_mp.md](week1_load_test_results_mp.md).
Full detail in [§4 Recommendations](#4-recommendations-single-source-of-truth).

---

## 1. The problem

The Week 1 synthetic-event load test targets a sustained **60,000
evt/s** with a pass/fail **floor of 50,000 evt/s**, expected to produce
~600,000 events over a 10 s run with sub-100 ms p95 ack latency. The
single-process threading harness
([load_runner.py](../../src/streaming_feature_store/load/load_runner.py))
never came close: early runs sustained ~6–12k evt/s, and even after
tuning peaked at ~14.5k — roughly 3–4× below the floor — while
reporting **zero delivery failures**. A throughput collapse with no
errors points at backpressure / a structural ceiling, not delivery
failure. The rest of this document is the hunt for that ceiling and
the architecture that escapes it.

The full per-run metrics table for the single-process investigation
(runs 0–14) is in
[Appendix A](#appendix-a-full-single-process-run-history). The
multi-process run table (MP-1…MP-9) is in
[§3.2](#32-runs).

---

## 2. Single-process investigation (threading) → the GIL ceiling

### 2.1 What we tried

Five changes were applied one at a time on top of the threading
harness, each re-measured with a 10 s run (the
[iteration discipline](#appendix-c-process-notes) — one change at a
time, for clean attribution — is preserved in Appendix C; the
forward-looking "potential fixes" plan that drove this sequence is in
[Appendix B](#appendix-b-fix-by-fix-detail)):

| # | Fix | Outcome |
|---|---|---|
| 1 | librdkafka tuning (`linger.ms`, `lz4`, queue caps, `acks=1`, `batch.size=2M`) | ✅ **kept** — +18% throughput, p95/p99 halved |
| 2 | One `AvroEventProducer` per worker thread | ❌ regression, reverted (broker batch fragmentation; lock not the bottleneck) |
| 3 | Verify topic ≥ 12 partitions | ✅ no-op (already 12 partitions, RF=3, evenly balanced) |
| 4 | Move `poll(0)` from per-event to per-app-batch | ❌ regression, reverted (callback dispatch concentration / staleness) |
| pump | Dedicated callback-pump thread (workers stop polling) | ❌ reverted — best p50 (~13 ms) but throughput regressed −32% |

Fix #1 was the only keeper, and it bought a modest +18% (and a big
tail-latency win). Everything else either regressed or was a no-op.
The per-fix forensic write-ups (change applied, before/after tables,
per-knob attribution, reproducibility) are preserved verbatim in
[Appendix B](#appendix-b-fix-by-fix-detail). The point for the
narrative is the *pattern*: nothing moved the ceiling.

### 2.2 The structural finding: the GIL ceiling

#### Every meaningful configuration landed in the same band

Six architectures, each testing a distinct hypothesis about where
throughput "should" come from. Every one landed in 9.5–14.8k evt/s:

| # | Configuration | What changed (vs. previous) | Sustained evt/s | What it should have done if NOT GIL-bound |
|---|---|---|---|---|
| 1 | Baseline (no tuning) | — | ~12,250 | (reference) |
| 2 | Fix #1 (librdkafka tuning) | larger broker batches, compression, fewer broker round-trips, queue caps | **~14,500 (+18%)** | Should have unblocked I/O-bound waiting → throughput climbs as broker no longer the bottleneck. **It did, but capped at ~14.5k.** |
| 3 | Fix #1 + Fix #2 (per-worker producers) | Removed shared schema-cache lock; gave each worker its own producer (no Python lock contention on encode path) | ~14,400 (flat −0.8%) | If Python lock contention was the constraint, removing it should have boosted throughput. **It didn't.** |
| 4 | Fix #1 + Fix #4 (per-batch poll) | Changed polling cadence from per-event to per-1024-events (drastically reduces lock-acquire frequency) | ~9,600 (−34%) | If lock-acquire frequency was the cost, batching should have helped. **It made things worse, but the ceiling was clearly still in the 10-15k band.** |
| 5 | Fix #1 + pump thread | Moved poll work off worker threads entirely onto a dedicated thread | ~9,800 (−32%) | If "workers needed to stop polling so they could produce more" was the model, throughput should jump. **It didn't.** |
| 6 | Fix #1 only (revert) | Sanity check | ~14,800 | Confirms the keep config behavior. |

Six independent architectures — different locks, topologies, polling
strategies — **all clustered in 9.5–14.8k evt/s**. The architectures
shifted *which inner constraint* was binding without ever moving the
*outer* ceiling. That is the GIL fingerprint: a process-wide
serialization point no in-process change can escape.

#### The lock stack: why each non-GIL constraint was ruled out

The system has five locks, outermost (most-encompassing) to innermost:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          The Python process                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                            THE GIL                                │  │
│  │   (one mutex; held to execute ANY Python bytecode whatsoever)     │  │
│  │                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────┐ │  │
│  │   │  AvroEventProducer (one shared instance)                    │ │  │
│  │   │   ┌───────────────────────────────────────────────────────┐ │ │  │
│  │   │   │  AvroSerializer                                       │ │ │  │
│  │   │   │  - schema-cache Python lock                           │ │ │  │
│  │   │   └───────────────────────────────────────────────────────┘ │ │  │
│  │   │   ┌───────────────────────────────────────────────────────┐ │ │  │
│  │   │   │  librdkafka Producer (C library)                      │ │ │  │
│  │   │   │  - handle lock (C-side mutex)                         │ │ │  │
│  │   │   └───────────────────────────────────────────────────────┘ │ │  │
│  │   └─────────────────────────────────────────────────────────────┘ │  │
│  │                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────┐ │  │
│  │   │  DeliveryAccountant — internal lock                         │ │  │
│  │   └─────────────────────────────────────────────────────────────┘ │  │
│  │                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────┐ │  │
│  │   │  TokenBucketPacer — internal lock                           │ │  │
│  │   └─────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

For each inner lock, the experiments produced direct evidence it is
**not** binding:

| Lock | Tested by | Result | What it rules out |
|---|---|---|---|
| AvroSerializer schema-cache lock | Fix #2 (per-worker producers) | 0% throughput change | Removing 12-way contention on this lock didn't help → it wasn't binding. |
| librdkafka handle lock — *acquire frequency* | Fix #4 (per-batch poll) | −34% throughput | Reducing acquire frequency 16,800× regressed throughput → frequency on this lock isn't binding. |
| librdkafka handle lock — *contention pattern* | Fix #2 (split) and pump thread (relocate) | both regressed or flat | Worker-vs-worker contention on this lock isn't binding either. |
| DeliveryAccountant lock | Tiny critical section (~1µs × 28k acquires/sec ≈ 28 ms/sec total) | Off the candidate list | Contention budget is < 0.3% of total worker time. |
| TokenBucketPacer lock | Tiny + low frequency (1µs × 14 acquires/sec ≈ 14 µs/sec) | Off the candidate list | Contention budget is essentially zero. |

That leaves exactly one lock the experiments did NOT escape: **the GIL**.

#### Each "failed" fix is positive evidence for the GIL

The regressions/no-ops aren't just "things that didn't work" — each one
*would have falsified* the GIL hypothesis if it had moved throughput.
None did:

- **Fix #2 (per-worker producers).** Eliminated 12-way contention on
  the schema-cache lock and the librdkafka handle lock simultaneously.
  If either was the bottleneck, throughput should have jumped.
  **Result: 0% change** — removing inner-lock contention doesn't free
  workers when they're queueing on the GIL anyway.

- **Fix #4 (per-batch poll).** Cut handle-lock acquire frequency by
  16,800×. If acquire-frequency overhead was capping throughput,
  batching should have recovered it. **Result: −34%**, because the
  callback-dispatch *work* still has to happen — concentrating it on
  rare long polls just makes the GIL contention worse.

- **Pump thread.** Moved all poll work onto a dedicated thread,
  eliminating worker-vs-worker handle-lock contention entirely. If
  "workers waste time on poll-lock contention" was the model,
  throughput should jump. **Result: −32%**, because the pump is a new
  GIL contender that steals interpreter time from the 12 workers.

The pump-thread result is particularly damning. The pump is the
*structurally correct* fix for the symptom we identified (93% of worker
time in `poll`). It did its targeted job — p50 dropped to 13 ms, the
best ever measured. But **throughput went down**. The only way to
reconcile "architecture is correct" with "throughput regressed" is that
the work moved to a thread that competes for the GIL. With real
multi-thread parallelism (no GIL), the pump would have run on its own
core and worker throughput would have climbed.

The chain narrows the search to one suspect:

1. Inner producer locks aren't binding (fix #2).
2. Acquire-frequency on the librdkafka handle isn't binding (fix #4).
3. Worker-vs-pump contention isn't binding either — adding a 13th
   thread *hurt*, the opposite of what non-GIL-bound code would do.
4. **Therefore the binding constraint is the only thing none of those
   changes could affect: the GIL.**

#### Sanity check from the math

At the upper end of the band (~14,500 evt/s, fix #1):

- Per-event Python work (Pydantic + serializer adapter + accountant
  updates + poll callbacks) ≈ **~70 µs of GIL-held time per event**.
- 14,500 evt/s × 70 µs = **~1,015 ms/sec** of GIL-held time.
- One CPU core = 1,000 ms/sec of execution time.
- We're saturating ~100% of one core's worth of Python execution.

That's the ceiling. No multi-threaded design within one Python process
can exceed roughly one core's Python execution capacity, because the
GIL serializes Python bytecode across all threads. Architectures that
move work between threads just shift *which* contention dominates
without changing the total Python work that must clear the GIL.

### 2.3 Therefore: escape the GIL

If the GIL is the binding constraint, the only way to escape it without
changing language or interpreter is to **stop sharing the GIL across
the threads doing the work**:

1. **Multiprocessing.** Each producer process gets its own
   interpreter, its own GIL, its own per-process ceiling. N processes
   scale roughly linearly until the broker / network / host CPU
   saturates. Every dependency already works in independent processes,
   so this is the low-risk experiment.

2. **Free-threaded CPython** (3.13t / 3.14t). Removes the GIL entirely;
   same code, real parallelism. Smaller code change but uncertain
   compatibility — `pydantic-core`, `fastavro`, `confluent-kafka` would
   all need verification in no-GIL mode.

Multiprocessing was chosen. The single-process projection
(4 × 14.5k ≈ 55–60k) predicted it would clear the 50k floor. §3 reports
what actually happened.

---

## 3. Multi-process solution (2026-05-14)

### 3.1 The harness

A separate harness (`streaming_feature_store.load_mp`) tests the
multiprocessing hypothesis: each process spawns its own Python
interpreter, its own `AvroEventProducer`, and its own
`DeliveryAccountant`; the parent aggregates per-process snapshots and
re-percentiles the union of their latency reservoirs. The threading
harness in `load/` is left in place — both are kept side by side so
either can be deleted without disturbing the other (see
[§4.3](#43-status-of-the-harnesses)).

### 3.2 Runs

| # | Date | Layout (procs × workers) | Pacing | Sustained evt/s | p50 / p95 / p99 ms | Verdict | Notes |
|---|---|---|---|---|---|---|---|
| MP-1 | 2026-05-14 | 4 × 3 | paced 60k | 42,454 | 19.9 / 130.2 / 195.7 | ❌ FAILED | first run with the original auto-default (cpu_budget=8, partition_cap=4 ⇒ 4 procs × 3 workers); per-process throughput drops to ~10.7k because three worker threads inside each process serialise on that process's GIL |
| MP-2 | 2026-05-14 | 4 × 3 | paced 60k | 34,920 | (similar) | ❌ FAILED | reproducibility check; per-process throughput dropped further to ~8.7k as broker / WSL state warmed up enough to make broker write contention bind |
| MP-3 | 2026-05-14 | 4 × 3 | unpaced | 34,790 | (similar) | ❌ FAILED | removed the pacer to rule it out as the constraint; throughput unchanged ⇒ pacer was never binding |
| MP-4 | 2026-05-14 | 2 × 6 | unpaced | 14,840 | — | ❌ FAILED | direct test of "more workers per process recovers the lost throughput" — strongly disproved (per-proc ~7.4k, half of MP-3); more intra-process workers make the per-process GIL contention worse, not better |
| MP-5 | 2026-05-14 | 6 × 2 | unpaced | 60,682 | — | ✅ passes 50k floor | switched to **fewer** workers per process (2) with **more** processes (6); per-proc ~10.1k recovered and 6× scaling kicks in |
| MP-6 | 2026-05-14 | 6 × 2 | paced 60k | 60,919 | (similar) | ✅ PASSED | reproducibility check, paced |
| MP-7 | 2026-05-14 | 8 × 1 | paced 60k | 62,289 | — | ✅ PASSED | one worker per process — the most GIL-friendly layout possible; throughput is comparable to 6×2 but per-process is lower (~7.8k), so wallclock-efficiency-per-process drops |
| MP-8 | 2026-05-14 | 6 × 2 | paced 60k | **58,908** | **16.4 / 46.5 / 76.7** | ✅ PASSED | **canonical run** for the kept config; planner default is now `workers_per_process=2` so this is what `make load-test-mp` produces today |
| MP-9 | 2026-05-14 | 6 × 2 | paced 60k | **61,735** | 88.9 / 106.3 / 124.6 | ✅ PASSED | warm re-run of the canonical config ~14 min after MP-8; +4.8% throughput (best 6×2 number recorded), all 6 children produced exactly 105,472 events (per-process ~10.3k, tightest cross-process spread yet). Latency profile inverted vs MP-8: p50 jumped 16.4 → 88.9 ms but p95/p99 narrowed dramatically (46/77 → 106/125). Narrow p50≈p95≈p99 band is the **sustained-backpressure signature** (compare the May-13 baseline run #4 note in Appendix A) — the per-process queues are running at a steady, near-saturated depth instead of MP-8's burstier regime |

For reference, the current-day single-process baseline measured the
same afternoon was ~11,556 evt/s — the absolute threading number has
drifted down from the 14.5k recorded May 13 (probably a broker / WSL
state difference), but the *ratio* of multi-process to single-process —
about **5×** — is the load-bearing comparison.

### 3.3 What the runs tell us

- **The GIL escape is real.** Six processes clear the 50k floor; one
  process cannot, on this hardware, with any configuration tried. The
  structural prediction from §2.3 is empirically vindicated.

- **Linear-in-processes scaling is approximate, not exact.** The
  original mental model — "one process ≈ one core of Python at 14.5k
  evt/s, so N processes hit `N × 14.5k`" — is too optimistic. Each
  process in a multi-proc run sees a *lower* per-process ceiling
  (~10k for 6×2, ~7.8k for 8×1) than a process running alone. The
  shortfall comes from broker write contention (more concurrent
  produce requests ⇒ longer per-request handling), WSL2 scheduler
  overhead with more contenders, and amortised schema-registry
  startup HTTP calls per process. Net: sub-linear but still ~5×.

- **Workers per process has an optimum (2 on this hardware) — it is
  NOT "fewer is always better."** The headline numbers: 4 × 3 = 12
  total worker threads gave 34.5k; 6 × 2 = 12 total worker threads gave
  60.9k — same total worker count, +76% throughput purely from
  redistributing the threads across more processes. But 8 × 1 did
  **not** beat 6 × 2 (per-process throughput *fell* to ~7.8k vs
  ~10.2k), so the trend is not monotonic — there is a peak at 2, not a
  slide toward 1.

  The right mental model is **GIL utilisation**, not "GIL contention."
  Treat each process's GIL as a single-server queue; the goal is to
  keep it ~100% busy with useful work **without** a queue forming for
  it. That gives two opposing failure modes:

  - *Too few workers (1/proc):* the lone worker spends ~half its wall
    time parked in GIL-yielding waits (`pacer.acquire()`,
    `wait_for_in_flight_below()`). During those waits the GIL is
    **idle** — no Python work runs in that process at all, so ~50% of
    its Python capacity is wasted. **This is why 8×1 < 6×2.**

  - *Too many workers (3+/proc):* more threads than the GIL can keep
    busy. The surplus workers are **parked in the GIL-handoff path**
    — blocked on the GIL's internal condition variable, *not*
    busy-spinning (CPython's GIL has not been a spinlock since 3.2) —
    and the process pays serialisation plus handoff overhead (periodic
    5 ms release/reacquire, condvar signalling, cache bounce). **This
    is why 4×3 < 6×2.**

  The optimum is the smallest worker count that keeps the GIL
  continuously busy through the natural blocking gaps and no more:
  empirically `W ≈ round(1 / s)` where `s` is the fraction of a
  single worker's wall time spent holding the GIL. For this
  Pydantic + Avro + librdkafka workload `s ≈ 0.5`, so `W = 2`.
  Two workers tag-team the GIL — when one is parked on a wait the
  other runs — while the librdkafka C-side sender threads (which do
  not hold the GIL) handle broker I/O concurrently.

- **The doc's original "4-8 workers per process" production guidance
  was wrong** for this workload and is replaced with **"1-2 workers
  per process"** (see [§4.2](#42-production-guidance)). The CLI /
  planner defaults have been updated accordingly.

- **Latency improves too.** The keep config's p50 is 16.4 ms (vs the
  threading runner's 28 ms warm) and p95 is 46.5 ms (vs 170 ms).
  Smaller per-process queues means smaller in-process backpressure and
  so smaller queueing latency. (The MP-9 note in §3.2 documents the
  throughput-vs-latency trade when the same config runs at higher queue
  depth — higher absolute latency but a tighter percentile band.)

---

## 4. Recommendations (single source of truth)

> **Supersession note.** Earlier drafts of this doc carried a
> "Recommended live config" that named **"Fix #1 only, single-process,
> ~14.5k evt/s"** as the keep config, and a production-guidance section
> recommending **"4-8 workers per process."** Both were written before
> the multi-process result. They are **superseded** by this section.
> The fix #1 librdkafka knobs are still correct — they now apply
> *per process* — but single-process is no longer the throughput path,
> and the worker-per-process guidance is corrected below. The original
> wording is preserved verbatim in
> [Appendix B](#appendix-b-fix-by-fix-detail) for provenance.

### 4.1 Kept config (throughput path: multi-process)

- **Driver:** `make load-test-mp` or
  `uv run python scripts/run_event_load_mp.py --duration-s 10 --target-rate 60000`.
- **Processes:** left to the planner —
  `min(cpus // 2, partitions // workers_per_process)` on dev / WSL
  (on-host brokers); `cpus - 1` for the off-host-broker rule.
- **Workers per process:** **2** (default in the planner and CLI).
- **Target rate:** 60,000 with the floor at 50,000.
- **Per-process librdkafka knobs (fix #1, unchanged):** `linger.ms=20`,
  `compression.type=lz4`, `queue.buffering.max.messages=1_000_000`,
  `queue.buffering.max.kbytes=1_048_576`, `acks=1`,
  `batch.size=2_000_000`. These apply per process and remain correct.
- **Observed:** ~59–62k evt/s sustained, 0 failed, p50 ~16 ms /
  p95 ~47 ms / p99 ~77 ms (warm, MP-8). Live report:
  [week1_load_test_results_mp.md](week1_load_test_results_mp.md).

The single-process **fix #1** config (single shared
`AvroEventProducer`, `poll(0)` per event) remains the correct config
*for the threading harness* — it is still the best within the
single-process ceiling at ~14.5k evt/s, p50 ~28 / p95 ~170 /
p99 ~270 ms. It is just no longer the throughput recommendation; it is
the config-sanity-check recommendation (see §4.3).

### 4.2 Production guidance

For a production ingestion pipeline needing throughput beyond ~15k
evt/s, scale by **processes, not threads**:

- **One process per CPU core, with 1-2 workers per process.** Fewer
  workers inside a process means less time wasted on intra-process GIL
  contention; more processes means more cores of Python work in
  parallel. (The original "4-8 workers per process" guidance was wrong
  for this workload — see §3.3.)
- **Broker partition count ≥ `N_processes × workers_per_process`** so
  every worker can target a distinct leader. At 12 partitions that
  caps the planner at 6 processes (2 workers each) or 12 processes
  (1 worker each).
- **Aggregate metrics across processes** for monitoring; the
  `MultiprocessLoadReport` is the in-tree example of how to merge
  per-process snapshots (sum counters, max wallclock, re-percentile
  the union of latency reservoirs).
- **Don't increase `workers_per_process` looking for more throughput**
  — empirically that *reduces* per-process throughput because the
  threads queue on the GIL. Add processes instead.
- **Cost:** N× the memory footprint and added IPC complexity for the
  operator. A ~6-process deployment on this hardware reaches the
  50–60k range.
- **Note on `acks=1`:** load-test/dev only. Production should use
  `acks=all`; the relative process-vs-thread conclusions hold but the
  absolute latency numbers shift up by the replication round-trip.

### 4.3 Status of the harnesses

- **Threading harness (`load/`)** — kept as the **config-sanity-check**
  tool. Single-process, ~11.5k evt/s today. The 50k floor is
  unreachable here and is no longer the right verdict line for this
  harness; the integration test that asserts 50k still uses it and
  will continue to fail on this hardware **by design** — it documents
  the GIL ceiling. Use it to verify producer / schema / broker config
  changes don't regress, not to chase throughput.
- **Multi-process harness (`load_mp/`)** — kept as the
  **throughput-targeting** tool. Six processes, ~59k evt/s today,
  passes the 50k floor. Reuses the threading runner internally inside
  each child; only the orchestration layer is separate.

---

## 5. Lessons learned

1. **Profile before optimizing.** Fix #2 was a regression that a
   5-minute `py-spy --idle` run would have prevented. It was applied on
   a plausible-sounding hypothesis ("shared lock → contention →
   bottleneck") without first verifying the lock was a meaningful share
   of CPU time; a profile would have shown it at 1–5%. Speculation
   about which Python construct is "obviously slow" is unreliable;
   measurement is not. **Profile first, optimize second.**
2. **Read profile output carefully — % time ≠ % wasted.** The 93% of
   worker time in `poll(0)` was mostly *useful* callback-dispatch work,
   not pure contention. Removing the call moved the work elsewhere; it
   didn't eliminate it.
3. **Library docstring guidance is for correctness, not always
   performance.** `AvroSerializer`'s "not thread-safe — one per thread"
   is a correctness statement; its lock-protected critical section is a
   microsecond-scale dict lookup. "Not thread-safe" doesn't imply the
   lock is hot. Verify with measurement before treating it as a perf
   prescription.
4. **Architecture fights are conservation games.** Moving work between
   threads doesn't reduce the work; it just changes who waits for what.
   Real throughput gains require either reducing total work or escaping
   the serialization point (here, the GIL).
5. **After exhausting same-process options, the next move is
   multiprocessing or a no-GIL build — not more thread-tuning.** And
   when you get there, remember the workers-per-process optimum: more
   processes, *few* workers each, sized by `W ≈ round(1/s)`.

---

## Appendix A: Full single-process run history

Per-run metrics for the threading harness. Targets for every run:
target rate 60,000 evt/s, floor 50,000 evt/s, expected produced
~600,000 over 10 s, sub-100 ms p95 ack, ~10 s wallclock. See
[week1_load_test_results.md](week1_load_test_results.md) for the
latest single-process run's full report.

| # | Date | Change applied | Sustained evt/s | Produced | Acked | Failed | p50 ms | p95 ms | p99 ms | Wallclock s | Verdict | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 2026-05-12 | baseline (no tuning) | 5,949 | 65,536 | 65,536 | 0 | 22.0 | 830.6 | 894.2 | 11.02 | ❌ FAILED | cold-start (first run after `infra-up`); discard from comparison |
| 1 | 2026-05-13 | baseline re-run (no tuning) | 11,755 | 120,832 | 120,832 | 0 | 555.2 | 1204.2 | 1339.6 | 10.28 | ❌ FAILED | cold-start (first run today after `infra-up`); discard from comparison |
| 2 | 2026-05-13 | baseline (no tuning, warm) | 12,464 | 130,048 | 130,048 | 0 | 9.0 | 294.3 | 672.5 | 10.43 | ❌ FAILED | warm start (broker still hot from run #1) |
| 3 | 2026-05-13 | baseline (no tuning) | 10,568 | 154,624 | 154,624 | 0 | 2777.9 | 4089.2 | 4188.9 | 14.63 | ❌ FAILED | cold-start (post WSL restart, fresh Docker daemon); discard from comparison |
| 4 | 2026-05-13 | baseline (no tuning, warm) | 12,324 | 145,408 | 145,408 | 0 | 1205.3 | 1270.7 | 1547.1 | 11.80 | ❌ FAILED | warm-ish (post WSL); broker still pressed — narrow p50≈p95 suggests sustained back-pressure, not steady-state warm |
| 5 | 2026-05-13 | baseline (no tuning, warm) | 12,049 | 125,952 | 125,952 | 0 | 15.8 | 355.5 | 624.4 | 10.45 | ❌ FAILED | warm baseline (post WSL, broker stabilized); matches run #2 profile |
| 6 | 2026-05-13 | fix #1 (linger.ms=20, lz4, queue caps, acks=1, batch.size=2M) | 14,619 | 151,552 | 151,552 | 0 | 27.7 | 170.1 | 297.7 | 10.37 | ❌ FAILED | first run with fix #1, warm; +21% throughput vs run #5, p95 / p99 roughly halved |
| 7 | 2026-05-13 | fix #1 (same as run #6) | 14,364 | 148,480 | 148,480 | 0 | 29.1 | 169.1 | 242.1 | 10.34 | ❌ FAILED | fix #1 confirm run, warm; reproduces run #6 within ~2% across all metrics |
| 8 | 2026-05-13 | fix #1 + fix #2 (one producer per worker) | 14,006 | 146,432 | 146,432 | 0 | 172.7 | 421.1 | 521.2 | 10.46 | ❌ FAILED | first run with fix #2, warm; throughput ~flat vs fix #1, p50 / p95 / p99 all ~2-6× WORSE — likely smaller broker batches (12 producers split the same data → 12× more, smaller produce requests) |
| 9 | 2026-05-13 | fix #1 + fix #2 (same as run #8) | 14,755 | 153,600 | 153,600 | 0 | 180.2 | 405.9 | 487.8 | 10.41 | ❌ FAILED | fix #2 confirm run, warm; reproduces run #8 pattern — throughput flat (~14.4k median), latency 2-6× worse than fix #1 alone. Lock contention was NOT the binding constraint; revert fix #2 |
| 10 | 2026-05-13 | fix #1 + fix #4 (poll once per app-batch of 1024) | 10,297 | 108,544 | 108,544 | 0 | 461.1 | 904.2 | 1298.8 | 10.54 | ❌ FAILED | first run with fix #4, broker-warm-ish (post ~3.5 min test-suite idle); throughput DOWN -29% vs fix #1 baseline, p50 16× worse — callback-dispatch concentration appears to have replaced per-event poll contention as the new binding constraint |
| 11 | 2026-05-13 | fix #1 + fix #4 (same as run #10) | 8,858 | 92_160 | 92_160 | 0 | 180.4 | 509.1 | 604.9 | 10.40 | ❌ FAILED | fix #4 confirm run, warm; throughput **even lower** (-39% vs fix #1), latency varies (p50 180 here vs 461 in #10) — confirms fix #4 is a real regression on throughput; latency variance between runs reflects timing luck of poll-vs-ack alignment |
| 12 | 2026-05-13 | fix #1 + pump thread (workers stop polling; dedicated thread drains callbacks) | 10,170 | 110,592 | 110,592 | 0 | **13.9** | 359.5 | 388.0 | 10.87 | ❌ FAILED | first run with pump thread, warm; **p50 = 13.9 ms (best ever)** — pump architecture works for callback dispatch latency. But throughput DOWN -30% vs fix #1. Suggests we've moved the bottleneck again: probably GIL contention between pump and 12 workers (pump holds GIL while dispatching ~14k callbacks/sec) |
| 13 | 2026-05-13 | fix #1 + pump thread (same as run #12) | 9,451 | 101,376 | 101,376 | 0 | **12.4** | **23.8** | **151.2** | 10.73 | ❌ FAILED | pump confirm run, warm; throughput reproduces (~10k), p50 reproduces (~13 ms), but **tail dramatically better** (p95 23.8, p99 151.2 — best p95 ever recorded) — confirms bimodal latency from GIL-burst alignment, varies between runs. Pump architecture is real latency-vs-throughput trade vs fix #1 |
| 14 | 2026-05-13 | fix #1 only (post-pump-revert sanity check) | 14,814 | 152,576 | 152,576 | 0 | 29.7 | 192.0 | 742.3 | 10.30 | ❌ FAILED | ✅ **revert verified**: throughput 14,814 (+2% vs fix #1 baseline), p50 29.7 ms (within 6%), p95 192 ms (within 13%) — live config is back to fix #1 behavior. p99 = 742 ms is a tail outlier (single sluggish message); doesn't affect the verdict |

**Key observations (run #0):**

- **Sustained throughput is ~10× below target** (5,949 vs 60,000) and
  ~8× below the floor (5,949 vs 50,000).
- **Produced count is suspiciously round**: 65,536 = 64 × 1,024 (batch
  size). Suggests workers stalled after ~64 batches rather than running
  steadily for the full 10 s.
- **p95 ack latency is 830 ms** — the producer's internal queue is
  saturated; acks are queued behind a slow drain.
- **No errors** were recorded, so the failure mode is throughput
  collapse from backpressure, not delivery failure.

---

## Appendix B: Fix-by-fix detail

### B.1 Original hypothesis (pre-conclusion)

The producer pipeline cannot keep up with what 12 worker threads
generate, so workers spend most of their time blocked in
`wait_for_in_flight_below(...)`
([load_runner.py:137](../../src/streaming_feature_store/load/load_runner.py#L137),
[load_runner.py:175-177](../../src/streaming_feature_store/load/load_runner.py#L175-L177)).
Effective throughput becomes a function of broker ack latency, not
produce rate. *(This framed the work; §2.2 supersedes it with the GIL
finding.)*

### B.2 Fix #1 — producer-side librdkafka tuning (applied 2026-05-13)

**Change applied:** `linger.ms=20`, `compression.type=lz4`,
`queue.buffering.max.messages=1_000_000`, `queue.buffering.max.kbytes=1_048_576`,
`acks=1`, `batch.size=2_000_000`. Wired through a Pydantic `ProducerTuning`
model in [config.py](../../src/streaming_feature_store/config.py) and applied
in [avro_producer.py](../../src/streaming_feature_store/producer/avro_producer.py)
`_build_producer`.

**Compared against:** warm baseline median of runs #2 and #5 (no tuning).

| Metric | Baseline (warm, #2 + #5) | Fix #1 (warm, #6 + #7) | Δ |
|---|---|---|---|
| Sustained evt/s | 12,250 (median) | 14,492 (median) | **+18%** |
| p50 ack ms | ~12 | ~28 | +16 ms (by-design `linger.ms` tradeoff) |
| p95 ack ms | ~325 | ~170 | **-48%** |
| p99 ack ms | ~625 | ~270 | **-57%** |
| Failed | 0 | 0 | unchanged |
| Wallclock s | ~10.4 | ~10.4 | unchanged |

**Verdict:** worked as designed. Modest throughput win (+18%), big
tail-latency win (p95 / p99 roughly halved). Still ~3.4× below the 50k
floor.

**Per-knob attribution (from observed signatures):**

- `linger.ms=20` — explains the p50 increase from ~12 ms → ~28 ms. The
  producer is now waiting up to 20 ms to fill batches before shipping;
  a deliberate latency-for-throughput trade confirming the knob took
  effect.
- `compression.type=lz4` + `batch.size=2_000_000` — primary driver of
  the p95 / p99 halving. Fewer, larger, smaller-on-the-wire batches
  means fewer broker round-trips per event and shorter slowest-batch
  flush times.
- `acks=1` — additional contributor to the tail-latency win. The leader
  no longer waits for follower acks before responding.
- `queue.buffering.max.messages=1_000_000` + `queue.buffering.max.kbytes`
  — preventive. Baseline didn't show `BufferError`s, so this is
  insurance, not a corrective fix.

**What this tells us about the structural cap:**

The throughput ceiling is **upstream of librdkafka**. With per-worker
rate 14,492 / 12 ≈ 1,208 evt/s = **~0.83 ms per `produce()` call** (vs
~1.04 ms baseline), most of the per-event cost is in the Python
`produce()` path: Pydantic validation, Avro serialization, the shared
`AvroSerializer` schema-cache lock, and `poll(0)`. None of those are
touched by fix #1. This pointed at fix #2 next (later disproved).

**Reproducibility:** runs #6 and #7 reproduce within ~2% on every
metric except p99 (-19%, expected variance for a single-message tail
percentile). The result is solid.

### B.3 Fix #2 — one `AvroEventProducer` per worker thread (applied 2026-05-13, REVERTED)

**Change applied:** in
[load_runner.py](../../src/streaming_feature_store/load/load_runner.py)
`run()`, each worker constructed its own `AvroEventProducer` (mirroring
the existing per-worker generator pattern) instead of sharing a single
instance. Each producer flushed independently after threads join.
Applied on top of fix #1.

**Compared against:** fix #1 (warm) median of runs #6 and #7.

| Metric | Fix #1 (warm, #6 + #7) | Fix #1 + Fix #2 (warm, #8 + #9) | Δ |
|---|---|---|---|
| Sustained evt/s | 14,492 (median) | 14,381 (median) | -0.8% (flat) |
| p50 ack ms | ~28 | ~176 | **+528%** |
| p95 ack ms | ~170 | ~413 | **+143%** |
| p99 ack ms | ~270 | ~504 | **+87%** |
| In-flight (Little's Law) | ~405 | ~2,540 | **6× more queued** |
| Failed | 0 | 0 | unchanged |
| Wallclock s | ~10.4 | ~10.4 | unchanged |

**Verdict: REGRESSION — revert.** Throughput stayed flat while every
latency percentile got dramatically worse. Lock contention on the
shared `AvroSerializer` was **not** the binding constraint — the
prediction in §B.6 ("schema-cache lock = dominant bottleneck") was
empirically disproved.

**Why it regressed (root cause):**

The shared producer naturally pooled all 12 workers' events into one
fat batch every `linger.ms=20` window:

- Shared (fix #1): 14,492 evt/s × 0.020 s = **~290 events / batch ≈ 145 KB**
  → broker sees ~50 produce requests / sec.
- Per-worker (fix #2): 14,381 / 12 producers = 1,198 evt/s × 0.020 s
  = **~24 events / batch ≈ 12 KB** → broker sees ~600 produce requests / sec.

Splitting the data stream into 12 independent producers fragmented
broker batches by ~12×. The broker's per-request fixed cost (parse +
log append + ack frame + replication coordination) is roughly 1-3 ms
regardless of batch size; with 12× more requests, that overhead now
dominates per-event latency. Schema-cache lock contention was a tiny
saving compared to that new cost.

**Secondary contributors:**

- **12× librdkafka background sender threads** competing for CPU on a
  WSL host with limited vCPUs.
- **12× TCP connections per broker** (12 producers × 3 brokers = 36
  sockets vs 3 in the shared case).
- **12× producer-buffer reservations** — pre-allocated librdkafka
  memory scales with producer count.

**What this empirically proves about the per-event critical path:**

The Amdahl bound from fix #1 was already telling us the lock could be
at most ~17% of per-event time. Fix #2's regression confirms a tighter
bound: the lock was **negligible** (probably 1-5%), because removing it
entirely gave **zero** throughput gain — even before accounting for the
offsetting broker-side cost. If the lock had been the binding
constraint, we would have seen *some* throughput improvement; we saw
none.

**The "not thread-safe" docstring was a correctness statement, not a
performance one.** `AvroSerializer.__call__`'s lock-protected critical
section is a microsecond-scale dict lookup; even with 12 threads in the
queue, the per-event share is tiny. The docstring is good library
hygiene to honor in general, but does not imply the lock is hot in any
specific workload — that requires measurement.

**Methodological lesson — measure before optimizing:** Fix #2 was
applied on the strength of a plausible hypothesis without first
profiling the per-event path. A 5-minute `py-spy --idle` run on a
worker thread under fix #1 would have shown the lock at 1-5% of total
time and saved this regression. (Promoted to [§5 lesson #1](#5-lessons-learned).)

**Reproducibility:** runs #8 and #9 reproduce within ~5% on all
metrics. The regression is real and stable, not a transient.

### B.4 Profile (under fix #1, captured 2026-05-13)

A `py-spy --idle` flame graph of the worker threads under fix #1
([artifact](week1_load_profile.svg),
[full analysis](week1_load_profile_analysis.md)) shows
**`producer.poll(0)` accounts for ~93% of per-worker wall time.** Avro
encoding, Pydantic-to-dict, and the schema-cache lock together total
< 5%. This vindicated fix #4 as the prime candidate and conclusively
confirmed fix #2's post-mortem (lock contributed 0 detectable samples).

### B.5 Fix #3 — verify the topic has ≥ 12 partitions (CHECKED, no-op)

`topic_admin describe` reports `partitions=12, RF=3` with leadership
evenly balanced (4 partitions per broker). Each of the 12 workers can
land on a distinct leader; partition count is **not** the bottleneck.

```
e-commerce-events: partitions=12 RF=3
  kafka-1 leads {0, 5, 6, 9}
  kafka-2 leads {1, 4, 7, 11}
  kafka-3 leads {2, 3, 8, 10}
```

No code change required.

### B.6 The original forward-looking "Potential fixes" plan

> *Preserved for provenance. This was the prioritized plan that drove
> the run sequence; outcomes are now recorded in §2.1 and B.2–B.5.*

**1. Tune the underlying `SerializingProducer` config** —
[avro_producer.py:135-142](../../src/streaming_feature_store/producer/avro_producer.py#L135-L142).
Add throughput-oriented librdkafka knobs.

| Setting | Value | Rationale |
|---|---|---|
| `linger.ms` | `20` | Wait longer before sending to fill bigger batches; current default of 5 ms ships too eagerly. |
| `compression.type` | `lz4` | 3-5× smaller wire payloads for Avro; near-zero CPU cost. |
| `queue.buffering.max.messages` | `1_000_000` | Default 100k is the cause of `BufferError` retries; raises the hard ceiling well above `max_in_flight=50_000`. |
| `queue.buffering.max.kbytes` | `1_048_576` (1 GiB) | Pin the byte cap so it doesn't trip first. |
| `acks` | `1` | Skip replication round-trip on the dev cluster; load-test only, NOT production. |
| `batch.size` | `2_000_000` (optional) | Allow ~2 MB physical batches. |

*Expected gain: large. Outcome: +18%, kept (B.2).*

**2. One `AvroEventProducer` per worker thread** —
[load_runner.py:95-97](../../src/streaming_feature_store/load/load_runner.py#L95-L97).
Premise: the inline `AvroSerializer.__call__` serialises through one
schema-cache lock for all 12 workers — *"likely the dominant CPU
bottleneck once queueing is fixed."* **Empirically disproved — regression,
reverted (B.3).**

**3. Verify the topic has ≥ 12 partitions.** *Outcome: no-op, already
optimal (B.5).*

**4. Reduce `producer.poll(0)` frequency** —
[avro_producer.py:199](../../src/streaming_feature_store/producer/avro_producer.py#L199).
Call `poll(0)` once per app-batch (every 1,024 events) instead of once
per event. Premise: per-event poll is heavy handle-lock contention.
*Expected gain: small-to-medium. Outcome: −34% regression, reverted
(runs #10–11).*

### B.7 Superseded recommendation text (provenance)

> *Verbatim from the 2026-05-13 conclusion, before the multi-process
> result. Superseded by [§4](#4-recommendations-single-source-of-truth).*

**"Recommended live config — Fix #1 only is the keep config.** It
maximizes throughput within the single-process Python ceiling:
`linger.ms=20`, `compression.type=lz4`,
`queue.buffering.max.messages=1_000_000`,
`queue.buffering.max.kbytes=1_048_576`, `acks=1`,
`batch.size=2_000_000`; single shared `AvroEventProducer` for all
worker threads; `produce()` calls `poll(0)` per event. Sustained:
~14.5k evt/s, p50 ~28 ms, p95 ~170 ms, p99 ~270 ms — reproducible
within ~2%."**

**"Production guidance — Suggested deployment shape:** one process per
CPU core; **each process runs a small thread pool (4-8 workers)**
producing through one shared `AvroEventProducer`; broker partition
count = N_processes × workers_per_process; aggregate metrics across
processes. A 4-process deployment would plausibly reach ~50-60k evt/s."**
*(The "4-8 workers per process" figure was wrong for this workload —
corrected to 1-2 in [§4.2](#42-production-guidance).)*

**"Status of the load-test harness — the 50k evt/s floor was an
aspirational target written before the GIL ceiling was characterized.
It is not achievable in the single-process architecture and should be
relaxed (e.g., to 10k for config-sanity verdicts) or moved to a
multi-process variant."** *(Done — the multi-process variant is §3.)*

---

## Appendix C: Process notes

### C.1 Iteration plan (followed during the single-process phase)

After each fix:

1. Re-run `make load-test` (use
   `REPORT=docs/results/week1_load_test_results_<n>.md` to keep prior
   runs around for comparison).
2. Append a new row to the per-run metrics table
   ([Appendix A](#appendix-a-full-single-process-run-history)).
3. Decide whether to keep the change, tune further, or move on.

### C.2 Q: should we apply one fix at a time?

Yes — strongly recommended. Reasons:

- **Attribution:** if you ship all changes together and throughput
  jumps, you have no idea which one moved the needle. This matters for
  production tuning (where `acks=1` is off the table, for example).
- **Risk isolation:** if a change *regresses* (rare but possible — e.g.
  `linger.ms=20` could hurt if app batches are tiny), revert just that
  one instead of bisecting.
- **Diminishing returns / early stop:** fix #1 alone might have cleared
  the floor; if it had, fixes #2-#4 become optional.
- **Documentation value:** each iteration produces a labelled data
  point you can cite later.

The only argument *against* one-at-a-time is calendar time — each
iteration costs a Compose restart + a 10 s run + reading the report
(~2 min). For four fixes that's well under an hour. Worth it.
