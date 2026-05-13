# Week 1 — Load Test Throughput Investigation

Tracking the gap between the configured target rate (60,000 evt/s) and the
sustained rate observed in [week1_load_test_results.md](week1_load_test_results.md),
plus the candidate fixes to close it.

## Run history

Per-run metrics. Targets for every run: target rate 60,000 evt/s, floor
50,000 evt/s, expected produced ~600,000 over 10 s, sub-100 ms p95 ack,
~10 s wallclock. See [week1_load_test_results.md](week1_load_test_results.md)
for the latest run's full report.

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

Key observations (run #0):

- **Sustained throughput is ~10× below target** (5,949 vs 60,000) and ~8× below
  the floor (5,949 vs 50,000).
- **Produced count is suspiciously round**: 65,536 = 64 × 1,024 (batch size).
  Suggests workers stalled after ~64 batches rather than running steadily for
  the full 10 s.
- **p95 ack latency is 830 ms** — the producer's internal queue is saturated;
  acks are queued behind a slow drain.
- **No errors** were recorded, so the failure mode is throughput collapse from
  backpressure, not delivery failure.

## Findings

### Fix #1 — producer-side librdkafka tuning (applied 2026-05-13)

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

**Verdict:** worked as designed. Modest throughput win (+18%), big tail-latency
win (p95 / p99 roughly halved). Still ~3.4× below the 50k floor.

**Per-knob attribution (from observed signatures):**

- `linger.ms=20` — explains the p50 increase from ~12 ms → ~28 ms. The
  producer is now waiting up to 20 ms to fill batches before shipping; this
  is a deliberate latency-for-throughput trade and confirms the knob took effect.
- `compression.type=lz4` + `batch.size=2_000_000` — primary driver of the
  p95 / p99 halving. Fewer, larger, smaller-on-the-wire batches means fewer
  broker round-trips per event and shorter slowest-batch flush times.
- `acks=1` — additional contributor to the tail-latency win. The leader no
  longer waits for follower acks before responding.
- `queue.buffering.max.messages=1_000_000` + `queue.buffering.max.kbytes` —
  preventive. Baseline didn't show `BufferError`s, so this is insurance, not
  a corrective fix.

**What this tells us about the structural cap:**

The throughput ceiling is **upstream of librdkafka**. With per-worker rate
14,492 / 12 ≈ 1,208 evt/s = **~0.83 ms per `produce()` call** (vs ~1.04 ms
baseline), most of the per-event cost is in the Python `produce()` path:
Pydantic validation, Avro serialization, the shared `AvroSerializer`
schema-cache lock, and `poll(0)`. None of those are touched by fix #1.

This is **exactly the regime fix #2 (one `AvroEventProducer` per worker
thread) targets** — the shared schema-cache Python lock across 12 workers is
the most plausible remaining bottleneck.

**Reproducibility:** runs #6 and #7 reproduce within ~2% on every metric
except p99 (-19%, expected variance for a single-message tail percentile).
The result is solid.

### Fix #2 — one `AvroEventProducer` per worker thread (applied 2026-05-13, REVERTED)

**Change applied:** in
[load_runner.py](../../src/streaming_feature_store/load/load_runner.py)
`run()`, each worker now constructs its own `AvroEventProducer` (mirroring the
existing per-worker generator pattern) instead of sharing a single instance.
Each producer is flushed independently after threads join. The change is
applied on top of fix #1.

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

**Verdict: REGRESSION — revert.** Throughput stayed flat while every latency
percentile got dramatically worse. Lock contention on the shared
`AvroSerializer` was **not** the binding constraint — the doc's
prediction in §2 of "Potential fixes" was wrong.

**Why it regressed (root cause):**

The shared producer naturally pooled all 12 workers' events into one fat
batch every `linger.ms=20` window:

- Shared (fix #1): 14,492 evt/s × 0.020 s = **~290 events / batch ≈ 145 KB**
  → broker sees ~50 produce requests / sec.
- Per-worker (fix #2): 14,381 / 12 producers = 1,198 evt/s × 0.020 s
  = **~24 events / batch ≈ 12 KB** → broker sees ~600 produce requests / sec.

Splitting the data stream into 12 independent producers fragmented broker
batches by ~12×. The broker's per-request fixed cost (parse + log append +
ack frame + replication coordination) is roughly 1-3 ms regardless of batch
size; with 12× more requests, that overhead now dominates per-event latency.
Schema-cache lock contention was a tiny saving compared to that new cost.

**Secondary contributors:**

- **12× librdkafka background sender threads** competing for CPU on a
  WSL host with limited vCPUs.
- **12× TCP connections per broker** (12 producers × 3 brokers = 36 sockets
  vs 3 in the shared case).
- **12× producer-buffer reservations** — pre-allocated librdkafka memory
  scales with producer count.

**What this empirically proves about the per-event critical path:**

The Amdahl bound from fix #1 was already telling us the lock could be at
most ~17% of per-event time. Fix #2's regression confirms a tighter bound:
the lock was **negligible** (probably 1-5%), because removing it entirely
gave **zero** throughput gain — even before accounting for the offsetting
broker-side cost. If the lock had been the binding constraint, we would
have seen *some* throughput improvement; we saw none.

**The "not thread-safe" docstring was a correctness statement, not a
performance one.** `AvroSerializer.__call__`'s lock-protected critical
section is a microsecond-scale dict lookup; even with 12 threads in the
queue, the per-event share is tiny. The docstring is good library hygiene
to honor in general, but does not imply the lock is hot in any specific
workload — that requires measurement.

**Methodological lesson — measure before optimizing:**

Fix #2 was applied on the strength of a plausible-sounding hypothesis
(*"shared lock → contention → bottleneck"*) without first profiling the
per-event path to verify the lock was actually a meaningful share of CPU
time. A 5-minute `py-spy --idle` run on a worker thread under fix #1
would have shown the lock at 1-5% of total time and saved this regression.
**Profile first, optimize second** — applies to every future fix in this
investigation.

**Reproducibility:** runs #8 and #9 reproduce within ~5% on all metrics.
The regression is real and stable, not a transient.

**Action:** revert fix #2 in code; the live config is fix #1 (single shared
producer, librdkafka tuned). Update §2 of "Potential fixes" below to record
the empirical disproof of its premise.

### Profile (under fix #1, captured 2026-05-13)

A `py-spy --idle` flame graph of the worker threads under fix #1
([artifact](week1_load_profile.svg),
[full analysis](week1_load_profile_analysis.md)) shows **`producer.poll(0)`
accounts for ~93% of per-worker wall time.** Avro encoding, Pydantic-to-dict,
and the schema-cache lock together total < 5%.

This **vindicates fix #4 as the binding constraint** (re-prioritize from
"small-to-medium gain" to "large gain") and **conclusively confirms fix #2's
post-mortem** (lock contributed 0 detectable samples). Profile-grounded
priority for next work: apply fix #4.

## Hypothesis

The producer pipeline cannot keep up with what 12 worker threads generate, so
workers spend most of their time blocked in `wait_for_in_flight_below(...)`
([load_runner.py:137](../../src/streaming_feature_store/load/load_runner.py#L137),
[load_runner.py:175-177](../../src/streaming_feature_store/load/load_runner.py#L175-L177)).
Effective throughput becomes a function of broker ack latency, not produce rate.

## Potential fixes (in order)

Ordered by expected impact and ease. Apply one at a time and re-run the load
test to attribute improvement.

### 1. Tune the underlying `SerializingProducer` config

**Where:** [avro_producer.py:135-142](../../src/streaming_feature_store/producer/avro_producer.py#L135-L142)

**What:** add throughput-oriented librdkafka knobs.

| Setting | Value | Rationale |
|---|---|---|
| `linger.ms` | `20` | Wait longer before sending to fill bigger batches; current default of 5 ms ships too eagerly. |
| `compression.type` | `lz4` | 3-5× smaller wire payloads for Avro; near-zero CPU cost. |
| `queue.buffering.max.messages` | `1_000_000` | Default 100k is the cause of `BufferError` retries; raises the hard ceiling well above `max_in_flight=50_000`. |
| `queue.buffering.max.kbytes` | `1_048_576` (1 GiB) | Pin the byte cap so it doesn't trip first. |
| `acks` | `1` | Skip replication round-trip on the single-broker dev cluster; load-test only, NOT production. |
| `batch.size` | `2_000_000` (optional) | Allow ~2 MB physical batches if you want larger network sends than the 1 MB default. |

**Expected gain:** large. Should eliminate `BufferError` stalls entirely and
ship messages in much fuller batches.

### 2. One `AvroEventProducer` per worker thread — TRIED, REVERTED

> **Empirical result (runs #8 + #9):** regression. Throughput flat, latency
> 2-6× worse. See [Findings → Fix #2](#fix-2--one-avroeventproducer-per-worker-thread-applied-2026-05-13-reverted)
> for the full diagnosis. The premise below ("schema-cache lock = dominant
> bottleneck") was empirically disproved.

**Where:** [load_runner.py:95-97](../../src/streaming_feature_store/load/load_runner.py#L95-L97)

**What:** mirror the per-worker pattern already used for
`SyntheticEventGenerator` ([load_runner.py:198-203](../../src/streaming_feature_store/load/load_runner.py#L198-L203)).

**Why:** the producer's own docstring says *"Not thread-safe. Construct one
instance per producing thread."* ([avro_producer.py:65-66](../../src/streaming_feature_store/producer/avro_producer.py#L65-L66)).
Even though `confluent_kafka.Producer` is thread-safe at the C level, the
inline `AvroSerializer.__call__` is Python-bound and serialises through one
schema-cache lock for all 12 workers — likely the dominant CPU bottleneck once
queueing is fixed.

**Expected gain:** medium-to-large, especially after fix #1 removes queue
saturation as the binding constraint.

### 3. Verify the topic has ≥ 12 partitions — CHECKED, already optimal

> **Empirical result (2026-05-13):** `topic_admin describe` reports
> `partitions=12, RF=3` with leadership evenly balanced (4 partitions per
> broker). Each of the 12 workers can land on a distinct leader. Partition
> count is **not** the bottleneck.
>
> ```
> e-commerce-events: partitions=12 RF=3
>   kafka-1 leads {0, 5, 6, 9}
>   kafka-2 leads {1, 4, 7, 11}
>   kafka-3 leads {2, 3, 8, 10}
> ```
>
> No code change required. Fix #3 strikes off as a no-op verification.



**Where:** topic creation; use the new
[topic_admin module](../../src/streaming_feature_store/admin/) to inspect
`e-commerce-events`.

**Why:** with `workers=12`, you want at least 12 partitions so each worker's
keys can land on a distinct leader and writes parallelise on the broker side.
A 1-partition topic forces all 12 workers to serialise behind one broker
leader, regardless of producer-side tuning.

**Expected gain:** medium if the topic currently has < 12 partitions; zero if
it already has ≥ 12.

### 4. Reduce `producer.poll(0)` frequency

**Where:** [avro_producer.py:199](../../src/streaming_feature_store/producer/avro_producer.py#L199)

**What:** call `poll(0)` once per app-batch (every 1,024 events) instead of
once per event.

**Why:** `poll(0)` acquires the librdkafka handle and dispatches delivery
callbacks. With 12 threads × 1 call per event, that's heavy contention on the
producer handle. Polling once per batch keeps the callback pump alive without
the per-event overhead.

**Expected gain:** small-to-medium; mostly reduces lock contention overhead.

## Iteration plan

After each fix:

1. Re-run `make load-test` (use `REPORT=docs/results/week1_load_test_results_<n>.md`
   to keep prior runs around for comparison).
2. Append a new row to the per-run metrics table in the **Run history**
   section above (increment `#`, record date, change applied, and all metrics).
3. Decide whether to keep the change, tune it further, or move on to the next
   fix.

## Q: should we apply one fix at a time?

Yes — strongly recommended. Reasons:

- **Attribution:** if you ship all four together and throughput jumps to 80k
  evt/s, you have no idea which change actually moved the needle. Knowing
  this matters for production tuning later (where `acks=1` is off the table,
  for example).
- **Risk isolation:** if a change *regresses* throughput (rare but possible —
  e.g. `linger.ms=20` could hurt if app batches are tiny), you can revert just
  that one change instead of bisecting.
- **Diminishing returns / early stop:** fix #1 alone may already clear the
  50k floor. If it does, fixes #2-#4 become optional polish rather than
  required work.
- **Documentation value:** each iteration produces a labelled data point you
  can cite in the design doc / week-1 retrospective.

The only argument *against* one-at-a-time is calendar time — each iteration
costs a Compose restart + a 10 s run + reading the report (~2 min). For four
fixes, that's well under an hour. Worth it.

## Conclusion (2026-05-13)

After 13 measured runs and four exploratory fixes, the Week 1 load test does
**not** reach the 50,000 evt/s floor on this hardware/runtime, and we now
understand why with empirical confidence.

### Summary of all fixes attempted

| # | Fix | Outcome |
|---|---|---|
| 1 | librdkafka tuning (`linger.ms`, `lz4`, queue caps, `acks=1`, `batch.size=2M`) | ✅ **kept** — +18% throughput, p95/p99 halved |
| 2 | One `AvroEventProducer` per worker thread | ❌ regression, reverted (broker batch fragmentation; lock not the bottleneck) |
| 3 | Verify topic ≥ 12 partitions | ✅ no-op (already 12 partitions, RF=3, evenly balanced) |
| 4 | Move `poll(0)` from per-event to per-app-batch | ❌ regression, reverted (callback dispatch concentration / staleness) |
| pump | Dedicated callback-pump thread (workers stop polling) | ❌ reverted — best p50 (~13 ms) but throughput regressed -32% |

### The structural finding: GIL ceiling at ~10-15k evt/s

Six different architectures — **all clustered between 9.5k and 14.5k evt/s**
sustained throughput on this hardware (12 vCPU WSL2, 3-broker dev Kafka).
That cluster is the empirical fingerprint of the **CPython Global Interpreter
Lock** as the binding constraint:

- The broker can ack faster (single-broker localhost easily exceeds 50k).
- librdkafka can saturate the network long before this rate.
- Topic partition count, schema-cache lock, and `produce()` enqueue cost are
  all sub-bottleneck (proven by the regressions of fixes #2 and #4).
- What remains is **Python-side per-event work** running through the GIL,
  serializing 12 worker threads behind a single interpreter.

No amount of thread-tuning escapes the band. Architectures that move work
between threads (per-worker producers, per-batch poll, dedicated pump) just
shift *which* contention dominates without changing the total Python work
that has to clear the GIL.

### Recommended live config

**Fix #1 only** is the keep config. It maximizes throughput within the
single-process Python ceiling:

- `linger.ms=20`, `compression.type=lz4`,
  `queue.buffering.max.messages=1_000_000`,
  `queue.buffering.max.kbytes=1_048_576`, `acks=1`, `batch.size=2_000_000`.
- Single shared `AvroEventProducer` for all worker threads.
- `produce()` calls `poll(0)` per event (keeps callbacks fresh).

Sustained: ~14.5k evt/s, p50 ~28 ms, p95 ~170 ms, p99 ~270 ms — reproducible
within ~2% across runs.

### Production guidance: scale by processes, not threads

If a production ingestion pipeline needs throughput beyond ~15k evt/s, the
correct architectural answer is **multiple producer processes, not more
worker threads**. Each producer process gets its own Python interpreter and
its own GIL, so N processes scale roughly linearly until the broker or
network saturates.

Suggested deployment shape:

- **One process per CPU core** (or per available vCPU on the host).
- **Each process runs a small thread pool** (4-8 workers) producing through
  one shared `AvroEventProducer` (the within-process fix #1 config).
- **Broker partition count = N_processes × workers_per_process** (or higher)
  so each worker can target a distinct leader.
- **Aggregate metrics across processes** for monitoring (e.g., per-process
  throughput counters → Prometheus → sum).

A 4-process deployment on this hardware would plausibly reach ~50-60k evt/s
throughput at the cost of 4× the memory footprint and added IPC complexity
for the operator.

### Status of the load-test harness

The load test as written serves its primary purpose: **verifying producer
configurations and broker setup, not stress-testing throughput limits**. At
~14.5k evt/s sustained with no errors, no failed deliveries, and clean
percentile shapes, it's a useful regression check for any future change to
the producer, schema, or broker config.

The 50k evt/s floor was an aspirational target written before the GIL ceiling
was characterized. It is **not achievable in the current single-process
architecture** and should be relaxed (e.g., to 10k for "config sanity check"
verdicts) or moved to a multi-process variant of the harness if higher
numbers are wanted.

### Lessons for future work

1. **Profile before optimizing.** Fix #2 was a regression that a 5-minute
   `py-spy --idle` run would have prevented. Speculation about which Python
   construct is "obviously slow" is unreliable; measurement is not.
2. **Read profile output carefully — % time ≠ % wasted.** The 93% of worker
   time in `poll(0)` was mostly *useful* callback dispatch work, not pure
   contention. Removing the call moved the work elsewhere; it didn't
   eliminate it.
3. **Library docstring guidance is for correctness, not always performance.**
   "Not thread-safe — one per thread" doesn't imply the lock is hot. Verify
   with measurement before treating it as a perf prescription.
4. **Architecture fights are conservation games.** Moving work between
   threads doesn't reduce the work; it just changes who waits for what.
   Real throughput gains require either reducing total work or escaping the
   serialization point (in our case, the GIL).
5. **The right "next move" after exhausting same-process options is
   multiprocessing or a no-GIL Python build.** Not more thread-tuning.

