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

