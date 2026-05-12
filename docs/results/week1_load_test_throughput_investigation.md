# Week 1 — Load Test Throughput Investigation

Tracking the gap between the configured target rate (60,000 evt/s) and the
sustained rate observed in [week1_load_test_results.md](week1_load_test_results.md),
plus the candidate fixes to close it.

## Symptom

From the most recent run (2026-05-12, see [week1_load_test_results.md](week1_load_test_results.md)):

| Metric | Observed | Expected |
|---|---|---|
| Target rate | 60,000 evt/s | — |
| Floor (pass/fail) | 50,000 evt/s | — |
| **Sustained rate** | **5,949 evt/s** | ≥ 50,000 evt/s |
| Produced | 65,536 events | ~600,000 over 10 s |
| Acked | 65,536 | — |
| Failed | 0 | — |
| Ack latency p50 / p95 / p99 | 22.0 / 830.6 / 894.2 ms | sub-100 ms |
| Wallclock | 11.02 s | ~10 s |
| Verdict | ❌ FAILED | ✅ PASS |

Key observations:

- **Sustained throughput is ~10× below target** (5,949 vs 60,000) and ~8× below
  the floor (5,949 vs 50,000).
- **Produced count is suspiciously round**: 65,536 = 64 × 1,024 (batch size).
  Suggests workers stalled after ~64 batches rather than running steadily for
  the full 10 s.
- **p95 ack latency is 830 ms** — the producer's internal queue is saturated;
  acks are queued behind a slow drain.
- **No errors** were recorded, so the failure mode is throughput collapse from
  backpressure, not delivery failure.

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

### 2. One `AvroEventProducer` per worker thread

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

### 3. Verify the topic has ≥ 12 partitions

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
2. Record the new sustained rate, p50/p95/p99, and any error counts in this
   document under a new "Run history" section.
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

## Run history

| # | Date | Change applied | Sustained evt/s | p95 ack ms | Notes |
|---|---|---|---|---|---|
| 0 | 2026-05-12 | baseline (no tuning) | 5,949 | 830.6 | initial failed run |
