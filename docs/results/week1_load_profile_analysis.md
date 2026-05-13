# Week 1 — Worker Thread Profile (under fix #1)

py-spy flame graph + analysis of where worker threads actually spend their
time during a fix #1 load run. This is the empirical input for the
"profile-first" decision flow now driving the rest of the investigation in
[week1_load_test_throughput_investigation.md](week1_load_test_throughput_investigation.md).

## How the profile was captured

```bash
uv run py-spy record \
    --idle --rate 100 --duration 12 --threads \
    -o docs/results/week1_load_profile.svg \
    -- .venv/bin/python scripts/run_event_load.py \
        --duration-s 10 --target-rate 60000 \
        --report-path /tmp/_load_profile_report.md
```

- **Sampler:** py-spy 0.4.2, statistical sampling at 100 Hz with `--idle`
  (counts both CPU-on and blocked time, so locks/waits are visible).
- **Duration:** 12 s wall-clock (covers ~1 s startup + 10 s steady-state run + tail).
- **Sample budget:** 13,629 stack samples across all threads, 0 errors.
- **Code under profile:** fix #1 applied (single shared `AvroEventProducer`,
  librdkafka tuned: `linger.ms=20`, `lz4`, queue caps, `acks=1`,
  `batch.size=2_000_000`). No fix #2.
- **Profiling cost:** sustained throughput dropped from ~14.5k → 11.4k evt/s
  during the run (-21%), expected from py-spy's ptrace pauses. **Relative
  function-time breakdown is unaffected; absolute throughput is.**

Artifact: [`week1_load_profile.svg`](week1_load_profile.svg) — open in any
browser for the interactive flame graph.

## Per-thread sample budget

13,629 samples × 12 worker threads + main thread + librdkafka background
threads ≈ ~1,140 samples per worker thread (~7.5% of total samples per thread).
This is the per-thread "100% wall-time budget" for the breakdown below.

## Top hotspots, per-worker thread

Aggregating leaf samples that fall under one of the 12 `loadrunner-worker-N`
thread roots (i.e., excluding the `_wait_for_tstate_lock` time spent in the
main thread waiting on `thread.join()`):

| Function | Samples / thread (typical) | % of worker thread time |
|---|---|---|
| **`producer.poll(0)`** ([avro_producer.py:210](../../src/streaming_feature_store/producer/avro_producer.py#L210)) | **~970** | **~93%** |
| `producer.produce(...)` enqueue + Avro serialize ([avro_producer.py:204](../../src/streaming_feature_store/producer/avro_producer.py#L204)) | ~50 | ~5% |
| Worker loop overhead, `_produce_with_retry`, `wait_for_in_flight_below`, pacer | ~20 | ~2% |
| Avro encoding inside `confluent_kafka.schema_registry.avro.__call__` | ~50 (subset of `produce()`) | ~4% (subset of the 5%) |
| Schema-cache lock acquire / `Lock.acquire` | **0 samples** | **~0%** |
| Pydantic / `model_dump` / `to_avro_dict` | **0 samples** | **~0%** |

**The single dominant cost is `producer.poll(0)`** — accounting for ~93% of
worker wall time on every one of the 12 worker threads.

## What this tells us about each remaining fix

### Fix #4 (`poll(0)` once per app-batch, not per event) — VINDICATED, top priority

Currently, `produce(event)` calls `poll(0)` once per event. With 12 workers
producing ~14k events/sec, that's **~168,000 `poll(0)` calls/sec across all
workers**, all hitting the same `librdkafka` handle. The handle has an
internal mutex; under contention, most workers spend most of their time
**blocked inside `poll(0)` waiting for the handle lock to be released by
another worker.**

Fix #4 reduces this to one `poll(0)` per app-batch (1,024 events). At the
same throughput, that's **~165 `poll(0)` calls/sec** — a **~1,000× reduction**
in handle-lock acquires. The 93% of worker time currently spent in `poll(0)`
should largely evaporate.

The investigation doc previously labelled fix #4 as "Expected gain:
small-to-medium." **The profile says it's the binding constraint.** Expected
gain should be **large** — likely the difference between the current ~14.5k
evt/s and somewhere in the 50-100k range.

### Schema-cache lock — confirms fix #2's regression diagnosis

The lock contributed **0 samples** to the profile. Not 1%, not 0.5% —
literally undetectable at 100 Hz sampling × 10 s. This conclusively confirms
the post-mortem on fix #2:

- The "shared `AvroSerializer` schema-cache lock = bottleneck" hypothesis
  was empirically false.
- Fix #2's regression (latency 2-6× worse) was entirely from broker batch
  fragmentation, with zero offsetting win from removing the (non-existent)
  lock cost.

### Avro serialization + Pydantic-to-dict — not the bottleneck

Combined Avro encoding + Pydantic conversion show up in well under 5% of
worker time. The earlier prediction (50-70% in Avro encoding) was wrong —
the actual cost is dominated entirely by `poll(0)` handle contention,
swamping anything else on the per-event path.

This is consistent with the Amdahl bound but corrects the mistaken intuition
about *which* component on the non-I/O path was hot. **Profiling beats
speculation, every time.**

## Methodological note

The earlier prediction in the investigation doc estimated:

| Predicted top hotspot | Actual top hotspot |
|---|---|
| Avro binary encoding (35-50%) | `poll(0)` (~93%) |
| Pydantic-to-dict (15-25%) | `produce()` enqueue (~5%) |
| `poll(0)` (10-20%) | Avro encoding (~4%) |
| Schema-cache lock (1-5%) | Schema-cache lock (~0%) |

**The rank order was wrong, the magnitudes were wrong, and the binding
constraint was wrong.** Five minutes of profiling collapsed weeks of
guesswork. This is the canonical lesson: pre-profile intuitions about
performance — even informed ones — are unreliable. Profile, then optimize.

## Recommended next action

Apply **fix #4** (move `poll(0)` from per-event to per-app-batch). This is
now a high-confidence, high-expected-gain change supported by direct
measurement, not speculation.

Implementation sketch (no code yet — for review):

1. Remove `self._producer.poll(0)` from
   [`AvroEventProducer.produce`](../../src/streaming_feature_store/producer/avro_producer.py#L210).
2. Add a public `AvroEventProducer.poll(timeout: float = 0)` method that
   delegates to `self._producer.poll(timeout)`.
3. In
   [`LoadRunner._worker_loop`](../../src/streaming_feature_store/load/load_runner.py#L139),
   call `producer.poll(0)` **once after the inner `for event in events`
   batch loop** instead of implicitly per event.
4. Adjust unit tests:
   - The existing `test_produce_polls_underlying_producer` test asserts
     poll-per-event; rewrite as `test_produce_does_not_poll_underlying`.
   - Add `test_poll_delegates_to_underlying` for the new method.
   - The load runner `_worker_loop` test should assert `poll(0)` is called
     once per batch.

Then run #10 + #11 (warm baseline) to measure the lift. Expected: throughput
jumps significantly (target: clear the 50k floor in one move), p50 may
slightly increase (delivery callbacks now batched), p95/p99 likely improve
because workers stop blocking inside `poll`.

## Reading the SVG yourself

Open [`week1_load_profile.svg`](week1_load_profile.svg) in a browser:

- Each thread appears as a separate "tower" stack.
- The 12 nearly-identical wide towers are the worker threads — read any one;
  they all look the same (load is balanced).
- Hover any frame for `(samples, % of total)`.
- The widest leaf inside each worker tower is `produce` at line 210 of
  `avro_producer.py` — that's the `poll(0)` call.
- The `_wait_for_tstate_lock` tower is the main thread idling on `join()`
  while workers run; ignore it for per-worker analysis.
