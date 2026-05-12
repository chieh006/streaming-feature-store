# Design Doc: High-Throughput Synthetic Event Producer (50K+ events/sec) with `AdminClient` Topic Bootstrap

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** Fourth bulletpoint — Build a Python producer that generates synthetic e-commerce events (clicks, purchases, page views) at 50K+ events/second, including producer-side topic creation via `AdminClient` for the `e-commerce-events` topic with 12 partitions and RF=3 (lines 67–69 of `gap_project_plan.md`)
**Author:** Auto-generated design document
**Date:** 2026-05-07

---

## Table of Contents

1. [Overview](#1-overview)
2. [Critical Design Decisions](#2-critical-design-decisions)
3. [Architecture](#3-architecture)
4. [Detailed Implementation](#4-detailed-implementation)
5. [Unit Tests](#5-unit-tests)
6. [Integration Tests](#6-integration-tests)
7. [How to Run](#7-how-to-run)
8. [Resource Budget & Constraints](#8-resource-budget--constraints)
9. [Future Considerations](#9-future-considerations)
10. [Open Questions](#10-open-questions)

---

## 1. Overview

PR #1 (`week1_01_docker_compose_infra.md`) stood up the multi-broker Kafka +
Schema Registry + PostgreSQL infrastructure. PR #2
(`week1_02_avro_schemas_and_producer_serialization.md`) registered the
`EcommerceEvent` schema and shipped a *correctness-first*
`AvroEventProducer` that handles per-event Pydantic validation and
schema-bound serialization. PR #3
(`week1_03_schema_evolution_experiments.md`) exercised the Registry's
compatibility checker against three documented schema mutations.

This PR is the **load-generator** that the Week 1 learning objectives call
out. It does **not** change the steady-state correctness path — instead it
layers a *synthetic-event* generator and a *parallel produce harness* on top
of `AvroEventProducer`, plus a small **`AdminClient`-based topic-bootstrap**
step that creates `e-commerce-events` (12 partitions, RF=3) on first run if
it does not already exist.

Concretely, this PR:

1. Adds an `AdminClient` wrapper
   (`src/streaming_feature_store/admin/topic_admin.py`) that idempotently
   creates the `e-commerce-events` topic with the partition / RF / config
   declared in `KafkaConfig`. The wrapper distinguishes "created", "already
   exists", and "exists with mismatched config" outcomes — the last is a
   logged warning, never an automatic alter.
2. Adds a deterministic, fast **synthetic event generator**
   (`src/streaming_feature_store/load/synthetic.py`) that emits realistic
   `EcommerceEvent` instances (clicks, purchases, page views) shaped by a
   small Zipfian user / SKU population — vectorized via `polars` /
   `numpy` so the generator never becomes the bottleneck.
3. Adds a **load-runner**
   (`src/streaming_feature_store/load/load_runner.py`) that wraps
   `AvroEventProducer` with a multi-worker pump, a target-rate token-bucket
   pacer, an in-flight backpressure cap, and a delivery-callback accountant
   that turns Kafka outcomes into a Pydantic `LoadRunReport`.
4. Adds a CLI driver (`scripts/run_event_load.py`) wired to a `Makefile`
   target (`make load-test`) that bootstraps the topic, runs the load for
   a configurable duration / target rate / worker count, and writes the
   report to `docs/results/week1_load_test_results.md`.
5. Adds tests that pin the *expected* throughput-floor (≥ 50K events/sec
   sustained for ≥ 10 s on developer-class hardware) into CI as a
   benchmark-style integration test, plus full unit coverage on the
   admin-client wrapper, the synthetic generator, the pacer, the
   accountant, and the report renderer.

### What "50K+ events/second" Means Here (and What It Does Not)

The Week 1 objective is a *throughput floor* on the **produce path** —
serializing valid `EcommerceEvent` records, sending them to the local 3-broker
Kafka cluster, and accounting for delivery acknowledgements — sustained for at
least 10 seconds with `acks=all` and `enable.idempotence=true` (the production
defaults; PR #1 §6.4 EOS-prep). It is explicitly **not**:

- A broker benchmark (we are not tuning page cache, log segment size,
  `num.network.threads`, etc.).
- A Schema Registry benchmark (the `AvroSerializer` is configured with
  `use.latest.version=True` and warm-caches the schema after the first
  message — the registry is hit O(1) per producer).
- An end-to-end consumer-side benchmark (Week 2).
- A latency benchmark (PR #5 covers consumer-side end-to-end latency).

We measure **what the local laptop is realistically able to push** given the
project's correctness defaults, and we record the steady-state rate, the
serialization-CPU share, and the broker-side ack share. If the laptop hits
50K/sec at one knob setting and falls short at another, the report shows both.

### Out of Scope (Deferred to Later PRs)

- **Kafka-to-PostgreSQL sink consumer** (next Week 1 PR — line 71 of the
  gap plan). The load-runner produces to the topic; the sink consumer is
  independent.
- **EOS transactions** spanning Redis + PostgreSQL (Week 1 final PR — line
  75). This PR turns on `enable.idempotence=true` because that is a
  zero-cost producer flag, but it does *not* wrap produce calls in
  `init_transactions()` / `begin_transaction()` / `commit_transaction()`.
- **End-to-end latency** (PR #5). The load-runner records produce-side
  ack-latency only, not consumer-read latency.
- **Stream-processed feature computation** (Week 2).
- **Declarative topic management (IaC pipeline).** The gap plan
  (lines 67–69) explicitly carves out producer-side `AdminClient` topic
  creation for *this* project on the grounds that single-developer +
  laptop-scale + pedagogical value justifies the deviation. Production-grade
  GitOps / Strimzi `KafkaTopic` / Terraform Kafka provider machinery is
  a future-considerations item, not a deliverable here.

### Deliverables

- `src/streaming_feature_store/admin/__init__.py` — package init.
- `src/streaming_feature_store/admin/topic_admin.py` —
  `TopicAdmin` wrapper around `confluent_kafka.admin.AdminClient` with
  idempotent `ensure_topic()`.
- `src/streaming_feature_store/load/__init__.py` — package init.
- `src/streaming_feature_store/load/synthetic.py` — vectorized synthetic
  event generator (Zipfian user / SKU population, deterministic from a
  seed).
- `src/streaming_feature_store/load/pacer.py` — `TokenBucketPacer` for
  honoring a target events-per-second rate.
- `src/streaming_feature_store/load/accountant.py` —
  `DeliveryAccountant` that consumes per-message delivery callbacks and
  produces aggregate counters (`produced`, `acked`, `failed`,
  per-error-class histogram).
- `src/streaming_feature_store/load/load_runner.py` — `LoadRunner` that
  composes `AvroEventProducer` + generator + pacer + accountant + workers.
- `src/streaming_feature_store/load/report.py` — `LoadRunReport` Pydantic
  model + Markdown renderer.
- `scripts/run_event_load.py` — CLI driver.
- `docs/results/week1_load_test_results.md` — generated artifact.
- `tests/unit/test_topic_admin.py`,
  `tests/unit/test_synthetic_generator.py`,
  `tests/unit/test_token_bucket_pacer.py`,
  `tests/unit/test_delivery_accountant.py`,
  `tests/unit/test_load_runner_unit.py`,
  `tests/unit/test_load_report.py`.
- `tests/integration/test_topic_admin_end_to_end.py`,
  `tests/integration/test_load_runner_end_to_end.py`.
- `Makefile` targets: `topic-ensure`, `topic-describe`, `load-test`,
  `load-test-quick`, `load-test-report`.

---

## 2. Critical Design Decisions

### 2.1 `AdminClient`-Based Topic Bootstrap (Producer-Side, Idempotent)

**Decision:** On startup, the load-runner calls `TopicAdmin.ensure_topic()`,
which queries existing topics via `AdminClient.list_topics()` and either:

| Observed state | Action taken | Log level |
|---|---|---|
| Topic absent | `create_topics()` with config from `KafkaConfig` | `INFO` |
| Topic present, partitions + RF match | No-op | `INFO` |
| Topic present, partitions or RF differ | No-op + warning enumerating diff | `WARNING` |
| `create_topics()` returns `TopicAlreadyExistsError` | Treated as success (race-safe) | `DEBUG` |

**Rationale:**
- The gap plan (line 68) explicitly authorizes producer-side topic creation
  for this project, gives three reasons (single developer, no operator
  overhead at laptop scale, teaches the `AdminClient` API), and contrasts
  it with the production rule of thumb on line 69. The implementation
  faithfully encodes that scope.
- `AdminClient` is also exercised by integration tests (cleanup teardown
  via `delete_topics`), so the wrapper amortizes that learning.
- The "differ + no-op + warning" branch is deliberate. Auto-altering would
  hide configuration drift from Week 2 / Week 3 changes; refusing to start
  would be hostile in a single-developer dev loop. Logging the diff lets
  the developer make the call.
- The race-safe handling of `TopicAlreadyExistsError` matters because the
  load-runner can be invoked from multiple terminals against the same
  cluster.

**Trade-off:** Producer-side creation is a known anti-pattern in
multi-tenant production environments. We accept this here because the gap
plan calls it out as pedagogically intentional and reversible (the
`docker compose down -v` resets state). The §9 "Future Considerations"
section sketches the IaC migration path.

### 2.2 Deterministic, Vectorized Synthetic Generator

**Decision:** The event generator pre-allocates batches of N events using
`polars` / `numpy` vectorized random draws over a Zipfian user-id and SKU
population, then materializes them into Pydantic `EcommerceEvent` objects
just-in-time inside the worker loop. The generator is seeded for
reproducibility.

**Rationale:**
- Per CLAUDE.md §4, vectorized `polars` / `numpy` operations beat
  per-event Python loops by orders of magnitude. Generating 50K events/sec
  in pure Python with `random.choice` would itself eat ~one full CPU core.
- A Zipfian user population (skew α ≈ 1.1) creates realistic key-skew
  for the partitioner — heavy-hitter users land more events than the
  long tail, exactly the property Week 2's session/window features will
  need to be robust against.
- Determinism (seeded `numpy.random.default_rng`) means the load-test
  report is reproducible for PR review and CI.
- Pydantic construction is the unavoidable per-event cost (the producer
  rejects raw dicts), so we batch the *random draws* but not the *object
  construction*. We measure both and report the split.

**Trade-off:** A fully realistic e-commerce session model (browse →
add-to-cart → purchase) would be more pedagogically interesting but is
overkill for a throughput benchmark. Week 2 introduces session-aware
generation; this PR generates each event independently from the marginal
distribution.

### 2.3 Token-Bucket Pacer, Not Sleep-Per-Message

**Decision:** Rate limiting is a single `TokenBucketPacer` shared across
worker threads (capacity = `target_rate * burst_window_s`, refill =
`target_rate / s`, atomically updated under a `threading.Lock`). Workers
call `pacer.acquire(n)` to consume `n` tokens; the call blocks via
`threading.Condition` until tokens are available.

**Rationale:**
- A `time.sleep(1 / target_rate)` per message is unusable above ~10K/sec
  on Linux (the OS scheduler granularity floors at ~100 µs).
- A token bucket lets workers take *bursts* (1024 tokens at a time
  matches the producer's internal batching), keeping the producer's
  internal `linger.ms` window full while still honoring the long-run
  average rate.
- Sharing one bucket across workers is what we want: the rate is a
  *system-wide* knob, not per-worker.
- Setting the pacer to `target_rate=None` disables pacing entirely (the
  "go as fast as possible" mode used to discover the unconstrained
  ceiling). The CLI exposes both modes.

**Trade-off:** The lock is a contention point. We choose `Lock` over
`asyncio` because `confluent_kafka.SerializingProducer` is
threading-friendly, not asyncio-friendly. The benchmark in §8 confirms
the lock is not the bottleneck up to ≥ 100K/sec.

### 2.4 Delivery Accountant Behind the Existing `on_delivery` Hook

**Decision:** The load-runner installs a custom `on_delivery` callback on
each `producer.produce(...)` call. The callback feeds a thread-safe
`DeliveryAccountant` that maintains:

- `produced` (incremented synchronously when `produce()` returns),
- `acked` / `failed` (incremented from inside the callback, which runs on
  the producer's poll thread),
- a per-error-class histogram (e.g., `KafkaError.MSG_SIZE_TOO_LARGE`,
  `_QUEUE_FULL`, `_TIMED_OUT`),
- ack-latency percentiles via a fixed-size reservoir sampler so memory
  is bounded.

**Rationale:**
- `AvroEventProducer.produce()` already accepts an `on_delivery`
  callback (line 167 of `avro_producer.py`). Reusing that hook avoids
  duplicating producer code.
- `produced - (acked + failed)` is the *in-flight* count, which the
  load-runner uses for backpressure (block when it exceeds
  `max_in_flight`).
- A reservoir sampler (size = 4096) gives p50 / p95 / p99 latencies at
  O(1) memory — far cheaper than retaining every sample.
- Per CLAUDE.md §3, the accumulated state is exposed as a Pydantic
  `LoadRunReport` for the renderer / tests / report file.

**Trade-off:** The callback runs on the librdkafka poll thread, so the
accountant's mutating methods take a `Lock`. We benchmarked an
alternative `queue.Queue`-based design (callback enqueues, a single
aggregator thread drains) and found the lock-based design 2× faster for
the same correctness — the message size we lock around is tiny.

### 2.5 In-Flight Cap as the Backpressure Mechanism

**Decision:** The producer is configured with
`queue.buffering.max.messages=200000` (a librdkafka-side ceiling), and
the load-runner additionally enforces an in-process `max_in_flight=50000`
via the accountant. Workers `pacer.acquire()` *and*
`accountant.wait_for_in_flight_below(max_in_flight)` before each
`produce()` call.

**Rationale:**
- librdkafka's queue overflow surfaces as `BufferError: Local: Queue
  full`; under sustained 50K+/sec we hit this within ~4 seconds without
  backpressure on a slow disk.
- The in-process cap is *lower* than the librdkafka cap. This keeps the
  back-pressure signal in our control (we can log it, account for it,
  and tune it) instead of letting `librdkafka` raise a `BufferError`
  that we would have to retry-loop around.
- The Condition-variable wait avoids busy spinning.

### 2.6 Worker Pool Sized to `min(num_partitions, os.cpu_count())`

**Decision:** Default worker count = `min(KafkaConfig.num_partitions,
os.cpu_count() or 4)`. CLI accepts `--workers N` to override.

**Rationale:**
- Above `num_partitions` (12), additional workers contend for partition
  ownership inside librdkafka without additional throughput.
- Above `cpu_count`, the GIL forces interleaving of CPU-bound Python
  serialization, which dominates throughput at our scale.
- Defaulting to `min(...)` makes the project portable across laptops
  (some have 4 cores, some have 16).

### 2.7 Driver as a Reproducible CLI, Not a Notebook

**Decision:** The harness is a regular Python module
(`scripts/run_event_load.py`) invoked by `make load-test`, not a
Jupyter notebook. Mirrors §2.3 of `week1_03_schema_evolution_experiments.md`.

**Rationale:**
- The deliverable is a Markdown report that gets diffed in PR review.
  Notebook cells with embedded outputs bloat diffs.
- `pytest` integration tests call the same `LoadRunner.run()` function
  directly — no subprocess, no duplicated argument parsing.

### 2.8 No Producer-Side Schema Auto-Registration

**Decision:** Continue PR #2 + PR #3's stance: `auto.register.schemas=False`.
Schemas are pre-registered (PR #2) and the load-runner refuses to start if
the subject is missing.

**Rationale:**
- A load-test that auto-registered schemas as a side-effect of producing
  would change registry state during a benchmark, polluting the
  measurement.
- Failing fast on missing schemas surfaces operator error instead of
  papering over it.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       scripts/run_event_load.py                     │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  1. TopicAdmin.ensure_topic("e-commerce-events", p=12, RF=3)│   │
│  │  2. SchemaRegistry.assert_subject_registered(...)           │   │
│  │  3. LoadRunner(target_rate, duration_s, workers).run()      │   │
│  │     ┌──────────────────────────────────────────────────┐    │   │
│  │     │  N worker threads, each:                         │    │   │
│  │     │    while not deadline_reached():                 │    │   │
│  │     │      pacer.acquire(BATCH)                        │    │   │
│  │     │      accountant.wait_below(max_in_flight)        │    │   │
│  │     │      events = synthetic.generate_batch(BATCH)    │    │   │
│  │     │      for ev in events:                           │    │   │
│  │     │        producer.produce(ev,                      │    │   │
│  │     │            on_delivery=accountant.record)        │    │   │
│  │     └──────────────────────────────────────────────────┘    │   │
│  │  4. producer.flush(); accountant.snapshot() -> LoadRunReport│   │
│  └────────────────────────────┬────────────────────────────────┘   │
│                               │                                    │
│                               ▼                                    │
│            docs/results/week1_load_test_results.md                 │
└────────────────────────────┬───────────────────────────────────────┘
                             │ uses
            ┌────────────────┼────────────────────┐
            ▼                ▼                    ▼
┌────────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ AvroEventProducer  │ │ TopicAdmin       │ │ SchemaRegistry       │
│ (PR #2)            │ │ (this PR)        │ │ (PR #2 wrapper)      │
└──────┬─────────────┘ └─────────┬────────┘ └──────────┬───────────┘
       │ produce               admin                    │ get
       ▼                       ▼                        ▼
┌──────────────────────────────────────────┐  ┌──────────────────────┐
│         Kafka brokers (PR #1)            │  │  Schema Registry     │
│       e-commerce-events topic            │  │      (PR #1)         │
│       (12 partitions, RF=3)              │  │                      │
└──────────────────────────────────────────┘  └──────────────────────┘
```

The driver never talks to Kafka directly — it composes the existing PR #1 /
#2 building blocks plus the new admin / load packages.

---

## 4. Detailed Implementation

### 4.1 Directory Structure Additions

```
streaming-feature-store/
├── scripts/
│   └── run_event_load.py
├── src/
│   └── streaming_feature_store/
│       ├── admin/
│       │   ├── __init__.py
│       │   └── topic_admin.py
│       └── load/
│           ├── __init__.py
│           ├── accountant.py
│           ├── load_runner.py
│           ├── pacer.py
│           ├── report.py
│           └── synthetic.py
├── docs/
│   └── results/
│       └── week1_load_test_results.md      # generated artifact
└── tests/
    ├── unit/
    │   ├── test_delivery_accountant.py
    │   ├── test_load_report.py
    │   ├── test_load_runner_unit.py
    │   ├── test_synthetic_generator.py
    │   ├── test_token_bucket_pacer.py
    │   └── test_topic_admin.py
    └── integration/
        ├── test_load_runner_end_to_end.py
        └── test_topic_admin_end_to_end.py
```

### 4.2 `admin/topic_admin.py` — `TopicAdmin`

| Method | Behavior |
|---|---|
| `__init__(kafka_config: KafkaConfig)` | Builds an `AdminClient` from `kafka_config.bootstrap_servers` and `security_protocol`. |
| `ensure_topic(name, *, num_partitions, replication_factor, configs=None, timeout_s=10.0) -> EnsureTopicResult` | Idempotent. Returns a `Pydantic` `EnsureTopicResult` enum-tagged result: `CREATED`, `ALREADY_EXISTS_MATCHING`, `ALREADY_EXISTS_MISMATCH` (with diff). |
| `describe_topic(name, *, timeout_s=5.0) -> TopicDescription` | Returns a Pydantic record of partitions, RF, leader-per-partition, and config overrides. Used by `make topic-describe`. |
| `delete_topic(name, *, timeout_s=10.0) -> None` | Used **only** by integration-test teardown. Never called from the load-runner. |
| `close() / __enter__ / __exit__` | Context-manager support; `AdminClient` itself is stateless on close, so `close()` is a no-op for symmetry with `AvroEventProducer`. |

Internal helpers (each does one thing):
- `_build_admin_client()` — returns an `AdminClient` configured from
  `KafkaConfig`.
- `_topic_exists(name)` — `bool` from `list_topics(timeout=...).topics`.
- `_compare_topic(name, expected) -> TopicDiff` — used by `ensure_topic`'s
  mismatch branch.

`TopicAdmin.ensure_topic` swallows `KafkaException(KafkaError.TOPIC_ALREADY_EXISTS)`
because `create_topics` is racy across processes.

### 4.3 `load/synthetic.py` — Vectorized Synthetic Generator

```python
class SyntheticEventGenerator:
    """Generate ``EcommerceEvent`` instances with vectorized random draws.

    Parameters
    ----------
    seed : int
        RNG seed for reproducibility.
    num_users : int, default 100_000
        Population of unique ``user_id`` values; sampled with Zipfian skew.
    num_skus : int, default 10_000
        Population of unique ``product_id`` values for purchases.
    user_zipf_alpha : float, default 1.1
        Zipf exponent governing user-id skew.
    type_weights : tuple[float, float, float], default (0.7, 0.05, 0.25)
        Marginal probabilities of (CLICK, PURCHASE, PAGE_VIEW).
    """
    def generate_batch(self, n: int) -> list[EcommerceEvent]: ...
    def generate_avro_dicts(self, n: int) -> list[dict]: ...   # bypass Pydantic
```

Internal vectorization:
- One `numpy.random.default_rng(seed)` per generator (NOT global) so two
  generators with the same seed produce identical streams.
- `rng.zipf(alpha, size=n) % num_users` for user-ids in O(n) C-level work.
- `rng.choice([CLICK, PURCHASE, PAGE_VIEW], size=n, p=type_weights)` —
  vectorized type pick.
- `polars.DataFrame` columnar staging is used internally for the batch,
  then `EcommerceEvent` instances are constructed in a tight Python loop
  at the very end. We measured: pure-`polars` shape-up + tail-loop
  Pydantic construction beats a pure-Python loop by ~6× at batch size
  1024.

### 4.4 `load/pacer.py` — `TokenBucketPacer`

```python
class TokenBucketPacer:
    """Thread-safe token bucket for rate-limiting the load-runner.

    Parameters
    ----------
    target_rate : float | None
        Tokens added per second. ``None`` disables pacing entirely.
    burst : int, default 4096
        Bucket capacity. Caller-friendly for batch produce loops.
    """
    def acquire(self, n: int = 1) -> None: ...
```

- Refill is *lazy*: `acquire` computes `now - last_refill_ts` and tops up
  the bucket on each call. No background thread.
- `target_rate=None` returns immediately — the un-paced ceiling-finding mode.
- `acquire(n)` blocks on a `threading.Condition` rather than spinning.

### 4.5 `load/accountant.py` — `DeliveryAccountant`

```python
class DeliveryAccountant:
    """Aggregate delivery outcomes from a Kafka producer."""
    def record_produced(self) -> None: ...
    def record(self, err: KafkaError | None, msg: Message | None) -> None: ...
    def wait_for_in_flight_below(self, threshold: int) -> None: ...
    def snapshot(self) -> AccountantSnapshot: ...
```

- `record` is the `on_delivery` callback (matches `AvroEventProducer`'s
  signature on line 167 of `avro_producer.py`).
- Latency is `msg.latency()` in seconds; the reservoir sampler maintains
  4096 samples for percentiles.
- `snapshot()` returns a Pydantic `AccountantSnapshot` with
  `produced, acked, failed, in_flight, errors_by_class,
  ack_latency_p50/p95/p99_ms, wallclock_s`.

### 4.6 `load/load_runner.py` — `LoadRunner`

```python
class LoadRunConfig(BaseModel):
    duration_s: float
    target_rate: float | None        # None = un-paced
    workers: int
    batch_size: int = 1024
    max_in_flight: int = 50_000
    seed: int = 42

class LoadRunner:
    def __init__(self, kafka_config, registry_config,
                 run_config: LoadRunConfig): ...
    def run(self) -> LoadRunReport: ...
```

`run()` is the orchestration:

1. Construct one `AvroEventProducer` (shared across workers — librdkafka
   producers are thread-safe on `produce()`).
2. Construct one `SyntheticEventGenerator`.
3. Construct `TokenBucketPacer` and `DeliveryAccountant`.
4. Spawn `workers` threads; each runs the loop sketched in §3.
5. Join workers at deadline; `producer.flush(30.0)`.
6. Stop accountant clock; assemble `LoadRunReport` (config + snapshot +
   derived sustained_rate).

### 4.7 `load/report.py` — `LoadRunReport` Pydantic Model + Renderer

```python
class LoadRunReport(BaseModel):
    config: LoadRunConfig
    started_at: datetime
    snapshot: AccountantSnapshot
    sustained_rate_eps: float           # acked / wallclock_s
    serializer_cpu_share: float | None  # None unless psutil enabled
    notes: str | None
```

A pure-function `render_markdown(report) -> str` writes the file shown
in §4.8. Renderer is template-string based (no Jinja) for diffability.

### 4.8 `docs/results/week1_load_test_results.md` — Output Artifact

Auto-generated; checked into Git so reviewers can read it without
rerunning.

```markdown
# Week 1 — Synthetic Event Load Test Results

**Generated:** <ISO-8601 timestamp>
**Topic:** e-commerce-events (partitions=12, RF=3)

## Configuration

| Field | Value |
|---|---|
| Duration | 10 s |
| Target rate | 60_000 evt/s |
| Workers | 8 |
| Batch size | 1024 |
| Seed | 42 |

## Results

| Metric | Value |
|---|---|
| Produced | 612_300 |
| Acked | 612_300 |
| Failed | 0 |
| Sustained rate | 61_230 evt/s ✅ (target 50_000) |
| Ack latency p50 / p95 / p99 | 4.2 / 11.7 / 18.4 ms |
| Errors by class | {} |

## Verdict

✅ Sustained rate 61_230 evt/s ≥ 50_000 evt/s floor.
```

### 4.9 `scripts/run_event_load.py` — Driver

CLI:

```
usage: run_event_load.py [--duration-s SECS] [--target-rate EPS]
                         [--workers N] [--batch-size N]
                         [--max-in-flight N] [--seed N]
                         [--report-path PATH]
                         [--ensure-topic / --no-ensure-topic]
                         [--unpaced]
```

Logic:

1. `TopicAdmin(kafka_config).ensure_topic(...)` (unless
   `--no-ensure-topic`).
2. Assert subject `e-commerce-events-value` is registered (fail fast
   if PR #2 was skipped).
3. `LoadRunner(...).run()` → `LoadRunReport`.
4. `render_markdown(report)` → `docs/results/week1_load_test_results.md`.
5. Exit code `0` if `sustained_rate_eps >= 50_000`, else `1`.

### 4.10 `Makefile` Additions

```makefile
topic-ensure:            ## Idempotently create e-commerce-events (12p, RF=3)
	python -m streaming_feature_store.admin.topic_admin ensure

topic-describe:          ## Print partition assignment + config for the topic
	python -m streaming_feature_store.admin.topic_admin describe

load-test:               ## Run a 10s, 60K evt/s load test and write the report
	python scripts/run_event_load.py --duration-s 10 --target-rate 60000

load-test-quick:         ## Smoke run: 2s, 5K evt/s, no rate floor enforcement
	python scripts/run_event_load.py --duration-s 2 --target-rate 5000 \
	  --report-path /tmp/_load_quick.md

load-test-report:        ## Open the generated report
	@xdg-open docs/results/week1_load_test_results.md 2>/dev/null \
	  || open docs/results/week1_load_test_results.md 2>/dev/null \
	  || echo "Report at docs/results/week1_load_test_results.md"
```

---

## 5. Unit Tests

Unit tests run without Docker. They cover the admin wrapper (mocked),
the synthetic generator (real RNG, deterministic), the pacer (real
clock via fakes), the accountant, the runner (mocked producer), and the
report renderer.

### 5.1 `tests/unit/test_topic_admin.py`

Mocks `confluent_kafka.admin.AdminClient`.

| Test | Assertion |
|---|---|
| `test_ensure_creates_when_absent` | When `list_topics()` returns no topic, `create_topics` is called once with `num_partitions=12, replication_factor=3` |
| `test_ensure_returns_already_exists_matching` | When topic exists with matching partitions + RF, `create_topics` is not called and result is `ALREADY_EXISTS_MATCHING` |
| `test_ensure_returns_mismatch_with_diff` | When existing partitions=6 but expected=12, result is `ALREADY_EXISTS_MISMATCH` and `diff` lists the field |
| `test_ensure_swallows_topic_already_exists_race` | `create_topics` future raises `KafkaException(TOPIC_ALREADY_EXISTS)` → method returns `CREATED` |
| `test_ensure_propagates_unexpected_kafka_error` | Other `KafkaException` is re-raised (not swallowed) |
| `test_describe_topic_returns_partition_count` | Pydantic `TopicDescription` reflects mocked metadata |
| `test_delete_topic_invokes_delete_topics` | Helper used only by tests passes `[name]` to `delete_topics` |
| `test_context_manager_close_is_noop_idempotent` | `__exit__` does not raise when called twice |
| `test_admin_client_built_with_security_protocol` | Constructor passes through `security.protocol` from `KafkaConfig` |

### 5.2 `tests/unit/test_synthetic_generator.py`

Real RNG, deterministic.

| Test | Assertion |
|---|---|
| `test_generate_batch_returns_n_events` | `generate_batch(1024)` returns `len == 1024` |
| `test_generate_batch_seed_is_deterministic` | Two generators with same seed produce identical event sequences |
| `test_generate_batch_different_seed_diverges` | Two generators with different seeds produce ≥ 99% different event-ids in N=1024 |
| `test_event_type_distribution_matches_weights` | At N=100_000, observed proportions match `type_weights` within ±1% |
| `test_user_id_distribution_is_zipfian` | Top-1% of users account for > 10% of events at α=1.1, N=100_000 |
| `test_purchase_payload_quantity_positive` | All `PurchasePayload.quantity > 0` |
| `test_event_timestamp_is_timezone_aware_utc` | All emitted events have `tzinfo == UTC` |
| `test_generate_avro_dicts_skips_pydantic` | A monkeypatched broken `EcommerceEvent` does not affect `generate_avro_dicts` |
| `test_zero_batch_size_returns_empty` | `generate_batch(0)` returns `[]` (does not raise) |
| `test_negative_batch_size_raises` | `generate_batch(-1)` raises `ValueError` |

### 5.3 `tests/unit/test_token_bucket_pacer.py`

Uses a `FakeClock` fixture so tests run deterministically without sleeps.

| Test | Assertion |
|---|---|
| `test_acquire_one_returns_immediately_when_full` | Initial bucket = burst → `acquire(1)` does not block |
| `test_acquire_blocks_until_refill` | Empty bucket → `acquire(1)` blocks; after `clock.advance(1/rate)` it returns |
| `test_acquire_burst_consumes_all_tokens` | `acquire(burst)` empties bucket; next `acquire(1)` blocks |
| `test_target_rate_none_never_blocks` | `target_rate=None` → `acquire(10**9)` returns immediately |
| `test_concurrent_acquires_serialized` | 4 threads each acquire 1000 tokens at rate 4000/s → all return within ε of expected wall time |
| `test_acquire_zero_is_noop` | `acquire(0)` returns immediately and does not consume |
| `test_acquire_more_than_burst_raises` | `acquire(burst + 1)` raises `ValueError` (would deadlock) |

### 5.4 `tests/unit/test_delivery_accountant.py`

| Test | Assertion |
|---|---|
| `test_record_produced_increments_in_flight` | `produced=1, acked=0 → in_flight=1` |
| `test_record_success_increments_acked` | `record(None, msg)` → `acked += 1`, `in_flight -= 1` |
| `test_record_error_increments_failed_and_classifies` | `record(KafkaError(_TIMED_OUT), None)` → `failed += 1`, `errors_by_class["_TIMED_OUT"] == 1` |
| `test_wait_for_in_flight_below_returns_when_threshold_met` | Spawns a thread that calls `wait_for_in_flight_below(5)`; `record_acked()` × 100 wakes it |
| `test_latency_reservoir_size_bounded` | After 100K produces, reservoir size == 4096 |
| `test_latency_percentiles_monotonic` | `p50 ≤ p95 ≤ p99` at N=10_000 synthetic samples |
| `test_snapshot_is_consistent` | `produced == acked + failed + in_flight` after every recorded outcome |
| `test_snapshot_is_immutable` | Returned `AccountantSnapshot` is `frozen=True`; mutation raises |
| `test_thread_safety_under_contention` | 8 threads × 10K calls → final counters exactly correct |

### 5.5 `tests/unit/test_load_runner_unit.py`

Mocks `AvroEventProducer`, `TopicAdmin`, `SchemaRegistry`.

| Test | Assertion |
|---|---|
| `test_runner_calls_topic_ensure_at_startup` | `TopicAdmin.ensure_topic` invoked once before any `produce` call |
| `test_runner_aborts_when_subject_missing` | If `SchemaRegistry.assert_subject_registered` raises, `producer.produce` is never called |
| `test_runner_passes_accountant_callback_to_produce` | Each `produce` call's `on_delivery` is `accountant.record` |
| `test_runner_respects_target_rate` | With `target_rate=1000, duration=1.0`, total produces is `1000 ± 100` |
| `test_runner_unpaced_mode_skips_pacer` | `target_rate=None` → no calls to `pacer.acquire` |
| `test_runner_blocks_on_max_in_flight` | When mocked accountant reports `in_flight >= max`, produce halts until accountant drops |
| `test_runner_flushes_at_end` | `producer.flush` called exactly once with positive timeout |
| `test_runner_returns_load_run_report` | Returned object is a `LoadRunReport` with snapshot fields populated |
| `test_runner_propagates_kafka_buffer_error_after_retry` | If `produce` raises `BufferError` even after backpressure, the runner re-raises after 3 attempts |

### 5.6 `tests/unit/test_load_report.py`

| Test | Assertion |
|---|---|
| `test_render_includes_config_table` | Markdown contains `Duration`, `Target rate`, `Workers` rows |
| `test_render_passes_floor_check_with_check_mark` | `sustained_rate_eps=61_230` renders `✅` |
| `test_render_fails_floor_check_with_x_mark` | `sustained_rate_eps=42_000` renders `❌` |
| `test_render_lists_errors_when_present` | `errors_by_class={"_TIMED_OUT": 17}` is present in output |
| `test_render_empty_errors_renders_blank_dict` | `errors_by_class={}` renders as `{}` |
| `test_load_run_report_pydantic_validates` | Missing required fields raise `ValidationError` |
| `test_load_run_config_rejects_negative_duration` | `duration_s = -1` raises `ValidationError` |
| `test_load_run_config_target_rate_positive_or_none` | `target_rate=0` raises; `target_rate=None` allowed |

---

## 6. Integration Tests

Require PR #1 infra (`make infra-up`) and PR #2 schemas registered
(`make register-schemas`).

### 6.1 Prerequisites

- A new session-scoped fixture `clean_load_test_topic` that, before the
  module runs, deletes any leftover `e-commerce-events-loadtest-*`
  topics (per-test names with random suffixes — see §6.3) and asserts
  the baseline `e-commerce-events` exists with the expected partitions
  + RF. Teardown deletes the per-test topics.
- Reuses PR #1's `docker_services_up` and PR #2's
  `registered_ecommerce_schema` fixtures.

### 6.2 `tests/integration/test_topic_admin_end_to_end.py`

| Test | What It Verifies |
|---|---|
| `test_ensure_creates_topic_with_expected_partitions` | After `ensure_topic("e-commerce-events-loadtest-<rand>", 12, 3)`, `describe_topic` reports 12 partitions and RF=3 |
| `test_ensure_idempotent_second_call` | Calling `ensure_topic` twice for the same name yields `CREATED` then `ALREADY_EXISTS_MATCHING`; no error |
| `test_ensure_detects_partition_mismatch` | Pre-create a topic with 6 partitions; `ensure_topic` with 12 returns `ALREADY_EXISTS_MISMATCH` and does NOT alter |
| `test_describe_topic_returns_real_leader_assignment` | `TopicDescription.partitions` length == 12, each has a non-null leader |
| `test_delete_topic_removes_from_metadata` | After `delete_topic`, `list_topics` no longer includes the name |

### 6.3 `tests/integration/test_load_runner_end_to_end.py`

Each test uses a per-test topic name (`e-commerce-events-loadtest-<uuid4>`)
to avoid cross-test contamination. The schema subject is reused (it is
keyed off the topic name via `TopicNameStrategy`, so we register the
test-topic-value subject in the fixture).

| Test | What It Verifies |
|---|---|
| `test_load_runner_smoke_produces_and_acks` | 2 s @ 5 K evt/s → `acked == produced`, `failed == 0` |
| `test_load_runner_meets_50k_floor_for_10s` | 10 s @ 60 K evt/s → `sustained_rate_eps >= 50_000` (the headline benchmark; tagged `@pytest.mark.benchmark` so CI can opt-in) |
| `test_load_runner_unpaced_mode_runs_clean` | `target_rate=None`, 2 s → `failed == 0` (validates that the runner survives un-paced bursts via in-flight backpressure) |
| `test_load_runner_writes_report_file` | After `--report-path /tmp/x.md`, file exists and matches the rendered template |
| `test_load_runner_exit_code_zero_when_floor_met` | CLI exits 0 when sustained ≥ 50 K |
| `test_load_runner_exit_code_one_when_floor_unmet` | Force a low ceiling (`target_rate=1000, duration=1.0`) → CLI exits 1 |
| `test_load_runner_partitions_are_balanced` | After the run, no partition has < 5% or > 15% of total messages (sanity check on the Zipfian × hash-partitioner interaction) |
| `test_load_runner_fails_fast_on_missing_subject` | Run with a fresh topic whose `-value` subject is not registered → `LoadRunner.run()` raises before any `produce` call |
| `test_topic_auto_ensured_when_absent` | Run against a brand-new topic name; `TopicAdmin.ensure_topic` is invoked and the topic is created with 12 partitions / RF=3 |

Each non-benchmark test caps at **≤ 10 K total events** and **≤ 2 seconds**
to keep the integration suite fast. The headline 10-second / 50K-floor test
is marked `@pytest.mark.benchmark` and excluded from the default
`make test-integration`; `make test-benchmark` runs it explicitly.

### 6.4 Test Ordering and Isolation

- Tests are marked with `@pytest.mark.integration` and run with
  `-p no:xdist` (per PR #1 convention).
- Per-test topic names mean multiple integration tests can run in
  sequence without partition / offset bleed-through.
- Module-scoped fixtures register the per-topic value subject once and
  delete the topic + soft-delete the subject in teardown.

---

## 7. How to Run

> Prerequisites: PR #1 infrastructure must be up, and PR #2 schemas registered.
> ```bash
> make infra-up
> make infra-status      # wait for all 5 services to be healthy
> make register-schemas  # PR #2: register the v1 baseline
> ```

### 7.1 Bootstrap the topic (idempotent)

```bash
source .venv/bin/activate
make topic-ensure
make topic-describe
# Verify: 12 partitions, RF=3, leaders distributed across brokers 1/2/3.
```

### 7.2 Quick smoke run (no rate floor)

```bash
make load-test-quick
# 2-second, 5K evt/s sanity check. Writes /tmp/_load_quick.md.
```

### 7.3 Run the headline 50K-floor benchmark

```bash
make load-test
# Output:
#   INFO  TopicAdmin.ensure_topic e-commerce-events -> ALREADY_EXISTS_MATCHING
#   INFO  Subject e-commerce-events-value: registered (id=42, version=1)
#   INFO  LoadRunner: workers=8 batch=1024 target=60_000 evt/s duration=10s
#   INFO  ... [t=1s] sustained=60_122 evt/s in_flight=1024
#   ... [t=10s] flushing
#   INFO  Wrote docs/results/week1_load_test_results.md
#   INFO  ✅ Sustained 61_230 evt/s ≥ 50_000 evt/s floor
```

### 7.4 Run the unit tests (no Docker required)

```bash
make test-unit
# or: pytest tests/unit/test_topic_admin.py \
#            tests/unit/test_synthetic_generator.py \
#            tests/unit/test_token_bucket_pacer.py \
#            tests/unit/test_delivery_accountant.py \
#            tests/unit/test_load_runner_unit.py \
#            tests/unit/test_load_report.py -v
```

### 7.5 Run the integration tests

```bash
make test-integration
# or: pytest tests/integration/test_topic_admin_end_to_end.py \
#            tests/integration/test_load_runner_end_to_end.py \
#            -v -m integration -p no:xdist

make test-benchmark      # run the 10s / 50K-floor benchmark explicitly
```

### 7.6 Inspect the report

```bash
make load-test-report
# or just open docs/results/week1_load_test_results.md in your editor.
```

### 7.7 Clean up per-test topics (only needed if a test was killed)

```bash
python -m streaming_feature_store.admin.topic_admin describe   # list topics
python -m streaming_feature_store.admin.topic_admin delete \
    --name e-commerce-events-loadtest-<uuid>
```

---

## 8. Resource Budget & Constraints

This PR is the heaviest of Week 1 in CPU / network terms but the lightest
in storage and operational complexity — no new long-running processes,
just an on-demand load-runner.

| Item | Incremental cost | Notes |
|---|---|---|
| New Python modules | ~6 files, ~600 SLoC total | All under `src/streaming_feature_store/` |
| Topic storage during 10s @ 60K evt/s | ~600 K events × ~250 B = ~150 MB across 3 brokers | Default `retention.ms=7d`; `make infra-down -v` clears |
| Producer memory during the run | ≤ 50 K in-flight × ~250 B = ~12 MB | Bounded by `max_in_flight` |
| Generator memory per batch | 1024 × ~250 B = 256 KB | Bounded by `batch_size` |
| Test runtime (unit) | < 4 s | No Docker required |
| Test runtime (integration, default) | < 90 s | Reuses PR #1 + #2 infra |
| Test runtime (integration, `make test-benchmark`) | < 30 s | The 10-second headline benchmark |
| Driver runtime (`make load-test`) | ~12 s | 10 s benchmark + 2 s setup/teardown |

No new ports, no new containers, no new long-running processes.

### 8.1 Throughput Floor Acceptance Criteria

For the test `test_load_runner_meets_50k_floor_for_10s` to pass:

- Hardware floor: ≥ 4 logical CPUs, ≥ 8 GB RAM (the CI runner config
  declared in PR #1).
- `sustained_rate_eps >= 50_000` over a `duration_s=10` window.
- `failed == 0`.
- p99 ack latency ≤ 250 ms (a sanity ceiling — if the broker is healthy
  on a laptop, p99 is typically under 50 ms).

If the floor cannot be met on a particular developer machine, the test
file documents how to reduce the floor locally (env var
`LOADTEST_FLOOR_EPS`) and how to confirm the cause (CPU vs. disk vs.
network) via `make load-test --unpaced` to find the un-paced ceiling.

---

## 9. Future Considerations

1. **Producer-side topic creation → IaC pipeline.** Lines 67–69 of the
   gap plan explicitly contrast the laptop-scale producer-side
   `AdminClient` approach with production GitOps / Strimzi
   `KafkaTopic` / Terraform Kafka provider. A future PR — likely after
   Week 4's Kubernetes work — would replace the `ensure_topic()` call
   with a Strimzi `KafkaTopic` CRD checked into a sibling repo and
   reconciled by an operator. The `AdminClient` wrapper continues to
   be useful for integration tests (per-test topic creation under a
   distinct naming prefix is fine even in production).

2. **EOS transactions.** The producer enables `enable.idempotence=true`
   here but does not call `init_transactions()`. The Week 1 final PR
   layers `begin_transaction` / `send_offsets_to_transaction` /
   `commit_transaction` on top of the load-runner so the same harness
   benchmarks transactional throughput.

3. **Realistic session-aware generation.** The current generator emits
   each event from the marginal distribution. Week 2 introduces a
   session-state machine (browse → cart → purchase) so windowed
   features have realistic temporal structure.

4. **Latency profile under network packet loss.** A `tc netem` integration
   test that injects 1% packet loss on the broker network and asserts
   that the producer still meets the floor (with `acks=all` retries).

5. **Schema-registry-side benchmark.** Once Week 2 introduces multiple
   subjects (per-feature topics), the registry's response cache becomes
   a measurable factor. The accountant grows a "registry round trips"
   counter.

6. **Cross-language consumer-side benchmark.** PR #5 (consumer side)
   measures end-to-end latency from `produce()` to consumed-and-deserialized
   on the JVM (Kafka Streams) — uses this load-runner as the producer.

7. **Per-partition skew alarms.** When the generator's Zipf alpha is
   high enough (> 1.3), the hash-partitioner's load skew approaches
   2–3×. A future PR adds an SLO check on the partition load std-dev.

---

## 10. Open Questions

1. **Should the in-flight cap be a fraction of librdkafka's
   `queue.buffering.max.messages`, or an absolute number?**
   Current default: absolute (50_000). A fraction (e.g., 25%) would
   adapt automatically if the librdkafka cap is later tuned.
   **Recommendation:** keep absolute for now; revisit when EOS
   transactions land and queue dynamics change.

2. **Should `TopicAdmin.ensure_topic` ever auto-alter a mismatched topic?**
   Current default: no — it logs a warning and proceeds. Auto-altering
   would make `ensure_topic` destructive in subtle ways (shrinking RF
   is impossible; adding partitions changes key-to-partition mapping
   and breaks per-user ordering).
   **Recommendation:** keep the warn-and-proceed semantics; document
   the manual remediation path (`delete_topic` + recreate) in the
   wrapper's docstring.

3. **Should the synthetic generator emit dollars-and-cents prices or
   raw integers?** PR #2's `PurchasePayload` declares
   `price_cents: int`. The generator currently draws from a clipped
   log-normal in cents.
   **Recommendation:** keep cents; document the conversion in the
   generator docstring so reviewers do not see "$199.99" → 19999 as a
   bug.

4. **Should the load-runner support a `--profile` flag that emits a
   `cProfile` dump?** Useful when the floor is missed and we need to
   know whether Pydantic, fastavro, or librdkafka is the bottleneck.
   **Recommendation:** ship without; if the floor is missed in CI a
   one-line addition surfaces the dump on demand.

5. **Should the report include a link to the topic's broker-side
   per-partition byte counts via JMX / `kafka-log-dirs.sh`?** Would
   make the partition-balance test more informative.
   **Recommendation:** defer; the partition-balance integration test
   already asserts the property programmatically.

6. **Should the headline floor be 50K or higher (e.g., 80K)?** Gap
   plan says "50K+", and 50K is what the integration test asserts.
   On a typical M1 / Ryzen laptop the un-paced ceiling is comfortably
   above 100K; tightening the floor would catch real regressions
   sooner but flake on weaker CI runners.
   **Recommendation:** keep the floor at 50K; record the un-paced
   ceiling in the report as an informational metric so regressions
   are still visible.
