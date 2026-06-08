# Design Doc: Sliding-Window Feature Computation (Plain Python Kafka Consumer, In-Memory Windowing)

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 2 — Validation & Feature Computation
**Scope:** First sub-bullet of the Week 2 feature-computation step in
[`gap_project_plan.md`](gap_project_plan.md) (line 81) — *"Sliding window
aggregations (clicks in last 5 minutes, purchase count in last 24 hours) —
the canonical streaming interview topic, invest the most time here."*
**Supersedes:** [`week2_02_sliding_window_features.md`](week2_02_sliding_window_features.md)
(the PyFlink design — kept as an interview artifact, not implemented).
**Author:** Auto-generated design document
**Date:** 2026-06-07

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

This PR computes the **same 12 sliding-window features across the same three
resolutions** as the superseded PyFlink design, but with a **plain Python
Kafka consumer group that maintains the sliding windows in process memory** —
no Flink cluster, no JVM, no Apache Beam portability bridge, no JARs.

| Family | Window | Slide | Per-emission features |
|---|---|---|---|
| **5 m** | 5 minutes | 1 minute | `clicks_5m`, `page_views_5m`, `purchases_5m`, `revenue_5m` |
| **1 h** | 1 hour | 5 minutes | `clicks_1h`, `page_views_1h`, `purchases_1h`, `revenue_1h`, `distinct_products_1h` |
| **24 h** | 24 hours | 1 hour | `purchases_24h`, `revenue_24h`, `avg_purchase_amount_24h`, `distinct_products_24h` |

### Why this exists (the pivot)

The PyFlink implementation
([`week2_02_sliding_window_features.md`](week2_02_sliding_window_features.md))
did not pan out at laptop scale. The operational surface — a JobManager + two
TaskManagers, the Python↔JVM crossing routed through Apache Beam's portability
runner, hand-managed connector JARs ([`flink/jars/`](../../flink/jars/)), and a
GIL-bound UDF hot path — was disproportionate to a ~200 evt/s synthetic feeder.
The first smoke run never emitted a window: it failed inside a Beam SDK worker
on a Redis-hostname resolution error. §2.1 records the decision in full.

Crucially, **none of the downstream Phase 1 work depends on the engine** —
Week 3 (online serving) reads the Redis hash, Week 4 (online/offline
consistency) compares the `sliding-features` Kafka topic against a DuckDB batch
recompute, and Week 5 (freshness monitoring) watches feature update lag. All
three are satisfied identically by this consumer. The *only* thing that
changes is what computes the features.

### What is preserved unchanged

The engine-agnostic core carries over verbatim:

- **`SlidingFeatureRecord`, `WindowResolution`, `SlidingAccumulator`** Pydantic
  / dataclass models ([`models.py`](../../src/streaming_feature_store/flink/sliding/models.py)).
- **The aggregator algebra** — `add` / `merge` / `get_result` per resolution
  ([`aggregators.py`](../../src/streaming_feature_store/flink/sliding/aggregators.py)).
  These are pure Python with no PyFlink dependency (the `AggregateFunction`
  base already shims to `object` when PyFlink is absent).
- **The Avro schema** `sliding_feature_record.avsc`, registered under
  `sliding-features-value` with `BACKWARD` compatibility.
- **The sink contract** (§2.8), **idempotency key** (§2.9), **sparsity /
  zero-fill read contract** (§2.7), and **topic configuration** (§2.11) — all
  identical to the PyFlink design.

### What changes

- **The windowing runtime.** Flink's `SlidingEventTimeWindows` + pane
  pre-aggregation + watermark machine + `allowedLateness` are reimplemented as
  a small, explicit in-memory pane ring-buffer (§2.3) driven by a hand-rolled
  event-time watermark (§2.4).
- **Fault tolerance.** RocksDB checkpoints are replaced by *at-least-once
  consumption + idempotent writes + a bounded cold-start warm-up* (§2.10).
- **Scaling.** One Flink job with operator parallelism becomes a **consumer
  group of OS processes** (§2.11), reusing the Week 1 GIL finding.

### Deliverables

- `src/streaming_feature_store/sliding/__init__.py` — new engine-neutral
  package (the PyFlink-free core moves here; see §2.13).
- `src/streaming_feature_store/sliding/models.py` — moved from
  `flink/sliding/`, unchanged except dropping the Flink-only fields from the
  config (new `SlidingConsumerConfig`, §3.3).
- `src/streaming_feature_store/sliding/aggregators.py` — moved from
  `flink/sliding/`; the `AggregateFunction` shim is removed (plain classes).
- `src/streaming_feature_store/sliding/panes.py` —
  `PanedSlidingWindow` (per `(user_id, resolution)` ring buffer of pane
  accumulators), `SlidingWindowManager` (owns all users' state for this
  process).
- `src/streaming_feature_store/sliding/watermark.py` —
  `WatermarkTracker` (bounded out-of-orderness + idleness fallback).
- `src/streaming_feature_store/sliding/sinks.py` —
  `RedisHashSink`, `KafkaSlidingFeaturesSink`, `KafkaLateEventsSink` (plain
  `redis-py` + `confluent-kafka`, no Flink `SinkFunction`).
- `src/streaming_feature_store/sliding/consumer.py` —
  `SlidingFeaturesConsumer`: the consume → window → emit main loop.
- `scripts/run_sliding_features_consumer.py` — CLI entry point.
- `docs/results/week2_sliding_features_results.md` — generated smoke-run
  report (per-resolution emission counts, per-feature p50/p95/p99, watermark
  lag, per-process active-user counts, sparsity audit).
- Unit + integration tests (§5, §6).
- `Makefile` targets: `sliding-run`, `sliding-run-group`, `sliding-report`,
  `sliding-redis-show` (the `flink-up` / `sliding-submit` targets are retired).

### Out of scope (unchanged from the PyFlink design)

Session-window features (gap plan line 82), EOS transactional wrapping
(Week 2 PR #3), continuous per-event emission, HyperLogLog `distinct_products`,
and cross-resolution merge — all remain deferred for the same reasons recorded
in the superseded doc §1.

---

## 2. Critical Design Decisions

### 2.1 Plain Python Consumer over Flink (the pivot)

**Decision:** Compute sliding-window features in a **plain Python process**
that uses `confluent_kafka.Consumer` to read `validated-events` and maintains
window state in ordinary Python dictionaries. Reject PyFlink for this project.

**Rationale:**

- **The operational weight was not earned at this scale.** PyFlink requires a
  JobManager + TaskManager cluster, ships UDF execution through the Apache Beam
  portability layer (a second process boundary on top of the JVM one), and
  needs manually-pinned connector JARs. The synthetic feeder produces ~200
  evt/s; a single Python process consumes that with >10× headroom (§8).
- **The failure modes were opaque.** The first smoke run died inside a Beam SDK
  worker (`apache_beam.runners.worker.sdk_worker`) on a Redis DNS error — a
  one-line networking issue buried under ten frames of JVM/Beam plumbing. A
  plain consumer surfaces the same error as a three-line traceback.
- **PyFlink's only real wins don't apply here.** Managed checkpoint/restore,
  cross-cluster state, and JVM-speed UDFs matter when per-key state exceeds a
  process's memory, when you need exactly-once with large state, or at
  100k+ evt/s. None hold at laptop scale (§8 shows total state ~tens of MB).
- **The interview narrative is *stronger*, not weaker.** "I prototyped on
  PyFlink, measured that its operational cost wasn't justified at my scale, and
  moved to a custom consumer while keeping identical windowing semantics — and
  I can state exactly what would force me back to Flink" is a senior-level
  tradeoff story. The superseded doc is retained precisely so the Flink
  mechanics (panes, watermarks, lateness) are still demonstrably understood.
- **What would reverse this decision** (recorded for the §10 / system-design
  write-up): per-user state larger than process memory; joins across keys or
  streams; sub-second freshness SLAs; exactly-once with large state; or an
  operational requirement for managed checkpoint/restore and rescaling.

### 2.2 Partition-by-`user_id` → Per-Process State Locality (the enabling property)

**Decision:** Rely on the existing `validated-events` partitioning (keyed by
`user_id`, 12 partitions, inherited from
[`week2_01`](week2_01_validation_layer_and_dlq.md)) so that **every event for a
given user is owned by exactly one consumer process**. Per-user window state is
therefore process-local; no shared/external state store is needed.

**Rationale:**

- **This is the single property that makes in-memory windowing correct.**
  Kafka guarantees all records with the same key land on the same partition,
  and the consumer-group protocol assigns each partition to exactly one member.
  So a user's entire event history (within the consumer group's lifetime) flows
  to one process — the same locality Flink achieves with `keyBy(user_id)`, but
  given to us for free by the partitioner.
- **It is the symmetric extension of the Week 1 finding.** Week 1 established
  "scale by processes, one per partition-subset" for the sink and latency
  consumers ([`gap_project_plan.md`](gap_project_plan.md) line 72). The feature
  consumer is the same shape: a consumer group of ≤12 processes, each owning a
  disjoint partition subset and thus a disjoint user subset.
- **Rebalances move state ownership, not correctness.** When a partition is
  reassigned, its users' in-memory state is dropped on the losing process and
  cold-started on the gaining process (§2.10, §2.12). The partitioner still
  guarantees one-owner-at-a-time, so no two processes ever double-count a user.

### 2.3 In-Memory Pane Ring-Buffer (replicating Flink pane pre-aggregation)

**Decision:** For each `(user_id, resolution)` maintain a dict
`pane_index -> SlidingAccumulator`, where
`pane_index = event_time_ms // slide_ms`. A window ending at boundary `E`
aggregates the `panes_per_window` panes whose index falls in
`[(E - size)/slide, E/slide)`, merged with the **existing
`SlidingWindowAggregator.merge`**. Panes older than `E - size - lateness` are
garbage-collected.

```
 pane_index:   k-4    k-3    k-2    k-1     k
              │ acc │ acc │ acc │ acc │ acc │
              └─────┴─────┴─────┴─────┴─────┘
                  ◀──── window ending at E=(k+1)·slide ────▶
              merge(5 panes) → get_result() → SlidingFeatureRecord
```

**Rationale:**

- **State is O(panes), not O(events) — same as Flink.** Each event does one
  `aggregator.add()` (O(1)) into its pane accumulator. The 24 h/1 h window
  holds 24 panes per user regardless of event volume. This is the exact
  pane-based pre-aggregation property the PyFlink doc §2.3 articulated; it is
  reproduced here in ~40 lines of explicit Python rather than delegated to
  Flink's window assigner.
- **The aggregator code is reused verbatim.** `add` / `merge` / `get_result`
  are engine-agnostic. The consumer calls the *same* `FiveMinuteAggregator`,
  `OneHourAggregator`, `TwentyFourHourAggregator` instances. Only the *driver*
  (which panes to merge, when to emit) is new.
- **GC is explicit and cheap.** On each watermark advance, drop panes with
  `index < (watermark - size - lateness) / slide`. A `dict` deletion per
  aged-out pane; bounded by `panes_per_window + lateness/slide` live panes per
  user per resolution.

### 2.4 Event-Time Watermark in Plain Python

**Decision:** Track a single per-process watermark per resolution:
`watermark_ms = max_event_time_seen_ms - out_of_orderness_ms` (default
`out_of_orderness = 5 s`, matching the PyFlink design). When no event arrives
for `idleness_seconds` (default 30 s) of wall-clock time, advance the watermark
using a wall-clock fallback so emissions do not stall on an idle partition
assignment.

**Rationale:**

- **Event-time, not processing-time — and this matters for Week 4.** The Week 4
  online/offline consistency comparison recomputes the same features in DuckDB
  from `raw_events` using *event* timestamps. If the online path windowed by
  processing time, divergence would be dominated by uninteresting clock skew.
  Event-time windowing keeps the divergence story focused on the *interesting*
  sources the plan calls out (late events, window boundaries, MP interleaving —
  [`gap_project_plan.md`](gap_project_plan.md) line 103).
- **`max_event_time - skew` is the standard bounded-out-of-orderness rule.**
  It is exactly what `WatermarkStrategy.forBoundedOutOfOrderness` computes; we
  just compute it ourselves. 5 s is comfortably above measured producer→consumer
  skew (Week 1 latency harness: tens of ms).
- **Idleness fallback prevents pathological stalls.** With a single-process
  assignment covering a quiet user subset, the max-event-time can freeze. The
  wall-clock fallback (advance watermark toward `now - skew` after 30 s idle)
  keeps the 1-minute slide cadence smooth, mirroring Flink's `withIdleness`.
- **Watermark is per process, over its assigned partitions.** Each process
  computes its own watermark from the events it sees. Because users are
  partition-local (§2.2), a user's window is governed entirely by its owning
  process's watermark — no cross-process watermark merge is needed.

### 2.5 Slide-Tick Emission, Watermark-Driven

**Decision:** After each consumed batch (and on a 1 s periodic tick when idle),
for each resolution advance an emission cursor: while
`watermark_ms >= next_window_end`, fire that window for every active user that
has a non-empty contributing pane, then advance `next_window_end += slide_ms`.

**Rationale:**

- **One emission per slide tick per active user — same cadence as Flink.** The
  5 m/1 m window emits once per minute of event-time, the 1 h/5 m once per
  5 min, the 24 h/1 h once per hour. The cursor is the explicit analogue of
  Flink firing a window when the watermark crosses its end.
- **Iterating active users at the slide cadence is cheap.** ~5 k active users ×
  once/minute for the 5 m resolution ≈ 80 emissions/s — trivially within a
  single process's budget and Kafka produce capacity (§8).
- **Sparsity contract preserved (§2.7).** A user with no events in any
  contributing pane is simply absent from the active-user iteration for that
  window → no record emitted. Zero-fill remains a downstream read-side concern.

### 2.6 Allowed Lateness: 30 s, Re-Emission Semantics

**Decision:** Retain panes for `allowed_lateness = 30 s` past their window's
end. An event whose `pane_index` belongs to an already-emitted-but-still-retained
window is added to its pane and that window is re-fired, emitting a new
`SlidingFeatureRecord` for the same `(user_id, window_end_ms, resolution)` with
`emission_seq` incremented. An event older than `watermark - lateness` is routed
to the `sliding-features-late` topic carrying the **raw event** (forensic).

**Rationale:** Identical semantics and rationale to the PyFlink design §2.6 —
re-emission (not silent update), `emission_seq` discriminates re-fires, and the
side-output preserves the raw event for the Week 4 consistency audit. The only
difference is mechanical: lateness is enforced by *delaying pane GC*, and the
re-fire is an explicit re-merge of the retained panes.

### 2.7 Sparsity & the Downstream-Default-Zero Read Contract

**Unchanged from the PyFlink design §2.7.** When a user has no events in a slide
interval, no record is emitted; the Redis read path treats a missing field as
zero, and per-resolution TTLs (1.5 × window size = 450 s / 5400 s / 129 600 s)
expire stale fields. Carried over verbatim — the contract is a property of the
sink and the read adapter, not the compute engine.

### 2.8 Sink Contract: Redis Hash + Kafka Topic

**Unchanged from the PyFlink design §2.8.** Each emission is sunk twice:

1. **Redis** — one pipelined `HSET feat:user:{user_id} <field>_<res> <value> …`
   plus a per-resolution `EXPIRE` (latest-wins, idempotent).
2. **Kafka** — produce to `sliding-features` with key
   `f"{user_id}:{resolution.value}"`, Avro-serialized value.

The `RedisHashSink` is reimplemented over plain `redis-py` (a `Pipeline` per
emission) instead of a Flink `SinkFunction`, and the Kafka sink over
`confluent_kafka.Producer` — but the *contract* (field names, keying, TTL `XX`
semantics) is byte-for-byte the same. `SlidingFeatureRecord.redis_field_updates()`
and `.kafka_key()` already encode it.

### 2.9 Idempotency: `(user_id, window_end_ms, resolution, emission_seq)`

**Unchanged from the PyFlink design §2.9.** Redis writes are latest-wins; Kafka
emits append with the per-(user, resolution) key so downstream can dedupe on the
4-tuple. The natural key is reproduced by `SlidingFeatureRecord.idempotency_key()`.
This is what makes the cold-start re-emission in §2.10 safe.

### 2.10 Fault Tolerance Without Checkpoints

**Decision:** Replace RocksDB checkpointing with **at-least-once consumption +
idempotent writes + a bounded cold-start warm-up**:

- **Offset commit:** commit consumed offsets *after* the corresponding events
  have been folded into pane state (at-least-once). On crash, replay from the
  last committed offset re-applies those events.
- **Cold-start warm-up:** in-memory pane state is lost on restart/rebalance. On
  partition assignment, **seek back `window_size` of event-time** for the 5 m
  and 1 h resolutions to rebuild their panes before trusting their emissions;
  the 24 h resolution accepts a bounded warm-up (it does **not** seek back 24 h
  on every restart — too heavy) and is flagged as "warming" until a full window
  of wall-clock has elapsed.
- **Idempotency absorbs the replay.** Because writes are latest-wins (Redis) and
  dedupe-able on the 4-tuple (Kafka, §2.9), re-emitting a window after restart
  produces no downstream corruption — at worst a duplicate Kafka record with an
  equal-or-higher `emission_seq`.

**Rationale:**

- **This is the honest "cruder" tradeoff, and it is acceptable here.** Flink's
  checkpointing gives exactly-once state and instant warm restart; we give up
  both. The cost is a warm-up window of under-counted features after a restart
  (bounded to `window_size`) and possible duplicate emissions — both absorbed
  by the idempotent sink contract. At laptop scale with infrequent restarts,
  this is a sound trade.
- **The 24 h warm-up cost is the one real wart.** A restart leaves
  `purchases_24h` / `revenue_24h` under-counted for up to 24 h. §9 records the
  mitigation: periodically snapshot per-user accumulators to Redis/Postgres and
  reload on assignment (a poor-man's checkpoint) — deferred because Week 4's
  offline batch is the authoritative 24 h view anyway, and restarts are rare.

### 2.11 Scaling: Consumer Group of Processes (GIL)

**Decision:** Scale by running **N ≤ 12 identical processes** in one Kafka
consumer group (`sliding-features-job`), each auto-assigned a disjoint subset of
the 12 partitions. Default `N = 1` for the laptop smoke run; `make
sliding-run-group` launches `N` via the `--num-workers` count.

**Rationale:**

- **The Week 1 GIL ceiling applies and the fix is the same.** A single Python
  process caps at ~11–14 k evt/s on the GIL
  ([`gap_project_plan.md`](gap_project_plan.md) line 72). The feeder runs at
  ~200 evt/s, so `N = 1` is fine for the demo, but the *design* is a process
  group so the narrative ("found the ceiling, scaled by processes, not
  threads") holds end-to-end.
- **No shared state across processes, by construction (§2.2).** Each process
  owns disjoint users, so there is no cross-process coordination, no shared
  state store, and no lock contention. Horizontal scaling is linear up to the
  partition count.

### 2.12 Rebalance Handling

**Decision:** Register `on_assign` / `on_revoke` callbacks on the consumer. On
**revoke**, flush nothing and drop the revoked partitions' user state (it will
be rebuilt by the new owner). On **assign**, initialize empty state and trigger
the §2.10 cold-start seek-back for the newly assigned partitions.

**Rationale:** Dropping-and-rebuilding is correct because the partitioner
guarantees single ownership (§2.2); the idempotent sink (§2.9) makes the
rebuild's re-emissions harmless. Attempting to *hand off* in-memory state across
processes would reintroduce exactly the distributed-state-coordination problem
that motivates Flink — explicitly out of scope (§2.1).

### 2.13 Reuse of the Engine-Agnostic Core (Module Relocation)

**Decision:** Move `models.py` and `aggregators.py` out of
`src/streaming_feature_store/flink/sliding/` into a new engine-neutral package
`src/streaming_feature_store/sliding/`. Update the (superseded, unrun) Flink
modules to import from the new location, or leave them as dead artifacts. Drop
the PyFlink `AggregateFunction` shim from the relocated `aggregators.py` (the
consumer needs only plain classes).

**Rationale:**

- **The core was already PyFlink-free.** `models.py`'s own docstring states it
  is "import-safe without PyFlink"; `aggregators.py` already shims its base to
  `object`. The relocation is a rename, not a rewrite — the existing unit tests
  for the aggregator algebra and the record model move with it essentially
  unchanged.
- **`flink/` as a package name for the *active* code would be a lie.** Putting
  the live consumer's models under `flink/` would mislead every future reader.
  The neutral `sliding/` package names what the code is.

---

## 3. Architecture

### 3.1 Process Topology

```
┌───────────────────────────────────────────────────────────────────────────┐
│            SlidingFeaturesConsumer  (one per process; group of N ≤ 12)      │
│                                                                             │
│   validated-events  (12 partitions, keyed by user_id, from week2_01)        │
│        │  confluent_kafka.Consumer  (group=sliding-features-job)            │
│        │  assigned: a disjoint subset of the 12 partitions                  │
│        ▼                                                                     │
│   ┌────────────────────────┐                                                │
│   │ Avro deserialize →     │  (re-uses the registry-backed decoder)         │
│   │ EcommerceEvent (Pydantic)                                               │
│   └───────────┬────────────┘                                                │
│               ▼                                                             │
│   ┌────────────────────────┐   WatermarkTracker: max_event_ts − 5 s,        │
│   │ WatermarkTracker.update│   30 s idleness fallback (§2.4)                │
│   └───────────┬────────────┘                                                │
│               ▼                                                             │
│   ┌────────────────────────────────────────────────────────────────────┐  │
│   │ SlidingWindowManager  (dict: user_id → per-resolution pane buffers) │  │
│   │   for res in (5m, 1h, 24h):                                          │  │
│   │       pane_index = event_ts_ms // slide_ms[res]                      │  │
│   │       agg.add(event, panes[user][res][pane_index])    (O(1))         │  │
│   └───────────┬────────────────────────────────────────────────────────┘  │
│               ▼   on each batch / 1 s tick: emit while watermark ≥ next end │
│   ┌────────────────────────────────────────────────────────────────────┐  │
│   │ Emitter:  merge panes → get_result → inject window bounds + seq      │  │
│   └───────────┬───────────────────────────────────┬────────────────────┘  │
│               ▼                                     ▼                       │
│   ┌────────────────────────┐         ┌──────────────────────────────────┐ │
│   │ RedisHashSink           │         │ KafkaSlidingFeaturesSink         │ │
│   │ HSET feat:user:{id} …   │         │ → sliding-features (Avro)        │ │
│   │ EXPIRE … <ttl_res> XX   │         │ key={user_id}:{res}              │ │
│   └────────────────────────┘         └──────────────────────────────────┘ │
│               (very-late events) ─────────────▶ KafkaLateEventsSink         │
│                                                 → sliding-features-late     │
└───────────────────────────────────────────────────────────────────────────┘
```

End-to-end placement is unchanged from the PyFlink design:

```
validator (week2_01) ─▶ validated-events ─▶ SlidingFeaturesConsumer (this PR)
                                              ─▶ Redis (feat:user:*) + sliding-features
```

### 3.2 Module Layout

```
src/streaming_feature_store/sliding/
├── __init__.py
├── models.py        # MOVED from flink/sliding/ — SlidingFeatureRecord,
│                    # WindowResolution, SlidingAccumulator (+ new
│                    # SlidingConsumerConfig; SlidingJobConfig retired)
├── aggregators.py   # MOVED — FiveMinute/OneHour/TwentyFourHour aggregators
│                    # (AggregateFunction shim removed; plain classes)
├── panes.py         # PanedSlidingWindow, SlidingWindowManager
├── watermark.py     # WatermarkTracker
├── sinks.py         # RedisHashSink, KafkaSlidingFeaturesSink,
│                    # KafkaLateEventsSink  (redis-py + confluent-kafka)
└── consumer.py      # SlidingFeaturesConsumer (the main loop)

src/streaming_feature_store/schemas/avro/
└── sliding_feature_record.avsc          # unchanged

scripts/
└── run_sliding_features_consumer.py     # CLI

docs/results/
└── week2_sliding_features_results.md    # generated
```

### 3.3 Config Model

`SlidingConsumerConfig` replaces the PyFlink `SlidingJobConfig`, dropping the
Flink-only fields (`checkpoint_interval_ms`, `parallelism`) and adding
consumer-runtime fields. Pydantic validators carry over (distinct topics,
positive TTL factor, lateness below the smallest window).

```python
class SlidingConsumerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bootstrap: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    registry_url: str = "http://schema-registry:8081"
    source_topic: str = "validated-events"
    sink_topic: str = "sliding-features"
    late_sink_topic: str = "sliding-features-late"
    consumer_group: str = "sliding-features-job"

    out_of_orderness_seconds: int = 5      # watermark skew budget (§2.4)
    idleness_seconds: int = 30             # watermark idleness fallback (§2.4)
    allowed_lateness_seconds: int = 30     # pane-retention budget (§2.6)
    emit_tick_seconds: float = 1.0         # periodic emission tick when idle
    poll_timeout_seconds: float = 1.0      # consumer poll timeout

    num_workers: int = 1                   # processes in the group (§2.11)
    warmup_seek_back: bool = True          # cold-start seek-back (§2.10)

    redis_host: str = "redis"
    redis_port: int = 6379
    ttl_factor: float = 1.5
```

---

## 4. Detailed Implementation

### 4.1 Pane Buffer & Window Manager

```python
class PanedSlidingWindow:
    """Per-(user, resolution) ring buffer of pane accumulators.

    Holds ``pane_index -> SlidingAccumulator`` and knows how to merge the
    panes composing the window ending at a given boundary, and to GC panes
    that no live window (incl. the lateness tail) can include.
    """

    def __init__(self, resolution: WindowResolution,
                 aggregator: SlidingWindowAggregator) -> None:
        self._resolution = resolution
        self._aggregator = aggregator
        self._slide_ms = resolution.slide_seconds * 1000
        self._size_ms = resolution.window_size_seconds * 1000
        self._panes: dict[int, SlidingAccumulator] = {}

    def add(self, event: EcommerceEvent, event_ts_ms: int) -> None:
        pane_index = event_ts_ms // self._slide_ms
        acc = self._panes.get(pane_index)
        if acc is None:
            acc = self._aggregator.create_accumulator()
            self._panes[pane_index] = acc
        self._aggregator.add(event, acc)

    def window_record(self, window_end_ms: int) -> SlidingFeatureRecord | None:
        """Merge the panes in [end-size, end); None if all are empty."""
        first = (window_end_ms - self._size_ms) // self._slide_ms
        last = window_end_ms // self._slide_ms        # exclusive
        merged = self._aggregator.create_accumulator()
        seen = False
        for idx in range(first, last):
            pane = self._panes.get(idx)
            if pane is not None:
                merged = self._aggregator.merge(merged, pane)
                seen = True
        if not seen:
            return None
        record = self._aggregator.get_result(merged)
        record.window_start_ms = window_end_ms - self._size_ms
        record.window_end_ms = window_end_ms
        return record

    def gc(self, watermark_ms: int, lateness_ms: int) -> None:
        cutoff = (watermark_ms - self._size_ms - lateness_ms) // self._slide_ms
        for idx in [i for i in self._panes if i < cutoff]:
            del self._panes[idx]
```

`SlidingWindowManager` owns `dict[user_id, dict[WindowResolution,
PanedSlidingWindow]]` plus per-`(user, resolution, window_end)` `emission_seq`
counters, and drives `add` (route an event to all three resolutions) and the
emission cursor (§4.3).

### 4.2 Watermark Tracker

```python
class WatermarkTracker:
    """Bounded out-of-orderness watermark with idleness fallback (§2.4)."""

    def __init__(self, out_of_orderness_ms: int, idleness_ms: int) -> None:
        self._skew = out_of_orderness_ms
        self._idleness = idleness_ms
        self._max_event_ts: int | None = None
        self._last_event_wallclock = time.monotonic()

    def observe(self, event_ts_ms: int) -> None:
        if self._max_event_ts is None or event_ts_ms > self._max_event_ts:
            self._max_event_ts = event_ts_ms
        self._last_event_wallclock = time.monotonic()

    def watermark_ms(self, now_wallclock_ms: int) -> int | None:
        if self._max_event_ts is None:
            return None
        wm = self._max_event_ts - self._skew
        idle_for = (time.monotonic() - self._last_event_wallclock) * 1000
        if idle_for >= self._idleness:                 # idleness fallback
            wm = max(wm, now_wallclock_ms - self._skew)
        return wm
```

### 4.3 Emission Cursor

```python
def emit_due_windows(self, watermark_ms: int) -> Iterator[SlidingFeatureRecord]:
    """Fire every window whose end has been crossed by the watermark."""
    for resolution, cursor in self._cursors.items():
        slide_ms = resolution.slide_seconds * 1000
        if cursor.next_end_ms is None:                 # initialise on first wm
            cursor.next_end_ms = (watermark_ms // slide_ms) * slide_ms
        while watermark_ms >= cursor.next_end_ms:
            end = cursor.next_end_ms
            for user_id, windows in self._users.items():
                record = windows[resolution].window_record(end)
                if record is None:                     # sparsity (§2.7)
                    continue
                record.emission_seq = self._next_seq(user_id, resolution, end)
                yield record
            cursor.next_end_ms += slide_ms
    self._gc(watermark_ms)
```

Late re-fires (§2.6) are handled by `add`: if an event lands in a pane whose
window-end is `< watermark` but `>= watermark - lateness`, the manager records
the `(user, resolution, end)` as dirty and re-emits it with the next
`emission_seq` on the following tick; events older than the lateness tail are
yielded to the late sink instead.

### 4.4 Sinks

```python
class RedisHashSink:
    """HSET feat:user:{id} with resolution-suffixed fields (§2.8).

    One pipelined round trip per emission: the HSETs plus a per-resolution
    EXPIRE (XX, so a longer existing TTL is preserved). First write for a
    fresh key issues a non-XX EXPIRE to establish the TTL.
    """

    def __init__(self, config: SlidingConsumerConfig) -> None:
        self._redis = redis.Redis(host=config.redis_host,
                                  port=config.redis_port)
        self._config = config

    def write(self, record: SlidingFeatureRecord) -> None:
        key = f"feat:user:{record.user_id}"
        fields = record.redis_field_updates()          # reused from models.py
        ttl = self._config.ttl_seconds_for(record.window_resolution)
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=fields)
        pipe.expire(key, ttl)                          # see §10 on XX nuance
        pipe.execute()
```

`KafkaSlidingFeaturesSink` wraps a `confluent_kafka.Producer`, serializing with
the registry-backed Avro serializer and keying on `record.kafka_key()`.
`KafkaLateEventsSink` produces the raw late `EcommerceEvent` to
`sliding-features-late`.

> **Note — the original failure is gone here.** Run *outside* Docker, set
> `--redis-host localhost`; run *inside* the compose network, `--redis-host
> redis`. There is no Beam worker indirection to obscure a misconfiguration.

### 4.5 Main Loop

```python
def run(self) -> None:
    self._consumer.subscribe([self._config.source_topic],
                             on_assign=self._on_assign,
                             on_revoke=self._on_revoke)
    while not self._stopping:
        msg = self._consumer.poll(self._config.poll_timeout_seconds)
        if msg is not None and not msg.error():
            event = self._decode(msg)
            ts_ms = event.event_timestamp_ms
            self._watermark.observe(ts_ms)
            late = self._manager.add(event, ts_ms,
                                     self._watermark.watermark_ms(_now_ms()))
            if late is not None:
                self._late_sink.write_raw(event)
        wm = self._watermark.watermark_ms(_now_ms())
        if wm is not None:
            for record in self._manager.emit_due_windows(wm):
                self._redis_sink.write(record)
                self._kafka_sink.write(record)
        self._consumer.commit(asynchronous=True)       # at-least-once (§2.10)
```

### 4.6 Avro Schema

Unchanged — `sliding_feature_record.avsc` (the PyFlink doc §4.5), registered
under `sliding-features-value` with `BACKWARD` compatibility on first run by the
same registry-bootstrap path the validator uses.

---

## 5. Unit Tests

All `pytest`; pure-Python, no Kafka/Redis. The aggregator-algebra and
record-model tests **carry over from the PyFlink design** (they test the reused
core). New tests cover the windowing runtime.

| Test | Assertion |
|---|---|
| *(carried over)* aggregator count/merge/avg/distinct tests | unchanged — exercise the reused `add`/`merge`/`get_result` |
| *(carried over)* `SlidingFeatureRecord` idempotency-key / redis-field / Avro round-trip | unchanged |
| `test_pane_add_routes_event_to_correct_pane_index` | event at t=130 s with slide=60 s → `pane_index=2` |
| `test_pane_window_record_merges_size_over_slide_panes` | 5 m/1 m: clicks spread across 5 panes → merged `click_count` = sum |
| `test_pane_window_record_none_when_all_panes_empty` | window with no contributing panes → `None` (sparsity) |
| `test_pane_window_record_decays_as_panes_age_out` | clicks only in pane k; query window ending past k+5 slides → `None` |
| `test_pane_gc_drops_panes_below_cutoff` | after watermark advance, panes older than `wm − size − lateness` are removed |
| `test_pane_gc_retains_lateness_tail` | a pane within the lateness tail is **not** GC'd |
| `test_watermark_is_max_event_ts_minus_skew` | observe ts=10_000 ms, skew=5 s → watermark=5_000 ms |
| `test_watermark_none_before_first_event` | no events → `watermark_ms is None` |
| `test_watermark_idleness_fallback_advances` | no events for >idleness → watermark advances toward wall-clock |
| `test_emit_cursor_fires_each_slide_once` | watermark jumps 5 min with slide=1 min → 5 window-ends fired, one each |
| `test_emit_cursor_does_not_refire_emitted_window` | second call with same watermark → no duplicate fires |
| `test_emit_skips_users_with_no_panes` | active-user map includes an empty user → no record for them |
| `test_emission_seq_zero_on_first_fire` | first fire of a window → `emission_seq=0` |
| `test_late_event_within_lateness_refires_with_higher_seq` | late event into retained window → re-fire with `emission_seq=1`, count increased |
| `test_very_late_event_routed_to_late_sink` | event older than `wm − lateness` → returned as late, not added to a pane |
| `test_manager_routes_event_to_all_three_resolutions` | one click → present in 5 m and 1 h panes, absent from 24 h (clicks excluded there) |
| `test_consumer_config_rejects_equal_topics` | `source==sink` → `ValidationError` |
| `test_consumer_config_rejects_lateness_above_smallest_window` | `allowed_lateness>=300` → `ValidationError` |
| `test_redis_sink_pipelines_hset_and_expire` | mock Redis → single pipeline with HSET + EXPIRE |
| `test_redis_sink_skips_null_fields` | record with `click_count=None` → no `clicks_*` field written |
| `test_kafka_sink_key_format` | mock producer → key `{user_id}:{resolution.value}` |
| `test_on_revoke_drops_partition_user_state` | revoke callback → users on revoked partitions cleared |

Coverage target: **100% line + branch** for
`src/streaming_feature_store/sliding/`.

## 6. Integration Tests

Real Kafka + real Redis (via `make infra-up`); **no Flink cluster needed**.
Marked `@pytest.mark.integration`; skipped if `docker compose ps` reports no
running services.

| Test | Setup → Assertion |
|---|---|
| `test_sliding_features_topic_auto_created` | start consumer → `sliding-features` (12 part, RF 3, 7 d, not compacted) + `sliding-features-late` (3 part, 30 d) |
| `test_sliding_features_avro_schema_registered_backward` | `sliding-features-value` registered `BACKWARD` |
| `test_5m_window_emits_per_minute` | 1 click for `u1`, drive event-time 5 min → ≥5 records on key `u1:5m`, one per minute |
| `test_5m_clicks_count_matches_input` | 7 clicks in one pane → `click_count=7` |
| `test_5m_clicks_decay_as_window_slides` | 7 clicks at t=0, advance to t=6 min → `click_count=0` |
| `test_1h_includes_distinct_products` | page-views on 3 distinct products in 1 h → `distinct_products=3` |
| `test_24h_excludes_clicks` | 24 h record `click_count is None` |
| `test_three_resolutions_emit_independently` | 65 min traffic → keys `u1:5m`, `u1:1h`, `u1:24h` at 1 min / 5 min / 1 h cadence |
| `test_late_event_re_emits_with_higher_seq` | late click within 30 s → 2 records for the window, second `emission_seq=1` |
| `test_very_late_event_goes_to_late_topic` | click 60 s past close → 0 new `sliding-features`, 1 on `sliding-features-late` |
| `test_redis_hash_carries_all_three_resolutions` | after 65 min → `HGETALL feat:user:u1` has `_5m`/`_1h`/`_24h` fields |
| `test_redis_ttl_per_resolution` | TTL of `feat:user:u1` ≈ longest resolution's TTL |
| `test_redis_zero_fill_after_ttl_expiry` | force short TTL → `HGET` returns nil → read adapter coerces to 0 |
| `test_at_least_once_after_restart_no_corruption` | run 30 s, kill consumer, restart from committed offset → Redis values converge, Kafka dupes share the 4-tuple |
| `test_consumer_group_partitions_disjoint_users` | 2-process group → no user emitted by both processes |
| `test_rebalance_rebuilds_state_on_new_owner` | kill one of two processes → its users continue emitting on the survivor after warm-up |
| `test_end_to_end_with_validator` | feeder + validator + this consumer, 65 min → per-resolution counts match feeder log within 1 emission of lateness |
| `test_no_emission_when_no_events_in_slide` | user emits once then silent → no records from t=2 min onward (zero-fill downstream) |

## 7. How to Run

### 7.1 Bootstrap (no Flink)

```
make infra-up                  # Kafka + Postgres + Registry + Redis
make topic-ensure              # e-commerce-events, -feed, validated-events
make register-schemas-feed     # e-commerce-events-feed-value (feeder needs this)
                               # validated-events* / sliding-features* schemas
                               # self-register when each stage starts
```

### 7.2 Start the pipeline

```
make feeder-run                # ~200 evt/s feeder
make sink-run                  # Postgres sink
make validator-run             # week2_01 validator
make sliding-run               # THIS PR: single consumer process
# or, for the process-group narrative:
make sliding-run-group N=4     # 4 processes in the sliding-features-job group
```

### 7.3 Inspect

```
# Online store
USER=$(docker exec redis redis-cli --scan --pattern "feat:user:*" | head -1)
docker exec redis redis-cli HGETALL "$USER"

# Offline / history topic (Avro; binary-ish output normal)
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features \
  --from-beginning --max-messages 6 \
  --property print.key=true --property key.separator=' | '
```

### 7.4 CLI

```
python scripts/run_sliding_features_consumer.py \
    --bootstrap kafka-1:9092 \
    --registry http://schema-registry:8081 \
    --source-topic validated-events \
    --sink-topic sliding-features \
    --late-sink-topic sliding-features-late \
    --out-of-orderness-seconds 5 \
    --allowed-lateness-seconds 30 \
    --num-workers 1 \
    --redis-host localhost --redis-port 6379
```

### 7.5 Tear down

```
make infra-down
```

## 8. Resource Budget & Constraints

| Component | CPU | RAM | Disk |
|---|---|---|---|
| Consumer process (×N≤12) | <0.3 core each at feeder rate | per §state-math below | none (state in memory) |
| Redis (sink) | <0.1 core | ~80 MB at 100 k active users | — |
| Kafka (sliding-features) | (existing) | (existing) | ~150 MB/day at 200 evt/s |

State per active user (identical to the PyFlink pane math, now in process heap
rather than RocksDB):

| Resolution | Panes / user | Bytes / pane | Bytes / user |
|---|---|---|---|
| 5 m / 1 m | 5 | ~40 | 200 |
| 1 h / 5 m | 12 | ~150 | 1 800 |
| 24 h / 1 h | 24 | ~250 | 6 000 |
| **Total / active user** | | | **~8 KB** |

At ~5 k active users → **~40 MB of pane state per process** (the whole group's
state if `N=1`; split across processes for `N>1`). Comfortably in heap — which
is exactly why the RocksDB/checkpoint machinery was unnecessary at this scale
(§2.1).

Constraints:

- **GIL ceiling** ~11–14 k evt/s per process; feeder is ~200 evt/s, so `N=1`
  has >50× headroom. Scale by processes if the feeder rate is raised (§2.11).
- **Cold-start warm-up** after restart: 5 m/1 h seek back one window; 24 h
  warms over wall-clock (§2.10). Bounded, idempotent.
- **Emission rate** ~80 rec/s for the 5 m resolution at 5 k users — trivial.

## 9. Future Considerations

1. **Poor-man's checkpoint for the 24 h window.** Snapshot per-user 24 h
   accumulators to Redis/Postgres every few minutes and reload on assignment,
   eliminating the §2.10 24 h cold-start warm-up. Deferred because Week 4's
   offline batch is the authoritative 24 h view.
2. **HyperLogLog `distinct_products`.** Same as PyFlink doc §9 — swap the exact
   `set[str]` for HLL once per-window product cardinality justifies it.
3. **Two-level aggregation for hot keys.** The Zipfian feeder's top user gets
   ~10× average traffic; at higher rates pre-aggregate per `(user, salt)` then
   re-key. Laptop scale does not need it.
4. **Transactional EOS wrapping (Week 2 PR #3).** `confluent-kafka` supports
   `init_transactions` / `send_offsets_to_transaction`; the consume→produce
   half becomes exactly-once, and the consumer default flips to
   `read_committed`. The Redis/Postgres cross-store atomicity still needs the
   outbox/idempotent-write pattern, unchanged.
5. **Migration to Flink** — recorded with the explicit triggers in §2.1; the
   superseded design is the ready-made target if any trigger fires.
6. **Sub-second processing-time path** and **zero-tick decay stream** — same
   §9 items as the PyFlink doc; both are additive parallel paths.

## 10. Open Questions

1. **Cold-start seek-back vs. accept-cold for the 1 h window.** Seeking back
   1 h on every restart re-reads ~720 k events at feeder rate. Is the accuracy
   worth it, or should 1 h also "warm over wall-clock" like 24 h? The smoke run
   measures restart frequency to decide.
2. **`EXPIRE … XX` first-write nuance.** The first write to a fresh
   `feat:user:{id}` key has no TTL for `XX` to preserve; the sink must issue a
   plain `EXPIRE` on creation and `XX` thereafter (or set a longer TTL
   unconditionally and accept the over-retention). Same open question as the
   PyFlink doc §10.8 — a code-review-time choice.
3. **Per-process watermark vs. a shared lower-bound.** Each process watermarks
   independently (§2.4). If one process's partition subset is idle, its users'
   features lag relative to a busy process's. Is that acceptable (each user is
   self-consistent) or should the group publish a shared lower-bound watermark?
   Leaning "acceptable" — cross-process coordination is exactly what §2.1
   avoids.
4. **`num_workers` orchestration.** `make sliding-run-group N=4` launching 4 OS
   processes — supervise with a shell loop, `multiprocessing`, or just document
   running N terminals? Decision deferred to implementation; favors a thin
   `multiprocessing.Process` supervisor for clean shutdown.
