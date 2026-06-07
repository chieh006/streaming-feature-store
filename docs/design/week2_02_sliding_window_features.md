# Design Doc: Sliding-Window Feature Computation (Flink Hopping Windows)

> ⚠️ **SUPERSEDED — DO NOT IMPLEMENT.** This PyFlink design did not pan out at
> laptop scale: the JVM cluster + Apache Beam portability bridge + JAR
> management + GIL-in-UDF cost made it operationally heavy and brittle (the
> first smoke run failed on a Beam-worker Redis connection error before any
> window emitted). It is **kept only as an interview/portfolio artifact** —
> the windowing *semantics* documented here (pane-based pre-aggregation,
> watermarks, allowed-lateness, the sink/idempotency contracts) are correct
> and carry forward unchanged. The **active implementation** is a plain Python
> Kafka consumer group with in-memory sliding windows:
> [`week2_02_sliding_window_features_plain_consumer.md`](week2_02_sliding_window_features_plain_consumer.md).

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 2 — Validation & Feature Computation
**Scope:** First sub-bullet of the Week 2 feature-computation step in
[`gap_project_plan.md`](gap_project_plan.md) (line 81) — *"Sliding
window aggregations (clicks in last 5 minutes, purchase count in last
24 hours) — the canonical streaming interview topic, invest the most
time here."* Builds a **stateful PyFlink job** that consumes from
`validated-events` (produced by
[`week2_01`](week2_01_validation_layer_and_dlq.md)), keys by `user_id`,
maintains **three parallel pane-aggregated sliding windows** — `5 min /
slide 1 min`, `1 h / slide 5 min`, `24 h / slide 1 h` — and emits one
`SlidingFeatureRecord` per (key, window, slide-tick) to Redis (online
store) and the `sliding-features` Kafka topic.
Out of scope: EOS transactional wrapping (Week 2 PR #3).
Session-window features (the sibling sub-bullet on line 82 of the gap
plan) are deferred per the plan's "skip if time-constrained" marker.
**Author:** Auto-generated design document
**Date:** 2026-05-26

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

This PR is the **interview-headlined deliverable of Week 2** —
[`gap_project_plan.md`](gap_project_plan.md) line 81 explicitly says
*"the canonical streaming interview topic, invest the most time
here."* Every online-feature-store interview at every target company
(DoorDash, Uber, Pinterest, Meta, Stripe) opens with some variation
of *"compute clicks-in-last-5-minutes for a user, in real time, with
this throughput / freshness SLA"*. This PR is the answer in code.

By the end of Week 2 PR #1
([`week2_01`](week2_01_validation_layer_and_dlq.md)) every event that
clears the validation gate lands on the `validated-events` Kafka topic
with the contract *"every message you read here has been schema- and
value-checked."* This PR builds on top of that contract a Flink
streaming job that maintains, per `user_id` and per resolution, three
*time-decaying* counters:

| Family | Window | Slide | Per-emission features |
|---|---|---|---|
| **5 m** | 5 minutes | 1 minute | `clicks_5m`, `page_views_5m`, `purchases_5m`, `revenue_5m` |
| **1 h** | 1 hour | 5 minutes | `clicks_1h`, `page_views_1h`, `purchases_1h`, `revenue_1h`, `distinct_products_1h` |
| **24 h** | 24 hours | 1 hour | `purchases_24h`, `revenue_24h`, `avg_purchase_amount_24h`, `distinct_products_24h` |

The 12 features above are the **demo-headline numbers**: each is one
sentence in the portfolio write-up, and together they cover the three
canonical resolutions (real-time, hour-scale, day-scale) that almost
every ranker, recommender, fraud model, and personalization system
consumes.

Three Flink properties make this PR pedagogically central:

- **Pane-based pre-aggregation.** A naive sliding window stores every
  event for the window duration. Flink's `AggregateFunction` collapses
  this to one accumulator per *pane* — the smallest non-overlapping
  sub-interval that any active window includes (§2.3). For the
  24 h / 1 h-slide window this is the difference between *unbounded* state
  and *constant* state (24 panes × counter-size per key). Articulating
  *why* this matters — and *what* an event-time interval-join would do
  differently — is interview-grade signal.
- **Three resolutions, one keyed stream, one job.** A single PyFlink
  application reads `validated-events` once, keys by `user_id` once,
  and fans out to three parallel windowed operators (§2.4). The
  alternative — three independent Flink jobs — was rejected because
  laptop-scale Kafka source bandwidth is not the bottleneck and
  shared-source-fan-out is the topology a production team would build.
- **The sparsity problem and the zero-fill answer.** A user idle for
  5 minutes should report `clicks_5m = 0`, but a windowed aggregator
  driven by event arrival never emits on a user that received no
  events. §2.7 documents the *downstream-default-zero* read contract
  (the Redis-side hash returns "missing field == 0") and notes the
  alternative — a parallel "tick" stream that periodically emits zero
  records for idle users — as a §9 follow-up. The choice has real
  feature-correctness consequences and is one of the questions a sharp
  interviewer reaches for after the candidate finishes the happy-path
  whiteboard.

### Out of Scope (Deferred to Later PRs)

- **Session-window features** (gap plan line 82). Deferred per the
  plan's "skip if time-constrained" marker.
- **EOS transactional wrapping** of the consume-window-produce cycle.
  Moved to Week 2 PR #3. This PR ships with the same
  *at-least-once-read + idempotent-write* contract as
  [`week2_01`](week2_01_validation_layer_and_dlq.md) §2.7.
- **Continuous emission** ("emit every event" instead of "emit every
  slide tick"). At feeder rate (~200 evt/s) this would multiply
  emission load by ~60× without meaningfully improving online
  freshness past the 1-minute slide. Recorded as §9 for sub-second
  online-inference paths.
- **HyperLogLog-approximated `distinct_products`.** The current
  implementation maintains an exact `set[str]` in the accumulator,
  bounded by per-window product cardinality. For the synthetic feeder
  this is fine (a few thousand distinct SKUs); for production-scale
  product catalogs (~10⁸ SKUs) HLL is the right answer. Recorded as
  §9.
- **Tumbling windows.** Tumbling is a degenerate sliding window
  (window size == slide interval); covered implicitly by the same
  code path. No separate tumbling job is shipped.
- **Cross-resolution merge into a single output record.** Each of the
  three resolutions emits on its own schedule (every 1 min, 5 min,
  1 h respectively). Merging them into a single `FeatureVector`
  record per user requires a temporal-join across asynchronous
  streams; the cleaner online-store contract is a per-user **Redis
  hash** (`HGETALL feat:user:{user_id}` returns the latest of each
  field), and the offline-store contract is a single Kafka topic with
  a `window_resolution` discriminator. The two contracts together
  give the consumer everything the temporal-joined record would, with
  less moving machinery.
- **Per-event "early-fire" trigger** on the 24 h window. A late-arriving
  high-value purchase could in principle warrant an immediate
  `revenue_24h` update without waiting for the next 1-h slide tick.
  Recorded as §9.
- **A `bot_score_5m` feature** computed from sub-second click bursts.
  Out of catalog scope for this PR; the validator's
  `UserIdShapeValidator` already filters obvious shape-level abuse,
  and behavioral abuse-detection is a separate model.

### Deliverables

- `src/streaming_feature_store/flink/sliding/__init__.py`.
- `src/streaming_feature_store/flink/sliding/job.py` —
  `build_sliding_features_job(env, config)`: assembles source,
  watermark strategy, three keyed-window operators, three aggregators,
  three sinks, one shared late-events side output.
- `src/streaming_feature_store/flink/sliding/aggregators.py` —
  `SlidingWindowAggregator`: per-resolution `AggregateFunction`
  implementations (`FiveMinuteAggregator`, `OneHourAggregator`,
  `TwentyFourHourAggregator`); shared accumulator dataclass; merge
  helpers.
- `src/streaming_feature_store/flink/sliding/watermarks.py` —
  `build_watermark_strategy()` returning a bounded-out-of-orderness
  WatermarkStrategy with 5 s skew tolerance and 30 s idleness; reads
  `event_timestamp` from the Pydantic `EcommerceEvent`.
- `src/streaming_feature_store/flink/sliding/models.py` —
  `SlidingFeatureRecord` Pydantic model, `WindowResolution` enum,
  `SlidingJobConfig`, `SlidingAccumulator` dataclass.
- `src/streaming_feature_store/flink/sliding/sinks.py` —
  `RedisHashSink`, `KafkaSlidingFeaturesSink`, `KafkaLateEventsSink`.
- `src/streaming_feature_store/schemas/avro/sliding_feature_record.avsc`
  — registered under `sliding-features-value` with `BACKWARD`
  compatibility on first job start.
- `scripts/run_sliding_features_job.py` — submits the PyFlink job
  to the local Flink JobManager (or executes against
  `LocalStreamEnvironment` for tests).
- `docs/results/week2_sliding_features_results.md` — generated 24 h
  smoke-run report: per-resolution emission count, per-feature
  distribution (p50/p95/p99 of each counter), watermark-lag time
  series, Flink checkpoint sizes, hot-key TaskManager imbalance
  measurement, and the *sparsity audit* — for what fraction of
  user × slide-tick pairs no record was emitted (downstream
  zero-fill rate, see §2.7).
- `tests/unit/test_sliding_aggregators.py`,
  `test_sliding_feature_record.py`,
  `test_sliding_pane_arithmetic.py`,
  `test_sliding_job_config.py`,
  `test_sliding_sinks.py`,
  `test_sliding_watermarks.py`.
- `tests/integration/test_sliding_features_end_to_end.py`,
  `test_sliding_features_three_resolutions_emit_independently.py`,
  `test_sliding_features_late_event_side_output.py`,
  `test_sliding_features_redis_hash_round_trip.py`,
  `test_sliding_features_restart_from_checkpoint.py`,
  `test_sliding_features_zipfian_hot_key_emission_continuity.py`.
- `Makefile` targets: `sliding-submit`, `sliding-cancel`,
  `sliding-report`, `flink-up`, `flink-down`.

---

## 2. Critical Design Decisions

### 2.1 Hopping ("Sliding") Windows over Tumbling

**Decision:** Use Flink's **hopping windows** (`SlidingEventTimeWindows
.of(size, slide)`) — the Flink term for *fixed-duration windows that
advance by a slide interval shorter than the window duration*, with
**slide < size** for all three resolutions. Tumbling (where slide ==
size) is rejected; session windows are deferred per
[`gap_project_plan.md`](gap_project_plan.md) line 82 ("skip if
time-constrained").

**Rationale:**

- **Sliding emits at the slide-interval cadence; tumbling emits at the
  window-size cadence.** For `clicks_5m`, a tumbling window means the
  feature value updates once every 5 minutes — too stale for any
  real-time use. A 5 m / 1 m-slide hopping window updates once a
  minute while still aggregating over a 5-minute lookback. This is
  *the* freshness vs cost tradeoff sliding windows exist to make.
- **The interview script almost always uses "in the last X minutes".**
  That phrasing maps to a *moving* lookback, not a wall-clock-bucket
  lookback. Tumbling produces calendar-aligned buckets ("between
  10:00 and 10:05"); sliding produces moving-window-aligned readouts
  ("the 5 minutes ending now"). Aligning the implementation with the
  interview phrasing matters for the portfolio narrative.
- **Tumbling is a degenerate case of sliding.** If a future feature
  needs tumbling, the same code path supports it by setting
  `slide == size`. No second job class.
### 2.2 The Three Canonical Resolutions: 5 m / 1 m, 1 h / 5 m, 24 h / 1 h

**Decision:** Ship exactly three windowed operators per job:

| Name | `size` | `slide` | `panes_per_window` | Emission cadence |
|---|---|---|---|---|
| `W_5M_SLIDE_1M`  | 5 minutes | 1 minute | 5 | once / 1 min / active key |
| `W_1H_SLIDE_5M`  | 1 hour | 5 minutes | 12 | once / 5 min / active key |
| `W_24H_SLIDE_1H` | 24 hours | 1 hour | 24 | once / 1 h / active key |

**Rationale:**

- **5 m / 1 m is the *real-time* resolution.** Targets sub-minute
  online inference freshness while keeping per-key state bounded at
  5 panes. This is the resolution every "online recommendation" or
  "fraud detection" interview reaches for.
- **1 h / 5 m is the *short-history* resolution.** Targets
  hour-scale behavioral features (e.g. *"how many distinct products
  viewed in the last hour"*) with 5-minute emission freshness.
  12 panes / key, still constant memory.
- **24 h / 1 h is the *daily-history* resolution.** Targets the
  bread-and-butter aggregates (`purchases_24h`, `revenue_24h`,
  `avg_purchase_amount_24h`) that almost every model wants. 24 panes
  / key.
- **Panes-per-window stays ≤ 24.** Window-emission cost is
  O(panes_per_window) per emission — the aggregator combines that
  many pane accumulators on each fire. 24 is comfortably cheap; 60+
  starts to deserve a streaming-incremental sum data structure
  instead, and is recorded as §9 for finer resolutions.
- **The three families are mathematically related.** A consumer that
  wants `clicks_15m` can compute `clicks_5m * 3 ± slide-skew` as a
  rough estimate without re-running the job. Articulating *which*
  feature combinations admit a clean derivation vs which need a new
  window is a §10 open question.
- **No resolution finer than 1 minute.** The slide cadence sets a
  *floor* on online freshness; below 1 minute the per-emission
  overhead (Kafka produce + Redis write + serialization) starts to
  dominate. A *processing-time* parallel path for sub-second
  freshness is the §9 follow-up.

### 2.3 Pane-Based Pre-Aggregation (the Key Technique)

**Decision:** All three windows use Flink's `AggregateFunction`
interface (incremental aggregation) rather than `WindowFunction` or
`ProcessWindowFunction` (which buffer raw events). State per key is
O(`panes_per_window`) — *not* O(events in window).

**Mechanics:**

```
                       slide        slide        slide
   pane │  pane │  pane │  pane │  pane │  pane │  ...
        │       │       │       │       │       │
   ─────┴───────┴───────┴───────┴───────┴───────┴───────▶ event time
              ◀──── window 1 (5 panes) ──────▶
                      ◀──── window 2 (5 panes) ──────▶
                              ◀──── window 3 (5 panes) ──▶
```

For the 5 m / 1 m configuration:
- Flink assigns each event to *exactly one* pane (the 1-minute pane
  containing its event-timestamp).
- The pane accumulator is updated in-place via `aggregator.add()` —
  O(1) work per event.
- At each slide tick, the *current* window's emission combines its 5
  composing panes via `aggregator.merge()` (associative, commutative).
- Panes that have aged out (no longer included in *any* active
  window) are garbage-collected by Flink.

**Rationale:**

- **State growth is *constant* in number-of-events.** A naive
  `WindowFunction` storing raw events for the 24 h window would, at
  the synthetic feeder rate (~200 evt/s), accumulate ~17 M events per
  key over 24 hours. The same window with pane-based aggregation
  stores 24 panes × ~40 bytes ≈ 1 KB per key. Three orders of magnitude
  smaller, and the gap widens with rate.
- **Per-event work is O(1).** The aggregator's `add()` updates a
  handful of integer counters and one set (the `distinct_products`
  field, see §2.14). This keeps the PyFlink UDF-crossing cost
  bounded — see §2.13 for the broader PyFlink-GIL discussion.
- **Merging is associative + commutative by construction.** All
  counters compose as integer / float sums; `distinct_products`
  composes as set union. This is the algebraic property Flink needs
  to merge panes in any order, which it sometimes does under restart
  from checkpoint.
- **Acknowledged tradeoff.** `ProcessWindowFunction` gives access to
  the *full window context* (start/end times, watermark, etc.) that
  `AggregateFunction.getResult()` does not. The standard Flink
  pattern when you need both — *"emit with full context but aggregate
  incrementally"* — is to pass an `AggregateFunction` as the
  `pre-aggregator` of a `ProcessWindowFunction` (a two-argument
  overload of `.aggregate()`). This PR uses that pattern (§4.1) to
  get both the O(1) per-event update *and* the window-end-timestamp
  in the emitted record.

### 2.4 Single Flink Job, Three Parallel Window Operators (Shared Source)

**Decision:** One PyFlink application. One Kafka source reading
`validated-events`. One `keyBy(user_id)`. *Three* parallel window
operators downstream of the keyed stream, one per resolution. Three
sinks, all writing to the same `sliding-features` Kafka topic (with
`window_resolution` discriminator) and the same per-user Redis hash.

```
validated-events ─▶ source ─▶ keyBy(user_id) ┬─▶ window(5m/1m)  ─▶ agg ─▶ sink5m
                                             ├─▶ window(1h/5m)  ─▶ agg ─▶ sink1h
                                             └─▶ window(24h/1h) ─▶ agg ─▶ sink24h
```

**Rationale:**

- **One source, three windows.** Three independent jobs would mean
  three consumer groups, three reads of `validated-events`, and
  three independent watermark machines. Sharing the source halves
  Kafka broker load and ensures all three resolutions see a
  consistent event ordering.
- **Three operators, not one omnibus operator.** A single
  `ProcessFunction` maintaining all three resolutions internally
  would centralize state and complicate emission scheduling (three
  triggers in one operator). Flink's window primitive is the natural
  unit; keeping them separate keeps each operator's job description
  one sentence long.
- **Failure isolation is *not* the goal here.** Splitting failure
  domains is the argument for separate *jobs*
  ([`week2_01`](week2_01_validation_layer_and_dlq.md) §2.2 made that
  point for validator vs feature-compute). Within
  *sliding-features*, the three resolutions are tightly coupled in
  semantics; isolating them across jobs would buy nothing.
- **Parallelism per operator is configurable.** Each window operator
  has its own `setParallelism(...)`. The default is 12 (matching the
  source partition count); the 24 h operator can be tuned down to
  4 if its longer state hold inflates checkpoint size disproportionately
  (recorded as §10).
- **Watermark inheritance.** All three windows receive the same
  watermark from the shared source. This means *the 5 m window
  cannot emit faster than the 24 h window's watermark advance* — a
  subtle but important property: a stuck partition stalls all three.
  Idleness detection (§2.5) is what prevents pathological stalls.

### 2.5 Watermark Strategy: 5 s Bounded Out-of-Orderness, 30 s Idleness

**Decision:** Source operator uses
`WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
.withIdleness(Duration.ofSeconds(30))`, keyed off the event's
`event_timestamp` field (logical `timestamp-micros`, divided by 1000
to surface ms-since-epoch).

**Rationale:**

- **5 s is intentionally tight.** Sliding windows emit on every
  slide tick, so watermark advance directly drives emission cadence.
  A larger watermark (e.g. 60 s) would delay every emission by 60 s —
  adding a full minute of staleness to a feature whose entire purpose
  is freshness.
- **5 s is comfortably above measured skew.** Week 1's end-to-end
  latency harness ([`week1_05`](week1_05_consumer_group_end_to_end_latency.md))
  measured producer-to-consumer p99 in the tens of milliseconds.
  Multi-producer-process interleaving
  ([`gap_project_plan.md`](gap_project_plan.md) line 104) bounds
  out-of-orderness above by per-partition broker order plus
  inter-process scheduling jitter; 5 s is ~3 orders of magnitude
  above that floor.
- **30 s idleness keeps the slide cadence smooth.** Sliding windows
  are emission-rate-sensitive; a partition idle for any longer
  before its watermark advances would visibly degrade the
  1-minute-slide cadence.
- **Per-partition watermarks, merged at source.** Flink merges
  per-partition watermarks at the source operator; the merged
  watermark is what governs each keyed-state window. This is correct
  for our case because event time is producer-stamped and is
  monotonic *within* each partition up to broker-side reordering,
  which the 5 s budget bounds above.

### 2.6 Allowed Lateness: 30 s, Re-Emission Semantics

**Decision:** Each window operator declares
`allowedLateness(Duration.ofSeconds(30))`. Events arriving inside
the lateness window cause the window to re-fire and a *new*
`SlidingFeatureRecord` to be emitted for the same `(user_id, window_end_ms,
resolution)` triple. Events arriving outside the lateness window are
routed to the shared `late-events` side output and sunk to
`sliding-features-late`.

**Rationale:**

- **30 s of allowed lateness is right for all three resolutions.**
  For the 5 m window, 30 s is 10% of window size — a generous tail.
  For the 1 h window, 30 s is 0.8%; for the 24 h window, 0.03%. The
  *absolute* lateness an event can be is what matters operationally
  (Kafka rebalance pauses, GC pauses), not the relative fraction —
  one global value covers all three.
- **Re-emission, not silent update.** A late event lands in its pane,
  the pane updates, and the window re-fires. Downstream sees two
  records with the same `(user_id, window_end_ms, resolution)` — that
  is the idempotency-key cue from §2.9.
- **`emission_seq` discriminates first vs late re-emission.** The
  `0` emission is the first fire of a window; `1+` is one per
  late-event-driven re-fire.
- **Side-output for very-late.** Forensic preservation: the
  side-output records carry the *raw event*, not a reduced
  accumulator, so the Week 4 consistency report can audit
  divergences between this online stream and the offline DuckDB
  recomputation.

### 2.7 Sparsity & the Downstream-Default-Zero Read Contract

**Decision:** When a user emits **no events in a slide interval**, the
window operator emits **no record** for that interval. The online-store
contract — encoded in the Redis-side read path — is *"missing field
means zero"*. The offline-store contract — encoded in the
`sliding-features` Kafka topic — is *"absence of a record for
`(user_id, window_end_ms, resolution)` means the counters are zero
at that emission time"*.

**Rationale:**

- **The naive alternative is unaffordable.** Emitting a record per
  user per slide-tick regardless of activity means `|users| ×
  emit_rate` records per resolution. For 5 m / 1 m at ~10⁵ active
  users that is ~10⁵ records / minute / resolution × 3 resolutions —
  three orders of magnitude above the actual event rate. Synthetic
  data scale hides this; production scale does not.
- **The serving contract is naturally zero-defaulted.** The
  Redis hash `feat:user:{user_id}` is read with `HGETALL`; a missing
  field is returned as absent, and the read-side adapter coerces to
  zero. There is no representation of "stale vs missing vs zero" the
  downstream model needs to distinguish — they are all "no recent
  activity."
- **Acknowledged tradeoff: freshness telemetry is lossy.** A user
  whose `clicks_5m` was last *emitted* at t=10:00:00 with value 7,
  and who is idle for the next 10 minutes, has a *true* `clicks_5m`
  trajectory that decays through 7, 7, 6, 5, 3, 0 over those
  10 minutes — but the *Redis value* stays at 7 until either the
  user fires again (emission updates) or a TTL expires. The TTL is
  set to **1.5 × window size** per resolution (7.5 min for 5 m,
  90 min for 1 h, 36 h for 24 h) so that a sufficiently-stale field
  expires before being read as fresh. The 1.5× multiplier covers
  both the slide interval and the allowed-lateness window.
- **The §9 alternative — a tick stream emitting zeros for idle keys
  — is recorded.** Its cost is the same as the "naive alternative"
  above; its benefit is *strictly correct* feature trajectories.
  For an online recommender that cares about decay-to-zero
  behavior, the zero-tick path is the right answer. For most use
  cases the TTL-based read contract is sufficient.

### 2.8 Sink Contract: Redis Hash + Kafka Topic (Per-Resolution Discriminator)

**Decision:** Each emitted `SlidingFeatureRecord` is sunk twice:

1. **Redis** — `HSET feat:user:{user_id} <field>:<resolution> <value>`
   for each feature in the record, with `EXPIRE feat:user:{user_id}
   <ttl_per_resolution>` set on first creation. The hash key is *per
   user, not per resolution*; the field names encode the resolution
   suffix.
2. **Kafka** — produce to `sliding-features` topic with
   `key = f"{user_id}:{resolution.name}"` and the
   `SlidingFeatureRecord` (Avro-serialized) as the value. The
   `window_resolution` field inside the record is also the
   discriminator a downstream consumer reads if it does not want to
   parse the key.

**Rationale:**

- **One Redis hash per user — *not* per (user, resolution).** A model
  doing online inference issues `HGETALL feat:user:{user_id}` once
  and gets all 12 features (across all 3 resolutions) in one round
  trip. Three separate hashes would mean three round trips and
  three TTL clocks to reason about.
- **Field names encode the resolution suffix.** The Redis fields are
  `clicks_5m`, `clicks_1h`, `purchases_24h`, etc. — exactly the
  names a downstream model expects. This means the *online read*
  layer is trivial and the *write* layer absorbs the
  encode-resolution-into-field-name complexity.
- **One Kafka topic with a discriminator — *not* three topics per
  resolution.** Three topics would mean three independent partition
  layouts, three independent retention clocks, and three independent
  monitoring dashboards. Discriminator-by-field is simpler and the
  consumer-side branching cost is one match expression.
- **Kafka key is `{user_id}:{resolution.name}`, not just `user_id`.**
  This makes log compaction (if a future operator turns it on)
  retain the latest record *per (user, resolution)*, which is the
  desired semantics. Keying on `user_id` alone would compact-away
  all but one resolution per user.
- **TTL per-resolution, not global.** A user whose `purchases_24h` is
  legitimately stable for 25 hours (one purchase, then nothing) must
  not have that feature expired at the 1.5× × 5 m = 7.5 min mark.
  Each resolution's TTL is computed from its own window size.

### 2.9 Idempotency: `(user_id, window_end_ms, resolution, emission_seq)`

**Decision:** The natural key for deduplication is the 4-tuple:

```
sliding_idempotency_key = (user_id, window_end_ms, resolution, emission_seq)
```

Redis writes are *latest-wins* on `emission_seq`; Kafka emits are
*append* with the key `{user_id}:{resolution.name}` so that downstream
consumers can dedupe on the 4-tuple via the message payload. The
`emission_seq` field starts at 0 for the first fire of a window and
increments by 1 for each allowed-lateness re-fire (§2.6).

**Rationale:**

- **`window_end_ms` is the stable per-window identifier.** Flink's
  hopping window assigner produces fixed window-end timestamps
  aligned to the slide cadence; restart-from-checkpoint reproduces
  the same `window_end_ms` for the same physical window. This is the
  property idempotency depends on.
- **`emission_seq` discriminates re-fires.** A consumer that wants
  the *final* value for a window can keep the highest `emission_seq`
  seen for that key triple. A consumer that wants
  *eventually-consistent* online-store updates can write each
  emission as it arrives (Redis-side overwrite semantics).
- **Redis side has no "version" field**, by design. The Redis hash
  *is* eventually-consistent — the latest write wins, and the lower
  `emission_seq` records are lost. This is the correct contract for
  an online store (you want the freshest value, not a history).
- **Kafka side preserves history.** All emissions, all
  `emission_seq` values, all re-fires are recorded; the offline
  consistency report in Week 4 needs this.
- **No transactional sink yet.** Same Week 2 PR #3 deferral as the
  validator.

### 2.10 State Backend & Checkpointing

**Decision:** RocksDB state backend with **30 s checkpoint interval**
to the local volume mounted at `/flink-checkpoints` in the
`jobmanager` container. Allowed checkpoint failures: 2 before
restart. Restart strategy: fixed-delay 10 s, infinite attempts.

**Rationale:**

- **RocksDB over heap state.** The 24 h window holds 24 panes per
  active user; at the synthetic feeder rate (~200 evt/s) the
  steady-state active-user set is small (~5k), but the state
  surface across all three resolutions sums to ~41 panes × ~40
  bytes × 5k users ≈ 8 MB. Heap would work; RocksDB is chosen
  because the absolute number can grow several orders of magnitude
  without OOM risk, and the same backend supports a future Phase 4
  K8s redeploy.
- **30 s checkpoint interval.** Short enough that restart redoes a
  small amount of work (~6k events at feeder rate), long enough
  that checkpoint overhead is amortized.
- **Local-volume checkpoint store.** Same Phase 4 migration to S3
  / MinIO recorded as §9.

### 2.11 Topic Configuration

The job creates (via `TopicAdmin.ensure_topic()` from PR #4) two
output topics on startup:

| Topic | Partitions | RF | retention.ms | cleanup.policy | Notes |
|---|---|---|---|---|---|
| `sliding-features` | 12 | 3 | 604_800_000 (7 d) | `delete` | not compacted (preserves emission history; §2.9) |
| `sliding-features-late` | 3 | 3 | 2_592_000_000 (30 d) | `delete` | mirrors `dead-letter-queue` shape; forensic |

**Rationale:**

- **`sliding-features` partition count = 12** — matches the source
  topic so the implicit per-`user_id` partition affinity from the
  validator survives end-to-end. A consumer that wants per-user
  ordering across all resolutions gets it for free.
- **Not compacted.** Compaction would discard older emissions; the
  Week 4 offline-consistency comparison needs the full re-emission
  history for late-event impact accounting.
- **`sliding-features-late` mirrors the DLQ shape** — 3 partitions,
  30 d retention — same forensic, low-volume access pattern as
  [`week2_01`](week2_01_validation_layer_and_dlq.md) §2.10.

### 2.12 Hot-Key / Zipfian Skew Impact

**Decision:** Accept the skew, monitor it, and articulate the
mitigation paths without pre-emptively implementing them. The
synthetic feeder's Zipfian-skewed `user_id` distribution
([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.5)
means a handful of TaskManager slots receive disproportionate state
load.

**Rationale:**

- **The skew is itself an interview talking point.** The portfolio
  narrative is: *"we measured the partition skew, articulated the
  three standard mitigations (two-level aggregation, salting,
  rebalancing the key), and chose to accept it at this scale."* That
  is a stronger story than silently pre-mitigating it.
- **Pane-based aggregation insulates per-event throughput.** A hot
  key receives more `add()` calls but each is O(1); the hot operator
  is not state-bound, it is throughput-bound. PyFlink's per-event
  UDF cost (Python/JVM crossing) is the actual ceiling.
- **Two-level aggregation as a §9 follow-up.** *Pre-aggregate per
  random sub-key (`user_id + hash(random) % N`), then re-key by
  `user_id` and final-aggregate* is the textbook hot-key mitigation.
  The synthetic feeder produces enough skew to demonstrate it but
  laptop scale does not need it for correctness; recorded for the
  Phase 4 K8s deploy and the Phase 5 system-design write-up.
- **Monitoring.** The smoke run captures per-TaskManager event-count
  and per-TaskManager checkpoint-size, which makes the skew
  visible quantitatively in the report
  (`docs/results/week2_sliding_features_results.md`).

### 2.13 PyFlink DataStream API (Not Flink SQL, Not Kafka Streams)

**Decision:** PyFlink DataStream API, executed against the Flink
cluster spun up by `flink-up` (added by this PR alongside the
existing infra compose file).

**Rationale:**

- **DataStream over Flink SQL.** Flink SQL's `HOP` window is real
  and works, but it hides the watermark strategy, the
  allowed-lateness policy, and the side-output mechanics behind
  dialect. The pedagogical point of this PR is *those* mechanics —
  the DataStream API surfaces them explicitly.
- **PyFlink over Java.** Stack consistency with the rest of the
  repo matters more than the 2-3× throughput advantage Java would
  buy at laptop scale. The continuous feeder produces ~200 evt/s
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md)
  §2.5); PyFlink steady-state benchmarks are comfortably above 10k
  evt/s on a single TaskManager slot.
- **PyFlink over Kafka Streams.** Kafka Streams' hopping windows
  are competitive, but its keyed-state model is
  per-application-instance (no separate state cluster). Flink's
  separate state backend + checkpoint coordinator is the cleaner
  mental model and is the one the portfolio benefits from being
  able to discuss.
- **Acknowledged tradeoff.** PyFlink GIL behavior inside a UDF is
  a real concern at high throughput
  ([`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md)
  §2.2 documented the same ceiling on the producer side). Sliding
  windows fire often, so the per-emission UDF cost is visible; the
  aggregator is shaped so that the per-event `add()` does the
  minimum work and the heavier Pydantic construction happens only
  in `getResult()`, once per slide emission.

### 2.14 Feature Catalog (12 Features Across 3 Resolutions)

**Decision:** Each emission carries the feature subset appropriate
for its resolution. The Avro schema (§4.5) declares all fields
nullable so that a single record class can carry any resolution's
slice.

| Field | 5 m | 1 h | 24 h | Computation |
|---|---|---|---|---|
| `click_count` | ✓ | ✓ | — | count of `event_type == CLICK` |
| `page_view_count` | ✓ | ✓ | — | count of `event_type == PAGE_VIEW` |
| `purchase_count` | ✓ | ✓ | ✓ | count of `event_type == PURCHASE` |
| `revenue` | ✓ | ✓ | ✓ | sum of `price × quantity` for purchases |
| `distinct_products` | — | ✓ | ✓ | count-distinct of `product_id` (set in accumulator) |
| `avg_purchase_amount` | — | — | ✓ | `revenue / purchase_count`, or null if purchase_count == 0 |

**Rationale:**

- **The 5 m resolution carries the "real-time" features only** —
  counts of high-frequency event types. Computing
  `distinct_products` at 5 m would require maintaining a 5 m
  distinct-product set per user, which is the most-likely-to-blow-up
  state in this PR. Skip at 5 m, include at 1 h and 24 h where the
  longer window absorbs the cardinality.
- **24 h excludes click/page-view counts.** Aggregating clicks over
  24 h is rarely useful as an online feature (browse intent decays
  fast); the 24 h resolution is the *purchase-history* resolution by
  convention.
- **`avg_purchase_amount` is null when `purchase_count == 0`.**
  Avoids division-by-zero downstream. The Avro schema declares it
  `["null", "double"]` with default `null`.
- **All counts are non-decreasing within a window.** Late-event
  re-emissions (§2.6) can only *add* events to a window, never remove,
  so `emission_seq+1` ≥ `emission_seq` for all counters.

---

## 3. Architecture

### 3.1 Job Topology

```
┌──────────────────────────────────────────────────────────────────────────┐
│                  SlidingWindowFeaturesJob (this PR)                      │
│                                                                          │
│   ┌────────────────────┐                                                 │
│   │ validated-events   │  (12 partitions, from week2_01)                 │
│   │ FlinkKafkaSource   │                                                 │
│   └─────────┬──────────┘                                                 │
│             │                                                            │
│             │  WatermarkStrategy: bounded out-of-orderness 5 s           │
│             │                     idleness 30 s                          │
│             ▼                                                            │
│   ┌────────────────────────────┐                                         │
│   │ DeserializeAvroToEvent     │  (re-uses Pydantic adapter from PR #2)  │
│   └────────────┬───────────────┘                                         │
│                │                                                         │
│                ▼                                                         │
│   ┌────────────────────────────┐                                         │
│   │ KeyBy(user_id)             │                                         │
│   └────────────┬───────────────┘                                         │
│                │                                                         │
│       ┌────────┼────────────────────┐                                    │
│       ▼        ▼                    ▼                                    │
│   ┌──────┐  ┌──────┐            ┌──────┐                                 │
│   │ 5m / │  │ 1h / │            │ 24h /│                                 │
│   │ 1m   │  │ 5m   │            │ 1h   │                                 │
│   │ slide│  │ slide│            │ slide│                                 │
│   │ +30s │  │ +30s │            │ +30s │                                 │
│   │ late │  │ late │            │ late │                                 │
│   └──┬───┘  └──┬───┘            └──┬───┘                                 │
│      │        │                    │                                     │
│      ▼        ▼                    ▼                                     │
│   ┌──────┐  ┌──────┐            ┌──────┐                                 │
│   │ Agg5m│  │ Agg1h│            │Agg24h│        each (resolution)        │
│   │ +     │  │ +    │            │ +    │       passes its events also   │
│   │ ProcW │  │ProcW │            │ProcW │       to a shared late-events  │
│   └──┬───┘  └──┬───┘            └──┬───┘       side output (next layer)  │
│      │        │                    │                                     │
│      ├────────┼────────────────────┤────────────────────┐                │
│      ▼        ▼                    ▼                    ▼                │
│   ┌──────────────────────────────────────────┐   ┌──────────────────┐    │
│   │ KafkaSink: sliding-features              │   │ KafkaSink:       │    │
│   │   (Avro: SlidingFeatureRecord,           │   │ sliding-features │    │
│   │    key={user_id}:{resolution.name})      │   │   -late          │    │
│   └──────────────────────────────────────────┘   └──────────────────┘    │
│      │        │                    │                                     │
│      ▼        ▼                    ▼                                     │
│   ┌──────────────────────────────────────────┐                           │
│   │ RedisHashSink: feat:user:{user_id}       │                           │
│   │   HSET ... HSET ... HSET ...             │                           │
│   │   EXPIRE feat:user:{user_id} <ttl_resN>  │                           │
│   └──────────────────────────────────────────┘                           │
└──────────────────────────────────────────────────────────────────────────┘
```

End-to-end, this PR's job sits downstream of the validator:

```
validator (week2_01) ─▶ validated-events ──▶ Sliding (this PR) ──▶ Redis (feat:user:*) + sliding-features
```

### 3.2 Module Layout

```
src/streaming_feature_store/flink/
└── sliding/
    ├── __init__.py
    ├── job.py             # build_sliding_features_job(env, config)
    ├── aggregators.py     # FiveMinuteAggregator, OneHourAggregator,
    │                      # TwentyFourHourAggregator, base
    │                      # SlidingWindowAggregator
    ├── watermarks.py      # build_watermark_strategy(5s, 30s)
    ├── models.py          # SlidingFeatureRecord, WindowResolution,
    │                      # SlidingJobConfig, SlidingAccumulator
    └── sinks.py           # RedisHashSink, KafkaSlidingFeaturesSink,
                           # KafkaLateEventsSink

src/streaming_feature_store/schemas/avro/
└── sliding_feature_record.avsc

scripts/
└── run_sliding_features_job.py

docs/results/
└── week2_sliding_features_results.md  # generated
```

### 3.3 Class & Type Sketch

```
class WindowResolution(str, Enum):
    """One of the three sliding-window resolutions.

    The string values double as Redis-field suffixes and the Avro
    enum symbols, so renames are coordinated across all three layers
    by editing this enum.
    """
    W_5M_SLIDE_1M  = "5m"
    W_1H_SLIDE_5M  = "1h"
    W_24H_SLIDE_1H = "24h"


@dataclass
class SlidingAccumulator:
    """Pane-level accumulator updated incrementally by ``add()``.

    Notes
    -----
    Per §2.3 this is the *per-pane* state, not the per-window state.
    Flink merges N panes via ``merge()`` to produce the window
    aggregate on emission.
    """
    user_id: str
    click_count: int = 0
    page_view_count: int = 0
    purchase_count: int = 0
    revenue: float = 0.0
    distinct_products: set[str] = field(default_factory=set)


class SlidingWindowAggregator(AggregateFunction):
    """Shared base; sub-classed per resolution to scope feature output.

    Notes
    -----
    The per-resolution sub-classes (``FiveMinuteAggregator`` etc.)
    only differ in the set of fields they read out of the merged
    accumulator inside ``get_result``.  ``add`` and ``merge`` are
    identical across resolutions.
    """
    resolution: ClassVar[WindowResolution]

    def create_accumulator(self) -> SlidingAccumulator: ...
    def add(self, event: EcommerceEvent,
            acc: SlidingAccumulator) -> SlidingAccumulator: ...
    def merge(self, a: SlidingAccumulator,
              b: SlidingAccumulator) -> SlidingAccumulator: ...
    def get_result(self, acc: SlidingAccumulator) -> SlidingFeatureRecord:
        ...


class SlidingFeatureRecord(BaseModel):
    user_id: str
    window_resolution: WindowResolution
    window_start_ms: int
    window_end_ms: int
    emission_seq: int = 0
    click_count: int | None = None
    page_view_count: int | None = None
    purchase_count: int | None = None
    revenue: float | None = None
    distinct_products: int | None = None
    avg_purchase_amount: float | None = None

    def idempotency_key(self) -> str:
        return (f"{self.user_id}:{self.window_resolution.value}:"
                f"{self.window_end_ms}:{self.emission_seq}")

    def redis_field_updates(self) -> dict[str, str]:
        """Pairs of ``(field_name_with_resolution_suffix, str(value))``."""
        ...


class SlidingJobConfig(BaseModel):
    bootstrap: str
    registry_url: str
    source_topic: str = "validated-events"
    sink_topic: str = "sliding-features"
    late_sink_topic: str = "sliding-features-late"
    consumer_group: str = "sliding-features-job"
    out_of_orderness_seconds: int = 5
    idleness_seconds: int = 30
    allowed_lateness_seconds: int = 30
    checkpoint_interval_ms: int = 30_000
    parallelism: int = 12
    redis_host: str = "redis"
    redis_port: int = 6379
    ttl_factor: float = 1.5    # § 2.7
```

---

## 4. Detailed Implementation

### 4.1 Aggregator (Shared Base + Per-Resolution Subclasses)

```
class SlidingWindowAggregator(AggregateFunction):
    """Pane-level incremental aggregator.

    The ``add`` and ``merge`` implementations are identical across all
    three resolutions; the only resolution-specific code is in
    ``get_result``, which decides which fields are populated in the
    emitted ``SlidingFeatureRecord``.
    """

    def add(self, event, acc):
        acc.user_id = event.user_id
        if event.event_type == "CLICK":
            acc.click_count += 1
        elif event.event_type == "PAGE_VIEW":
            acc.page_view_count += 1
            acc.distinct_products.add(event.payload.product_id)
        elif event.event_type == "PURCHASE":
            acc.purchase_count += 1
            acc.revenue += event.payload.price * event.payload.quantity
            acc.distinct_products.add(event.payload.product_id)
        return acc

    def merge(self, a, b):
        merged = SlidingAccumulator(user_id=a.user_id or b.user_id)
        merged.click_count = a.click_count + b.click_count
        merged.page_view_count = a.page_view_count + b.page_view_count
        merged.purchase_count = a.purchase_count + b.purchase_count
        merged.revenue = a.revenue + b.revenue
        merged.distinct_products = a.distinct_products | b.distinct_products
        return merged


class FiveMinuteAggregator(SlidingWindowAggregator):
    resolution = WindowResolution.W_5M_SLIDE_1M

    def get_result(self, acc) -> SlidingFeatureRecord:
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            # window_start_ms / window_end_ms / emission_seq are
            # injected by the ProcessWindowFunction wrapper below
            window_start_ms=0,
            window_end_ms=0,
            click_count=acc.click_count,
            page_view_count=acc.page_view_count,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue)


class OneHourAggregator(SlidingWindowAggregator):
    resolution = WindowResolution.W_1H_SLIDE_5M

    def get_result(self, acc) -> SlidingFeatureRecord:
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            window_start_ms=0,
            window_end_ms=0,
            click_count=acc.click_count,
            page_view_count=acc.page_view_count,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue,
            distinct_products=len(acc.distinct_products))


class TwentyFourHourAggregator(SlidingWindowAggregator):
    resolution = WindowResolution.W_24H_SLIDE_1H

    def get_result(self, acc) -> SlidingFeatureRecord:
        avg = (acc.revenue / acc.purchase_count
               if acc.purchase_count > 0 else None)
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            window_start_ms=0,
            window_end_ms=0,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue,
            distinct_products=len(acc.distinct_products),
            avg_purchase_amount=avg)
```

The `ProcessWindowFunction` wrapper (Flink's two-argument
`.aggregate(pre_aggregator, window_function)` form, §2.3) is what
fills in `window_start_ms`, `window_end_ms`, and `emission_seq`.

### 4.2 Watermark Strategy

```
def build_watermark_strategy() -> WatermarkStrategy:
    """Bounded out-of-orderness with idleness detection (5 s / 30 s).

    Notes
    -----
    The timestamp extractor reads ``event_timestamp`` (logical
    ``timestamp-micros``) and divides by 1000 to surface
    ms-since-epoch to Flink, which is what the windowing layer
    expects.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimestampAssigner())
        .with_idleness(Duration.of_seconds(30)))
```

### 4.3 Job Builder

```
def build_sliding_features_job(env: StreamExecutionEnvironment,
                                config: SlidingJobConfig) -> None:
    env.enable_checkpointing(config.checkpoint_interval_ms)
    env.set_parallelism(config.parallelism)

    src = build_validated_events_source(config)
    events = (src
        .assign_timestamps_and_watermarks(build_watermark_strategy())
        .map(deserialize_avro_to_event,
             output_type=EVENT_TYPE_INFO))

    keyed = events.key_by(lambda e: e.user_id)
    late_tag = OutputTag("late-events", EVENT_TYPE_INFO)

    five_m = (keyed
        .window(SlidingEventTimeWindows.of(Time.minutes(5),
                                            Time.minutes(1)))
        .allowed_lateness(Time.seconds(config.allowed_lateness_seconds))
        .side_output_late_data(late_tag)
        .aggregate(FiveMinuteAggregator(),
                   _SlidingWindowEmitter(),
                   output_type=SLIDING_FEATURE_RECORD_TYPE_INFO))

    one_h = (keyed
        .window(SlidingEventTimeWindows.of(Time.hours(1),
                                            Time.minutes(5)))
        .allowed_lateness(Time.seconds(config.allowed_lateness_seconds))
        .side_output_late_data(late_tag)
        .aggregate(OneHourAggregator(),
                   _SlidingWindowEmitter(),
                   output_type=SLIDING_FEATURE_RECORD_TYPE_INFO))

    twenty_four_h = (keyed
        .window(SlidingEventTimeWindows.of(Time.hours(24),
                                            Time.hours(1)))
        .allowed_lateness(Time.seconds(config.allowed_lateness_seconds))
        .side_output_late_data(late_tag)
        .aggregate(TwentyFourHourAggregator(),
                   _SlidingWindowEmitter(),
                   output_type=SLIDING_FEATURE_RECORD_TYPE_INFO))

    for stream in (five_m, one_h, twenty_four_h):
        stream.sink_to(build_kafka_sliding_features_sink(config))
        stream.add_sink(RedisHashSink(config))

    # Side output is union'd across all three windows
    late_union = five_m.get_side_output(late_tag) \
        .union(one_h.get_side_output(late_tag),
               twenty_four_h.get_side_output(late_tag))
    late_union.sink_to(build_kafka_late_events_sink(config))


class _SlidingWindowEmitter(ProcessWindowFunction):
    """Inject ``window_start_ms``, ``window_end_ms`` and
    ``emission_seq`` from Flink's window context into the
    SlidingFeatureRecord coming out of the AggregateFunction.

    ``emission_seq`` is read from per-key keyed state — incremented
    each time this function fires for the same window context.
    """
    EMISSION_SEQ_STATE = ValueStateDescriptor(
        "emission_seq_by_window_end", Types.LONG())

    def process(self, key, context, elements):
        record: SlidingFeatureRecord = next(iter(elements))
        record.window_start_ms = context.window().start
        record.window_end_ms = context.window().end
        seq_state = context.global_state().get_state(self.EMISSION_SEQ_STATE)
        prev = seq_state.value() or 0
        record.emission_seq = prev
        seq_state.update(prev + 1)
        yield record
```

### 4.4 Sinks

```
class RedisHashSink(SinkFunction):
    """HSET into ``feat:user:{user_id}`` with resolution-suffixed fields.

    Atomicity
    ---------
    Each emission executes a single Redis pipeline:
        HSET feat:user:{user_id} <suffixed_field> <value> ...
        EXPIRE feat:user:{user_id} <ttl_for_this_resolution> XX
    The pipeline is sent as one round-trip.  The ``XX`` flag on EXPIRE
    keeps the longest already-set TTL; per-resolution TTL set on the
    *first* emission for a fresh hash key.

    Idempotency
    -----------
    The hash field overwrite is naturally idempotent — re-emissions
    write the same field with the same key and an equal or larger
    value.
    """

    def invoke(self, record: SlidingFeatureRecord,
               context: SinkFunction.Context) -> None: ...


def build_kafka_sliding_features_sink(config: SlidingJobConfig):
    """KafkaSink with Avro serializer and per-resolution-discriminator key.

    Key
    ---
    ``f"{record.user_id}:{record.window_resolution.value}"``.
    """
    ...
```

### 4.5 Avro Schema

`sliding_feature_record.avsc` mirrors the Pydantic model. The
record's `window_resolution` is an Avro `enum`; all numeric feature
fields are declared `["null", "<numeric>"]` with default `null` so
the per-resolution subsets fit a single record class.

```
{
  "type": "record",
  "name": "SlidingFeatureRecord",
  "namespace": "com.featurestore.sliding",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "window_resolution", "type":
      {"type": "enum", "name": "WindowResolution",
       "symbols": ["W_5M_SLIDE_1M", "W_1H_SLIDE_5M", "W_24H_SLIDE_1H"]}},
    {"name": "window_start_ms", "type": "long"},
    {"name": "window_end_ms",   "type": "long"},
    {"name": "emission_seq",    "type": "int", "default": 0},
    {"name": "click_count",        "type": ["null", "long"],   "default": null},
    {"name": "page_view_count",    "type": ["null", "long"],   "default": null},
    {"name": "purchase_count",     "type": ["null", "long"],   "default": null},
    {"name": "revenue",            "type": ["null", "double"], "default": null},
    {"name": "distinct_products",  "type": ["null", "long"],   "default": null},
    {"name": "avg_purchase_amount","type": ["null", "double"], "default": null}
  ]
}
```

Registered on first job start under `sliding-features-value` with
`BACKWARD` compatibility, by the same registry-bootstrap path the
validator uses
([`week2_01`](week2_01_validation_layer_and_dlq.md) §4.7).

---

## 5. Unit Tests

All unit tests use `pytest`. Flink is exercised through the
`LocalStreamEnvironment` for the aggregator and watermark logic; no
real Kafka, no real Redis.

| Test | Assertion |
|---|---|
| `test_five_minute_aggregator_counts_clicks` | 3 click events → `click_count=3`, other counters 0 or null |
| `test_five_minute_aggregator_counts_page_views` | 4 page-view events → `page_view_count=4` |
| `test_five_minute_aggregator_skips_distinct_products` | The 5 m record never carries `distinct_products` (excluded from §2.14) |
| `test_one_hour_aggregator_counts_distinct_products` | 3 page-views on products A, B, A → `distinct_products=2` |
| `test_one_hour_aggregator_distinct_products_union_on_merge` | merge of accs with `{A,B}` and `{B,C}` → `{A,B,C}`, count 3 |
| `test_twenty_four_hour_aggregator_avg_purchase_amount_null_when_no_purchases` | 0 purchases → `avg_purchase_amount is None` |
| `test_twenty_four_hour_aggregator_avg_purchase_amount` | 2 purchases (\$10, \$30) → `avg_purchase_amount=20.0` |
| `test_twenty_four_hour_aggregator_excludes_click_count` | record's `click_count is None` regardless of input |
| `test_aggregator_revenue_sums_price_times_quantity` | Two purchases (\$10×2, \$5×3) → `revenue=35.0` |
| `test_aggregator_merge_is_associative` | `merge(a, merge(b, c)) == merge(merge(a, b), c)` for randomized accumulators |
| `test_aggregator_merge_is_commutative` | `merge(a, b) == merge(b, a)` |
| `test_aggregator_add_is_idempotent_on_unknown_event_type` | Event with unrecognized `event_type` → accumulator unchanged |
| `test_pane_arithmetic_5m_has_5_panes` | `panes_per_window(size=5min, slide=1min) == 5` |
| `test_pane_arithmetic_1h_has_12_panes` | `12` |
| `test_pane_arithmetic_24h_has_24_panes` | `24` |
| `test_pane_arithmetic_rejects_non_divisible` | `panes_per_window(size=5min, slide=2min)` raises `ValueError` |
| `test_sliding_feature_record_idempotency_key_format` | `{user_id}:{resolution.value}:{window_end_ms}:{emission_seq}` |
| `test_sliding_feature_record_redis_field_updates_5m` | Returns dict with `clicks_5m`, `page_views_5m`, `purchases_5m`, `revenue_5m` |
| `test_sliding_feature_record_redis_field_updates_omits_none` | A null field is not present in the returned dict |
| `test_sliding_feature_record_avro_round_trip_5m` | Pydantic → Avro → Pydantic → equal (with nullable fields preserved) |
| `test_sliding_feature_record_avro_round_trip_24h` | Same as above for the 24 h slice |
| `test_window_resolution_enum_values_stable` | `WindowResolution.W_5M_SLIDE_1M.value == "5m"` (locks the on-the-wire value) |
| `test_window_resolution_enum_avro_symbols_stable` | Avro enum symbols match `WindowResolution.__members__.keys()` |
| `test_sliding_job_config_validates_topics_distinct` | `source_topic == sink_topic` → `ValidationError` |
| `test_sliding_job_config_rejects_negative_ttl_factor` | `ttl_factor <= 0` → `ValidationError` |
| `test_sliding_job_config_rejects_lateness_above_window` | `allowed_lateness_seconds > 300` (5 m window's full size) → `ValidationError` |
| `test_watermark_strategy_uses_5s_bound` | Inspect the returned strategy → 5 s |
| `test_watermark_strategy_extracts_event_timestamp_ms` | Extract from Pydantic `event_timestamp_ms` field |
| `test_redis_hash_sink_uses_hset_pipeline` | Mock Redis client → single pipeline call with HSET + EXPIRE |
| `test_redis_hash_sink_expire_uses_xx_flag` | TTL set with `XX` so a longer existing TTL is preserved |
| `test_redis_hash_sink_field_names_carry_resolution_suffix` | 5 m record → fields end with `_5m` |
| `test_redis_hash_sink_skips_null_fields` | A SlidingFeatureRecord with `click_count=None` → no HSET for `clicks_*` |
| `test_kafka_sliding_sink_key_format` | Mock producer → `key=f"{user_id}:{resolution.value}"` |
| `test_kafka_sliding_sink_value_is_avro_encoded` | Mock producer → `value` decodes back via Avro deserializer to equal record |
| `test_sliding_window_emitter_assigns_window_end_ms` | Mock window context with end=1000 → emitted record has `window_end_ms=1000` |
| `test_sliding_window_emitter_increments_emission_seq_on_refire` | Two fires for same window → `emission_seq` 0, then 1 |
| `test_sliding_window_emitter_resets_emission_seq_on_new_window` | First fire of next window → `emission_seq=0` |
| `test_redis_field_resolution_suffix_clicks_5m` | `click_count` on a 5 m record → field `clicks_5m` (note: plural; encoded by `redis_field_updates`) |
| `test_redis_field_resolution_suffix_purchases_24h` | `purchase_count` on a 24 h record → field `purchases_24h` |
| `test_redis_field_resolution_suffix_distinct_products_1h` | → field `distinct_products_1h` |
| `test_ttl_per_resolution_5m_is_7p5_min` | TTL for 5 m resolution = 1.5 × 5 min = 450 s |
| `test_ttl_per_resolution_1h_is_90_min` | TTL for 1 h resolution = 5400 s |
| `test_ttl_per_resolution_24h_is_36h` | TTL for 24 h resolution = 129600 s |

Coverage target: **100% line + branch** for
`src/streaming_feature_store/flink/sliding/`.

---

## 6. Integration Tests

Integration tests use real Kafka + real Flink (via the `flink-up`
Makefile target — brings up the Flink JobManager and two
TaskManagers) + real Redis. Marked `@pytest.mark.integration`;
skipped if `docker compose ps` reports no running services.

| Test | Setup → Assertion |
|---|---|
| `test_sliding_features_topic_auto_created` | Start job → `sliding-features` exists with 12 partitions, RF=3, 7 d retention, not compacted |
| `test_sliding_features_late_topic_auto_created` | `sliding-features-late` with 3 partitions, 30 d retention |
| `test_sliding_features_avro_schema_registered_backward` | `sliding-features-value` registered with `BACKWARD` compat |
| `test_sliding_features_5m_window_emits_per_minute` | Produce 1 click for `u1`, drive event time forward 5 min in 1 min increments → ≥ 5 records on `sliding-features` for key `u1:5m`, one per minute |
| `test_sliding_features_5m_clicks_count_matches_input` | Produce 7 clicks for `u1` in the same 1 min pane → emitted 5 m record has `click_count=7` |
| `test_sliding_features_5m_clicks_decay_as_panes_age_out` | Produce 7 clicks at t=0, advance to t=6 min → 5 m record at t=6 has `click_count=0` (window has slid past the original pane) |
| `test_sliding_features_1h_includes_distinct_products` | Produce page-views on 3 distinct products in 1 h → `distinct_products=3` |
| `test_sliding_features_24h_excludes_clicks` | A 24 h record's `click_count is None`, regardless of how many clicks were sent |
| `test_sliding_features_three_resolutions_emit_independently` | Produce traffic 65 min, observe all 3 keys (`u1:5m`, `u1:1h`, `u1:24h`) on `sliding-features` — note their emission cadences differ (every 1 min, every 5 min, every 1 h) |
| `test_sliding_features_late_event_re_emits_with_higher_seq` | Produce 3 clicks, allow window to close, produce 1 late click within 30 s → 2 records observed for the affected window, second has `emission_seq=1` and higher count |
| `test_sliding_features_very_late_event_goes_to_side_output` | Produce one click 60 s past window close → 0 new records on `sliding-features` for that window, 1 record on `sliding-features-late` |
| `test_sliding_features_redis_hash_carries_all_three_resolutions` | After 65 min of traffic for `u1` → `HGETALL feat:user:u1` returns fields with `_5m`, `_1h`, `_24h` suffixes |
| `test_sliding_features_redis_ttl_per_resolution` | After first emission of each resolution → `TTL feat:user:u1` matches the longest resolution's TTL (36 h, because `EXPIRE XX` retains the longest) |
| `test_sliding_features_redis_zero_fill_after_ttl_expiry` | Force TTL expiry (set TTL to 5 s in test fixture) on a 5 m field → `HGET feat:user:u1 clicks_5m` returns nil, downstream adapter coerces to 0 |
| `test_sliding_features_idempotent_under_checkpoint_restart` | Run job 30 s, kill TaskManager, restart from checkpoint → no duplicates on `sliding-features` for the same `(user_id, window_end_ms, resolution, emission_seq)` 4-tuple |
| `test_sliding_features_handles_zipfian_skew` | Drive feeder traffic 5 min → no partition lag grows unbounded, per-key counts match feeder ground truth |
| `test_sliding_features_hot_key_emission_continuity` | Force one user to receive ~10× the average rate → 5 m emissions for that user fire on the same 1 min cadence as for cold users (no head-of-line blocking) |
| `test_sliding_features_watermark_advances_under_idle_partition` | Kill the feeder for one partition's keys, leave others active → watermark advances after 30 s idleness, sliding emissions for active partitions continue on schedule |
| `test_sliding_features_end_to_end_with_validator` | Stack: feeder + validator (week2_01) + this job; produce 65 min of synthetic traffic → `sliding-features` per-resolution per-key counts match feeder log within 1 emission of lateness tolerance |
| `test_sliding_features_24h_avg_purchase_amount_null_for_no_purchase_user` | A user with only clicks → 24 h record has `avg_purchase_amount is None` (round-trips through Avro) |
| `test_sliding_features_kafka_key_per_resolution` | Inspect raw Kafka records → keys are `{user_id}:{5m|1h|24h}` |
| `test_sliding_features_no_emission_when_no_events_in_slide` | A user emits 1 event at t=0, then is silent → no emissions for that user from t=2min onward (zero-fill is downstream's job, §2.7) |

---

## 7. How to Run

### 7.1 One-time bootstrap

```
make infra-up                  # Kafka + Postgres + Registry + Redis
make flink-up                  # Flink JobManager + 2 TaskManagers
make topic-ensure              # ensures e-commerce-events, -feed
make register-schemas-feed     # PR #2 prereq
                               # sliding-features-value and the late
                               # topic schema are auto-registered on
                               # job startup (§4.5)
```

### 7.2 Start the pipeline

```
make feeder-run                # PR #6: 200 evt/s feeder
make sink-run                  # PR #6: Postgres sink
make validator-run             # week2_01: validator
make sliding-submit            # THIS PR: PyFlink job
```

### 7.3 Inspect

> **Wait before inspecting.** The first 5-minute window emission only
> fires after the watermark advances past a window-end + slide tick — so
> expect ~5 min of event-time before any record lands. The 1 h window
> takes ~1 h of event-time; the 24 h window takes ~24 h.  All commands
> below assume the job is already running (§7.2).

#### Step 1 — Confirm records are flowing into the output Kafka topic

Check partition end-offsets first (cheap; tells you at a glance whether
anything has been written). Any non-zero offset means emissions are
reaching Kafka:

```
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features
docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features-late
```

Then peek at a few records (Avro-encoded; binary-ish output is normal):

```
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features \
  --from-beginning --max-messages 6
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features-late \
  --from-beginning --max-messages 5
```

#### Step 2 — Online-store inspection (Redis)

Discover a user that has actually been emitted (don't hardcode an id —
pick one the running job has touched):

```
USER=$(docker exec redis redis-cli --scan --pattern "feat:user:*" | head -1)
echo "$USER"
# example output: feat:user:user-00042
```

Read all three resolutions in one round trip:

```
docker exec redis redis-cli HGETALL "$USER"
# expected fields (subset):
#   clicks_5m, page_views_5m, purchases_5m, revenue_5m,
#   clicks_1h, page_views_1h, purchases_1h, revenue_1h, distinct_products_1h,
#   purchases_24h, revenue_24h, distinct_products_24h, avg_purchase_amount_24h
```

Per-resolution Kafka filtering for that specific user (the Kafka key is
`{user_id}:{resolution.name}`):

```
USER_ID="${USER#feat:user:}"           # strip the "feat:user:" prefix
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features \
  --from-beginning --max-messages 5 \
  --property print.key=true --property key.separator=' | ' \
  | grep "^${USER_ID}:W_5M_SLIDE_1M"
```

#### Step 3 — Flink web UI (optional)

`http://localhost:8082` reaches the Docker JobManager (per
[docker-compose.yml:320](../../docker/docker-compose.yml#L320), external
8082 maps to the JM's internal 8081).  Use the UI for per-operator
backpressure / watermark / checkpoint-size panels.

> **Caveat:** the current
> [`_build_execution_environment`](../../scripts/run_sliding_features_job.py)
> returns a local minicluster environment for *both* branches, so the
> running job is *in-process inside the Python script* and the Docker
> JobManager's UI will report an empty job list.  To make the UI useful
> the script needs a `RemoteStreamEnvironment(host, port)` for the
> non-`--local` path (recorded as a §9 follow-up).

#### Sanity check #1 — Are the three resolutions firing at the expected cadences?

```
USER_ID="${USER#feat:user:}"
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic sliding-features \
  --from-beginning --max-messages 30 \
  --property print.key=true --property print.timestamp=true \
  --property key.separator=' | ' \
  | grep "${USER_ID}:W_5M_SLIDE_1M"
```

Records for the `5m` resolution should appear at approximately 1-minute
timestamp intervals (one per slide tick).  For `1h` the cadence is
5 min; for `24h` it is 1 h.  A cadence noticeably slower than expected
indicates a stuck watermark (see §7.4).

#### Sanity check #2 — Is the Redis hash actually populated for every active user?

```
docker exec redis redis-cli --scan --pattern "feat:user:*" | wc -l
docker exec redis redis-cli --scan --pattern "feat:user:*" | head -20
```

The number of returned keys should be roughly equal to the count of
distinct users that emitted at least one event in the last 36 h (the
24 h TTL × 1.5 ceiling, §2.7). A *much* smaller number indicates the
Redis sink is dropping writes; a *much* larger number indicates TTLs
are not firing.

#### Sanity check #3 — Is the watermark advancing?

Flink web UI → Job → Watermarks. All three windowed operators should
report a watermark within ~5 s of wall-clock time. A growing
"watermark lag" on any operator stops all downstream emissions for
that resolution. *(See the Step 3 caveat above — only meaningful once
the script submits to the remote JobManager rather than a local
minicluster.)*  As a local-minicluster substitute, watch the
`Retrieving state N times costed M seconds` warnings in
`.venv/lib/python3.12/site-packages/pyflink/log/flink-*.log`: if the
state-retrieval count keeps climbing but no records appear in Redis
within the first 5–10 min of event-time, the watermark is the suspect.

### 7.4 CLI

```
python scripts/run_sliding_features_job.py \
    --bootstrap kafka-1:9092 \
    --registry http://schema-registry:8081 \
    --source-topic validated-events \
    --sink-topic sliding-features \
    --late-sink-topic sliding-features-late \
    --out-of-orderness-seconds 5 \
    --allowed-lateness-seconds 30 \
    --parallelism 12 \
    --redis-host redis --redis-port 6379
```

### 7.5 Tear down

```
make sliding-cancel
make flink-down
make infra-down
```

---

## 8. Resource Budget & Constraints

| Component | CPU | RAM | Disk |
|---|---|---|---|
| Flink JobManager | <0.2 core | ~500 MB | minimal |
| Flink TaskManager × 2 | ~1 core total | ~2 GB total | ~500 MB RocksDB |
| Redis (sliding sink) | <0.1 core | ~80 MB at 100 k active users | — |
| Kafka (sliding-features) | (existing) | (existing) | ~150 MB/day at 200 evt/s, ≤ 36 h retention |

State math:

| Resolution | Panes / user | Bytes / pane (approx) | Bytes / user |
|---|---|---|---|
| 5 m / 1 m | 5 | ~40 | 200 |
| 1 h / 5 m | 12 | ~150 (incl. distinct-product set) | 1800 |
| 24 h / 1 h | 24 | ~250 (incl. distinct-product set) | 6000 |
| **Total per active user** | | | **~8 KB** |

At ~5 k active users at feeder rate → ~40 MB of pane state in
RocksDB, with the `distinct_products` set being the dominant
contributor at the 1 h and 24 h resolutions. The exact-set
representation is the planned replacement target for §9's
HyperLogLog follow-up.

Constraints:

- **Watermark lag.** Steady-state watermark lag is bounded above by
  the 5 s out-of-orderness budget plus per-event processing time.
  Alerted by the Week 5 freshness monitor when
  `watermark_lag_seconds > 30` for any operator.
- **Emission rate ceiling.** ~5 k active users × 1 emission / min for
  the 5 m resolution = ~80 emissions / second, well within Kafka
  produce capacity at any RF.
- **Hot-user TaskManager.** The Zipfian feeder's top-bucket user
  receives ~10× the average rate. The per-event aggregator cost is
  O(1); the bottleneck is PyFlink UDF-crossing, not state. Recorded
  in §2.12.

---

## 9. Future Considerations

1. **HyperLogLog `distinct_products`.** The exact-set representation
   in `SlidingAccumulator.distinct_products` is bounded by per-window
   product cardinality (a few thousand for synthetic data, ~10⁵+ for
   production). Swapping in HLL gives ~1.5 % error at 1.5 KB of state
   regardless of cardinality. Touches `add`, `merge`, `get_result` —
   localized to `SlidingWindowAggregator`. Ship once the §10
   open-question on `distinct_products` cardinality is settled.
2. **Two-level aggregation for hot-key mitigation.** §2.12 deferred
   this. Pattern: pre-aggregate per `(user_id, hash(salt) % N)`
   sub-key, then re-key by `user_id` and final-aggregate. Reduces
   the per-key fan-in for the hottest users by a factor of N at the
   cost of one extra shuffle. Recorded for the Phase 4 K8s deploy.
3. **Zero-tick stream for true decay-to-zero.** §2.7 deferred this.
   A parallel `ProcessFunction` keyed on the same `user_id` space
   that fires a `TimerService` event at every slide tick and emits
   a zero record if the user had no events in that pane. Adds
   `users × slide_rate` emissions; cost is real but the feature
   correctness is closer to what some downstream models want.
4. **Sub-second processing-time path.** §2.2 noted the 1-minute slide
   floor. A *processing-time*-driven parallel emitter that reads
   from the same keyed state and emits on every event (or every
   100 ms) is the right answer for sub-second-freshness online
   inference. Recorded for the Phase 4 recommendation feature
   platform.
5. **Cross-resolution temporal join into one record.** §1's "out of
   scope" enumerates this; the Redis hash already gives the
   read-side merge effectively for free. A Flink-side temporal join
   that produces a single `UnifiedFeatureVector` record per emission
   tick is recorded as a portfolio-write-up extension.
6. **Java rewrite of the aggregators.** PyFlink UDFs serialize Python
   objects across the JVM boundary on every event; at benchmark-rate
   this becomes the dominant cost. A Java AggregateFunction with the
   same semantics is a clean migration — topology declaration stays
   in PyFlink, only the hot path (`add` + `merge`) crosses to the
   JVM. The resolution-specific `getResult` sub-classing pattern
   translates 1-to-1.
7. **S3-backed checkpoint store.** Local-volume checkpoints work for
   laptop scope; the Phase 4 K8s deploy needs S3-/MinIO-backed
   checkpoints to survive pod restarts.
8. **`bot_score_5m` and behavioral abuse features.** §1 noted the
   exclusion. Computed from sub-second click-burstiness inside the
   5 m window's per-pane state; needs a per-event timestamp list in
   the accumulator, which violates the O(1)-state-per-pane property
   §2.3 builds on. The right home is a *separate* fraud-feature job
   downstream of `validated-events`, not this one.
9. **Per-resolution Kafka topic split.** §2.8 went with one topic +
   discriminator. The §10 open-question on offline-consumer access
   patterns may revise this in favor of three topics.
10. **Continuous (per-event) emission alongside per-slide emission.**
    A parallel `ProcessFunction` that re-reads the keyed window
    state on every event and emits a "fresh" record to a separate
    `sliding-features-realtime` topic. Cost: ~one Kafka produce per
    event. Recorded for a future Phase 3 LLM-recommendation
    integration that wants intra-slide freshness.

---

## 10. Open Questions

1. **Is `distinct_products` worth keeping in `SlidingAccumulator` at
   exact precision?** The synthetic feeder uses a small product
   catalog (low hundreds of SKUs), so the per-pane set never blows
   up. But a 24 h window over real e-commerce traffic with a
   ~10⁵-SKU catalog would. The 24 h smoke run will surface the
   cardinality distribution; the §9 HLL swap is gated on it
   exceeding a threshold (proposal: 10 k distinct products in any
   24 h pane per user).
2. **Should `clicks_24h` and `page_views_24h` be added to the 24 h
   resolution after all?** §2.14 excluded them on the "browse-intent
   decays fast" argument, but a recommender that wants long-tail
   engagement signal might want them. Cost is two extra integer
   counters per 24 h pane → negligible. Decision deferred until the
   first downstream consumer (Phase 3 LLM serving) declares its
   feature schema.
3. **24 h-pane parallelism.** §2.4 noted that the 24 h operator's
   longer state hold may justify lower parallelism. The smoke run's
   per-TaskManager-checkpoint-size measurement is the discriminator;
   if 24 h checkpoints exceed 100 MB on a single slot, drop
   parallelism to 4.
4. **Hot-user emission-cadence drift.** §2.12 assumes pane-based
   aggregation insulates the per-event throughput. If the smoke run
   shows the top-bucket user's 5 m emissions arriving >5 s after
   the slide tick (indicating UDF-crossing back-pressure on that
   keyed channel), the §9 two-level aggregation mitigation moves
   from "future" to "this PR's blocker."
5. **Should `sliding-features` be split into three per-resolution
   topics?** §2.8 chose one topic with a discriminator. The argument
   is operational simplicity; the counter-argument is per-resolution
   retention tuning (24 h-pane records may benefit from a longer
   retention than 5 m-pane records). Deferred until a real
   offline-consumer access pattern is established (Week 4 will be
   the test).
6. **Compatibility of the `WindowResolution` enum with future
   resolutions.** Adding e.g. `W_15M_SLIDE_5M` requires extending the
   Avro enum, which is `BACKWARD`-compatible only for *appending*
   symbols. The integration test
   `test_window_resolution_enum_avro_symbols_stable` enforces no
   reordering, but the "is this enum the right abstraction?"
   question deserves a §9-level revisit if more than ~6 resolutions
   ever ship.
7. **Should `emission_seq` participate in the Kafka message key?**
   Currently the key is `{user_id}:{resolution.value}` and the seq
   lives in the payload. A consumer that wants to read *only* the
   latest emission per window cannot do so by Kafka-level filtering;
   it has to deserialize each payload. Moving `emission_seq` into
   the key would enable compaction-by-key but breaks the
   "latest-emission-wins per window" semantics. Decision: leave as
   is; revisit if compaction becomes a need.
8. **TTL `XX` flag semantics under multi-resolution writes.** The
   §2.8 design uses `EXPIRE feat:user:{user_id} <ttl> XX` so each
   emission tries to set the TTL but only if one already exists.
   The first emission for a user — when *no* TTL exists yet — needs
   a *non-XX* `EXPIRE` to set the initial value. Current sink does
   this via `SET feat:user:{user_id} __init__ NX EX 1` followed by
   the actual writes; whether this two-step is right or whether a
   `SETEX feat:user:{user_id}:tombstone` sentinel approach is
   cleaner is a code-review-time choice rather than a design-time
   one. Recorded here so reviewers know the question exists.
