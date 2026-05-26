# Design Doc: Inline Validation Stage + Dead-Letter-Queue Routing

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 2 — Validation & Feature Computation
**Scope:** First bulletpoint of Week 2 — Implement an **inline validation stage** in the stream processor that checks every incoming event for (a) null/missing required fields, (b) out-of-range values, (c) malformed records, and (d) schema conformance against the registry, and **routes invalid events to a `dead-letter-queue` topic** with structured error metadata for debugging — line 79 of `gap_project_plan.md`. Out of scope: feature computation (Week 2 PR #2), EOS transactional wrapping (Week 2 PR #3).
**Author:** Auto-generated design document
**Date:** 2026-05-25

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

By the end of Week 1 the repository has a working end-to-end Kafka
pipeline: an idempotent producer ([`week1_04`](week1_04_synthetic_event_producer.md)),
a consumer-group-of-processes latency harness ([`week1_05`](week1_05_consumer_group_end_to_end_latency.md)),
a Postgres sink with idempotent inserts and a low-rate background feeder
([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md)), and a
Confluent Schema Registry that mediates Avro de/serialization
([`week1_02`](week1_02_avro_schemas_and_producer_serialization.md),
[`week1_03`](week1_03_schema_evolution_experiments.md)).

What the pipeline does **not** yet have is an **in-stream quality gate**.
The existing `AvroEventConsumer` already rejects messages whose bytes
fail to deserialize against the registered schema (Avro
`SerializationException`) and whose decoded payload fails the
`avro_dict_to_event → EcommerceEvent` Pydantic validation. But:

- A deserialize/validation failure today is a **dropped message** with a
  single `WARN` log line. The bad bytes are gone, the offset is
  committed, and there is no forensic trail.
- Only *schema-conformance* and *Pydantic-required-field* errors are
  caught. There is **no out-of-range checking** (negative prices,
  timestamps in the future, quantity ≤ 0, unknown event types) — those
  values pass through to PostgreSQL and would silently poison Week 2's
  windowed feature computation.
- There is no separate **`validated-events`** topic that Week 2 PR #2
  (feature computation) can subscribe to with the contract *"every
  message you read here has already been schema- and value-checked."*

This PR closes those three gaps:

- **`ValidationPipeline`** (`src/streaming_feature_store/validate/`) — a
  composable chain of stateless validators (schema conformance,
  required-field non-null, out-of-range, malformed-record, event-type
  allowlist). Each validator returns either `Valid(event)` or
  `Invalid(reason)`; the pipeline short-circuits on the first
  `Invalid` and emits a structured `DlqRecord` describing exactly which
  validator rejected the message.
- **`ValidatorRunner`** — a single-process daemon (with the same
  multi-process consumer-group escape hatch as PR #5 / PR #6) that
  consumes from `e-commerce-events-feed`, applies the pipeline, and
  *routes* each message to one of two output topics:
  - `validated-events` for `Valid(...)` results — Week 2 PR #2 will read
    from this topic;
  - `dead-letter-queue` for `Invalid(...)` results — Avro-serialized
    `DlqRecord` envelopes with the original raw bytes preserved for
    forensic replay.
- **`DlqRecord`** Avro schema — registered in the Schema Registry under
  subject `dead-letter-queue-value` with `BACKWARD` compatibility, so
  future validator additions can extend the error taxonomy without
  breaking downstream forensic readers.

The validator is structurally a sibling of the Postgres sink from
PR #6 (same consume→process→produce→commit loop, same idempotent-write
pattern), so this PR also benefits from the operational lessons of
that PR: graceful-shutdown signal handlers, per-partition message
accounting, at-least-once-read + idempotent-write contract, and a
generated Markdown run report.

### Out of Scope (Deferred to Later PRs)

- **Stateful feature computation (windowed + session features).** Moved
  to Week 2 PR #2 ([`gap_project_plan.md`](gap_project_plan.md) — Week 2
  second bulletpoint). Validation is stateless on purpose; bundling
  windowing in here would conflate two distinct review concerns.
- **EOS transactional wrapping of the consume-validate-produce cycle.**
  Moved to Week 2 PR #3 ([`gap_project_plan.md`](gap_project_plan.md) —
  Week 2 EOS bullet). The validator ships with the same
  at-least-once-read + idempotent-write contract as the sink (idempotency
  keyed on `event_id` UUID for `validated-events`; idempotency keyed on
  `(original_topic, original_partition, original_offset)` for the DLQ —
  see §2.7). Transactional wrapping is a strict additive layer on top of
  that contract, not a rewrite of it.
- **DLQ replay tooling.** Reading from `dead-letter-queue` and feeding
  fixed/corrected messages back into `e-commerce-events` is operationally
  important but pedagogically separate. Deferred to a §9 follow-up.
- **Statistical drift detection on the validator's reject rate.** A
  spike in the `Invalid` rate is a smoke signal worth alerting on, but
  the alerting wiring lands in Week 5
  ([`gap_project_plan.md`](gap_project_plan.md) — freshness monitoring
  bullet), not here.
- **Flink/Kafka Streams implementation.** This PR uses Python on top
  of the existing `AvroEventConsumer` and `AvroEventProducer`. The
  rationale and the Flink migration path are recorded in §2.1.
- **Validation of the benchmark topic** (`e-commerce-events`). The
  benchmark is a ~10 s burst at ~60k evt/s; validating it would
  contaminate latency measurements with an extra hop. A CLI flag opts
  in for benchmark-mode validation runs (separate consumer group,
  separate run-report), but the default consumes only from
  `e-commerce-events-feed`.

### Deliverables

- `src/streaming_feature_store/validate/__init__.py` — package init.
- `src/streaming_feature_store/validate/validators.py` —
  `Validator` protocol + concrete validators
  (`RequiredFieldsValidator`, `PriceRangeValidator`,
  `QuantityRangeValidator`, `TimestampRangeValidator`,
  `EventTypeAllowlistValidator`, `UserIdShapeValidator`).
- `src/streaming_feature_store/validate/pipeline.py` —
  `ValidationPipeline`: composes validators, short-circuits on first
  failure, returns `Valid(event)` or `Invalid(error)`.
- `src/streaming_feature_store/validate/dlq.py` —
  `DlqRecord` Pydantic model, `DlqRecordAvroAdapter`,
  `DlqProducer` wrapper.
- `src/streaming_feature_store/validate/runner.py` —
  `ValidatorRunner`, `ValidatorRunConfig`, `RouteDecision`.
- `src/streaming_feature_store/validate/accountant.py` —
  `ValidatorAccountant`: counters per error class, per-partition
  message counts, validation-latency reservoir.
- `src/streaming_feature_store/validate/report.py` —
  `ValidatorRunReport` Pydantic model + Markdown renderer.
- `src/streaming_feature_store/schemas/avro/dead_letter_record.avsc`
  — Avro schema for DLQ envelopes (registered on first run via
  `SchemaRegistryClient.register_schema`, BACKWARD compatibility).
- `scripts/run_validator.py` — single-process CLI driver.
- `scripts/run_validator_mp.py` — multi-process consumer-group CLI
  driver (mirrors PR #5's `run_consume_test_mp.py`).
- `docs/results/week2_validator_results.md` — generated artifact: 24 h
  smoke-run results including per-validator reject counts, top-10
  failing fields, per-partition skew table.
- `tests/unit/test_validators.py`,
  `test_validation_pipeline.py`,
  `test_dlq_record.py`,
  `test_validator_runner_unit.py`,
  `test_validator_accountant.py`,
  `test_validator_report.py`.
- `tests/integration/test_validator_runner_end_to_end.py`,
  `test_validator_dlq_round_trip.py`,
  `test_validator_multiprocess_pipeline.py`,
  `test_validator_feeder_to_validated_pipeline.py`.
- `Makefile` targets: `validator-run`, `validator-run-mp`,
  `validator-report`, `validator-up`, `validator-down`.

---

## 2. Critical Design Decisions

### 2.1 Python (with MP Consumer Group) vs. Flink for the Validation Stage

**Decision:** Implement the validator as a **Python daemon on top of the
existing `AvroEventConsumer` and `AvroEventProducer`**, with the
multi-process consumer-group escape hatch from PR #5 for throughput
scaling. Defer Flink/Kafka Streams to **Week 2 PR #2** (feature
computation) where stateful processing is the actual driver.

**Rationale:**

- **Validation is stateless.** Each event is checked in isolation; no
  windowing, no keyed state, no watermark reasoning. The two
  capabilities that justify the JVM/Flink complexity tax for feature
  computation
  ([`gap_project_plan.md`](gap_project_plan.md) — Week 2 GIL caveat,
  line 88) are not exercised by this PR.
- **The GIL throughput ceiling is irrelevant at the feeder rate and
  cheap to dodge at benchmark rate.** The continuous feeder produces
  ≈200 evt/s
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.5).
  A single Python validator process at the measured GIL ceiling
  (~11–14k evt/s,
  [`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md)
  §2.2) has ~60× headroom. For the opt-in benchmark mode against
  `e-commerce-events`, the multi-process consumer-group pattern from
  PR #5 (`run_validator_mp.py` shipped in this PR) extends linearly
  to ≈60k evt/s.
- **Code reuse.** The `AvroEventConsumer` already implements
  registry-bound deserialization + the `avro_dict_to_event` adapter
  into `EcommerceEvent`. Building on top of it gives us *schema
  conformance* and *required-field* checks essentially for free —
  the new code is the out-of-range validators and the DLQ routing.
- **Pedagogical sequencing.** Each Week 2 PR introduces one new
  concept on top of Week 1: PR #1 introduces *DLQ routing*, PR #2
  introduces *stateful streaming on Flink*, PR #3 introduces *Kafka
  transactions*. Bundling Flink into PR #1 conflates "the data quality
  pattern" with "the streaming engine choice" and weakens both
  stories.
- **Acknowledged tradeoff.** A production validator on top of a
  high-throughput firehose (≫ feeder rate) would be implemented as a
  Flink `ProcessFunction` for backpressure-aware flow control and
  one-engine-for-the-whole-pipeline operational simplicity. The
  Python-on-`AvroEventConsumer` choice here is deliberate for laptop
  scale and pedagogical cleanliness; the Flink migration is recorded
  in §9 as a portfolio-talking-point follow-up.

### 2.2 Validator as a Separate Stream Processor (Not Bundled with Feature Compute)

**Decision:** The validator and the feature-computation processor
([`gap_project_plan.md`](gap_project_plan.md) — Week 2 second bullet)
are **two physically separate stream processors** with the
`validated-events` topic as the boundary between them, even though they
are listed under the same Week 2 plan section.

**Rationale:**

- **Independent failure isolation.** A bug in the feature processor
  must not block validation from draining the input topic. With the
  topic boundary, the validator keeps running and DLQ keeps emitting
  even when feature compute is stopped for a hotfix.
- **Independent scaling and restart.** Validation is CPU-light and
  stateless; a single process suffices at feeder rate. Feature
  compute is stateful (windowed aggregates, session state) and may
  need its own checkpoint/restart story. Decoupling them means each
  scales on its own axis.
- **Independent review and rollout.** The validator's PR is reviewed
  by reviewers who care about data quality and DLQ semantics; the
  feature processor's PR is reviewed by reviewers who care about
  windowing correctness and Redis sink idempotency. Bundling them
  forces every reviewer to context-switch.
- **Backpressure boundary.** The `validated-events` topic acts as a
  buffer: a transient slowdown in the feature processor lets
  `validated-events` accumulate offset lag without dropping data
  from the input topic. This is the standard streaming
  decomposition pattern.
- **Plan-level alignment.** The earlier "Are these two bulletpoints
  preferably for two separate PRs?" discussion ([this PR is the first
  of the two](gap_project_plan.md#L79-L82)) reached the same
  conclusion. This decision codifies it at the design-doc level.

### 2.3 DLQ Format: Avro Envelope with Preserved Original Bytes

**Decision:** The dead-letter-queue topic carries Avro-serialized
`DlqRecord` envelopes. Each envelope contains the **original raw value
bytes** of the rejected message verbatim, plus structured error
metadata (validator name, error class, error field path, human-readable
message), plus Kafka source coordinates
(`original_topic`, `original_partition`, `original_offset`,
`original_timestamp_ms`, `original_key_bytes`).

**Schema (`dead_letter_record.avsc`):**

```
{
  "type": "record", "namespace": "com.featurestore.dlq", "name": "DlqRecord",
  "fields": [
    {"name": "schema_version", "type": "int", "default": 1},
    {"name": "original_topic",       "type": "string"},
    {"name": "original_partition",   "type": "int"},
    {"name": "original_offset",      "type": "long"},
    {"name": "original_timestamp_ms","type": "long"},
    {"name": "original_key_bytes",   "type": ["null", "bytes"], "default": null},
    {"name": "original_value_bytes", "type": "bytes"},
    {"name": "rejected_at_ms",       "type": "long"},
    {"name": "error_class",          "type":
      {"type": "enum", "name": "ErrorClass",
       "symbols": ["DESERIALIZE_FAILURE", "SCHEMA_MISMATCH",
                   "NULL_REQUIRED_FIELD", "OUT_OF_RANGE",
                   "MALFORMED_RECORD", "UNKNOWN_EVENT_TYPE",
                   "PIPELINE_INTERNAL_ERROR"]}
    },
    {"name": "validator_name",       "type": "string"},
    {"name": "error_field_path",     "type": ["null", "string"], "default": null},
    {"name": "error_message",        "type": "string"},
    {"name": "validator_version",    "type": "string", "default": "1.0.0"}
  ]
}
```

**Rationale:**

- **Original bytes preserved → replay is loss-free.** A future
  validator-fix-plus-replay job (§9) reads the DLQ, lifts
  `original_value_bytes`, and re-publishes to `e-commerce-events-feed`.
  If we stored a *decoded* representation, we would lose any field that
  the rejected message had but the current schema did not — exactly
  the kind of subtle bug DLQ forensics is for.
- **Avro on the DLQ for stack consistency.** The whole repo speaks
  Avro/Registry; adding a JSON-only DLQ would mean an exception path
  for every downstream tool (forensic reader, replay job, monitoring
  dashboard).
- **`error_class` as a registered enum.** Adding a new validator may
  require a new enum symbol. Avro `BACKWARD` compatibility allows
  adding symbols *if and only if* the writer's old symbols remain in
  the new schema. The enum is **append-only** going forward — this is
  enforced by `RegistryCompatibilityModeError` checks in CI (PR #3
  added the test harness for this).
- **`schema_version` field defaulted to 1.** A literal version-int
  alongside the schema-registry-id lets a forensic reader make
  schema-aware decisions without round-tripping through the registry
  (cheaper for one-off scripts).
- **`original_key_bytes` is nullable.** Some test producers send
  null-keyed messages; the DLQ must accept them too.
- **`validator_version` semver string.** Lets §9 replay tools skip
  DLQ records produced by validator versions that have since been
  fixed (i.e. "replay only `validator_version <= 1.0.3` records,
  current is 1.0.4").

### 2.4 Topology: `e-commerce-events-feed` → `validated-events` + `dead-letter-queue`

**Decision:** The default topology is:

```
e-commerce-events-feed  ──▶  Validator  ──▶  validated-events
                                       ╲──▶  dead-letter-queue
```

End-to-end, this PR's validator sits in the middle of the following
fan-out — *parallel to* the Postgres sink, *upstream of* the Week 2 PR #2
Flink feature processor:

```
                                  ┌──▶ sink ──▶ raw_events (Postgres)         [forensic-complete]
feeder ──▶ e-commerce-events-feed ┤
                                  └──▶ Validator (this PR) ──┬──▶ validated-events ──▶ Flink FeatureProcessor ──▶ Redis (online store)
                                                             │                              [Week 2 PR #2]            [Week 2 PR #2]
                                                             └──▶ dead-letter-queue ──▶ forensic / replay
```

The validator does **not** consume from `e-commerce-events` (the
benchmark topic) by default. An opt-in CLI flag (`--source bench`)
switches to consuming from `e-commerce-events`, using a separate
consumer group (`validator-bench`) so the daily-flow validator
(`validator-feed`) is unaffected.

**Rationale:**

- **Continuous flow → validated flow.** The continuous feeder
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.4) is
  the steady-state source of truth for downstream processors. Feature
  computation in PR #2 will read from `validated-events`, which is fed
  from `e-commerce-events-feed` via this validator.
- **Benchmark-mode isolation.** Validating a 10 s burst of ~60k evt/s
  on the *same* consumer group as the continuous validator would
  thrash partition assignment and contaminate the steady-state
  numbers in `validator-feed`'s accountant. A separate group keeps
  the two modes operationally independent.
- **`raw_events` (Postgres) bypasses validation by design.** The sink
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.7)
  writes every event — valid or not — to `raw_events` so the offline
  table has *forensic-complete* coverage of what the producer actually
  emitted. Offline computation in Week 4 can join `raw_events` against
  `dead-letter-queue` on `(topic, partition, offset)` to reconstruct
  which raw rows were excluded from `validated-events` and why. This
  is one of the structural-skew sources the Week 4 consistency report
  will quantify.

### 2.5 Validator Catalog and Per-Field Checks

**Decision:** Six concrete validators are shipped in this PR, applied
in this fixed order:

1. **`RequiredFieldsValidator`** — `event_id`, `event_type`, `user_id`,
   `event_timestamp` non-null.
2. **`EventTypeAllowlistValidator`** — `event_type ∈ {click, page_view,
   add_to_cart, purchase, search}`.
3. **`UserIdShapeValidator`** — `1 ≤ len(user_id) ≤ 256`, no embedded
   newlines (cheap PII-sanity guard).
4. **`PriceRangeValidator`** — when `event_type == "purchase"`,
   `0 < price ≤ 1e9` and `quantity ≥ 1`. (Other event types skip this
   validator via `applies_to`.)
5. **`QuantityRangeValidator`** — `1 ≤ quantity ≤ 10_000` for all event
   types that carry `quantity`.
6. **`TimestampRangeValidator`** — `event_timestamp ∈
   [now − 7 days, now + 1 hour]`. The future-allowance accommodates
   producer-side clock skew (a few minutes is typical;
   1 hour is generous).

**Pipeline semantics:** the pipeline is **short-circuit on first
failure** — the first validator to return `Invalid(...)` wins. Reasons:

- A message with `null user_id` and `negative price` has two real
  problems; reporting only the first (null required field) is fine
  because the message must be fixed at the source anyway.
- Short-circuit avoids one pathological case: a malformed payload
  causing a validator-internal exception that masks the *actual*
  upstream defect.

**Rationale for the catalog:**

- These checks are the *minimum* set that catches the failure modes
  Week 2 feature computation cannot tolerate:
  - **Null required field** → keyed-state lookups crash, windows
    accumulate on a phantom key.
  - **Out-of-range price/quantity** → aggregated features (sum,
    average) become NaN or absurd.
  - **Future timestamp** → window assignment puts the event into a
    not-yet-emitted bucket; watermark advances unexpectedly; downstream
    sees a 7-day-late `clicks_5m` "spike."
  - **Schema mismatch / deserialize failure** → caught by the
    underlying `AvroEventConsumer`; this PR's contribution is *routing*
    those to DLQ instead of dropping.
- The per-`event_type` `applies_to` filter on `PriceRangeValidator` is
  important: a `click` event has no `price` field, and triggering an
  out-of-range failure on `null price` for a `click` would be a
  validator bug, not real data quality signal.

### 2.6 Error Classification Taxonomy

**Decision:** Every rejection maps to exactly one `ErrorClass` enum
value from §2.3. The mapping is:

| Validator | `error_class` |
|---|---|
| Underlying `AvroEventConsumer` deserialize failure | `DESERIALIZE_FAILURE` |
| Underlying `AvroEventConsumer` schema-incompatibility | `SCHEMA_MISMATCH` |
| `RequiredFieldsValidator` | `NULL_REQUIRED_FIELD` |
| `EventTypeAllowlistValidator` | `UNKNOWN_EVENT_TYPE` |
| `UserIdShapeValidator` | `MALFORMED_RECORD` |
| `PriceRangeValidator`, `QuantityRangeValidator`, `TimestampRangeValidator` | `OUT_OF_RANGE` |
| Pipeline-internal exception (validator code bug) | `PIPELINE_INTERNAL_ERROR` |

**Rationale:**

- **Cardinality matters.** A `WARN`-level histogram by `error_class`
  is the first thing operators look at; six classes is small enough
  to fit on one dashboard row, large enough to discriminate root
  causes.
- **Per-validator detail is preserved in `validator_name` +
  `error_field_path`.** The enum is the *bucket*; the human-readable
  string is the *story*. Splitting them lets dashboards aggregate by
  the bucket while drill-down still has the full context.
- **`PIPELINE_INTERNAL_ERROR` exists specifically to surface
  validator-code bugs.** A `try/except` around each validator's
  `validate()` call converts any unexpected exception into
  `Invalid(PIPELINE_INTERNAL_ERROR)` rather than letting the
  validator process crash. The integration test
  `test_validator_runner_routes_internal_exception_to_dlq` enforces
  this: a deliberately-broken validator must not kill the runner,
  it must DLQ the offending message with the exception traceback in
  `error_message`.

### 2.7 At-Least-Once Read + Idempotent Write (Same Contract as the Sink)

**Decision:** The validator inherits the sink's read-batch-write-commit
ordering ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md)
§2.7), with **two** downstream writes per source message:

```
1. poll() / deserialize                 # at-least-once read
2. validate()
3. if Valid:   produce → validated-events
   if Invalid: produce → dead-letter-queue (DlqRecord envelope)
4. producer.flush()                     # forces broker ack on both topics
5. consumer.commit(asynchronous=False)  # offset commit ordered after step 4
```

**Idempotency keys:**

- `validated-events`: message **key = `event_id` UUID**. A future
  replay of the same source offset re-emits with the same key; a
  downstream consumer that wants exactly-once feature compute (PR #3)
  can dedupe on `event_id` if needed. Within this PR, the contract is
  *at-least-once write to `validated-events`*; PR #3 upgrades it.
- `dead-letter-queue`: message **key = `f"{topic}:{partition}:{offset}"`**.
  Replays of the same rejection produce a DLQ record with the
  *identical* key, so forensic readers can dedupe at-will. Note: the
  DLQ topic is **not** compacted (a compacted DLQ would discard the
  *first* failure for a given source offset if a later replay
  re-publishes — exactly the wrong direction for a forensic store).

**Rationale:**

- **Symmetric to the sink** — preserves the operational mental model
  developers already learned in PR #6.
- **No Kafka transactions yet.** PR #3 (EOS) adds the atomic
  consume-process-produce wrap. Until then, a crash between steps 3
  and 5 means the message was published to its destination topic
  twice — once before crash, once after restart-and-retry. Both
  consumers (`validated-events` and `dead-letter-queue`) tolerate
  this:
  - `validated-events` consumer is the Week 2 PR #2 feature processor,
    which keys its windowed state on `event_id` and will be designed
    idempotent.
  - `dead-letter-queue` consumers are forensic readers; they
    already have to handle replay/dedupe by source-offset key
    (above).
- **`producer.flush()` before `commit()`.** This is the load-bearing
  ordering. Without the flush, the offset commit could overtake an
  unacked produce request and a broker restart would lose the
  in-flight messages.

### 2.8 Multi-Process Consumer-Group (Default 4 Procs, Opt-In via `--mp 4`)

**Decision:** The single-process `run_validator.py` is the default
deployment; the multi-process `run_validator_mp.py` is opt-in. Default
proc count is **4** when MP mode is active.

**Rationale:**

- **Default = single-process** because the steady-state feeder rate
  (200 evt/s) leaves 60× headroom on a single Python process and
  multi-process orchestration is unnecessary complexity at that scale.
- **Opt-in MP for benchmark mode.** When `--source bench` is set and
  the validator subscribes to `e-commerce-events` (the ~60k evt/s
  burst topic), single-process Python becomes the bottleneck per the
  GIL investigation. The MP pattern from PR #5 lifts directly: one
  Python process per partition-subset, each its own consumer-group
  member with the same `group.id` so Kafka does the work assignment.
- **4 procs by default in MP mode.** With 12 partitions on
  `e-commerce-events`, 4 procs gives each proc 3 partitions —
  consistent with PR #5's choice and well within the ~5× MP speedup
  ceiling measured in the load-test investigation.
- **Per-process accountant + final aggregation.** Same shape as PR #5
  / PR #6: each proc snapshots its own accountant on shutdown; the
  parent CLI aggregates and renders one merged Markdown report.

### 2.9 Graceful Shutdown via Signal Handlers

**Decision:** `SIGTERM` and `SIGINT` set a `threading.Event` flag;
the main poll loop exits at the next iteration. On exit:

- **Flush in-flight Kafka producer queue** before committing offsets.
- **Commit consumer offsets** synchronously, then `consumer.close()`.
- **Aggregate accountant snapshot**, render and emit the run report.

**Rationale:** Identical reasoning to the sink — these are long-running
daemons, `librdkafka` is not signal-handler-safe for actual Kafka
operations, and a SIGKILL would leak in-flight produce queue state.
See [`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.9
for the underlying mechanism.

### 2.10 DLQ Topic Configuration

**Decision:** The validator creates (via the existing
`TopicAdmin.ensure_topic()` from PR #4) two output topics on startup,
both idempotent:

| Topic | Partitions | RF | `retention.ms` | `cleanup.policy` | Compaction key |
|---|---|---|---|---|---|
| `validated-events` | 12 | 3 | 604_800_000 (7 d) | `delete` | — |
| `dead-letter-queue` | 3 | 3 | 2_592_000_000 (30 d) | `delete` | — |

**Rationale:**

- **`validated-events` partition count = 12.** Matches the source
  topic so partition→key mapping is preserved end-to-end (the
  validator re-uses the source message's `user_id` as the key on
  `validated-events`, so downstream feature computation can do
  per-user keyed state without a repartition).
- **`dead-letter-queue` partition count = 3.** A DLQ does not need
  high parallelism — it is a forensic topic with bounded volume. Three
  partitions allow modest concurrent forensic readers without the
  operational overhead of 12-way partitioning.
- **30-day DLQ retention.** Forensic investigations sometimes take
  weeks; 7 days is too short. 30 days is the conventional "we'll
  notice within a sprint" horizon. Storage cost is trivial because
  DLQ volume is expected to be a low single-digit percentage of the
  source rate.
- **Both topics use `cleanup.policy=delete`, not `compact`.** A
  compacted DLQ would drop earlier rejections for the same source
  offset under replay — see §2.7.

---

## 3. Architecture

### 3.1 Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     Validation Pipeline (this PR)                        │
│                                                                          │
│   ┌────────────────────┐                                                 │
│   │ e-commerce-events- │                                                 │
│   │ feed (PR #6 src)   │                                                 │
│   │ (12 partitions)    │                                                 │
│   └─────────┬──────────┘                                                 │
│             │                                                            │
│             ▼                                                            │
│   ┌────────────────────────────────────────┐                             │
│   │ ValidatorRunner                        │                             │
│   │  (single proc default; --mp 4 opt-in)  │                             │
│   │  ┌──────────────────────────────────┐  │                             │
│   │  │ AvroEventConsumer  (PR #2)       │  │                             │
│   │  │  → schema conformance check      │  │                             │
│   │  │  → Pydantic required-field check │  │                             │
│   │  │     (raises ValidationError)     │  │                             │
│   │  └────────────┬─────────────────────┘  │                             │
│   │               │                        │                             │
│   │               ▼                        │                             │
│   │  ┌──────────────────────────────────┐  │                             │
│   │  │ ValidationPipeline               │  │                             │
│   │  │  1. RequiredFieldsValidator      │  │                             │
│   │  │  2. EventTypeAllowlistValidator  │  │                             │
│   │  │  3. UserIdShapeValidator         │  │                             │
│   │  │  4. PriceRangeValidator          │  │                             │
│   │  │  5. QuantityRangeValidator       │  │                             │
│   │  │  6. TimestampRangeValidator      │  │                             │
│   │  └────────────┬─────────────────────┘  │                             │
│   │               │                        │                             │
│   │  Valid(event) │      Invalid(reason)   │                             │
│   │               ▼                        │                             │
│   │  ┌──────────────────────────────────┐  │                             │
│   │  │ RouteDecision                    │  │                             │
│   │  └────────────┬────────────────┬────┘  │                             │
│   │               │                │       │                             │
│   │      AvroEventProducer  DlqProducer    │                             │
│   │       (PR #2)         (DlqRecord)      │                             │
│   └─────────────┬────────────────┬─────────┘                             │
│                 ▼                ▼                                       │
│       ┌─────────────────┐   ┌────────────────────┐                       │
│       │ validated-events│   │ dead-letter-queue  │                       │
│       │ (12 partitions, │   │ (3 partitions,     │                       │
│       │  RF=3, 7d ret)  │   │  RF=3, 30d ret)    │                       │
│       └────────┬────────┘   └─────────┬──────────┘                       │
│                │                      │                                  │
└────────────────┼──────────────────────┼──────────────────────────────────┘
                 │                      │
                 ▼                      ▼
   ┌──────────────────────┐   ┌──────────────────────┐
   │ Week 2 PR #2:        │   │ Forensic readers,    │
   │ FeatureProcessor     │   │ §9 replay tooling    │
   │ (Flink/KStreams)     │   │                      │
   └──────────────────────┘   └──────────────────────┘
```

### 3.2 Main Loop (`ValidatorRunner`)

```
┌── ValidatorRunner.run() ──────────────────────────────────────────────┐
│                                                                       │
│  while not self._shutdown.is_set():                                   │
│      msgs = consumer.poll_batch(timeout=1.0, max_records=500)         │
│                                                                       │
│      for raw in msgs:                                                 │
│          try:                                                         │
│              event = avro_dict_to_event(decode(raw))                  │
│          except DeserializeError as e:                                │
│              dlq_producer.send(                                       │
│                  DlqRecord.from_raw(raw, ErrorClass.DESERIALIZE_     │
│                                     FAILURE, "AvroEventConsumer",    │
│                                     None, str(e)))                    │
│              accountant.record_invalid(ErrorClass.DESERIALIZE_FAILURE)│
│              continue                                                 │
│          except SchemaMismatchError as e:                             │
│              dlq_producer.send(                                       │
│                  DlqRecord.from_raw(raw, ErrorClass.SCHEMA_MISMATCH,  │
│                                     "AvroEventConsumer",              │
│                                     None, str(e)))                    │
│              accountant.record_invalid(ErrorClass.SCHEMA_MISMATCH)    │
│              continue                                                 │
│          except ValidationError as e:                                 │
│              # Pydantic-required-field violation                      │
│              dlq_producer.send(                                       │
│                  DlqRecord.from_raw(raw,                              │
│                                     ErrorClass.NULL_REQUIRED_FIELD,   │
│                                     "PydanticAdapter",                │
│                                     _field_path(e), str(e)))          │
│              accountant.record_invalid(ErrorClass.NULL_REQUIRED_FIELD)│
│              continue                                                 │
│                                                                       │
│          decision = pipeline.validate(event)                          │
│          match decision:                                              │
│              case Valid(ev):                                          │
│                  validated_producer.produce(                          │
│                      topic="validated-events",                        │
│                      key=ev.user_id, value=ev,                        │
│                      on_delivery=accountant.on_validated_delivery)    │
│                  accountant.record_valid()                            │
│              case Invalid(reason):                                    │
│                  dlq_producer.send(                                   │
│                      DlqRecord.from_event(raw, event, reason))        │
│                  accountant.record_invalid(reason.error_class)        │
│                                                                       │
│      validated_producer.poll(0)   # drain delivery callbacks          │
│      dlq_producer.poll(0)                                             │
│                                                                       │
│      if msgs:                                                         │
│          validated_producer.flush(timeout=5.0)                        │
│          dlq_producer.flush(timeout=5.0)                              │
│          consumer.commit(asynchronous=False)                          │
│                                                                       │
│  # graceful shutdown                                                  │
│  validated_producer.flush(timeout=10.0)                               │
│  dlq_producer.flush(timeout=10.0)                                     │
│  consumer.commit(asynchronous=False)                                  │
│  consumer.close()                                                     │
└───────────────────────────────────────────────────────────────────────┘
```

### 3.3 Module Layout

```
src/streaming_feature_store/
├── validate/
│   ├── __init__.py
│   ├── validators.py          # Validator protocol + 6 concrete validators
│   ├── pipeline.py            # ValidationPipeline, Valid, Invalid, RouteDecision
│   ├── dlq.py                 # DlqRecord (Pydantic), DlqRecordAvroAdapter, DlqProducer
│   ├── runner.py              # ValidatorRunner, ValidatorRunConfig
│   ├── accountant.py          # ValidatorAccountant, ValidatorSnapshot
│   └── report.py              # ValidatorRunReport (Pydantic) + Markdown renderer
├── schemas/avro/
│   └── dead_letter_record.avsc
├── consumer/                  # PR #2 — AvroEventConsumer (reused)
├── producer/                  # PR #2 — AvroEventProducer  (reused)
└── admin/                     # PR #4 — TopicAdmin (reused)

scripts/
├── run_validator.py           # single-process
└── run_validator_mp.py        # multi-process consumer-group

docs/results/
└── week2_validator_results.md   # generated artifact
```

### 3.4 Class & Type Sketch

```
class Validator(Protocol):
    name: str               # e.g. "PriceRangeValidator"
    applies_to: frozenset[str] | None    # None == all event_types

    def validate(self, event: EcommerceEvent) -> Valid | Invalid: ...


@dataclass(frozen=True)
class Valid:
    event: EcommerceEvent


@dataclass(frozen=True)
class Invalid:
    error_class: ErrorClass
    validator_name: str
    error_field_path: str | None
    error_message: str


class ValidationPipeline:
    def __init__(self, validators: Sequence[Validator]): ...
    def validate(self, event: EcommerceEvent) -> Valid | Invalid:
        """Apply validators in order; return first Invalid or final Valid."""


class DlqRecord(BaseModel):
    """Pydantic mirror of the Avro DlqRecord schema."""
    schema_version: int = 1
    original_topic: str
    original_partition: int
    original_offset: int
    original_timestamp_ms: int
    original_key_bytes: bytes | None
    original_value_bytes: bytes
    rejected_at_ms: int
    error_class: ErrorClass
    validator_name: str
    error_field_path: str | None
    error_message: str
    validator_version: str = "1.0.0"

    @classmethod
    def from_raw(cls, msg: KafkaMessage, error_class: ErrorClass,
                 validator_name: str, error_field_path: str | None,
                 error_message: str) -> "DlqRecord": ...

    @classmethod
    def from_event(cls, msg: KafkaMessage, event: EcommerceEvent,
                   reason: Invalid) -> "DlqRecord": ...
```

---

## 4. Detailed Implementation

### 4.1 Validators

```
class PriceRangeValidator:
    """Reject ``purchase`` events with non-positive or absurdly large price.

    Parameters
    ----------
    min_price : float, optional
        Exclusive lower bound.  Default ``0.0``.
    max_price : float, optional
        Inclusive upper bound (sanity cap).  Default ``1e9``.

    Notes
    -----
    Skips events whose ``event_type`` is not in ``applies_to``.  This is
    enforced by ``ValidationPipeline``, not by this class, so
    ``validate()`` does not need to re-check.
    """

    name: str = "PriceRangeValidator"
    applies_to: frozenset[str] = frozenset({"purchase"})

    def __init__(self, min_price: float = 0.0,
                 max_price: float = 1e9) -> None: ...

    def validate(self, event: EcommerceEvent) -> Valid | Invalid: ...
```

Sketches for the remaining validators (`RequiredFieldsValidator`,
`QuantityRangeValidator`, `TimestampRangeValidator`,
`EventTypeAllowlistValidator`, `UserIdShapeValidator`) follow the same
pattern — minimal constructor, `validate(event) → Valid | Invalid`,
declared `name` and `applies_to`.

### 4.2 `ValidationPipeline`

```
class ValidationPipeline:
    """Compose validators with first-failure-wins semantics.

    Parameters
    ----------
    validators : Sequence[Validator]
        Applied in the order given.  The first to return ``Invalid``
        short-circuits and is returned to the caller.

    Notes
    -----
    Validators that declare ``applies_to`` are skipped for events whose
    ``event_type`` is not in that set; this is *not* a failure, it is a
    no-op.  Validators that declare ``applies_to=None`` apply to all
    event types.
    """

    def __init__(self, validators: Sequence[Validator]) -> None: ...

    def validate(self, event: EcommerceEvent) -> Valid | Invalid: ...
```

The pipeline catches any unexpected exception from a validator's
`validate()` and re-wraps it as
`Invalid(ErrorClass.PIPELINE_INTERNAL_ERROR, validator_name=v.name,
error_field_path=None, error_message=traceback.format_exc())`.
This prevents a buggy validator from killing the runner.

### 4.3 `DlqRecord` and `DlqProducer`

```
class DlqProducer:
    """Thin wrapper over confluent-kafka Producer + Schema-Registry-bound
    Avro serializer for ``DlqRecord``.

    Parameters
    ----------
    bootstrap : str
    registry_url : str
    topic : str, optional
        Default ``"dead-letter-queue"``.

    Notes
    -----
    Registers ``dead_letter_record.avsc`` under subject
    ``"{topic}-value"`` on construction if not already present, with
    compatibility mode ``BACKWARD``.  Idempotent.
    """

    def __init__(self, bootstrap: str, registry_url: str,
                 topic: str = "dead-letter-queue") -> None: ...

    def send(self, record: DlqRecord) -> None:
        """Async produce; delivery callbacks update the accountant.

        Idempotency key is
        ``f"{record.original_topic}:{record.original_partition}:"
        f"{record.original_offset}"``.
        """

    def flush(self, timeout: float = 10.0) -> int: ...
    def poll(self, timeout: float = 0.0) -> int: ...
    def close(self) -> None: ...
```

### 4.4 `ValidatorRunner`

```
class ValidatorRunner:
    """Single-process consume → validate → route → produce loop.

    Parameters
    ----------
    config : ValidatorRunConfig
        Pydantic config (source_topic, validated_topic, dlq_topic,
        bootstrap, registry_url, consumer_group_id, max_poll_records,
        flush_timeout_s).
    consumer : AvroEventConsumer
        Reused from PR #2 with ``enable.auto.commit=False``.
    validated_producer : AvroEventProducer
        Reused from PR #2 with ``acks=1`` by default (PR #3 may upgrade
        to ``acks=all`` + EOS via config).
    dlq_producer : DlqProducer
        See §4.3.
    pipeline : ValidationPipeline
        See §4.2.
    accountant : ValidatorAccountant
        See §4.5.
    """

    def run(self) -> ValidatorSnapshot: ...
    def request_shutdown(self) -> None: ...
```

### 4.5 `ValidatorAccountant`

```
class ValidatorAccountant:
    """Counters + per-partition tallies + reservoir-sampled latencies.

    Tracks
    ------
    consumed : int
    validated : int
    invalid_by_class : dict[ErrorClass, int]
    invalid_by_validator : dict[str, int]
    invalid_by_field_path : Counter[str]      # top-N report uses this
    deserialize_failures : int
    schema_mismatches : int
    pipeline_internal_errors : int
    validation_latency_us_reservoir : Reservoir[float]
    partition_counts_source : dict[int, int]
    partition_counts_validated : dict[int, int]
    partition_counts_dlq : dict[int, int]
    """
```

### 4.6 `ValidatorRunReport`

```
class ValidatorRunReport(BaseModel):
    started_at: datetime
    ended_at: datetime
    duration_s: float
    consumed: int
    validated: int
    invalid_total: int
    invalid_rate: float    # invalid_total / consumed
    invalid_by_class: dict[ErrorClass, int]
    invalid_by_validator: dict[str, int]
    top_10_failing_fields: list[tuple[str, int]]
    validation_latency_us_p50: float
    validation_latency_us_p95: float
    validation_latency_us_p99: float
    partition_counts_source: dict[int, int]
    partition_skew_ratio_source: float

    def render_markdown(self) -> str: ...
```

### 4.7 Topic Bootstrap

On startup, `ValidatorRunner` calls `TopicAdmin.ensure_topic()` from
PR #4 for both output topics, with the configurations from §2.10. The
calls are idempotent (no-op if exists), so multiple validator processes
in the consumer-group MP mode do not race.

Two Schema-Registry subjects are also registered on the same startup
path, both via `SchemaRegistryClient.register_schema(...)`:

- `dead-letter-queue-value` — registers `dead_letter_record.avsc` with
  compatibility mode `BACKWARD`.
- `validated-events-value` — registers the composite `EcommerceEvent`
  schema (the same schema produced under `e-commerce-events-feed-value`
  by the Week 1 feeder). `AvroEventProducer` runs with
  `auto.register.schemas=False` so this subject must exist before the
  first `produce()` call to `validated-events`; the runner registers it
  rather than pushing that step onto the operator (cf. how Week 1 PR #6
  requires a manual `make register-schemas-feed`).

PR #3 added the registry compatibility tooling that this PR depends on.

---

## 5. Unit Tests

All unit tests use `pytest` fixtures, no real Kafka, no real registry.
`confluent_kafka.Producer` / `Consumer` and `SchemaRegistryClient` are
mocked via `unittest.mock.MagicMock` wrapped in fixtures
(`unittest` is **not** used as a test framework, only its `mock`
helpers — see the project's CLAUDE.md).

| Test | Assertion |
|---|---|
| `test_required_fields_validator_passes_complete_event` | Event with all required fields → `Valid(event)` |
| `test_required_fields_validator_rejects_null_user_id` | `user_id=None` → `Invalid(NULL_REQUIRED_FIELD, validator_name="RequiredFieldsValidator", error_field_path="user_id")` |
| `test_event_type_allowlist_validator_rejects_unknown` | `event_type="quantum_teleport"` → `Invalid(UNKNOWN_EVENT_TYPE)` |
| `test_event_type_allowlist_validator_accepts_each_allowed_value` | parametrized over the 5 allowlist values → all `Valid` |
| `test_price_range_validator_skips_non_purchase_events` | A `click` event with no price → `Valid` (validator skipped via `applies_to`) |
| `test_price_range_validator_rejects_negative_price` | purchase with `price=-1.0` → `Invalid(OUT_OF_RANGE, "PriceRangeValidator", "price")` |
| `test_price_range_validator_rejects_above_max` | purchase with `price=2e9` → `Invalid(OUT_OF_RANGE, ...)` |
| `test_price_range_validator_rejects_zero` | purchase with `price=0.0` → `Invalid(...)` (exclusive lower bound) |
| `test_quantity_range_validator_rejects_zero` | quantity=0 → `Invalid` |
| `test_quantity_range_validator_rejects_above_max` | quantity=10001 → `Invalid` |
| `test_timestamp_range_validator_rejects_far_future` | ts = now + 2h → `Invalid(OUT_OF_RANGE, ..., "event_timestamp")` |
| `test_timestamp_range_validator_accepts_modest_future_skew` | ts = now + 30 min → `Valid` (within 1h tolerance) |
| `test_timestamp_range_validator_rejects_too_old` | ts = now − 30d → `Invalid` |
| `test_user_id_shape_validator_rejects_empty` | `user_id=""` → `Invalid(MALFORMED_RECORD)` |
| `test_user_id_shape_validator_rejects_too_long` | `user_id = "x" * 257` → `Invalid` |
| `test_user_id_shape_validator_rejects_embedded_newline` | `user_id="a\nb"` → `Invalid` |
| `test_pipeline_first_failing_wins` | Both `RequiredFields` and `PriceRange` would fail → `Invalid` from `RequiredFields` (declared first) |
| `test_pipeline_passes_when_all_validators_pass` | Construct a valid event → `Valid(event)` |
| `test_pipeline_applies_to_filter_skips_correctly` | A `click` event passes `PriceRangeValidator` because of `applies_to` |
| `test_pipeline_internal_exception_wrapped_as_invalid` | Inject a validator that raises `RuntimeError("boom")` → `Invalid(PIPELINE_INTERNAL_ERROR, validator_name=..., error_message contains "boom")` |
| `test_pipeline_internal_exception_preserves_traceback` | The `error_message` field contains a `Traceback (most recent call last):` line |
| `test_dlq_record_from_raw_populates_kafka_coordinates` | `DlqRecord.from_raw(raw_msg, ...)` carries `original_topic/partition/offset/timestamp_ms` verbatim |
| `test_dlq_record_from_raw_preserves_value_bytes` | The `original_value_bytes` field equals `raw_msg.value()` byte-for-byte |
| `test_dlq_record_from_event_carries_reason_fields` | `DlqRecord.from_event(raw, event, invalid)` includes `validator_name`, `error_field_path`, `error_message` from `invalid` |
| `test_dlq_record_idempotency_key_format` | `record.idempotency_key() == f"{topic}:{partition}:{offset}"` |
| `test_dlq_record_avro_round_trip` | Serialize via `DlqRecordAvroAdapter`, deserialize, equal to original |
| `test_dlq_producer_calls_producer_with_correct_key` | Mock the underlying `Producer.produce` → assert `key=idempotency_key` |
| `test_validator_runner_routes_valid_to_validated_topic` | Valid event → `validated_producer.produce` called with `topic="validated-events"`, key=`user_id` |
| `test_validator_runner_routes_invalid_to_dlq` | Out-of-range price → `dlq_producer.send` called with the right `error_class` |
| `test_validator_runner_routes_deserialize_failure_to_dlq` | Bad bytes raise `DeserializeError` → DLQ with `DESERIALIZE_FAILURE` |
| `test_validator_runner_routes_pydantic_error_to_dlq` | `ValidationError` (null required field) → DLQ with `NULL_REQUIRED_FIELD` |
| `test_validator_runner_routes_internal_exception_to_dlq` | Validator raises → DLQ with `PIPELINE_INTERNAL_ERROR`, runner does NOT crash |
| `test_validator_runner_flush_before_commit` | `MagicMock.mock_calls` order: producer.flush index < consumer.commit index |
| `test_validator_runner_shutdown_flushes_in_flight` | Set shutdown flag → both producers flushed, offset committed, consumer closed |
| `test_validator_runner_rejects_auto_commit_true_config` | `ValueError` at construction if `enable_auto_commit=True` |
| `test_validator_accountant_invalid_by_class_increments` | Record 3 OUT_OF_RANGE → `invalid_by_class[OUT_OF_RANGE] == 3` |
| `test_validator_accountant_top_failing_fields` | Record 5 failures on `price`, 3 on `user_id` → `top_10_failing_fields[0] == ("price", 5)` |
| `test_validator_accountant_validation_latency_recorded` | Record 100 latencies → `Reservoir.size == 100`, p50 plausible |
| `test_validator_report_render_markdown_includes_invalid_rate` | Rendered Markdown contains `Invalid rate: X.XX%` line |
| `test_validator_report_flags_high_invalid_rate` | `invalid_rate > 0.05` → rendered Markdown contains `⚠ elevated invalid rate` marker |
| `test_validator_run_config_validates_topics_distinct` | `validated_topic == dlq_topic` → `ValidationError` |
| `test_validator_run_config_rejects_validating_self_loop` | `source_topic == validated_topic` → `ValidationError` |

Coverage target: **100% line + branch** for everything in `validate/`.

---

## 6. Integration Tests

Integration tests use real Kafka + real Schema Registry via the
`docker compose` infra and the `infra-up` fixture from PR #1. Marked
`@pytest.mark.integration`; skipped if `docker compose ps` reports no
running services.

| Test | Setup → Assertion |
|---|---|
| `test_validator_dlq_topic_auto_created_with_correct_config` | Start runner against a fresh broker → `dead-letter-queue` exists with 3 partitions, RF=3, retention.ms=2_592_000_000 |
| `test_validator_validated_topic_auto_created_with_correct_config` | Similar — `validated-events` with 12 partitions, RF=3, 7d retention |
| `test_validator_dlq_schema_registered_with_backward_compatibility` | After runner starts → `SchemaRegistryClient.get_compatibility('dead-letter-queue-value') == 'BACKWARD'` |
| `test_validator_round_trip_valid_event_lands_on_validated_topic` | Produce 1 valid event to `e-commerce-events-feed`, run validator 10 s, consume `validated-events` → 1 event with matching `event_id` |
| `test_validator_round_trip_invalid_event_lands_on_dlq` | Produce 1 event with `price=-5.0`, run validator → 1 `DlqRecord` on `dead-letter-queue` with `error_class=OUT_OF_RANGE`, `validator_name="PriceRangeValidator"` |
| `test_validator_dlq_preserves_original_value_bytes` | Produce 1 invalid event, capture raw bytes; consume DLQ; assert `DlqRecord.original_value_bytes == raw_bytes` |
| `test_validator_dlq_idempotency_key_dedupes_on_replay` | Produce 1 invalid event, run validator twice (second time forces offset rewind via `seek_to_beginning`); consume DLQ; assert 2 records share the same key |
| `test_validator_runs_against_feed_topic_by_default` | Run with no `--source` flag → consumer group `validator-feed`, subscribes to `e-commerce-events-feed` |
| `test_validator_runs_against_bench_topic_with_flag` | Run with `--source bench` → consumer group `validator-bench`, subscribes to `e-commerce-events` |
| `test_validator_feeder_to_validated_pipeline_60s` | Run feeder (PR #6) + validator concurrently for 60 s at 200 evt/s → `validated-events` has ≈12000 events, DLQ has 0, `invalid_rate < 0.001` |
| `test_validator_multiprocess_pipeline_drains_benchmark` | Launch `run_validator_mp.py --procs 4` against a 60k evt/s 10 s burst on `e-commerce-events` → all messages routed within 30 s, sum of validated + invalid ≈ produced count |
| `test_validator_partition_skew_under_zipfian` | Run validator 60 s against feeder traffic → `partition_skew_ratio_source < 2.0` (same threshold as PR #6) |
| `test_validator_handles_corrupted_value_bytes` | Manually craft a Kafka message with bytes that do not decode as Avro → DLQ receives 1 `DESERIALIZE_FAILURE` record, runner does not crash |
| `test_validator_handles_schema_id_unknown_to_registry` | Produce a message with a fabricated schema-id header → DLQ receives 1 `SCHEMA_MISMATCH` record |
| `test_validator_at_least_once_semantics_under_crash` | Kill validator after `produce` succeeds but before `commit` (fault injection) → restart, expect the same event on `validated-events` twice with same key (downstream dedupes on `event_id`) |
| `test_validator_graceful_shutdown_drains_inflight` | Send SIGTERM mid-run → all in-flight produces ack before exit, no offset commits past unacked produces |

---

## 7. How to Run

### 7.1 One-time bootstrap

```
make infra-up                  # PR #1: Kafka + Postgres + Registry
make topic-ensure              # PR #4: ensures e-commerce-events
                               # (the feeder ensures -feed)
                               # (the validator ensures validated-events
                               #   and dead-letter-queue itself)
make register-schemas          # PR #2: registers e-commerce-events-value
                               #   (only needed for benchmark-mode runs;
                               #    --source bench, see §2.4)
make register-schemas-feed     # Week 1 PR #6 prereq: registers
                               #   e-commerce-events-feed-value so the
                               #   feeder / sink / this validator can
                               #   (de)serialize against the feed topic.
                               # `dead-letter-queue-value` is self-registered
                               #   by the validator on startup (§4.7).
                               # `validated-events-value` reuses the same
                               #   composite EcommerceEvent schema as the
                               #   feed topic. AvroEventProducer runs with
                               #   auto.register.schemas=False, so the
                               #   validator registers this subject on
                               #   startup as part of §4.7 alongside the
                               #   DLQ subject.
```

### 7.2 Start the continuous pipeline

In three terminals (or use `make validator-up` to daemonize all three):

```
make feeder-run                # PR #6 — 200 evt/s feeder
make sink-run                  # PR #6 — Postgres sink (raw_events)
make validator-run             # this PR — validator (single proc)
```

### 7.3 Benchmark mode (validate the 60k burst)

```
make validator-run-mp -- --source bench --procs 4
make load-test-mp              # PR #4 — 10 s burst on e-commerce-events
```

The benchmark validator finishes ~20 s after the burst ends; its run
report lands in `docs/results/week2_validator_results.md`.

### 7.4 Inspect

```
make psql
feature_store=# SELECT count(*) FROM raw_events;
                                              -- All raw events (PR #6)
```

```
kafkactl consume validated-events --from-beginning --max-messages 5
kafkactl consume dead-letter-queue --from-beginning --max-messages 5
```

#### Sanity check #1 — Are validated events actually landing?

Tail the output topic.  You should see Avro-serialized payloads
(binary-ish, with a leading magic byte + 4-byte schema id) interleaved
with readable UUIDs, `user_id`s, and payload strings such as
`btn-cta`, `/products`, or `sku-…`.  A clean `Processed a total of 5
messages` exit confirms the validator's happy path.

```
docker compose -f docker/docker-compose.yml exec kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic validated-events --from-beginning --max-messages 5
```

#### Sanity check #2 — Is anything being rejected?

Same trick on the DLQ.  With the feeder producing clean synthetic
data, the DLQ should be empty and the command will exit on the
3 s timeout with `Processed a total of 0 messages` (the
`TimeoutException` line is `kafka-console-consumer`'s ungraceful
"no more messages" — not a real error).  **Seeing records here is a
real signal something is mis-validating.**

```
docker compose -f docker/docker-compose.yml exec kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic dead-letter-queue --from-beginning --max-messages 5 --timeout-ms 3000
```

#### Sanity check #3 — Is the validator keeping up with the feeder?

Check consumer lag for the `validator-feed` group (use
`validator-bench` instead when running benchmark mode).  Run twice
~10 s apart and compare:

- `LAG` flat or oscillating around the same value → validator is
  keeping pace.
- `LAG` growing linearly → validator cannot match feeder throughput
  (consider `validator-run-mp`).
- `CONSUMER-ID = -` on some partitions → rebalance in progress; re-check.

At the default 200 evt/s feeder rate, expect per-partition `LAG` in
the 0–50 range (≈ 1 s of backlog total across 12 partitions).

```
docker compose -f docker/docker-compose.yml exec kafka-1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --describe --group validator-feed
```

### 7.5 CLI

```
python scripts/run_validator.py \
    --source-topic e-commerce-events-feed \
    --validated-topic validated-events \
    --dlq-topic dead-letter-queue \
    --group-id validator-feed \
    --bootstrap kafka-1:9092 \
    --registry http://schema-registry:8081

python scripts/run_validator_mp.py \
    --procs 4 \
    --source bench \
    --group-id validator-bench
```

### 7.6 Tear down

```
make validator-down            # SIGTERM both validator + feeder + sink
make infra-down                # PR #1
```

---

## 8. Resource Budget & Constraints

| Component | CPU (steady state) | RAM | Disk |
|---|---|---|---|
| ValidatorRunner @ 200 evt/s | <0.1 core | ~80 MB | 0 |
| ValidatorRunner MP @ 60k evt/s burst | ~4 cores (4 procs × 1.0) | ~320 MB | 0 |
| Kafka brokers (validated-events + DLQ) | (existing) | (existing) | ~0.5 GB/day for `validated-events` at 200 evt/s; DLQ negligible |

Constraints:

- **Kafka log retention.** `validated-events` retention 7 d is fine
  because Week 2 PR #2's feature processor reads it within seconds.
  DLQ retention 30 d to accommodate forensic investigations.
- **Schema-registry rate.** Avro deserialization uses the registry
  cache; cold-cache cost is one HTTP round-trip per schema ID, then
  cached forever in `librdkafka`. No throughput impact at steady
  state.
- **Topic-creation race in MP mode.** All 4 MP procs call
  `ensure_topic` at startup. `TopicAdmin.ensure_topic()` from PR #4 is
  idempotent and races safely (Kafka admin API returns
  `TOPIC_ALREADY_EXISTS` and the wrapper swallows it).

---

## 9. Future Considerations

1. **Flink port of the validator.** A `ProcessFunction` doing the same
   validation chain inside the Week 2 PR #2 Flink job would collapse
   the two stream processors into one and avoid the
   `validated-events` topic hop. The portfolio-narrative argument is
   that **the topic boundary is intentional for failure isolation
   (§2.2)**; collapsing it is a deliberate operational tradeoff. Worth
   building once Week 2 PR #2 lands, as a comparative benchmark
   exercise.
2. **DLQ replay tooling.** A small script that reads
   `dead-letter-queue`, filters by `error_class` and/or
   `validator_version`, lifts `original_value_bytes`, and re-publishes
   to `e-commerce-events-feed`. Useful operationally; deferred because
   replay correctness depends on the validator-fix being non-regressive
   (an open verification problem worth its own design discussion).
3. **Statistical drift on the invalid-rate.** A spike in DLQ rate is
   the canonical "something changed upstream" signal. Week 5's
   freshness-monitoring framework
   ([`gap_project_plan.md`](gap_project_plan.md) — Week 5 first bullet)
   will subscribe to `dead-letter-queue` and emit a `WARN` if
   `invalid_rate` exceeds a rolling 1-h baseline by >3σ.
4. **Validator config as Avro schema in the registry.** Currently the
   validator catalog and its thresholds are wired in Python. A future
   step is a YAML/Avro DSL (compiled to `ValidationPipeline`) so
   threshold changes are deployable without a code change. Maps onto
   the broader DSL-compiled-feature-store conversation in the project
   notes.
5. **Outbox pattern for downstream Redis writes.** Week 2 PR #2's
   feature processor will write features to Redis *and* to a Kafka
   topic. Cross-store atomicity needs an outbox; this validator's
   at-least-once-write contract on `validated-events` is the upstream
   half of that pattern. Designed in PR #2.
6. **Header-based vs body-based DLQ metadata.** Currently all DLQ
   metadata lives in the Avro envelope body. A production alternative
   is to put `(error_class, validator_name, validator_version)` in
   Kafka **message headers** so forensic readers can filter by header
   without deserializing the body. Cheap to add when load is high;
   deferred here because the entire DLQ is low-volume.
7. **PII handling in `error_message`.** Synthetic data carries no
   real PII, but a production deployment would need to redact
   user-identifying substrings in the human-readable error message
   before publishing to a DLQ that might be read by lower-trust
   operators. The Pydantic `DlqRecord.error_message` field is the
   natural place to apply a redactor; deferred for now.

---

## 10. Open Questions

1. **Should `RequiredFieldsValidator` run *before* or *after* the
   Pydantic adapter?** The current design has Pydantic raise
   `ValidationError` on null required fields *before* the pipeline
   even sees the message; `RequiredFieldsValidator` then re-checks
   the post-decode `EcommerceEvent` for null *non-required-by-Pydantic*
   fields. There may be redundancy. The integration test
   `test_validator_runner_routes_pydantic_error_to_dlq` is the
   discriminator: if it ever flakes because Pydantic is too lenient,
   the validator catches the gap; if it never fires, the validator
   may be dead code. Decision deferred until the smoke run produces a
   real distribution.
2. **DLQ topic partition count.** The §2.10 choice of 3 partitions is
   a guess. If a high-volume validator-introduced bug ever floods the
   DLQ (e.g., a misconfigured threshold rejecting 90% of traffic), 3
   partitions may bottleneck. The 24 h smoke run will surface this if
   it materializes.
3. **Invalid-rate alerting threshold.** Default 5% is conservative for
   synthetic data (Week 1 producer emits well-formed events; expected
   DLQ rate is ≪1%). The threshold should be re-calibrated after the
   first 24 h smoke run; it lives in `ValidatorRunReport`'s rendering
   layer and is a one-line change.
4. **Should the validator emit a per-validator "tripped" metric to
   Prometheus directly?** Week 5 will likely surface this; for now
   the run report is the source of truth. Decision: defer; the
   accountant's `invalid_by_validator` dict is the upstream
   data source whichever path Week 5 picks.
5. **Should `applies_to` be enforced by the pipeline or by each
   validator?** Currently the pipeline filters; this means each
   validator's `validate()` can assume the event matches. The
   alternative — each validator does its own filter and returns
   `Valid` for non-matching events — is more uniform but more
   verbose. Decision recorded as pipeline-side filter (§4.2); revisit
   if a future validator needs `applies_to` logic that depends on
   non-`event_type` fields.
