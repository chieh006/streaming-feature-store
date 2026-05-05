# Design Doc: Schema Evolution Experiments under `BACKWARD` Compatibility

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** Third bulletpoint — Configure `BACKWARD` compatibility mode and run real-world schema-evolution drills (add a new optional field, remove a deprecated field, promote `int` → `long`); verify both forward-direction and reverse-direction deserialization (line 66 of `gap_project_plan.md`)
**Author:** Auto-generated design document
**Date:** 2026-04-28

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

PR #1 (`week1_01_docker_compose_infra.md`) stood up the Kafka + PostgreSQL +
Schema Registry infrastructure. PR #2
(`week1_02_avro_schemas_and_producer_serialization.md`) defined the
`EcommerceEvent` envelope plus three payload records, registered them under
`e-commerce-events-value`, and shipped an `AvroEventProducer` that serializes
against the latest registered schema.

This PR is the **schema-evolution drill** that the Week 1 learning objectives
call out. It does **not** change the steady-state production path — instead it
exercises the Schema Registry's compatibility checker by deliberately mutating
the on-disk schemas in three documented ways and observing both the
**registration outcome** and the **runtime serde outcome** when producers and
consumers are on different schema versions.

Concretely, this PR:

1. Pins the `e-commerce-events-value` subject's compatibility level to
   `BACKWARD` explicitly (rather than relying on the global default).
2. Introduces a versioned schema-experiments tree: `schemas/ecommerce/v1.1/`,
   `v1.2/`, `v1.3/` — one directory per drill — leaving the baseline `v1/`
   untouched.
3. Adds a **driver** module (`scripts/run_schema_evolution.py`) that, for each
   experiment, registers the candidate schema, captures the Registry's verdict
   (accepted / rejected with reason), and — when accepted — runs a small
   producer-on-vN / consumer-on-v(N-1) and producer-on-v(N-1) / consumer-on-vN
   round-trip using `AvroEventProducer` and a new `AvroEventConsumer`.
4. Captures the results of all three experiments into a Markdown report
   (`docs/results/week1_schema_evolution_results.md`) with the registered IDs,
   versions, and the pass/fail pattern of every cross-version serde matrix
   cell. This report is the **deliverable artifact** for the Week 1 learning
   objective "handle schema evolution gracefully."
5. Adds tests that pin the *expected* `BACKWARD` semantics into CI: every
   future schema change in `v1/` must satisfy `BACKWARD`, and the three
   evolution drills must keep producing the same verdicts.

### What `BACKWARD` Means Here (and Why It's the Default)

`BACKWARD` compatibility on a Confluent subject means: **a consumer using the
new schema must be able to read data produced with the most recent prior
schema**. Concretely, the Registry rejects a new schema version if it:

- Adds a required field with no default (a v(N-1) record will be missing it
  when read with vN).
- Removes a field that did not have a default in v(N-1) (the new reader does
  not know how to fill it).
- Changes a field's type to one that is not Avro-resolution-compatible
  (e.g., `string` → `int`).

It explicitly **allows**:

- Adding an optional field that has a default.
- Removing a field that had a default.
- Promoting a numeric type along Avro's promotion lattice
  (`int` → `long` / `float` / `double`, `long` → `float` / `double`,
  `float` → `double`).

The three drills below were chosen so each one lands on a different cell of
that matrix.

### Out of Scope (Deferred to Later PRs)

- `FORWARD` / `FULL` / `NONE` compatibility experiments. This PR is scoped to
  `BACKWARD` per the gap plan; other modes are a natural extension for a
  future "compatibility deep-dive" PR.
- Multi-language consumers (a JVM Flink / Kafka Streams consumer eating Python
  Avro). Week 2 will introduce stream processing and may revisit.
- The high-throughput synthetic generator (PR #4) and the Kafka → PostgreSQL
  sink (later Week 1 PR). This PR uses small (≤50 message) batches.

### Deliverables

- `schemas/ecommerce/v1.1/` — drill 1: add optional `device_type` field.
- `schemas/ecommerce/v1.2/` — drill 2: remove a defaulted field
  (`PageViewPayload.referrer`).
- `schemas/ecommerce/v1.3/` — drill 3: promote `PurchasePayload.quantity`
  from `int` → `long`.
- `src/streaming_feature_store/consumer/avro_consumer.py` —
  `AvroEventConsumer` (symmetric counterpart to `AvroEventProducer`).
- `src/streaming_feature_store/schemas/evolution.py` — schema-mutation helpers
  used by the driver and tests (pure functions, no I/O).
- `scripts/run_schema_evolution.py` — CLI driver that runs all three drills
  end-to-end and writes the results report.
- `docs/results/week1_schema_evolution_results.md` — output artifact.
- `tests/unit/test_schema_evolution.py` — unit tests on the mutation helpers
  and the verdict parser.
- `tests/integration/test_schema_evolution_end_to_end.py` — integration tests
  that exercise the Registry and Kafka.
- `Makefile` targets: `schema-evolution`, `schema-evolution-clean`,
  `schema-evolution-report`.

---

## 2. Critical Design Decisions

### 2.1 Drill Selection: One Cell per `BACKWARD` Outcome Class

**Decision:** Run exactly three drills, each landing on a different
`BACKWARD` outcome class.

| Drill | Mutation | `BACKWARD` verdict | Why this drill |
|---|---|---|---|
| 1: Add optional field | `EcommerceEvent` gains `device_type: ["null", "string"], default=null` | **Accepted** | The textbook safe change; verifies our pipeline lets the safe case through unchanged |
| 2: Remove defaulted field | `PageViewPayload.referrer` (which has `default: null`) is removed | **Accepted** | Tests the "removing-with-default-is-fine" rule; many engineers expect any removal to fail |
| 3: Promote `int` → `long` | `PurchasePayload.quantity` becomes `long` | **Accepted** | Tests Avro's numeric promotion lattice — a real-world need (counter overflows) that is often misunderstood |

**Rationale:**
- The gap plan specifies these three exact mutations (line 66). Following the
  spec lets the deliverable map 1:1 onto interview talking points.
- Three is also the minimum that exercises three distinct Registry rules.
  Fewer would not cover the lattice; more would be repetitive at this stage.
- We additionally include **two negative-control drills** as `pytest`-only
  cases (not in the driver report) so we can prove the Registry rejects what
  it should: (a) adding a required field with no default; (b) removing a
  field that had no default. These show up in §5/§6 but do not produce
  artifacts under `schemas/`.

**Trade-off:** Promoting a field type is the most subtle drill — Avro's spec
allows it but `confluent-kafka-python` requires `use.latest.version=True` so
the *consumer* uses its own reader schema (the new `long`) and applies
resolution rules to bytes written under the old `int`. We surface this in the
report so the learning is explicit.

### 2.2 Per-Subject Compatibility, Not Global

**Decision:** Pin compatibility on the `e-commerce-events-value` subject
(`PUT /config/e-commerce-events-value`) to `BACKWARD`, instead of relying on
the Registry-level default that PR #1 set.

**Rationale:**
- A future PR may want to set the global default to `FULL` or experiment with
  another subject. Per-subject pinning keeps drills in this PR isolated.
- Confluent's docs explicitly recommend per-subject overrides for production —
  this drill teaches the production muscle now.
- The setting survives `docker compose down` / `up` because compatibility
  config is stored in the `_schemas` Kafka topic, which lives on a named
  volume from PR #1.

### 2.3 Driver as a Reproducible CLI, Not a Notebook

**Decision:** The drill harness is a regular Python module
(`scripts/run_schema_evolution.py`) invoked by `make schema-evolution`, not a
Jupyter notebook.

**Rationale:**
- The gap plan emphasizes "reproducible" and the deliverable is a Markdown
  report that gets diffed in PR review. A notebook with embedded outputs
  bloats diffs and breaks the "one source of truth" rule.
- A CLI integrates with the same `pytest` integration test layer (the tests
  call into the driver functions directly, not via subprocess).
- The driver is pure functions returning dataclasses; only the entry point
  does I/O. This keeps unit-testable logic separated from registry traffic.

### 2.4 Schema Mutations as Pure-Function Transforms

**Decision:** Each drill is implemented as a pure function in
`src/streaming_feature_store/schemas/evolution.py` that takes a baseline
composite schema dict (loaded from `v1/`) and returns a new dict.

```python
def add_optional_field(base: dict, *, name: str, avro_type: str) -> dict: ...
def remove_field(base: dict, *, record_name: str, field: str) -> dict: ...
def promote_field_type(base: dict, *, record_name: str, field: str,
                       new_type: str) -> dict: ...
```

**Rationale:**
- Pure functions are unit-testable without Docker (CLAUDE.md §1, §6).
- Producing the new schema dict in-memory means the on-disk
  `schemas/ecommerce/v1.x/` files are *generated* from `v1/` plus a recipe,
  rather than hand-written. This guarantees the diffs in `v1.x/` differ from
  `v1/` by exactly the mutation under test — no accidental rename, no stray
  `doc` reword.
- A small `make schema-evolution-snapshot` target writes the mutated dicts to
  disk so reviewers can read the diff in Git.

### 2.5 Symmetric `AvroEventConsumer`

**Decision:** Add an `AvroEventConsumer` mirroring PR #2's
`AvroEventProducer`, so the drill can run real end-to-end serdes — not just
schema-registration verdicts.

**Rationale:**
- The Week 1 learning objective explicitly says "verify that consumers on
  the old schema can still deserialize new events without breaking, and vice
  versa." That requires a consumer object.
- Subsequent Week 1 PRs (Kafka-to-PostgreSQL sink, exactly-once benchmarking)
  will reuse this consumer. Building it here amortizes the cost.
- The consumer accepts a **reader schema string** at construction time
  (defaults to "fetch latest from Registry"). For drills, we explicitly pass
  the prior version's schema so the consumer is "stuck on v(N-1)" while the
  producer writes vN.

### 2.6 Verdict Capture: Structured, Not Log-Scraped

**Decision:** The driver returns a `EvolutionDrillResult` Pydantic model:

```python
class EvolutionDrillResult(BaseModel):
    drill_id: str
    description: str
    mutation: dict
    registration_accepted: bool
    registration_error: str | None
    registered_schema_id: int | None
    registered_version: int | None
    serde_matrix: dict[str, str]   # e.g. {"producer=v2,consumer=v1": "ok",
                                   #       "producer=v1,consumer=v2": "ok"}
    notes: str | None
```

**Rationale:**
- Per CLAUDE.md §3 all data models use Pydantic. Verdicts are data; they
  belong in a model, not in stringly-typed dicts or log lines.
- Tests can assert on the structured result (`result.serde_matrix["producer=v2,consumer=v1"] == "ok"`) instead of grep-matching log output.
- The Markdown report is rendered from a list of `EvolutionDrillResult` —
  templated, not concatenated.

### 2.7 No `auto.register.schemas`, Ever

**Decision:** Continue PR #2's stance: `auto.register.schemas=False` for
producers throughout this PR. The driver registers schemas explicitly via
the Registry client.

**Rationale:**
- A drill that registered schemas as a side-effect of producing would mask
  exactly what we are trying to observe (the Registry's accept/reject
  decision). Explicit registration is the whole point.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     scripts/run_schema_evolution.py                 │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  for each drill in [drill1, drill2, drill3]:                │   │
│  │    1. mutate baseline schema (pure function)                │   │
│  │    2. dump to schemas/ecommerce/v1.X/ (snapshot)            │   │
│  │    3. POST /subjects/<sub>/versions   ──► verdict           │   │
│  │    4. if accepted:                                          │   │
│  │        a. produce N events with vN producer                 │   │
│  │        b. consume with v(N-1) consumer  ──► serde cell      │   │
│  │        c. produce N events with v(N-1) producer            │   │
│  │        d. consume with vN consumer      ──► serde cell      │   │
│  │    5. record EvolutionDrillResult                           │   │
│  └────────────────────────────┬────────────────────────────────┘   │
│                               │                                    │
│                               ▼                                    │
│            docs/results/week1_schema_evolution_results.md          │
└────────────────────────────┬───────────────────────────────────────┘
                             │ uses
            ┌────────────────┼────────────────────────┐
            ▼                ▼                        ▼
┌────────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ AvroEventProducer  │ │ AvroEventConsumer│ │ SchemaRegistryClient │
│ (PR #2)            │ │ (this PR)        │ │ (PR #2 wrapper)      │
└──────┬─────────────┘ └─────────┬────────┘ └──────────┬───────────┘
       │ produce               consume                  │ register/get
       ▼                       ▲                        ▼
┌──────────────────────────────────────────┐  ┌──────────────────────┐
│         Kafka brokers (PR #1)            │  │  Schema Registry     │
│       e-commerce-events topic            │  │      (PR #1)         │
└──────────────────────────────────────────┘  └──────────────────────┘
```

The driver never talks to Kafka or the Registry directly — it composes the
existing PR #1/#2 building blocks plus the new consumer.

---

## 4. Detailed Implementation

### 4.1 Directory Structure Additions

```
streaming-feature-store/
├── schemas/
│   └── ecommerce/
│       ├── v1/                             # baseline (PR #2) — unchanged
│       ├── v1.1/                           # drill 1: + device_type
│       │   └── ecommerce_event.avsc
│       ├── v1.2/                           # drill 2: - PageViewPayload.referrer
│       │   ├── page_view_payload.avsc
│       │   └── ecommerce_event.avsc
│       └── v1.3/                           # drill 3: PurchasePayload.quantity int → long
│           ├── purchase_payload.avsc
│           └── ecommerce_event.avsc
├── scripts/
│   └── run_schema_evolution.py
├── src/
│   └── streaming_feature_store/
│       ├── consumer/
│       │   ├── __init__.py
│       │   └── avro_consumer.py
│       └── schemas/
│           └── evolution.py
├── docs/
│   └── results/
│       └── week1_schema_evolution_results.md   # generated artifact
└── tests/
    ├── unit/
    │   ├── test_schema_evolution.py
    │   └── test_avro_consumer_unit.py
    └── integration/
        └── test_schema_evolution_end_to_end.py
```

### 4.2 `schemas/evolution.py` — Pure Mutation Functions

Each function does exactly one mutation and is unit-testable without Docker.

| Function | Behavior |
|---|---|
| `add_optional_field(base, *, name, avro_type, default=None)` | Returns a new schema dict with a nullable field appended to the envelope record. `avro_type` is e.g. `"string"`; result field type is `["null", "string"]`. Raises `SchemaMutationError` if field name collides. |
| `remove_field(base, *, record_name, field)` | Returns a new dict with the named field removed from the named record. Raises if the field has no `default` (would not be a `BACKWARD`-safe removal — caller can opt into the negative-control case via `force=True`). |
| `promote_field_type(base, *, record_name, field, new_type)` | Returns a new dict with the field's type swapped. Raises if `(old_type, new_type)` is not on Avro's promotion lattice (`int → long/float/double`, `long → float/double`, `float → double`). |
| `dump_to_directory(base, dest_dir)` | Splits a composite schema dict back into per-record `.avsc` files under `dest_dir` (only for files whose record was mutated, to keep diffs minimal). Pure I/O — separated for testability. |

All functions take and return plain `dict` objects (deep-copied internally to
avoid mutating the input).

### 4.3 `consumer/avro_consumer.py` — `AvroEventConsumer`

Mirrors `AvroEventProducer` from PR #2.

```python
class AvroEventConsumer:
    """Avro-deserializing Kafka consumer for EcommerceEvent messages.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration from PR #1.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings from PR #1.
    group_id : str
        Consumer group ID. Drills generate a per-test group_id to avoid
        offset bleed-through.
    topic : str, optional
        Defaults to ``kafka_config.default_topic``.
    reader_schema_str : str | None, optional
        If provided, the deserializer uses this as the reader schema (the
        consumer's own view of the data). If ``None``, the deserializer
        falls back to the schema referenced by the message's wire-format
        schema ID (writer schema = reader schema). Drills explicitly pass
        the prior-version schema here to simulate a stuck consumer.
    auto_offset_reset : str, optional
        ``"earliest"`` for drills (small topics, want to read everything).
    """
```

| Method | Behavior |
|---|---|
| `consume(timeout_s, max_messages) -> list[EcommerceEvent]` | Polls in a loop, deserializes via `AvroDeserializer`, validates each result against the Pydantic `EcommerceEvent` model, returns the list. Stops at `max_messages` or `timeout_s`. |
| `consume_raw(...) -> list[dict]` | Same, but skips Pydantic validation (used in the negative drills where the bytes may not match the local model). |
| `__enter__` / `__exit__` | Closes the underlying `Consumer` on exit. |
| `close()` | Idempotent. |

Internal helpers (each one thing):
- `_build_deserializer()` — returns an `AvroDeserializer` configured with
  `from_dict=lambda d, _ctx: d` and the optional reader schema.
- `_build_consumer()` — returns a `DeserializingConsumer`.
- `_subscribe_and_assign()` — subscribes to the topic and waits for partition
  assignment before returning (so the test does not race the rebalance).

### 4.4 `scripts/run_schema_evolution.py` — Driver

CLI:

```
usage: run_schema_evolution.py [--drill {1,2,3,all}]
                                [--snapshot-only]
                                [--report-path PATH]
                                [--keep-subject]
```

Logic:

1. Load baseline `v1/` composite schema.
2. Use `SchemaRegistryClientWrapper.set_compatibility(
   "e-commerce-events-value", "BACKWARD")`.
3. For each selected drill:
   1. Compute mutated schema via the pure helpers.
   2. `dump_to_directory(...)` into `schemas/ecommerce/v1.X/` (so the diff
      lives in Git).
   3. Attempt registration; capture `(accepted, error, schema_id, version)`.
   4. If accepted: run the 4-cell serde matrix
      (producer × consumer ∈ {prior, new}) using
      `AvroEventProducer` + `AvroEventConsumer`. Each cell produces 5 events
      and asserts the consumer reads back 5 (or expected error).
   5. Record an `EvolutionDrillResult`.
4. Render results to `docs/results/week1_schema_evolution_results.md` via a
   small Jinja-free f-string template.
5. Unless `--keep-subject` is passed, **soft-delete the experiment versions
   from the subject** (using the Registry's `DELETE
   /subjects/.../versions/<v>?permanent=false` call) and reset the
   subject's compatibility level back to the global default — so the
   baseline `v1` registration left by PR #2 remains the live version. This
   keeps the laptop dev environment clean for subsequent Week 1 PRs.

The driver does **not** delete `schemas/ecommerce/v1.X/` on disk — those are
the human-readable artifacts.

### 4.5 `docs/results/week1_schema_evolution_results.md` — Output Artifact

Auto-generated; checked into Git so reviewers can read it without rerunning.

```markdown
# Week 1 — Schema Evolution Drill Results

**Generated:** <ISO-8601 timestamp>
**Subject:** e-commerce-events-value
**Compatibility level:** BACKWARD

## Drill 1 — Add optional `device_type` field

| Field | Value |
|---|---|
| Mutation | Added `device_type: ["null","string"], default=null` to `EcommerceEvent` |
| Registration | ✅ accepted (schema id 42, version 2) |
| Serde producer=v2, consumer=v1 | ✅ ok (5/5) |
| Serde producer=v1, consumer=v2 | ✅ ok (5/5; new field returns null) |

[ ... drills 2 and 3 follow the same template ... ]

## Negative controls (run only in pytest, not registered)

| Mutation | Expected | Observed |
|---|---|---|
| Add required field with no default | ❌ rejected | ❌ rejected ✓ |
| Remove field with no default | ❌ rejected | ❌ rejected ✓ |
```

### 4.6 `Makefile` Additions

```makefile
schema-evolution:        ## Run all 3 schema evolution drills end-to-end
	python scripts/run_schema_evolution.py --drill all

schema-evolution-snapshot: ## Generate v1.x/ on disk without contacting the Registry
	python scripts/run_schema_evolution.py --drill all --snapshot-only

schema-evolution-clean:  ## Soft-delete experiment versions; keep baseline v1
	python scripts/run_schema_evolution.py --drill all --report-path /tmp/_discard.md

schema-evolution-report: ## Open the generated report (xdg-open / open)
	@xdg-open docs/results/week1_schema_evolution_results.md 2>/dev/null \
	  || open docs/results/week1_schema_evolution_results.md 2>/dev/null \
	  || echo "Report at docs/results/week1_schema_evolution_results.md"
```

---

## 5. Unit Tests

Unit tests run without Docker. They cover the pure mutation helpers, the
verdict model, the consumer's wiring (mocked), and the report renderer.

### 5.1 `tests/unit/test_schema_evolution.py` — Mutation helpers

| Test | Assertion |
|---|---|
| `test_add_optional_field_appends_nullable_union` | New field's type is `["null","string"]` and `default` is `null` |
| `test_add_optional_field_does_not_mutate_input` | `id(input) != id(output)`; input is byte-identical after call |
| `test_add_optional_field_rejects_existing_name` | Re-adding `event_id` raises `SchemaMutationError` |
| `test_remove_field_removes_from_named_record` | Field is gone from the right record only; sibling records untouched |
| `test_remove_field_rejects_field_without_default` | `force=False` (default) raises if the field has no default |
| `test_remove_field_force_overrides` | `force=True` allows the removal (used by negative-control drills) |
| `test_promote_field_type_int_to_long_allowed` | Result has `type: long` |
| `test_promote_field_type_string_to_int_rejected` | Raises `SchemaMutationError` (not on the lattice) |
| `test_promote_field_type_same_type_is_noop` | Returns an equal dict; emits a `WARNING` log |
| `test_dump_to_directory_writes_only_changed_records` | If only `PurchasePayload` mutated, only that file + envelope written |
| `test_dump_to_directory_creates_dir_with_pathlib` | Works on Windows-style `\\?\` test paths via `tmp_path` (cross-platform per CLAUDE.md §2) |
| `test_evolution_drill_result_pydantic_validates` | Missing required fields raise `ValidationError`; `serde_matrix` keys must match `producer=v\d,consumer=v\d` |
| `test_render_report_includes_all_drills` | Markdown output contains the drill IDs, schema IDs, and ✓/✗ icons |
| `test_render_report_handles_rejected_registration` | When `registration_accepted=False`, the serde matrix is omitted and the error string is rendered verbatim |

### 5.2 `tests/unit/test_avro_consumer_unit.py`

Mocks `confluent_kafka.DeserializingConsumer` and `SchemaRegistryClient`.

| Test | Assertion |
|---|---|
| `test_consumer_builds_with_expected_config` | `auto.offset.reset=earliest`, `enable.auto.commit=False`, `group.id` propagated |
| `test_consumer_passes_reader_schema_when_provided` | `AvroDeserializer` constructed with `schema_str=<reader_schema>` |
| `test_consumer_falls_back_to_writer_schema_when_no_reader` | `AvroDeserializer` constructed with `schema_str=None` |
| `test_consume_returns_list_of_pydantic_events` | Mocked poll returns 3 dicts → `consume()` returns 3 `EcommerceEvent` instances |
| `test_consume_raw_skips_pydantic_validation` | A dict that fails Pydantic still returns from `consume_raw()` |
| `test_consume_respects_max_messages` | `max_messages=2` over 5 polled messages stops at 2 |
| `test_consume_respects_timeout` | Empty polls → returns `[]` after `timeout_s` (uses `monotonic` mocked via `freezegun`-style fixture, not real wall clock) |
| `test_context_manager_closes_consumer` | `__exit__` calls `.close()` exactly once |
| `test_close_is_idempotent` | Two `close()` calls → one underlying `.close()` |

### 5.3 `tests/unit/test_evolution_driver.py` — Driver logic, mocked

| Test | Assertion |
|---|---|
| `test_driver_calls_set_compatibility_to_backward` | Driver invokes `set_compatibility("e-commerce-events-value", "BACKWARD")` exactly once at startup |
| `test_driver_skips_serde_matrix_on_registration_failure` | When the mocked Registry raises on `register`, the resulting `EvolutionDrillResult.serde_matrix == {}` |
| `test_driver_writes_v1x_directory` | After `--snapshot-only`, the expected files exist under `tmp_path / "schemas/ecommerce/v1.1"` |
| `test_driver_keeps_baseline_clean` | Without `--keep-subject`, mocked Registry receives a delete call for the experiment version but **not** for version 1 |
| `test_driver_negative_controls_run_in_pytest_only` | Driver entry point with `--drill all` does not register the negative-control schemas (those run only via pytest in §6) |

---

## 6. Integration Tests

Require PR #1 infra (`make infra-up`) and PR #2 schemas registered
(`make register-schemas`).

### 6.1 Prerequisites

- A new session-scoped fixture `clean_evolution_subject` that, before the
  module runs, deletes any leftover experiment versions on
  `e-commerce-events-value` (via `DELETE /subjects/.../versions/<v>?permanent=true`)
  and re-registers the baseline `v1` schema. Teardown does the same. This
  guarantees test independence across runs.
- Reuses PR #1's `docker_services_up` and PR #2's `registered_ecommerce_schema`
  fixtures.

### 6.2 `tests/integration/test_schema_evolution_end_to_end.py`

| Test | What It Verifies |
|---|---|
| `test_subject_compatibility_is_backward_after_setup` | `GET /config/e-commerce-events-value` returns `BACKWARD` after the driver's setup phase |
| `test_drill1_add_optional_field_is_accepted` | Registration succeeds; new version > previous version; schema body in Registry contains `device_type` |
| `test_drill1_old_consumer_reads_new_producer` | Producer on v(N), consumer on v(N-1), 5 events round-trip; `device_type` is dropped by old reader |
| `test_drill1_new_consumer_reads_old_producer` | Producer on v(N-1), consumer on v(N), 5 events round-trip; `device_type` is `None` in the resulting Pydantic events |
| `test_drill2_remove_defaulted_field_is_accepted` | Removing `PageViewPayload.referrer` registers cleanly |
| `test_drill2_old_consumer_reads_new_producer` | v(N-1) consumer reading v(N) bytes — Avro fills `referrer` from default (`null`) |
| `test_drill2_new_consumer_reads_old_producer` | v(N) consumer reading v(N-1) bytes — `referrer` simply not deserialized |
| `test_drill3_promote_int_to_long_is_accepted` | `PurchasePayload.quantity: int` → `long` registers cleanly |
| `test_drill3_old_consumer_reads_new_producer_with_in_range_value` | v(N-1) consumer reading a v(N) message with `quantity ≤ 2³¹-1` succeeds |
| `test_drill3_old_consumer_fails_on_overflow_value` | v(N-1) consumer reading a v(N) message with `quantity > 2³¹-1` raises `SerializationError` (this is the *runtime* failure mode that motivates the promotion in the first place — explicitly observed in the report) |
| `test_drill3_new_consumer_reads_old_producer` | v(N) consumer reading v(N-1) `int` message — Avro promotes to `long` transparently |
| `test_negative_control_add_required_field_is_rejected` | Registration of a schema with a new no-default field raises a `SchemaRegistryError` whose body contains "incompatible" |
| `test_negative_control_remove_no_default_field_is_rejected` | Removing `EcommerceEvent.event_id` (no default) is rejected with the same error class |
| `test_driver_writes_report_with_three_passing_drills` | After `run_schema_evolution.py --drill all`, the report file exists, contains all three drill sections, and each `Registration` row reads ✅ |
| `test_driver_cleanup_restores_baseline` | After driver run without `--keep-subject`, only version 1 (the baseline) is left registered on the subject |

Each test that produces messages caps at **5 messages** per direction (≤20
per drill total) — well under the PR #1 §6.3 ceiling.

### 6.3 Test Ordering and Isolation

- Tests are marked with `@pytest.mark.integration` and run with `-p no:xdist`
  (per PR #1 convention).
- Each drill's tests share a module-scoped fixture that registers the
  candidate schema once; the four serde tests then read against the same
  registered version. Cleanup runs once per module via fixture teardown.
- The negative-control tests do not leave residue (the registry rejects them
  before they take effect), but the fixture teardown still asserts the
  subject's version count is unchanged afterward.

---

## 7. How to Run

> Prerequisites: PR #1 infrastructure must be up, and PR #2 schemas registered.
> ```bash
> make infra-up
> make infra-status      # wait for all 5 services to be healthy
> make register-schemas  # PR #2: register the v1 baseline
> ```

### 7.1 Generate the candidate schemas on disk (no Registry contact)

```bash
source .venv/bin/activate
make schema-evolution-snapshot
git status schemas/ecommerce/v1.1/ schemas/ecommerce/v1.2/ schemas/ecommerce/v1.3/
# Inspect the diffs vs v1/ to confirm each drill is a single targeted change.
```

### 7.2 Run all three drills end-to-end

```bash
make schema-evolution
# Output:
#   INFO  Set compatibility for e-commerce-events-value -> BACKWARD
#   INFO  Drill 1: add optional `device_type` ...  REGISTERED v2 (id=42)
#   INFO    serde producer=v2,consumer=v1  ok (5/5)
#   INFO    serde producer=v1,consumer=v2  ok (5/5)
#   INFO  Drill 2: remove `PageViewPayload.referrer` ... REGISTERED v3 (id=43)
#   ...
#   INFO  Wrote docs/results/week1_schema_evolution_results.md
```

### 7.3 Run the unit tests (no Docker required)

```bash
make test-unit
# or: pytest tests/unit/test_schema_evolution.py \
#            tests/unit/test_avro_consumer_unit.py \
#            tests/unit/test_evolution_driver.py -v
```

### 7.4 Run the integration tests

```bash
make test-integration
# or: pytest tests/integration/test_schema_evolution_end_to_end.py \
#            -v -m integration -p no:xdist
```

### 7.5 Inspect the report

```bash
make schema-evolution-report
# or just open docs/results/week1_schema_evolution_results.md in your editor.
```

### 7.6 Clean up experiment registrations

```bash
make schema-evolution-clean
# Soft-deletes versions 2/3/4 from e-commerce-events-value, leaving v1 live.
```

---

## 8. Resource Budget & Constraints

This PR is the lightest of Week 1 — it adds a small Python module, a CLI
script, two test files, and three short `.avsc` files. No new long-running
processes.

| Item | Incremental cost | Notes |
|---|---|---|
| On-disk `.avsc` files (v1.1, v1.2, v1.3) | < 12 KB | Three small variants of v1 |
| Schema Registry storage | 3 schema versions on `_schemas` topic | Bytes |
| Per-drill network | ~1 register call + 5 produces × 2 directions | Trivial |
| Test runtime (unit) | < 3 s | No Docker required |
| Test runtime (integration) | < 60 s | Reuses PR #1 + #2 infra |
| Driver runtime (`make schema-evolution`) | ~20 s | Dominated by 6 producer flushes |

No new ports, no new containers, no new long-running processes.

---

## 9. Future Considerations

1. **`FORWARD` / `FULL` / `NONE` deep-dive PR.** This PR is `BACKWARD`-only
   per the gap plan. A natural follow-up runs the same matrix under the
   other compatibility modes and collates the outcomes — useful for
   interview talking points like "why does FULL reject what BACKWARD allows?"

2. **CI gate on `BACKWARD`.** A GitHub Actions workflow that, on every PR
   touching `schemas/ecommerce/v1/`, runs `register_schemas.py --check-only`
   against a throwaway Registry container. Deferred — local workflow first.

3. **Polyglot consumers.** When Week 2 adds Flink (JVM) stream processing,
   re-run drill 3 (int → long promotion) with a JVM consumer to confirm
   that Avro's promotion lattice is enforced consistently across runtimes.
   Java's Avro lib is the reference, so this should "just work" — but the
   exercise validates the assumption.

4. **Schema-as-code generator.** When the schema count grows past ~10, the
   `dump_to_directory` helper becomes a hand-rolled mini-codegen. A real
   codegen tool (`datamodel-code-generator`, `avro-tools`) replaces it.

5. **Compatibility-level drift detection.** Add a periodic check
   (cron / GitHub Actions schedule) that asserts every subject's
   compatibility level is its declared value. Schema Registry config can be
   changed by anyone with admin access; this catches accidental drift.

6. **Drill 4 — symbol addition to the `EventType` enum.** Adding a new
   enum symbol is `BACKWARD`-incompatible by Avro's rules (old readers do
   not know the new symbol), but `BACKWARD_TRANSITIVE` would be even more
   restrictive. Worth a future drill since real teams hit this often.

---

## 10. Open Questions

1. **Should the driver leave the experiment versions registered or clean
   them up?** Current default: clean them up so the baseline `v1` remains
   the live version for downstream PRs (sink, EOS work). Pass
   `--keep-subject` to retain. **Recommendation:** keep the auto-clean
   default; manual retention is one flag away.

2. **Should drill 3 (int → long) write a value > 2³¹-1 in production code,
   or only in a test?** The overflow value lives only in the integration
   test `test_drill3_old_consumer_fails_on_overflow_value`. The driver's
   serde matrix uses an in-range value so the report shows ✅ — and the
   *additional* failure mode is documented in the test, not the report.
   **Recommendation:** keep this split. The report is the success story;
   the test is the cautionary tale.

3. **Should we generate the `v1.x/` directories at all, or run the drills
   purely in-memory?** Generating them costs three small files but gives
   reviewers a Git-diff view of each mutation. **Recommendation:** keep
   generating; the diff is the most pedagogically valuable artifact.

4. **Should `AvroEventConsumer` live under `consumer/` or beside the
   producer under `producer_consumer/`?** Mirror-symmetric layout
   (`consumer/`) wins on readability and matches the producer's nesting
   depth in tests. **Recommendation:** `consumer/`.

5. **Should the negative controls produce a registered version that we then
   roll back, or never register at all?** The Registry rejects them before
   they take effect, so no rollback is needed. **Recommendation:** never
   register; the rejection IS the assertion.

6. **Pydantic models and the v1.x schemas — do we need new model variants?**
   No: the Pydantic models in `schemas/models.py` represent the *current
   live* contract (v1). The drills exercise the Registry's view of
   compatibility, not the producer's local typing. The driver feeds the
   producer raw `dict` payloads when it needs to bypass Pydantic for the
   "old-producer-on-v(N-1)" cell. **Recommendation:** no model changes;
   document this clearly in the driver's docstring.
