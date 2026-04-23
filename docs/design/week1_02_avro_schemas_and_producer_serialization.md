# Design Doc: Avro Event Schemas, Schema Registry Registration & Serializing Producer

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** Second bulletpoint — Define event schemas in Avro, register them with the Confluent Schema Registry, and configure a Python producer to serialize events against the registered schema (line 65 of `gap_project_plan.md`)
**Author:** Auto-generated design document
**Date:** 2026-04-22

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

With the Docker Compose infrastructure (Kafka + PostgreSQL + Schema Registry) in
place from PR #1, this PR introduces **strongly-typed, registry-backed event
contracts** for the e-commerce event stream.

Concretely, this PR:

1. Defines Avro schemas for the three event types (`click`, `purchase`,
   `page_view`) under a versioned directory tree in Git — **Git is the source of
   truth** for schema definitions.
2. Provides a registration tool that pushes schema files to the Schema Registry
   under the `TopicNameStrategy` subject naming convention
   (`<topic>-value`, `<topic>-key`).
3. Provides a thin, reusable `AvroEventProducer` wrapper around
   `confluent-kafka-python`'s `SerializingProducer` that:
   - Fetches (or auto-registers) the schema from the Registry on first produce.
   - Caches the schema locally for the producer lifetime.
   - Emits wire-format-valid Avro messages with the
     [Confluent Wire Format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
     (magic byte + schema ID + Avro-encoded payload).
4. Includes unit and integration tests covering schema-on-disk correctness,
   registration idempotency, and end-to-end serialization against a live Registry.

### Out of Scope (Deferred to Later PRs)

- **Schema evolution experiments** (adding/removing/promoting fields,
  `BACKWARD` compatibility drills) — covered by PR #3 (line 66 of the gap plan).
- **High-throughput synthetic event generation** (50K+ evt/sec benchmarking) —
  covered by PR #4 (line 67 of the gap plan).
- **Consumer-side deserialization** and end-to-end latency measurement —
  covered by PR #4 / #5.
- **Kafka-to-PostgreSQL sink** — later Week 1 PR.
- **Exactly-once semantics (idempotent producer + transactions)** — later Week 1
  PR; this PR uses the default at-least-once producer.

### Deliverables

- `schemas/ecommerce/v1/click.avsc`, `purchase.avsc`, `page_view.avsc` — Avro schema files (source of truth in Git).
- `src/streaming_feature_store/schemas/` — schema-loading utilities + Pydantic event models mirroring the Avro schemas for in-Python validation.
- `src/streaming_feature_store/schemas/registry.py` — Schema Registry client wrapper.
- `src/streaming_feature_store/producer/avro_producer.py` — `AvroEventProducer` class.
- `scripts/register_schemas.py` — CLI to register all `.avsc` files under `schemas/` with the Registry.
- `tests/unit/test_schemas.py`, `tests/unit/test_avro_producer.py` — unit tests.
- `tests/integration/test_schema_registration.py`, `tests/integration/test_avro_producer_end_to_end.py` — integration tests.
- `Makefile` targets: `register-schemas`, `produce-sample`.

---

## 2. Critical Design Decisions

### 2.1 Avro over Protobuf

**Decision:** Use Avro as the wire format for Kafka messages in this project.

**Rationale:**
- The gap plan explicitly calls out Avro (or Protobuf) and the Week 1 learning
  objectives list "handle schema evolution gracefully … using a schema registry
  with compatibility modes." Avro's schema evolution rules are the most directly
  exercised on the Confluent Schema Registry — `BACKWARD`, `FORWARD`, `FULL`
  compatibility all map cleanly onto Avro's resolution rules, which is what the
  next PR will exercise.
- Avro carries field names and types in the schema itself, making on-the-wire
  messages **self-describing when paired with the Registry** (the schema ID in
  the magic-byte prefix). Protobuf wire format is positional (field numbers
  only) and depends on compiled stubs.
- The `confluent-kafka-python` Avro serializer is the reference client for the
  Confluent Schema Registry and is well-documented.
- Avro's JSON schema syntax (`.avsc`) is human-reviewable in Git PRs, which
  supports the "schemas-as-code" workflow noted in PR #1's industry sidebar.

**Trade-off:** Protobuf has better cross-language tooling in polyglot
environments and is the de-facto choice for gRPC. Since this project is
single-language (Python) and the evolution experiments are the focal point, Avro
is the better fit.

### 2.2 Subject Naming Strategy: `TopicNameStrategy`

**Decision:** Use the default `TopicNameStrategy` — one schema per topic, subject
name `<topic>-value` (and `<topic>-key` if/when we use keyed Avro records; for
now the key is a plain string `user_id`).

**Rationale:**
- Simplest strategy; matches the Schema Registry default. Most tooling (Kafka
  Connect, ksqlDB, kcat) assumes it.
- The `e-commerce-events` topic will carry multiple event *types*
  (`click`, `purchase`, `page_view`). Under `TopicNameStrategy`, they must share
  **one union schema** at the topic subject. We use an Avro `union` of three
  named records wrapped in an `EcommerceEvent` envelope record, which keeps one
  subject per topic while still allowing polymorphic event types.
- Alternatives considered:
  - **`RecordNameStrategy`** — one subject per record type, regardless of topic.
    Loses topic isolation, allows cross-topic schema reuse. Overkill for our
    single-topic case.
  - **`TopicRecordNameStrategy`** — one subject per `(topic, record)` pair.
    Useful when multiple unrelated event types flow through one topic *without*
    a shared envelope. Our envelope approach achieves the same ergonomic goal
    within the default strategy and avoids teaching a non-default naming scheme.

### 2.3 Envelope Record with Tagged Union

**Decision:** Each Kafka message is an `EcommerceEvent` envelope record:

```
EcommerceEvent {
  event_id:        string (UUID)
  event_type:      enum { click, purchase, page_view }
  user_id:         string
  session_id:      string
  event_timestamp: long (timestamp-micros logical type)
  payload:         union { ClickPayload, PurchasePayload, PageViewPayload }
}
```

**Rationale:**
- The envelope's top-level fields exactly match the typed columns of the
  `raw_events` PostgreSQL table from PR #1 — the future sink can read them
  without descending into the union.
- The `payload` union carries event-type-specific fields (e.g., `product_id`,
  `quantity`, `price_cents` for purchases; `url`, `referrer` for page views).
  These map to the `properties` JSONB column in `raw_events`.
- A single-record-per-topic model plays well with `TopicNameStrategy` and makes
  `BACKWARD` compatibility experiments in PR #3 meaningful — adding a field to a
  specific payload exercises nested evolution.

### 2.4 Git as Source of Truth; Registration is a Deploy Step

**Decision:** Schema `.avsc` files live under `schemas/ecommerce/v<N>/` in Git.
A separate, idempotent registration tool (`scripts/register_schemas.py`) pushes
them to the Registry. The producer **does not** auto-register on first produce.

**Rationale:**
- Matches the "schemas-as-code + Schema Registry" pattern described in PR #1's
  §2.8 industry sidebar: Git holds the reviewable truth, CI/CD (or a manual
  deploy step here) publishes to the Registry, the Registry enforces at runtime.
- Producer auto-registration (`auto.register.schemas=true`) is convenient but
  hazardous — any buggy local producer can mutate the Registry and introduce
  schemas that no PR review has ever seen. Disabling it early teaches the
  production-grade workflow.
- The registration tool is idempotent: re-running it with unchanged `.avsc`
  files is a no-op (the Registry returns the existing schema ID). Running with
  changed schemas triggers a compatibility check against the current
  `BACKWARD` level and fails loudly if incompatible.

**Trade-off:** Developer has to remember to run `make register-schemas` after
editing a schema. Acceptable at this scale; CI will enforce it in a later PR.

### 2.5 Producer: `confluent-kafka-python` Avro Serializer

**Decision:** Use `confluent_kafka.SerializingProducer` with
`confluent_kafka.schema_registry.avro.AvroSerializer`, wrapped in a
project-specific `AvroEventProducer` class.

**Rationale:**
- `confluent-kafka-python` is built on `librdkafka` (C) — the highest-throughput
  Python Kafka client, which matters for the later 50K+ evt/sec benchmark in
  PR #4. Choosing the client now avoids a rewrite.
- `AvroSerializer` implements the Confluent wire format correctly and caches
  schema-ID lookups in-memory, so only the first produce per schema hits the
  Registry.
- The wrapper class:
  - Accepts a Pydantic event model, validates it, converts to a plain
    `dict` matching the Avro schema, and hands it to the serializer.
  - Exposes `.produce(event)` and `.flush()` methods.
  - Owns the `SchemaRegistryClient` and `Producer` lifecycles; context-manager
    friendly (`with AvroEventProducer(...) as p: ...`).
  - Configures `auto.register.schemas=False` and `use.latest.version=True` so
    the producer always serializes against the *latest registered* schema
    version for the subject and fails if one is missing.

### 2.6 Pydantic Models Mirror Avro Schemas (Two-Layer Validation)

**Decision:** Maintain Pydantic models (`ClickEvent`, `PurchaseEvent`,
`PageViewEvent`, `EcommerceEvent`) that mirror the Avro schemas. The producer
accepts a Pydantic instance and converts it to a dict for Avro serialization.

**Rationale:**
- Pydantic validates at call-site construction time, giving clear error messages
  (`price_cents: must be > 0`) **before** the event ever reaches the serializer.
  The Avro serializer's errors (e.g., `AvroTypeException`) are less actionable.
- CLAUDE.md mandates Pydantic for all data models.
- Hand-maintaining two parallel definitions is a known cost. For this PR's 3
  event types it is trivial; if the schema count grows, a later PR can generate
  Pydantic models from `.avsc` files. A unit test asserts **structural parity**
  between the two layers (same field names, compatible types) so drift is
  caught in CI.

### 2.7 Schema Versioning on Disk (`v1/`)

**Decision:** Store schemas under `schemas/ecommerce/v<N>/`. The initial set
lives in `v1/`. Future breaking-ish changes (even if Registry-compatible) get
a new directory.

**Rationale:**
- Gives reviewers a clear signal about schema intent in PRs
  (`+++ schemas/ecommerce/v2/purchase.avsc` screams louder than a diff inside
  `v1/`).
- The Registry itself versions schemas via the subject's version list — this
  directory scheme is orthogonal and exists for human-readable Git history, not
  runtime behavior. The subject name does **not** include `v1`; only one subject
  (`e-commerce-events-value`) exists at the Registry level.
- For PR #3's evolution experiments, each experiment can create `v1.1/` etc.
  without losing the baseline.

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          Developer laptop                        │
│                                                                  │
│  schemas/ecommerce/v1/*.avsc   ◄── edited, PR-reviewed in Git    │
│           │                                                      │
│           │  make register-schemas                               │
│           ▼                                                      │
│  scripts/register_schemas.py                                     │
│           │                                                      │
└───────────┼──────────────────────────────────────────────────────┘
            │  HTTP POST /subjects/<subject>/versions
            ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Schema Registry                           │
│                       (from PR #1, :8081)                        │
│                                                                  │
│  subjects/                                                       │
│    e-commerce-events-value  →  [v1: EcommerceEvent (envelope)]   │
│                                                                  │
│  storage: `_schemas` topic on the Kafka cluster                  │
└──────────────────────────────────────────────────────────────────┘
            ▲
            │  GET /schemas/ids/<id>  (on first produce, cached)
            │
┌───────────┴──────────────────────────────────────────────────────┐
│                    AvroEventProducer (Python)                    │
│                                                                  │
│  user code ──► Pydantic validate ──► dict ──► AvroSerializer     │
│                                                │                 │
│                                                ▼                 │
│                                          magic byte | schema id  │
│                                             | Avro payload       │
│                                                │                 │
│                                                ▼                 │
│                              confluent_kafka.SerializingProducer │
└─────────────────────────────────┬────────────────────────────────┘
                                  │ PLAINTEXT to :19092/3/4
                                  ▼
                        e-commerce-events topic
                        (partitioned by user_id)
```

---

## 4. Detailed Implementation

### 4.1 Directory Structure Additions

```
streaming-feature-store/
├── schemas/
│   └── ecommerce/
│       └── v1/
│           ├── click_payload.avsc
│           ├── purchase_payload.avsc
│           ├── page_view_payload.avsc
│           └── ecommerce_event.avsc       # envelope (references the above)
├── scripts/
│   └── register_schemas.py
├── src/
│   └── streaming_feature_store/
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── loader.py                  # read .avsc files from disk
│       │   ├── registry.py                # SchemaRegistryClient wrapper
│       │   └── models.py                  # Pydantic mirrors of the Avro schemas
│       └── producer/
│           ├── __init__.py
│           └── avro_producer.py           # AvroEventProducer
└── tests/
    ├── unit/
    │   ├── test_schemas.py
    │   ├── test_schema_loader.py
    │   ├── test_schema_models.py
    │   └── test_avro_producer_unit.py
    └── integration/
        ├── test_schema_registration.py
        └── test_avro_producer_end_to_end.py
```

### 4.2 Avro Schemas

**`schemas/ecommerce/v1/ecommerce_event.avsc`** (envelope)

```json
{
  "type": "record",
  "name": "EcommerceEvent",
  "namespace": "com.featurestore.ecommerce.v1",
  "doc": "Envelope for all e-commerce events on the e-commerce-events topic.",
  "fields": [
    { "name": "event_id",        "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "event_type",      "type": { "type": "enum",
                                           "name": "EventType",
                                           "symbols": ["CLICK", "PURCHASE", "PAGE_VIEW"] } },
    { "name": "user_id",         "type": "string" },
    { "name": "session_id",      "type": "string" },
    { "name": "event_timestamp", "type": { "type": "long", "logicalType": "timestamp-micros" } },
    { "name": "payload",         "type": ["com.featurestore.ecommerce.v1.ClickPayload",
                                          "com.featurestore.ecommerce.v1.PurchasePayload",
                                          "com.featurestore.ecommerce.v1.PageViewPayload"] }
  ]
}
```

**`schemas/ecommerce/v1/click_payload.avsc`**

```json
{
  "type": "record",
  "name": "ClickPayload",
  "namespace": "com.featurestore.ecommerce.v1",
  "fields": [
    { "name": "element_id", "type": "string" },
    { "name": "page_url",   "type": "string" }
  ]
}
```

**`schemas/ecommerce/v1/purchase_payload.avsc`**

```json
{
  "type": "record",
  "name": "PurchasePayload",
  "namespace": "com.featurestore.ecommerce.v1",
  "fields": [
    { "name": "product_id",   "type": "string" },
    { "name": "quantity",     "type": "int" },
    { "name": "price_cents",  "type": "long" },
    { "name": "currency",     "type": { "type": "string", "avro.java.string": "String" },
                              "default": "USD" }
  ]
}
```

**`schemas/ecommerce/v1/page_view_payload.avsc`**

```json
{
  "type": "record",
  "name": "PageViewPayload",
  "namespace": "com.featurestore.ecommerce.v1",
  "fields": [
    { "name": "page_url", "type": "string" },
    { "name": "referrer", "type": ["null", "string"], "default": null }
  ]
}
```

> **Note on union resolution:** The envelope references payload records by
> fully-qualified name. The loader (§4.3) resolves the four files into a single
> combined schema string before registration, so the Registry sees one
> self-contained schema per subject.

### 4.3 `schemas/loader.py` — Composite Schema Assembly

Responsibilities (single responsibility per function — per CLAUDE.md §1):

| Function | Purpose |
|---|---|
| `load_avro_file(path: Path) -> dict` | Read a single `.avsc`, parse as JSON, return dict. Raises `SchemaLoadError` on malformed JSON. |
| `load_schema_set(dir: Path) -> dict` | Read all `.avsc` in a version directory, resolve named type references, return one combined schema dict suitable for `fastavro.parse_schema`. |
| `dump_schema(schema: dict) -> str` | Canonical JSON dump (sorted keys, no whitespace) for consistent registration hashing. |
| `SCHEMAS_ROOT` constant | `Path(__file__).parents[3] / "schemas"` — works cross-platform via `pathlib`. |

Uses `fastavro.parse_schema` for named-type resolution rather than hand-rolling
it.

### 4.4 `schemas/registry.py` — Registry Client Wrapper

Thin wrapper over `confluent_kafka.schema_registry.SchemaRegistryClient`.

| Method | Purpose |
|---|---|
| `__init__(config: SchemaRegistryConfig)` | Builds underlying client from the PR #1 `SchemaRegistryConfig`. |
| `register(subject: str, schema_str: str) -> int` | Registers (or re-uses) schema; returns the global schema ID. |
| `get_latest(subject: str) -> RegisteredSchema` | Fetches the latest version for a subject. |
| `set_compatibility(subject: str, level: str) -> None` | Sets per-subject compatibility; defaults here are left at global. |
| `list_subjects() -> list[str]` | Introspection for tests and the `make schema-subjects` target from PR #1. |

All methods emit structured logs (`logging` module + f-strings per CLAUDE.md §5).

### 4.5 `schemas/models.py` — Pydantic Mirrors

```python
class EventType(str, Enum):
    CLICK = "CLICK"
    PURCHASE = "PURCHASE"
    PAGE_VIEW = "PAGE_VIEW"


class ClickPayload(BaseModel):
    element_id: str = Field(..., min_length=1)
    page_url:   str = Field(..., min_length=1)


class PurchasePayload(BaseModel):
    product_id:  str  = Field(..., min_length=1)
    quantity:    int  = Field(..., ge=1)
    price_cents: int  = Field(..., ge=0)
    currency:    str  = Field(default="USD", pattern=r"^[A-Z]{3}$")


class PageViewPayload(BaseModel):
    page_url: str           = Field(..., min_length=1)
    referrer: str | None    = None


class EcommerceEvent(BaseModel):
    event_id:        UUID
    event_type:      EventType
    user_id:         str        = Field(..., min_length=1)
    session_id:      str        = Field(..., min_length=1)
    event_timestamp: datetime
    payload:         ClickPayload | PurchasePayload | PageViewPayload

    def to_avro_dict(self) -> dict:
        """Convert to a dict matching the EcommerceEvent Avro schema."""
```

`to_avro_dict` handles:
- UUID → string.
- `datetime` → timezone-aware microsecond epoch (Avro `timestamp-micros`).
- Payload → `{"com.featurestore.ecommerce.v1.PurchasePayload": {...}}`
  **tagged-union encoding** required by `fastavro`/`confluent-kafka-python`.

### 4.6 `producer/avro_producer.py`

```python
class AvroEventProducer:
    """Avro-serializing Kafka producer for EcommerceEvent messages.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap server configuration from PR #1.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings from PR #1.
    topic : str, optional
        Target topic. Defaults to ``kafka_config.default_topic``.

    Notes
    -----
    * Uses the default ``TopicNameStrategy``; the subject is ``<topic>-value``.
    * ``auto.register.schemas=False`` — schemas must be pre-registered via
      ``scripts/register_schemas.py``.
    * ``use.latest.version=True`` — serializes against the latest registered
      schema for the subject.
    * Not thread-safe. Construct one instance per producing thread.
    """
```

Key methods:

| Method | Behavior |
|---|---|
| `produce(event: EcommerceEvent, on_delivery: Callable \| None = None) -> None` | Validates, serializes, enqueues. Key = `event.user_id` (UTF-8 bytes) for per-user partition stickiness (per gap plan line 73). |
| `flush(timeout_s: float = 10.0) -> int` | Delegates to underlying `Producer.flush`; returns unflushed message count. |
| `__enter__` / `__exit__` | Context-manager flush on exit. |
| `close()` | Explicit flush + drop references. |

Internal helpers (each does one thing):
- `_build_serializer()` → returns configured `AvroSerializer`.
- `_build_producer()` → returns configured `SerializingProducer`.
- `_delivery_report(err, msg)` → default logger-based callback (WARNING on err,
  DEBUG on success).

### 4.7 `scripts/register_schemas.py`

CLI built with `argparse`:

```
usage: register_schemas.py [--schemas-dir PATH] [--subject SUBJECT]
                           [--compatibility LEVEL] [--dry-run]
```

Logic:
1. Load `SchemaRegistryConfig` (env-overridable).
2. Enumerate `schemas/ecommerce/v*/` directories (sorted; latest wins).
3. Build the composite `EcommerceEvent` schema string via `loader.load_schema_set`.
4. Log the target subject + schema ID that would be produced (dry-run mode stops here).
5. Register; log returned schema ID and version.
6. Optionally set per-subject compatibility level (default: leave at Registry default).
7. Exit non-zero if registration rejected (e.g., incompatible schema vs current
   `BACKWARD` level) with the Registry's error body in the log.

### 4.8 `Makefile` additions

```makefile
register-schemas:        ## Register all .avsc files under schemas/ with the Registry
	python scripts/register_schemas.py

register-schemas-dry:    ## Show what would be registered without writing
	python scripts/register_schemas.py --dry-run

produce-sample:          ## Send a handful of sample events end-to-end
	python -m streaming_feature_store.producer.avro_producer --sample 5
```

---

## 5. Unit Tests

Unit tests run without Docker. They exercise on-disk correctness, Pydantic
validation, and producer wiring with mocks.

### 5.1 `tests/unit/test_schemas.py` — Avro file correctness

| Test | Assertion |
|---|---|
| `test_all_avsc_files_exist` | All 4 expected `.avsc` files present under `schemas/ecommerce/v1/` |
| `test_each_avsc_is_valid_json` | Each file parses as JSON |
| `test_each_avsc_has_namespace` | Every record declares `com.featurestore.ecommerce.v1` |
| `test_envelope_references_all_payloads` | `EcommerceEvent.fields.payload` union contains all 3 payload FQNs |
| `test_envelope_has_required_top_level_fields` | All 6 top-level fields present with correct types/logical types |
| `test_event_type_enum_symbols` | Enum symbols are exactly `{CLICK, PURCHASE, PAGE_VIEW}` |
| `test_purchase_has_defaulted_currency` | `currency` field has `default: "USD"` |
| `test_page_view_referrer_is_nullable` | `referrer` field is `["null", "string"]` with `default: null` |
| `test_composite_schema_parses_with_fastavro` | `load_schema_set` output round-trips through `fastavro.parse_schema` |

### 5.2 `tests/unit/test_schema_loader.py`

| Test | Assertion |
|---|---|
| `test_load_avro_file_reads_valid` | Known-good file produces expected dict |
| `test_load_avro_file_raises_on_bad_json` | Corrupted temp file raises `SchemaLoadError` |
| `test_load_avro_file_raises_on_missing` | Nonexistent path raises `FileNotFoundError` |
| `test_load_schema_set_combines_records` | Composite schema contains all 4 named types |
| `test_load_schema_set_rejects_empty_dir` | Empty version dir raises `SchemaLoadError` |
| `test_dump_schema_is_canonical` | Same dict → identical string across calls (sorted keys) |
| `test_schemas_root_is_absolute_path` | `SCHEMAS_ROOT` resolves to an absolute path cross-platform |

### 5.3 `tests/unit/test_schema_models.py` — Pydantic parity

| Test | Assertion |
|---|---|
| `test_click_payload_valid` | Well-formed `ClickPayload` constructs |
| `test_click_payload_rejects_empty_url` | `page_url=""` raises `ValidationError` |
| `test_purchase_payload_quantity_must_be_positive` | `quantity=0` raises |
| `test_purchase_payload_price_non_negative` | `price_cents=-1` raises |
| `test_purchase_payload_currency_pattern` | `currency="usd"` raises (must be 3 uppercase) |
| `test_page_view_referrer_nullable` | `PageViewPayload(page_url="/", referrer=None)` valid |
| `test_ecommerce_event_discriminates_by_payload` | A `PurchasePayload` instance populates the union correctly |
| `test_to_avro_dict_encodes_uuid_as_string` | `event_id` is `str` in dict |
| `test_to_avro_dict_encodes_timestamp_as_int_micros` | `event_timestamp` is `int` microseconds since epoch |
| `test_to_avro_dict_uses_tagged_union_for_payload` | Dict key is the payload's FQN |
| `test_pydantic_fields_match_avro_fields` | Parametrized: for each Avro record, the Pydantic model's fields are a superset with compatible types (guards against schema/model drift) |

### 5.4 `tests/unit/test_avro_producer_unit.py` — Producer wiring, mocked

Uses `pytest` fixtures and `unittest.mock.patch` on
`confluent_kafka.SerializingProducer` and `SchemaRegistryClient`.

| Test | Assertion |
|---|---|
| `test_producer_builds_with_expected_config` | Serializer config contains `auto.register.schemas=False` and `use.latest.version=True` |
| `test_producer_uses_topic_from_kafka_config` | `default_topic` is propagated |
| `test_produce_serializes_and_sends` | `.produce(event)` calls underlying producer with key = `user_id` bytes |
| `test_produce_rejects_non_pydantic_input` | Passing a plain dict raises `TypeError` |
| `test_context_manager_flushes_on_exit` | `flush()` is called on `__exit__` |
| `test_delivery_report_logs_error` | Simulated error triggers a WARNING log entry (capture via `caplog`) |
| `test_close_is_idempotent` | Calling `close()` twice does not raise |

---

## 6. Integration Tests

Integration tests require the PR #1 Docker Compose stack to be up (`make infra-up`).

### 6.1 Prerequisites

- Kafka brokers + Schema Registry healthy (reuses PR #1's `docker_services_up`
  session-scoped fixture in `tests/conftest.py`).
- A session-scoped fixture `registered_ecommerce_schema` that invokes
  `scripts/register_schemas.py` programmatically and returns the registered
  schema ID. Teardown deletes the subject (soft + hard delete) so the test run
  is repeatable.

### 6.2 `tests/integration/test_schema_registration.py`

| Test | What It Verifies |
|---|---|
| `test_register_schemas_creates_subject` | After registration, `GET /subjects` contains `e-commerce-events-value` |
| `test_registered_schema_has_expected_version_1` | `GET /subjects/.../versions/latest` returns version 1 |
| `test_registered_schema_payload_matches_disk` | Server-side schema body parses to the same `fastavro.parse_schema` output as the on-disk composite |
| `test_reregistration_is_idempotent` | Running the script twice produces the same schema ID and still one version |
| `test_registration_rejects_incompatible_change` | Manually mutate an on-disk schema to remove a required field without default; registration exits non-zero and the Registry reports `Schema being registered is incompatible with an earlier schema` |
| `test_compatibility_level_defaults_to_backward` | `GET /config` (from PR #1) is `BACKWARD`; per-subject `GET /config/<subject>` inherits it |

### 6.3 `tests/integration/test_avro_producer_end_to_end.py`

| Test | What It Verifies |
|---|---|
| `test_producer_connects_and_sends_single_event` | Produce 1 `ClickEvent`, flush succeeds, no delivery errors |
| `test_produced_bytes_have_confluent_wire_format` | Consume raw bytes via a plain `confluent_kafka.Consumer`; assert first byte is `0x00` (magic) and bytes[1:5] decode to a registered schema ID |
| `test_roundtrip_click_event` | Produce → consume with `AvroDeserializer` → resulting dict matches the Pydantic input via `to_avro_dict()` |
| `test_roundtrip_purchase_event` | Same, covers the `PurchasePayload` branch of the union |
| `test_roundtrip_page_view_event_with_null_referrer` | Covers the nullable-field branch |
| `test_same_user_id_lands_in_same_partition` | Produce 10 events for `user_id="u-42"`; all arrive on the same partition (validates the per-user partitioning key choice from gap plan line 73) |
| `test_produce_fails_when_schema_not_registered` | Delete the subject, attempt produce → raises `SerializationError` referencing "subject not found"; confirms `auto.register.schemas=False` is effective |
| `test_small_batch_produce_flush_count` | Produce 50 mixed events; `flush()` returns 0 |

Volume cap per integration test function: **≤50 messages** (per PR #1 §6.3
resource-conscious design). Heavy throughput work is deferred to PR #4.

---

## 7. How to Run

> Prerequisites: PR #1's infrastructure must be running. If not, start it:
> ```bash
> make infra-up
> make infra-status   # wait until all 5 services are healthy
> ```

### 7.1 Install the new Python dependencies

New runtime dependencies (added to `pyproject.toml`):

- `confluent-kafka[avro,schemaregistry] >= 2.4`
- `fastavro >= 1.9`

```bash
source .venv/bin/activate
make install
```

### 7.2 Register the schemas

```bash
# Dry run — shows what would be registered, does not contact the registry
make register-schemas-dry

# Actually register
make register-schemas

# Verify
make schema-subjects        # from PR #1 Makefile
# Expected: ["e-commerce-events-value"]
```

### 7.3 Run unit tests (no Docker required)

```bash
make test-unit
# or: pytest tests/unit/test_schemas.py tests/unit/test_schema_loader.py \
#            tests/unit/test_schema_models.py tests/unit/test_avro_producer_unit.py -v
```

### 7.4 Run integration tests

```bash
# Ensure infra is up and schemas are registered (the fixture will do this too)
make test-integration
# or: pytest tests/integration/test_schema_registration.py \
#            tests/integration/test_avro_producer_end_to_end.py -v -m integration -p no:xdist
```

### 7.5 Produce a few sample events by hand

```bash
make produce-sample
# Expected log output:
#   INFO  Producing sample event 1/5: CLICK user=u-0001
#   ...
#   INFO  Flushed 5 message(s) to e-commerce-events

# Inspect them via kcat or the cluster:
docker compose -f docker/docker-compose.yml exec kafka-1 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic e-commerce-events --from-beginning --max-messages 5 \
    --property print.key=true
# Note: the payload is binary Avro; the first 5 bytes are the Confluent wire
# prefix (0x00 + 4-byte schema ID). Use the Avro-aware consumer in
# tests/integration/ for human-readable output.
```

---

## 8. Resource Budget & Constraints

This PR adds **no new long-running processes**. All footprint is incremental
against PR #1.

| Item | Incremental cost | Notes |
|---|---|---|
| On-disk `.avsc` files | < 8 KB total | Negligible |
| New Python deps (`confluent-kafka`, `fastavro`) | ~15 MB in venv | One-time install |
| Per-produce network | ~1 RTT to Registry on first message per schema (cached thereafter) + usual broker RTT | Fine for laptop |
| Schema Registry storage | 1 schema in the `_schemas` topic | Bytes |
| Test runtime (unit) | < 3 s | No Docker required |
| Test runtime (integration) | < 30 s | Reuses PR #1 infra |

No new ports. No new containers.

---

## 9. Future Considerations

1. **Schema evolution experiments (PR #3, gap plan line 66):** `BACKWARD` mode
   drills — add optional `device_type`, remove a deprecated field, promote
   `int` → `long`. The subject-level `set_compatibility` method on the Registry
   wrapper is already in place; the experiments just flip compatibility and run
   registration diffs.

2. **High-throughput producer (PR #4, gap plan line 67):** 50K+ evt/sec
   synthetic e-commerce event generator. Will reuse `AvroEventProducer` as-is
   but wrap it in an asyncio / multiprocessing harness. Current design
   deliberately caches the serializer and pre-fetches the schema, which is the
   hot path.

3. **Consumer-side deserialization (later PR):** Will pair `AvroDeserializer`
   with the same Pydantic models — symmetric to the producer.

4. **CI-enforced registration:** A GitHub Actions job should run
   `register_schemas.py --dry-run` and also `--check-compatibility` against a
   throwaway Registry container, failing the PR if `.avsc` changes are
   incompatible. Deferred — local developer workflow first.

5. **Code generation from `.avsc`:** If the schema count grows past ~10,
   auto-generate Pydantic models from `.avsc` via a small
   `datamodel-code-generator`-style tool to eliminate the two-layer maintenance
   cost. Not worth the tooling investment for 3 event types.

6. **Key schemas:** Currently the Kafka message key is a raw UTF-8 `user_id`
   string. A later PR may promote it to a typed Avro `UserKey` record (for
   stronger contracts and tools like ksqlDB). `TopicNameStrategy` with both
   `-key` and `-value` subjects is already the default.

7. **Protobuf-based variant:** If a later phase of the broader gap project
   (Phase 3 LLM serving, Phase 4 K8s) standardizes on Protobuf + gRPC, Phase 1
   could grow a parallel Protobuf schema set under `schemas/ecommerce-proto/`
   for comparison. Out of scope here.

---

## 10. Open Questions

1. **Schema `null` defaults vs field-omission semantics:** `PageViewPayload.referrer`
   is `["null", "string"]` with `default: null`. Under `BACKWARD`, adding a
   nullable field with a default is always safe — this is intentional
   preparation for PR #3. **Recommendation:** Keep the default; it also aligns
   with the CLAUDE.md "explicit over implicit" spirit.

2. **Subject name:** `e-commerce-events-value` (hyphens in topic name carried
   over from PR #1) vs `ecommerce_events-value` (underscore). The `KafkaConfig`
   in PR #1 uses `e-commerce-events` as the default topic name, and the subject
   is mechanically derived. **Recommendation:** Keep the hyphen; do not rename.

3. **Auto-registration flag:** Should the producer ever allow
   `auto.register.schemas=True`, for example in a local dev mode? The argument
   for keeping it permanently off: it trains the production muscle. The
   argument for a dev-mode flag: quicker iteration when editing schemas.
   **Recommendation:** Keep off everywhere; `make register-schemas` is one
   command.

4. **Logical types for UUID:** Avro's `uuid` logical type is supported by
   `fastavro` but *not* by every language's Avro library. Since this project is
   Python-only, we use it. If we ever add a JVM consumer (e.g., a Flink job in
   Week 2), we revisit. **Recommendation:** Use `uuid` logical type now;
   re-evaluate when polyglot consumers appear.

5. **Pydantic model drift vs Avro:** The `test_pydantic_fields_match_avro_fields`
   parity test gives a CI guard but not a generator. If drift becomes a recurring
   pain point (e.g., 2+ PRs in a row add a field to the `.avsc` and forget the
   model), switch to code generation. **Recommendation:** Parity test for now;
   revisit at the 10-schema mark.
