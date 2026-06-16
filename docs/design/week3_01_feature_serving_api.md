# Design Doc: Online Feature Serving API (FastAPI + Redis)

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 3 — Online Feature Serving Layer
**Scope:** The first Week 3 bullet of [`gap_project_plan.md`](gap_project_plan.md)
(lines 90–91): *"Build a REST API (FastAPI) that serves features for a given
entity (user ID) from Redis — skip gRPC here; you will build gRPC streaming in
Phase 3 (LLM serving) where it actually matters."* Also folds in the Week 3
thematic note (line 95): scale the API via **uvicorn worker processes, not
threads** — the same process-not-threads reasoning as Weeks 1–2, restated at
the serving edge. The feature-vector **assembly** endpoint (plan line 92) and
the formal **<5 ms p99 benchmark harness** (plan line 93) are the *next* Week 3
PRs and are explicitly out of scope here (§1, §9).
**Supersedes / superseded-by:** none — strictly additive. Reads the Redis
online store written by
[`week2_02_sliding_window_features_plain_consumer.md`](week2_02_sliding_window_features_plain_consumer.md)
§2.7–§2.9; writes nothing.
**Author:** Auto-generated design document
**Date:** 2026-06-11

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
11. [Top 5 Concepts Worth Understanding (Interview Prep)](#11-top-5-concepts-worth-understanding-interview-prep)

---

## 1. Overview

By the end of Week 2 the pipeline computes sliding-window features in real time
and sinks them to the **online store**: one Redis hash per user,
`feat:user:{user_id}`, with resolution-suffixed fields
(`clicks_5m`, `revenue_1h`, `purchases_24h`, …) written latest-wins by the
`RedisHashSink`
([`sliding/sinks.py`](../../src/streaming_feature_store/sliding/sinks.py),
design [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.8).
What is missing is the **read path**: nothing serves those features to an
inference client.

This PR ships that read path — a small, read-only **FastAPI** service exposing

```
GET /v1/features/users/{user_id}
```

which performs **one `HGETALL`** against Redis and returns a fully-typed,
fixed-shape **13-field feature vector** (4 fields at 5 m, 5 at 1 h, 4 at 24 h),
synthesizing zeros for absent fields per the downstream-default-zero read
contract ([`week2_02`](week2_02_sliding_window_features_plain_consumer.md)
§2.7). The service is deliberately boring: no Kafka, no writes, no cache, no
auth — a thin, correct, typed adapter from the Redis write contract to HTTP.

### What ships

- **`serving/` package** — `RedisFeatureReader` (async, one `HGETALL` per
  request, default-zero synthesis), Pydantic response models
  (`FeatureVector`, `UserFeaturesResponse`), `ServingConfig`, and a FastAPI
  **app factory** (`create_app()`) with lifespan-scoped connection pooling
  (§3.2, §4).
- **The read schema derived from the write schema** — the serving field
  universe is *imported* from
  [`sliding/models.py`](../../src/streaming_feature_store/sliding/models.py)
  (`WindowResolution` + a newly **promoted-to-public** field-prefix map and an
  explicit per-resolution feature table), with a drift-guard unit test pinning
  it to what the aggregators actually emit (§2.2, §4.1).
- **Health endpoints** — `GET /healthz` (liveness: process up) and
  `GET /readyz` (readiness: Redis `PING`) (§2.9).
- **Process-based scaling at the edge** — `scripts/run_feature_api.py`
  launches uvicorn with `--workers N` (processes, not threads), keeping the
  Week 1/2 GIL narrative consistent end-to-end (§2.6).
- Unit + integration tests (§5, §6) and `Makefile` targets
  (`serving-run`, `serving-run-group`, `serving-smoke`) (§7).

### What this PR does **not** do

1. **Feature-vector assembly across feature groups** (plan line 92). Today
   there is exactly **one** feature group — the sliding-window group — and one
   hash per user, so "assembly" would be a no-op wearing an architecture
   costume. The assembly endpoint becomes meaningful when a second group
   exists (Week 4 batch features, or session features), and gets its own
   design doc (§9.1).
2. **The formal <5 ms p99 benchmark** (plan line 93). This PR includes only a
   curl-level smoke check (§7.3). The proper harness — load generator,
   p50/p95/p99 percentiles, worker-count sweep, results doc — is the next
   Week 3 PR (§9.2). The plan's calibration stands: Redis on a laptop will hit
   <5 ms without explicit caching/pooling work beyond the connection pool that
   ships here.
3. **gRPC.** Deliberately deferred to Phase 3 (LLM serving), where its actual
   advantage — **streaming within a single call** (token-by-token delivery) —
   applies. For a single-entity point lookup, gRPC's wins (binary framing,
   HTTP/2 multiplexing) are microseconds against a budget dominated by the
   Redis RTT; REST keeps the surface curl-able and the Pydantic contract
   first-class (§2.1, §11.3).
4. **Caching, auth, rate limiting.** Out of scope at laptop scale; discussed
   as interview talking points only (§9.4).

### Deliverables

- `src/streaming_feature_store/serving/__init__.py` — package init.
- `src/streaming_feature_store/serving/models.py` — `FeatureVector`,
  `UserFeaturesResponse`, `ServingConfig`.
- `src/streaming_feature_store/serving/store.py` — `RedisFeatureReader`.
- `src/streaming_feature_store/serving/app.py` — `create_app()` factory,
  lifespan, routes, error mapping.
- Surgical edit to `src/streaming_feature_store/sliding/models.py` — promote
  the private `_REDIS_FIELD_PREFIXES` to public `REDIS_FIELD_PREFIXES` (alias
  retained) and add `RESOLUTION_FEATURES` + `expected_redis_fields()` (§4.1).
- `scripts/run_feature_api.py` — uvicorn entry point with CLI flags.
- `pyproject.toml` — new `serving` extra (`fastapi`, `uvicorn[standard]`,
  `redis`); `test` extra gains `fastapi`, `httpx`, `uvicorn`.
- `Makefile` targets: `serving-run`, `serving-run-group`, `serving-smoke`.
- Unit + integration tests under `tests/`.

---

## 2. Critical Design Decisions

### 2.1 REST (FastAPI) over gRPC — a Deliberate Deferral, Not an Omission

**Decision:** Serve features over plain REST/JSON with FastAPI. Do not build a
gRPC surface in Phase 1.

**Rationale:**

- **The interaction shape is a point lookup, not a stream.** gRPC's decisive
  advantage is incremental, ordered, point-to-point delivery *within one call*
  (server streaming) plus HTTP/2 multiplexing under high concurrency. A
  single-entity feature fetch is one small request → one small response; the
  latency budget is dominated by the Redis round trip and Python-side
  serialization, not by JSON-vs-protobuf framing (µs-scale at this payload
  size). The protocol choice does not move the p99 needle here.
- **Phase 3 is where gRPC earns its complexity.** LLM token streaming is the
  canonical server-streaming workload — that is where `.proto` contracts,
  codegen, and HTTP/2 framing pay rent. Building gRPC twice (here as a toy,
  there for real) would dilute both. The plan encodes this explicitly
  (line 91).
- **REST maximizes inspectability at laptop scale.** `curl`, the auto-generated
  OpenAPI docs at `/docs`, and human-readable JSON make the smoke loop and the
  Week 5 integration tests cheap.
- **FastAPI specifically (vs Flask et al.):** Pydantic-native request/response
  validation matches the repo-wide Pydantic mandate (CLAUDE.md §3) with zero
  adapter code, and the async-first design fits an I/O-bound Redis read path
  (§2.5).

### 2.2 The Read Schema Is the Write Schema, Imported (Single Source of Truth)

**Decision:** The serving layer does **not** define its own list of feature
names. It derives the 13-field universe from
[`sliding/models.py`](../../src/streaming_feature_store/sliding/models.py):

- `WindowResolution` — already public; its string values (`"5m"`/`"1h"`/`"24h"`)
  are documented as *the* cross-layer field suffixes.
- `_REDIS_FIELD_PREFIXES` — **promoted to public** `REDIS_FIELD_PREFIXES`
  (the private name kept as a deprecated alias so `redis_field_updates()` is
  untouched).
- A new explicit table `RESOLUTION_FEATURES: dict[WindowResolution,
  tuple[str, ...]]` recording which feature prefixes each resolution's
  aggregator actually populates, plus `expected_redis_fields()` returning the
  13-field cross product (§4.1).

A **drift-guard unit test** asserts that `RESOLUTION_FEATURES` matches what
`FiveMinuteAggregator` / `OneHourAggregator` / `TwentyFourHourAggregator`
`get_result()` actually emit, and that the serving `FeatureVector` model's
field names equal `expected_redis_fields()` exactly (§5).

**Rationale:**

- **Today the per-resolution field sets live only implicitly in the
  aggregators' `get_result()` bodies.** The write side never needed the
  explicit table (each record carries its own non-`None` fields); the read
  side does, because it must synthesize zeros for *absent* fields and needs to
  know which fields are *supposed* to exist. Declaring the table next to the
  enum — the documented "single coordination point" for renames — and pinning
  it to the aggregators with a test makes contract drift a CI failure instead
  of a silent all-zeros bug in production reads.
- **Duplicating the list in `serving/` is the obvious wrong move** — two
  hand-maintained copies of a 13-string contract drift apart exactly once,
  silently, and the default-zero contract (§2.3) means the symptom is wrong
  *values*, not errors.

The resulting field universe (write-side ground truth, from the aggregators):

| Resolution | Fields |
|---|---|
| `5m` (300 s window / 60 s slide) | `clicks_5m`, `page_views_5m`, `purchases_5m`, `revenue_5m` |
| `1h` (3600 s / 300 s) | `clicks_1h`, `page_views_1h`, `purchases_1h`, `revenue_1h`, `distinct_products_1h` |
| `24h` (86400 s / 3600 s) | `purchases_24h`, `revenue_24h`, `distinct_products_24h`, `avg_purchase_amount_24h` |

### 2.3 Missing Field → 0, Missing Key → 200 With Zeros (the Sparsity Contract, Extended to HTTP)

**Decision:** The API returns **HTTP 200 with a complete, all-defaults feature
vector** in both sparse cases:

- **Field absent in the hash** (user active recently, but e.g. no purchases in
  the window, or that resolution's TTL expired the *key* refresh) → that field
  is `0` / `0.0`.
- **Key absent entirely** (user inactive past all TTLs, or never seen) → all
  13 fields default, and the response carries `key_present: false`.

`404` is **not** used for unknown users. `422` is reserved for malformed
`user_id` path values (FastAPI validation); `503` for Redis unavailability.

**Rationale:**

- **This is the Week 2 contract, finished.**
  [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.7
  states: *"the Redis read path treats a missing field as zero"* — the writer
  never emits zero-valued records (sparsity), so the zeros **must** be
  synthesized at read time. This PR is where that read path finally exists;
  the contract is a property of the sink **and the read adapter**, and the
  adapter half lands here.
- **For ML inference, a cold user is a feature vector, not an error.** The
  consumer of this endpoint is a model scorer: "no recent activity" is a
  legitimate, meaningful input (all-zeros = cold start), and forcing every
  client to translate 404 → zeros would just push the same contract into N
  callers. `key_present: false` preserves the signal for callers that *do*
  want to distinguish "expired/cold" from "active but quiet" without breaking
  the always-a-vector shape.
- **TTL expiry is feature decay, not data loss.** Per-resolution TTLs
  (1.5 × window = 450 s / 5400 s / 129 600 s,
  [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.7) mean
  an idle user's hash evaporates by design; the read contract turns that
  evaporation back into the semantically correct value — zero activity.

### 2.4 One `HGETALL` per Request (Not `HMGET`, Not N Reads)

**Decision:** Fetch the user's features with a single `HGETALL
feat:user:{user_id}`, drop unknown fields, and let the Pydantic model apply
types and defaults.

**Rationale:**

- **One round trip is the entire performance design.** The hash-per-entity
  layout chosen in Week 2 §2.8 exists precisely so the serving read is one
  O(fields) command. At ≤13 small fields, `HGETALL` and a 13-field `HMGET`
  are equivalent on the wire and in Redis time; `HGETALL` is simpler (no field
  list to send) and degrades gracefully if the hash carries fields from a
  newer writer (forward compatibility: unknown fields are dropped, not
  errors).
- **`HMGET` wins only when the hash is much wider than the read set** — not
  the case here, and if a second feature group ever shares the key (it should
  not — §9.1 keeps groups in separate keys), that is a redesign moment, not a
  command-choice moment.
- **Typing via Pydantic, not hand parsing.** Redis returns strings;
  `FeatureVector(**hash)` coerces `"3"` → `3` and `"59.98"` → `59.98`,
  validates, and fills defaults in one shot — the model *is* the parser
  (§4.2).

### 2.5 Async Redis Client, Lifespan-Scoped, One Pool per Process

**Decision:** Use `redis.asyncio` with `decode_responses=True`. Create **one**
client (with its connection pool) per worker process inside the FastAPI
**lifespan** context; close it on shutdown. Handlers are `async def` and
`await` the read. No per-request connections; no module-level client.

**Rationale:**

- **The endpoint is pure I/O wait** — an async handler lets one worker process
  overlap hundreds of in-flight Redis RTTs on a single event loop. A sync
  handler + thread pool would reintroduce threads exactly where the project
  narrative says processes (§2.6) and add context-switch overhead to a <5 ms
  budget.
- **One blocking call anywhere in the handler path would stall every in-flight
  request** on that worker — hence the sync `redis` client (as used by the
  Week 2 *sink*) is not reused here. Writer and reader hold different
  performance contracts; sharing a client class between them buys nothing.
- **Lifespan scoping is the FastAPI-idiomatic injection seam.** The app
  factory accepts an optional pre-built client (`create_app(config,
  redis_client=...)`) mirroring the `RedisHashSink(config, client=...)`
  test-injection convention already in the repo.

### 2.6 Scale via uvicorn Worker *Processes*, Not Threads (GIL Symmetry)

**Decision:** The entry point exposes `--workers N` mapping to uvicorn worker
**processes** (default `N = 1` for the laptop smoke run). Threads are not a
scaling axis anywhere in this design.

**Rationale:**

- **The Week 1 investigation's conclusion recurs at the edge, as the plan
  predicted** (line 95). CPU work in this service (HTTP parse, routing,
  Pydantic validation/serialization) is GIL-bound Python; past one core's
  worth of it, the escape is identical to the producer (Week 1 §4.2) and the
  consumer group (Week 2 §2.11): **independent processes**, here behind one
  listening socket (`SO_REUSEPORT` via uvicorn) instead of one consumer group.
- **Async and processes are orthogonal axes, and this design uses both
  deliberately:** the event loop provides *concurrency* (many overlapped I/O
  waits per process); workers provide *parallelism* (many cores of CPU-bound
  framework work). Conflating them — or reaching for threads to get either —
  is the classic error the portfolio narrative calls out.
- **Stateless by construction, so scaling is trivial.** Unlike the Week 2
  consumer (per-partition state ownership), the API holds no per-user state —
  any worker can serve any user — so there is no partition-affinity story to
  design; `N` workers is purely a throughput knob.

### 2.7 Fixed, Fully-Typed 13-Field Response; No Field Selection in v1

**Decision:** The response embeds a `FeatureVector` model with **all 13 fields
always present** (typed `int`/`float`, defaults `0`/`0.0`), flat, named
**exactly** as the Redis fields (`clicks_5m`, …). No `?resolutions=` /
`?features=` selection parameters in v1.

**Rationale:**

- **Zero-transform passthrough.** The Redis field names are already the
  cross-layer contract (`WindowResolution` docstring); inventing a second
  naming scheme (nested `{"5m": {"clicks": …}}`) would create a mapping layer
  with no consumer asking for it. A model scorer wants a stable, flat,
  fixed-width vector — exactly what a fixed Pydantic model guarantees and
  what its OpenAPI schema documents for free.
- **Selection is the assembly endpoint's job.** Choosing *which* features to
  return is precisely the feature-group/vector-assembly concern of the next
  PR (plan line 92, §9.1). Bolting a query-param filter onto v1 would
  complicate the response-model semantics (omitted vs zero) for a capability
  the next PR delivers properly.
- **Fixed shape makes the default-zero contract self-documenting:** a client
  can never observe a missing field, so the §2.3 semantics have no client-side
  edge cases.

### 2.8 Strictly Read-Only: No Kafka, No Writes, No Cache

**Decision:** The service's only backend dependency is Redis, read-only. It
consumes no Kafka topic, writes nothing anywhere, and adds no in-process or
sidecar cache.

**Rationale:**

- **The serving path is independent of the producer-side EOS/GIL machinery by
  design** (plan line 95). Kafka isolation levels, transactions, and watermark
  semantics ([`week2_03`](week2_03_exactly_once_transactions.md)) shape what
  *lands in* Redis — the API inherits their guarantees through the store and
  needs to know nothing about them. Keeping the API Kafka-free preserves that
  clean seam (and keeps its dependency footprint to `fastapi` + `redis`).
- **A cache in front of Redis is a solution without a problem at this scale.**
  Localhost Redis serves a hash read in ~0.1–0.5 ms; an in-process cache would
  add a staleness layer *on top of* a store whose entire job is freshness, for
  no measurable latency win. The interview-grade discussion (when a serving
  cache *does* make sense: hot-key skew at network-attached-Redis scale,
  request coalescing) is exactly that — discussion, recorded in §9.4 per the
  plan's "be ready to discuss without having implemented" framing (line 93).

### 2.9 Liveness ≠ Readiness: `/healthz` and `/readyz`

**Decision:** Two health endpoints: `GET /healthz` returns 200 if the process
is up (no I/O); `GET /readyz` performs a Redis `PING` and returns 200/503.
Feature reads that hit a Redis connection failure map to **503** (with a
`Retry-After` hint), not 500.

**Rationale:**

- **The distinction is the Phase 4 on-ramp.** Kubernetes (Phase 4 Week 1
  explicitly deploys this service with "liveness/readiness probes") restarts
  on liveness failure and de-routes on readiness failure; an API that conflates
  them gets restart-looped when Redis blips. Building the split now costs ~10
  lines and makes the Phase 4 deployment a config exercise.
- **503 vs 500 is contract, not pedantry:** Redis-down is a *dependency*
  outage (retryable, expected during ops), and load balancers/clients treat
  5xx classes differently. 500 is reserved for actual bugs.

---

## 3. Architecture

### 3.1 Request Topology

```
                         (write path — Week 2, unchanged)
 validated-events ──▶ SlidingFeaturesConsumer ──▶ HSET feat:user:{uid} + EXPIRE
                                                        │
                                                        ▼
                                              ┌───────────────────┐
                                              │       Redis        │
                                              │  feat:user:{uid}   │
                                              │  hash, ≤13 fields  │
                                              └─────────┬─────────┘
                                                        │ HGETALL (1 RTT)
                  (read path — this PR)                 │
 inference client ──HTTP GET──▶ ┌───────────────────────┴───────────┐
 /v1/features/users/{user_id}   │  uvicorn (worker processes, N≥1)  │
                                │  ┌─────────────────────────────┐  │
                                │  │ FastAPI app (per process)    │  │
                                │  │  route → RedisFeatureReader  │  │
                                │  │  → FeatureVector(**hash)     │  │
                                │  │  (defaults fill the gaps)    │  │
                                │  └─────────────────────────────┘  │
                                │  redis.asyncio pool (per process) │
                                └───────────────────────────────────┘
```

One request = one `HGETALL` = one Redis round trip. No other I/O on the hot
path. Each worker process owns its own event loop and connection pool; workers
share nothing.

### 3.2 Module Layout

```
src/streaming_feature_store/
├── serving/                        # NEW — this PR
│   ├── __init__.py
│   ├── models.py                   # FeatureVector, UserFeaturesResponse, ServingConfig
│   ├── store.py                    # RedisFeatureReader (async read adapter)
│   └── app.py                      # create_app() factory, lifespan, routes
├── sliding/
│   └── models.py                   # EDITED — promote field map to public,
│                                   #   add RESOLUTION_FEATURES + expected_redis_fields()
scripts/
└── run_feature_api.py              # NEW — uvicorn entry point (CLI flags → ServingConfig)
```

### 3.3 Config Model

`ServingConfig` follows the `SlidingConsumerConfig` conventions (Pydantic
`BaseModel`, validated, NumPy-style docstring):

| Field | Default | Notes |
|---|---|---|
| `redis_host` | `"localhost"` | Online store host. |
| `redis_port` | `6379` | Compose maps `6379:6379`. |
| `redis_pool_max_connections` | `32` | Per-process pool ceiling. |
| `redis_socket_timeout_seconds` | `0.5` | Fail fast — a slow Redis read should 503, not queue. |
| `api_host` | `"0.0.0.0"` | uvicorn bind address. |
| `api_port` | `8000` | (8081 is taken by Schema Registry.) |
| `workers` | `1` | uvicorn worker **processes** (§2.6). |
| `key_prefix` | `"feat:user:"` | Must match the `RedisHashSink` key scheme; a constant shared in spirit, kept overridable for tests. |

---

## 4. Detailed Implementation

### 4.1 `sliding/models.py` — Publishing the Read-Side Contract

Surgical, behavior-preserving edit:

```python
# Public name; the old private name is kept as a deprecated alias so the
# write path (redis_field_updates) is untouched.
REDIS_FIELD_PREFIXES: dict[str, str] = {
    "click_count": "clicks",
    "page_view_count": "page_views",
    "purchase_count": "purchases",
    "revenue": "revenue",
    "distinct_products": "distinct_products",
    "avg_purchase_amount": "avg_purchase_amount",
}
_REDIS_FIELD_PREFIXES = REDIS_FIELD_PREFIXES  # deprecated alias

# Which feature prefixes each resolution's aggregator populates (design
# week3_01 §2.2).  Mirrors {Five,OneHour,TwentyFourHour}Aggregator.get_result;
# pinned to them by tests/test_sliding_models.py::test_resolution_features_match_aggregators.
RESOLUTION_FEATURES: dict[WindowResolution, tuple[str, ...]] = {
    WindowResolution.W_5M_SLIDE_1M: ("clicks", "page_views", "purchases", "revenue"),
    WindowResolution.W_1H_SLIDE_5M: (
        "clicks", "page_views", "purchases", "revenue", "distinct_products",
    ),
    WindowResolution.W_24H_SLIDE_1H: (
        "purchases", "revenue", "distinct_products", "avg_purchase_amount",
    ),
}


def expected_redis_fields() -> frozenset[str]:
    """Full universe of online-store hash fields (the 13-field contract).

    Returns
    -------
    frozenset of str
        Every ``"{prefix}_{resolution}"`` combination a conformant
        ``RedisHashSink`` may write — the exact field set the Week 3 serving
        layer types and defaults (design week3_01 §2.2).
    """
    return frozenset(
        f"{prefix}_{resolution.value}"
        for resolution, prefixes in RESOLUTION_FEATURES.items()
        for prefix in prefixes
    )
```

### 4.2 `serving/models.py` — Response Models + Config

The `FeatureVector` model *is* the parser, the validator, the default-zero
synthesizer, and the OpenAPI schema:

```python
class FeatureVector(BaseModel):
    """The fixed 13-field online feature vector for one user.

    Field names match the Redis hash fields byte-for-byte (design week3_01
    §2.7); integer counts and monetary floats are coerced from the strings
    Redis returns; absent fields take their declared zero defaults — the
    downstream-default-zero contract of week2_02 §2.7.
    """

    model_config = ConfigDict(extra="ignore")  # forward-compat: drop unknown fields

    clicks_5m: int = 0
    page_views_5m: int = 0
    purchases_5m: int = 0
    revenue_5m: float = 0.0

    clicks_1h: int = 0
    page_views_1h: int = 0
    purchases_1h: int = 0
    revenue_1h: float = 0.0
    distinct_products_1h: int = 0

    purchases_24h: int = 0
    revenue_24h: float = 0.0
    distinct_products_24h: int = 0
    avg_purchase_amount_24h: float = 0.0


class UserFeaturesResponse(BaseModel):
    """Envelope for ``GET /v1/features/users/{user_id}``."""

    user_id: str
    key_present: bool          # False ⇒ hash absent (cold / expired user), §2.3
    features: FeatureVector
```

Note `extra="ignore"` (not the repo-typical `extra="forbid"`): on the *read*
boundary, an unknown hash field means a **newer writer**, and the correct
posture is forward compatibility (drop), not rejection — the same reasoning as
`BACKWARD` schema compatibility on the Kafka side.

### 4.3 `serving/store.py` — `RedisFeatureReader`

```python
class RedisFeatureReader:
    """Async read adapter over the ``feat:user:{user_id}`` hash (design §2.4).

    Parameters
    ----------
    config : ServingConfig
        Supplies host/port/pool/timeout and the key prefix.
    client : redis.asyncio.Redis or None, optional
        Pre-built client (injected in tests, mirroring ``RedisHashSink``).
    """

    def __init__(self, config: ServingConfig,
                 client: redis.asyncio.Redis | None = None) -> None:
        self._config = config
        self._redis = client if client is not None else redis.asyncio.Redis(
            host=config.redis_host,
            port=config.redis_port,
            max_connections=config.redis_pool_max_connections,
            socket_timeout=config.redis_socket_timeout_seconds,
            decode_responses=True,
        )

    async def read(self, user_id: str) -> UserFeaturesResponse:
        """Fetch one user's feature vector (one ``HGETALL``).

        Absent fields and an absent key both materialize as zero-valued
        features (week2_02 §2.7 / week3_01 §2.3); ``key_present`` records
        which case occurred.
        """
        raw = await self._redis.hgetall(f"{self._config.key_prefix}{user_id}")
        return UserFeaturesResponse(
            user_id=user_id,
            key_present=bool(raw),
            features=FeatureVector(**raw),   # coercion + defaults in one shot
        )

    async def ping(self) -> bool:
        """Readiness probe — ``PING`` the store (design §2.9)."""

    async def close(self) -> None:
        """Close the client and its pool (idempotent)."""
```

### 4.4 `serving/app.py` — App Factory, Lifespan, Routes

```python
def create_app(config: ServingConfig | None = None,
               redis_client: redis.asyncio.Redis | None = None) -> FastAPI:
    """Build the serving app (factory pattern; clients injectable for tests)."""

    cfg = config if config is not None else ServingConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        reader = RedisFeatureReader(cfg, client=redis_client)
        app.state.reader = reader
        try:
            yield
        finally:
            await reader.close()

    app = FastAPI(title="streaming-feature-store serving API", lifespan=lifespan)

    @app.get("/v1/features/users/{user_id}", response_model=UserFeaturesResponse)
    async def get_user_features(
        user_id: Annotated[str, Path(min_length=1, max_length=128,
                                     pattern=r"^[\w\-.:]+$")],
    ) -> UserFeaturesResponse:
        try:
            return await app.state.reader.read(user_id)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            logging.error(f"Redis unavailable serving user_id={user_id}: {exc}")
            raise HTTPException(status_code=503, detail="online store unavailable",
                                headers={"Retry-After": "1"}) from exc

    @app.get("/healthz")   # liveness: no I/O
    @app.get("/readyz")    # readiness: PING → 200 / 503
    ...
    return app
```

Error surface, complete:

| Condition | Status | Body |
|---|---|---|
| Known/unknown user, Redis up | `200` | full vector (+ `key_present`) |
| Malformed `user_id` (pattern/length) | `422` | FastAPI validation detail |
| Redis down / timed out | `503` | `{"detail": "online store unavailable"}` + `Retry-After` |
| Anything else | `500` | (a bug — fix it) |

### 4.5 `scripts/run_feature_api.py` — Entry Point

`argparse` flags (matching repo CLI conventions) → `ServingConfig` →
`uvicorn.run("...:app", workers=cfg.workers, ...)`. With `--workers > 1`
uvicorn requires an import string, so the script exposes a module-level
factory target; each forked worker builds its own app, loop, and pool (§2.6).
`print()` is acceptable in this CLI layer only (CLAUDE.md §5); the app itself
uses `logging` with f-strings.

---

## 5. Unit Tests

All async tests via `httpx.AsyncClient`/`fastapi.testclient.TestClient`
against `create_app(config, redis_client=stub)` with a hand-rolled
`_StubAsyncRedis` (canned `hgetall` responses + failure injection), following
the repo's injected-client convention. 100 % line + branch coverage on the new
package (CLAUDE.md §6).

| Test | Asserts |
|---|---|
| `test_full_hash_round_trip` | All 13 fields present in Redis → typed values pass through unchanged (`"3"`→`3`, `"59.98"`→`59.98`). |
| `test_sparse_hash_defaults_to_zero` | Hash with 2 fields → other 11 are `0`/`0.0`; `key_present is True`. |
| `test_missing_key_returns_200_all_zeros` | Empty `hgetall` → 200, all defaults, `key_present is False` (§2.3). |
| `test_unknown_field_ignored` | Hash containing `weird_new_field_5m` → dropped, no error (§4.2 forward-compat). |
| `test_malformed_user_id_422` | Whitespace / 200-char / empty `user_id` → 422 (unhappy path). |
| `test_redis_connection_error_503` | Stub raises `ConnectionError` → 503 + `Retry-After` header (unhappy path). |
| `test_redis_timeout_503` | Stub raises `TimeoutError` → 503 (unhappy path). |
| `test_healthz_no_redis_io` | `/healthz` 200 even when stub raises on every call (liveness ≠ readiness). |
| `test_readyz_reflects_ping` | PING ok → 200; PING raises → 503. |
| `test_value_coercion_garbage_field_value` | Hash field `clicks_5m = "abc"` → 500-class behavior is **not** silent-zero: Pydantic `ValidationError` surfaces (a corrupt store is a bug, not a default). |
| **Drift guards** (in `tests/test_sliding_models.py`) | `RESOLUTION_FEATURES` matches each aggregator's populated `get_result` fields; `FeatureVector.model_fields.keys() == expected_redis_fields()`; `REDIS_FIELD_PREFIXES is _REDIS_FIELD_PREFIXES`. |
| `test_config_validation` | Negative port / zero pool size / empty prefix rejected by `ServingConfig`. |

## 6. Integration Tests

Marked `@pytest.mark.integration` (require `make infra-up`):

1. **Seed-and-serve round trip** — write a `feat:user:it-user-1` hash through
   the *real* `RedisHashSink` (constructing a `SlidingFeatureRecord` per
   resolution), start the app against real Redis, `GET` the user, assert the
   exact typed vector. This exercises writer→store→reader as one contract —
   the seam the drift guards protect statically, verified dynamically.
2. **TTL expiry semantics** — seed with a 1-second TTL override, wait, assert
   the read flips to `key_present: false` + zeros (feature decay, §2.3).
3. **End-to-end smoke (optional, slow)** — feeder → validator → sliding
   consumer → API: poll `GET /v1/features/users/{hot_user}` until non-zero
   `clicks_5m`, bounding staleness ≤ slide + watermark delay. (This is the
   seed of the Week 5 end-to-end correctness suite; kept minimal here.)
4. **Readiness against real Redis** — `/readyz` 200 with compose Redis up.

## 7. How to Run

### 7.1 Bootstrap

```sh
make infra-up          # Kafka ×3 + Schema Registry + PostgreSQL + Redis
make install           # one-time: Python deps incl. fastapi/uvicorn/redis
                       #   (= uv pip install -e ".[test]"; the test extra now
                       #   carries the serving deps and the sliding-run deps)
```

### 7.2 Populate the online store, then serve

```sh
make register-schemas-feed  # registers e-commerce-events-feed-value in the
                            #   Schema Registry — required before the feeder.
                            #   The producer uses auto.register.schemas=False,
                            #   so a fresh stack (or `infra-up` after a
                            #   `down -v`) needs this once or `feeder-run`
                            #   fails with SR 40401 (subject not found).

make feeder-run &      # background event feeder (Week 1)
make validator-run &   # validation layer (Week 2 PR #1)
make sliding-run &     # sliding-window consumer → Redis (Week 2 PR #2)

make serving-run       # uvicorn, 1 worker, port 8000
# — OR, for the process-group narrative (§2.6):
make serving-run-group W=4
```

### 7.3 Inspect

```sh
# Pick a hot user from the store, then hit the API:
make sliding-redis-show
# `u-000042` is just a placeholder matching the feeder's u-NNNNNN id format;
# substitute a real id printed by `make sliding-redis-show` above.
curl -s localhost:8000/v1/features/users/u-000042 | python -m json.tool

# Cold user — 200, zeros, key_present=false (§2.3):
curl -s localhost:8000/v1/features/users/never-seen-user | python -m json.tool

# Health:
curl -i localhost:8000/healthz
curl -i localhost:8000/readyz

# Interactive OpenAPI docs:
xdg-open http://localhost:8000/docs

# Crude latency sanity (the real harness is the next PR, §9.2):
make serving-smoke     # N sequential curls; prints rough wall-clock per call
```

### 7.4 CLI

| Flag | Default | Maps to |
|---|---|---|
| `--redis-host` | `localhost` | `ServingConfig.redis_host` |
| `--redis-port` | `6379` | `.redis_port` |
| `--host` | `0.0.0.0` | `.api_host` |
| `--port` | `8000` | `.api_port` |
| `--workers` | `1` | `.workers` (uvicorn **processes**) |
| `--log-level` | `info` | uvicorn log level |

### 7.5 Tear down

```sh
make infra-down
```

## 8. Resource Budget & Constraints

| Resource | Budget | Notes |
|---|---|---|
| Memory | ~50–80 MB RSS per worker | FastAPI + pool; `W=4` ≈ 0.3 GB — negligible beside Kafka. |
| CPU | ~0 idle; ~1 core per ~3–5 k req/s per worker | GIL-bound framework work; scale via `--workers` (§2.6). |
| Redis | no new provisioning | Read load ≪ the Week 2 write load at laptop scale. |
| Ports | `8000` | 8081 (Schema Registry), 5432, 6379, 19092–19094 already taken. |
| Latency (informal; harness in next PR) | p99 < 5 ms target | Budget: HTTP parse+route ~0.2 ms, validation ~0.1 ms, `HGETALL` RTT ~0.2–0.5 ms localhost, Pydantic build+serialize ~0.2 ms — comfortable headroom, as the plan predicted (line 93). |

Constraint worth stating: the API adds **zero** load to Kafka and cannot
back-pressure the write path — reader and writer meet only at Redis, which
serves both far below capacity.

## 9. Future Considerations

1. **Feature-vector assembly endpoint (next Week 3 PR).** When a second
   feature group exists (Week 4 batch features; session features), add
   `GET /v1/features/users/{user_id}/vector?groups=…` joining **multiple Redis
   keys** (one key per group — do *not* widen the existing hash) with a
   pipelined multi-`HGETALL`. The v1 endpoint stays as the single-group fast
   path.
2. **Latency benchmark harness (next Week 3 PR).** Closed-loop and open-loop
   load against `GET /v1/features/users/{id}` with Zipf-distributed user IDs
   (reuse the Week 1 generator's skew), p50/p95/p99, worker sweep `W ∈ {1, 4}`,
   results doc `docs/results/week3_serving_latency.md`. Deliverable of plan
   line 93–94.
3. **Freshness metadata.** The hash carries no timestamps; the only freshness
   signal is key TTL. Options when Week 5 monitoring lands: pipeline a `PTTL`
   alongside the `HGETALL` (cheap, key-granular) or have the writer stamp an
   `updated_ms_{res}` field (field-granular, +3 fields). Deferred with the
   freshness-SLA work it belongs to.
4. **Caching / pooling interview talking points (not implemented, per plan
   line 93).** Hot-key request coalescing (single-flight), per-process LRU
   with ~100 ms TTL for skewed traffic, Redis client-side caching
   (`CLIENT TRACKING`), and why none of them pay rent below ~10 k req/s on a
   localhost store.
5. **gRPC (Phase 3).** The serving-layer concepts that transfer: the typed
   vector contract (→ protobuf message), readiness semantics (→ gRPC health
   protocol), process-based workers (→ server thread/loop tuning where the
   GIL no longer rules).
6. **AuthN/Z, rate limiting** — platform concerns deferred to the Phase 4
   ingress (where they are config, not code).

## 10. Open Questions

1. **`200`-with-zeros vs `404` for an absent key (§2.3).** Decided `200` +
   `key_present: false` for the ML-consumer reasons given; revisit if a
   non-inference consumer (ops tooling, debugging UI) turns out to want 404
   semantics — likely answer: a separate `?strict=true` knob or an ops
   endpoint, not a default change.
2. **Should `key_present` be trusted as a "cold user" signal?** A user whose
   activity is *only* outside all windows but inside TTL refresh cadence can
   hold a key with stale-zero fields; conversely TTL semantics depend on the
   acknowledged week2_02 §10.2 EXPIRE-nuance open question. If that question
   is resolved toward per-resolution `XX` TTLs, re-examine what `key_present`
   means per resolution.
3. **Worker default for the benchmark.** `W=1` is the honest baseline;
   whether the headline number in the results doc should be `W=1` or
   `W=nproc` — decide in the benchmark PR (report both).
4. **`uvloop`.** `uvicorn[standard]` installs it and uses it automatically;
   worth one sentence in the benchmark doc (it is a free ~10–20 % on the
   framework overhead), but no design impact.

## 11. Top 5 Concepts Worth Understanding (Interview Prep)

> **Ordering note.** The items follow the document's architectural flow,
> inside-out (storage layout → read contract → protocol → process model →
> system-wide semantics), each building context for the next — *not*
> interview criticality. Ranked by criticality for a Senior/Staff ML-infra
> loop, study them as: **11.5** (freshness-budget composition — the most
> senior signal) > **11.1** (data layout drives latency — the most commonly
> probed) > **11.4** (GIL/processes — this portfolio's running narrative) >
> **11.2** (sparsity contract — depth differentiator) > **11.3** (REST vs
> gRPC — frequently asked, shallowest).

### 11.1 The online-store read path: hash-per-entity, one round trip

The whole sub-5 ms story is **data layout**, not cleverness: all of a user's
features live in one Redis hash, so a feature vector is a single O(13)
`HGETALL` — one network RTT, one store operation, no joins, no fan-out. Every
serving-latency war story (N+1 key reads, cross-shard fan-out, "we added a
cache because reads were slow") is downstream of getting this layout wrong,
because the read access pattern is fixed — *given one `user_id`, return all 13
features fast* — while the write is negotiable, so you bend the write contract
and the data layout to serve the read, never the reverse. The hash was
*designed* for this read in Week 2 §2.8 before the reader existed — the
read-path-backwards reasoning behind denormalization, materialized views, and
NoSQL modeling, where you pay once at write time (and in storage) so every
read is a single cheap lookup; it is the right trade precisely because a
feature store is read-heavy (written periodically by the window consumer, read
on every prediction request), so the rule generalizes to: let the *dominant*
access pattern drive the layout.

**TL;DR:** one hash per user makes a feature read a single fixed-cost
round trip — design the write layout backwards from the read.

### 11.2 Sparsity and default-zero: why the writer never writes zeros

The Week 2 consumer emits nothing for a quiet user (no events in a slide ⇒ no
record ⇒ no `HSET`), and TTLs erase idle users entirely. That keeps the store
O(active users), not O(all users) — but it moves work to the reader, which
must *synthesize* zeros and treat key expiry as **feature decay, not data
loss**. The contract spans writer and reader as one unit; this PR ships the
reader half and pins both halves together with drift-guard tests. Interview
framing: "missing data" in an online store is usually *semantically zero*, and
deciding that explicitly (vs 404-ing) is a modeling decision, not an HTTP one.

### 11.3 REST vs gRPC: protocol economics of a point lookup

gRPC's real advantages — binary framing, HTTP/2 multiplexing, and above all
**in-call streaming** — pay off when calls are chatty, payloads are large, or
delivery is incremental (LLM tokens). A 13-field point lookup is none of
those: the budget is Redis RTT + Python framework time, and JSON-vs-protobuf
is microseconds of it. So the decision is *sequencing*, not religion: REST now
(curl-able, Pydantic-native, OpenAPI for free), gRPC in Phase 3 where
token streaming makes it structurally necessary. Being able to say *when each
wins and why this workload doesn't care* is the senior answer.

### 11.4 Two orthogonal concurrency axes: event loop × worker processes

Async I/O gives **concurrency** — one process overlaps hundreds of in-flight
Redis waits on one event loop, but the GIL still serializes the CPU work
(parse, validate, serialize). Worker processes give **parallelism** — more
cores of that CPU work, shared-nothing behind one socket. Threads provide
neither cleanly in CPython, which is why they appear nowhere in this design —
the same conclusion the Week 1 producer investigation and Week 2 consumer
group reached, now at the serving edge. One system, one rule, three layers:
**scale Python by processes**.

### 11.5 What the API can and cannot promise: composing the freshness budget

The API's own p99 (<5 ms) is the *smallest* term in end-to-end staleness. A
feature value a client reads is at best as fresh as: producer latency
(`acks=all` under EOS — Week 1 §4.4.1) + validation hop + **window refresh
interval** (the window's *slide* — a 5 m/1 m window updates at most once a
minute) + watermark delay
(out-of-orderness budget, Week 2 §2.4) + Redis write. There is no
read-your-writes guarantee — an event a client just produced is invisible
until its window fires. The store is therefore **eventually consistent** with
the event stream; more precisely it offers **bounded staleness** — because the
freshness gap above is a *sum of known stage latencies* (window refresh
interval + watermark delay + write), the lag has a knowable upper bound rather than the
unbounded "eventually" of plain eventual consistency, assuming a healthy
pipeline that is not backed up. Articulating that the serving SLA (read-path latency —
p99 < 5 ms) and the freshness SLA (end-to-end staleness — producer +
validation + window refresh interval + watermark + Redis write, on the order of seconds
to minutes) are different numbers composed from different pipeline stages is
the core online/offline-consistency literacy: a read can be answered in
milliseconds yet return a value a minute old, so "fast to answer" never
implies "fresh or consistent." Week 4 then makes this quantitative —
measuring the online value against an offline recomputation to put real
numbers on the divergence (train/serve skew).

**TL;DR:** "fast to answer" (5 ms serving SLA) is not "fresh" (seconds-to-minutes
freshness SLA) — the API's read is the smallest contributor to total staleness;
the upstream stages (mainly the window refresh interval and watermark delay) dominate.

## Appendix A: Why the freshness gap is "eventual consistency", not CAP's "C"

The §11.5 staleness is sometimes mislabeled as a CAP-theorem *Consistency*
tradeoff. It is not. The precise term is **eventual consistency** (more
sharply, **bounded staleness**), and the distinction matters:

* **CAP's "C" is only defined *during a network partition.*** CAP states that
  *when the network splits so nodes cannot communicate*, a system must
  sacrifice Consistency or Availability. (Note the naming collision: the "P"
  in CAP is a **network split / communication failure** — unrelated to a
  *Kafka* partition, which is a deliberate topic shard.) The §11.5 staleness
  exists under **perfectly healthy networking**, all the time, by design — no
  partition is involved, so the CAP choice is never invoked.

* **CAP's "C" (linearizability) is about *replicas of one value disagreeing*;
  our gap is *one value still in transit through pipeline stages.*** CAP-C
  describes two copies of the same register disagreeing *because they cannot
  sync*. Our lag is a single event still flowing through
  produce → validate → window-fire → Redis write — propagation latency, not
  replica disagreement. Same symptom ("the read is not current"), different
  cause.

* **The lag is bounded and deliberate.** Because the freshness gap is a *sum
  of known stage latencies* (window refresh interval + watermark delay + write), it has
  a knowable upper bound — **bounded staleness**, stronger than the unbounded
  "eventually" of plain eventual consistency (assuming a healthy pipeline that
  is not backed up). Watermarks and the window refresh interval *intentionally* hold data to
  emit correct windows; we trade a little freshness for correctness, not
  because a failure forced us to.

* **The framework that fits is PACELC, not CAP.** PACELC extends CAP with the
  missing normal-operation case: *if Partitioned, choose Availability or
  Consistency; **Else** (no partition), choose **L**atency or **C**onsistency.*
  §11.5 lives in that "ELC" branch — in normal operation we trade Consistency
  (freshness) for Latency (the p99 < 5 ms read path). CAP only covers the
  partition branch.
