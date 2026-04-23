# Changelog — 2026-04-20 — Confluent Schema Registry

**Scope:** Wire Confluent Schema Registry into the Week 1 Docker Compose stack
and its supporting Python scaffolding (config model, tests, Makefile targets).

**Design reference:** [week1_01_docker_compose_infra.md §2.8 and §4.x](../design/week1_01_docker_compose_infra.md#28-confluent-schema-registry)

**Motivation:** The original Week 1 infrastructure PR shipped a 3-broker Kafka
cluster + PostgreSQL, but did not include a Schema Registry. The revised Week 1
plan requires Avro/Protobuf schemas registered with a registry and `BACKWARD`
compatibility experiments in follow-up PRs. Adding Schema Registry now — before
the producer/consumer/sink work — avoids a later Compose-file refactor and lets
every subsequent Week 1 PR assume the registry is present.

---

## Files added

| Path | Purpose |
|------|---------|
| `docs/changelog/2026-04-20_schema_registry.md` | This changelog. |

## Files modified

### 1. `docker/docker-compose.yml`

- Added a `schema-registry` service to the `streaming-feature-store` stack.
  - Image: `confluentinc/cp-schema-registry:7.8.0` (Confluent Platform 7.8.x,
    paired with Apache Kafka 3.9.x).
  - Hostname `schema-registry` on the existing `feature-store-net` bridge.
  - Host port mapping `8081:8081` for tooling access (curl, `make schema-*`).
  - Kafka-backed store via
    `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9092,PLAINTEXT://kafka-3:9092`.
  - `_schemas` topic replication factor set to `3` to match the cluster default.
  - Global compatibility level set to `backward` (explicit, though also the
    Kafka default).
  - JVM heap constrained to `-Xmx256m -Xms256m` via `SCHEMA_REGISTRY_HEAP_OPTS`.
  - `deploy.resources.limits`: 512 MB memory, 0.5 CPUs.
  - `depends_on` blocks on all 3 Kafka brokers being `service_healthy` before
    startup.
  - Healthcheck: `curl -fsS http://localhost:8081/subjects` (10s interval,
    10 retries, 30s start period).
- Updated the top-of-file comment to reflect the registry addition.

### 2. `src/streaming_feature_store/config.py`

- Added a new `SchemaRegistryConfig` Pydantic settings model.
  - Fields: `url`, `default_compatibility`, `request_timeout_s`.
  - Env prefix: `SCHEMA_REGISTRY_` (so `SCHEMA_REGISTRY_URL`,
    `SCHEMA_REGISTRY_DEFAULT_COMPATIBILITY`, `SCHEMA_REGISTRY_REQUEST_TIMEOUT_S`).
  - `default_compatibility` validated via regex against the full set of
    Registry-supported levels (`BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`,
    `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE`, `NONE`).
  - `request_timeout_s` constrained to `gt=0`.
  - Derived properties `subjects_url` and `config_url` for the two most-used
    REST endpoints; both strip trailing slashes on the base URL.
  - Module docstring updated to reflect the new model.

### 3. `tests/unit/test_config.py`

- Imported `SchemaRegistryConfig`.
- Added the following test classes (all pure-Python, no Docker required):
  - `TestSchemaRegistryConfigDefaults` — default URL (`http://localhost:8081`),
    default compatibility (`BACKWARD`), default timeout (`5.0`).
  - `TestSchemaRegistryConfigUrls` — `subjects_url`/`config_url` derivation,
    including correct handling of trailing slashes.
  - `TestSchemaRegistryConfigValidation` — rejects unknown compatibility
    (e.g. `"SIDEWAYS"`), rejects empty string, rejects zero/negative timeout,
    and parametrized acceptance of all seven valid compatibility levels.
  - `TestSchemaRegistryConfigCustomValues` — custom URL, custom timeout.
  - `TestSchemaRegistryConfigEnvOverride` — env-var overrides for URL,
    compatibility, and timeout.

### 4. `tests/integration/test_docker_infrastructure.py`

- Added `json`, `urllib.error`, `urllib.request` imports (stdlib only — no new
  third-party test dependency).
- Added a private helper `_http_get_json(url, timeout_s)` that performs a GET
  and returns `(status_code, parsed_json_body)`.
- Added a new `TestSchemaRegistry` class with three integration tests:
  - `test_schema_registry_reachable` — `GET /subjects` returns HTTP 200 with a
    JSON array body.
  - `test_schema_registry_default_compatibility` — `GET /config` reports
    `{"compatibilityLevel": "BACKWARD"}`.
  - `test_schema_registry_backed_by_kafka` — the `_schemas` topic exists on
    the Kafka cluster and each partition has 3 replicas.

### 5. `tests/conftest.py`

- Added `_SCHEMA_REGISTRY_PORT = 8081` constant.
- Added `8081` to the list of ports the `docker_services_up` fixture waits on
  before integration tests run.
- Added two new session-scoped fixtures:
  - `schema_registry_url` — reads from `SchemaRegistryConfig().url`.
  - `schema_registry_timeout_s` — reads from
    `SchemaRegistryConfig().request_timeout_s`.

### 6. `Makefile`

- Registered `schema-subjects` and `schema-compat` under `.PHONY`.
- Added two new targets under a new **Schema Registry helpers** section:
  - `make schema-subjects` → `curl -fsS http://localhost:8081/subjects | jq .`
  - `make schema-compat`   → `curl -fsS http://localhost:8081/config | jq .`

---

## Verification

- Unit tests: `pytest tests/unit/ -v` → **80 passed** (was 66 before; +14 new
  tests covering `SchemaRegistryConfig`).
- Integration tests (require `make infra-up`): three new tests under
  `TestSchemaRegistry` exercise registry reachability, default compatibility,
  and the Kafka-backed `_schemas` topic.
- Compose file still validates as Compose v2 YAML; the new service reuses
  the existing network, follows the same healthcheck / `deploy.resources`
  conventions, and does not affect the existing Kafka or Postgres services.

## Out of scope (deferred to later Week 1 PRs)

- Avro/Protobuf `.avsc`/`.proto` schema definitions.
- Schema registration code and producer/consumer integration with the
  registry's serializer/deserializer clients.
- `BACKWARD` compatibility evolution experiments
  (add-optional-field / remove-deprecated-field / `int`→`long` promotion).
- Automatic creation of the `e-commerce-events` topic — still deferred to the
  producer PR per the original design decision in §9 of the design doc.

## Resource impact

Per §8.1 of the design doc, the registry adds ~512 MB RAM limit and 0.5 CPU
limit, bringing total infrastructure footprint from ~2.8 GB to ~3.3 GB —
comfortably within budget on a 16 GB+ laptop.
