# Design Doc: Multi-Broker Kafka Cluster, PostgreSQL & Schema Registry in Docker Compose

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** First bulletpoint — Docker Compose infrastructure setup (Kafka + PostgreSQL + Confluent Schema Registry)
**Author:** Auto-generated design document
**Date:** 2026-03-24
**Last Updated:** 2026-04-19 — Added Confluent Schema Registry service per revised Week 1 plan

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

This PR sets up the foundational infrastructure for the entire Phase 1 project: a
multi-broker Apache Kafka cluster, a PostgreSQL database, and a Confluent Schema
Registry, orchestrated via Docker Compose. This infrastructure supports all subsequent
Week 1–5 work, including event ingestion, Avro/Protobuf schema management, stream
processing, feature serving, and offline feature computation.

### Deliverables

- `docker-compose.yml` — 3-broker Kafka cluster (KRaft mode) + PostgreSQL + Confluent Schema Registry
- PostgreSQL initialization script with `raw_events` table schema
- Pydantic configuration models for Kafka, PostgreSQL, and Schema Registry connection settings
- Health-check mechanisms for all services
- Unit tests for configuration models and schema validation
- Integration tests verifying cluster formation, basic connectivity, and Schema Registry reachability
- A `Makefile` (or equivalent) for common operations (start, stop, status, logs, schema subjects listing)

---

## 2. Critical Design Decisions

### 2.1 KRaft Mode (No ZooKeeper)

**Decision:** Use Kafka in KRaft (Kafka Raft) mode, not ZooKeeper mode.

**Rationale:**
- ZooKeeper is removed entirely in Kafka 4.0. KRaft is the only supported consensus
  mechanism going forward.
- KRaft reduces operational complexity (one fewer service to manage) and lowers
  resource usage on a constrained laptop.
- KRaft has faster controller failover and topic creation compared to ZooKeeper-based
  mode.

**Trade-off:** KRaft combined mode (broker + controller in the same JVM) uses fewer
containers but means a broker failure also loses a controller. This is acceptable for a
local development environment — we are not running a production cluster.

### 2.2 Three Brokers in Combined Mode

**Decision:** Run 3 Kafka brokers, each operating in combined mode (acting as both a
broker and a controller).

**Rationale:**
- 3 is the minimum for meaningful replication testing (`replication-factor=3` is the
  production standard).
- Combined mode (vs. separate controller nodes) saves 3 additional containers, which
  matters on a single laptop.
- 3 controllers provide Raft quorum (can tolerate 1 controller failure).
- With 3 brokers, we can test ISR (in-sync replica) behavior, leader election, and
  partition reassignment — all skills needed for the Week 1 experiments.

**Trade-off:** Combined mode means a noisy-neighbor risk where a broker under heavy I/O
load could slow the controller. On a local dev environment with moderate throughput, this
is not a concern.

### 2.3 Kafka Image: `apache/kafka`

**Decision:** Use the official `apache/kafka` Docker image.

**Rationale:**
- The `apache/kafka` image is the officially maintained image from the Apache Kafka
  project, available since Kafka 3.7.0.
- It supports KRaft out of the box with environment variable configuration.
- Avoids dependency on third-party images (e.g., Confluent, Bitnami) that bundle
  extras we do not need and may have different configuration semantics.

### 2.4 Listener Configuration (Dual Listeners)

**Decision:** Each broker exposes two listeners — one for inter-broker communication and
one for host access.

**Rationale:**
- Docker containers communicate via the Docker network using internal hostnames
  (e.g., `kafka-1:9092`). The host machine cannot resolve these names.
- A second listener on `localhost:<unique-port>` allows the Python producer/consumer
  (running on the host) to reach each broker individually.
- This dual-listener pattern is the standard approach for Kafka-in-Docker setups.

**Listener map:**

| Broker   | Internal Listener (Docker network) | External Listener (host)  |
|----------|------------------------------------|---------------------------|
| kafka-1  | `INTERNAL://kafka-1:9092`          | `EXTERNAL://0.0.0.0:19092` → `localhost:19092` |
| kafka-2  | `INTERNAL://kafka-2:9092`          | `EXTERNAL://0.0.0.0:19093` → `localhost:19093` |
| kafka-3  | `INTERNAL://kafka-3:9092`          | `EXTERNAL://0.0.0.0:19094` → `localhost:19094` |

The controller listener is separate: `CONTROLLER://kafka-<id>:9093`.

### 2.5 PostgreSQL Version & Configuration

**Decision:** Use PostgreSQL 17 (latest stable release).

**Rationale:**
- PostgreSQL 17 has mature JSONB support, which we may leverage for semi-structured
  event data if needed.
- Well-documented, widely used, and the default choice in the project plan.

**Tuning for a single laptop:**
- `shared_buffers`: 256 MB (default 128 MB is too low for batch inserts)
- `work_mem`: 16 MB (for sort/join operations during offline feature computation in
  Week 4)
- `max_connections`: 50 (adequate for dev; avoids over-allocating shared memory)
- `wal_level`: `logical` (enables CDC / logical replication if needed later)

### 2.6 `raw_events` Table Schema

**Decision:** Use a typed, flat schema with a JSONB `properties` column for
event-specific attributes.

**Rationale:**
- Core fields (`event_id`, `event_type`, `user_id`, `timestamp`) are typed columns for
  fast indexing and queries.
- Event-specific fields (e.g., `product_id` for purchases, `page_url` for page views)
  go into a JSONB `properties` column to avoid wide, sparse tables and to accommodate
  future event types without schema migrations.
- This is the standard pattern for event-driven systems (Segment, Snowplow, etc.).

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS raw_events (
    event_id        UUID            PRIMARY KEY,
    event_type      VARCHAR(50)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    session_id      VARCHAR(64)     NOT NULL,
    event_timestamp TIMESTAMPTZ     NOT NULL,
    properties      JSONB           NOT NULL DEFAULT '{}',
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index for time-range scans (offline feature computation in Week 4)
CREATE INDEX idx_raw_events_timestamp ON raw_events (event_timestamp);

-- Index for per-user queries (point-in-time joins in Week 4)
CREATE INDEX idx_raw_events_user_id ON raw_events (user_id);

-- Index for filtering by event type
CREATE INDEX idx_raw_events_event_type ON raw_events (event_type);

-- Composite index for the most common query pattern: user + time range
CREATE INDEX idx_raw_events_user_timestamp
    ON raw_events (user_id, event_timestamp);
```

**Why `event_id` is UUID and primary key:**
- Kafka messages may be replayed (consumer restart, rebalance). A UUID primary key
  with `ON CONFLICT DO NOTHING` provides idempotent inserts, which is part of the
  exactly-once semantics strategy described in the Week 1 plan.

### 2.7 Docker Network & Volume Strategy

**Decision:** Use a single dedicated bridge network and named volumes.

- **Network:** A user-defined bridge network `feature-store-net` so containers can
  resolve each other by service name (Docker's embedded DNS).
- **Volumes:** Named volumes for Kafka log dirs and PostgreSQL data dir. Named volumes
  persist across `docker compose down` (without `-v`) so cluster state is retained
  between dev sessions. Bind mounts are avoided to keep the setup cross-platform
  (Windows path issues).

### 2.8 Confluent Schema Registry

**Decision:** Include Confluent Schema Registry (`confluentinc/cp-schema-registry`) as a
5th service in the Docker Compose stack.

**Rationale:**
- The revised Week 1 plan requires Avro (or Protobuf) schemas registered with a Schema
  Registry, plus `BACKWARD` compatibility experiments in follow-up Week 1 PRs. The
  infrastructure needs to be ready from the start.
- Confluent's `cp-schema-registry` image is the reference implementation, licensed under
  the Confluent Community License (free to use). It is compatible with Apache Kafka
  brokers — it only uses Kafka itself as its durable storage backend (the
  `_schemas` topic).
- Shipping Schema Registry alongside Kafka from the very first PR avoids a later
  Compose-file refactor and ensures every subsequent Week 1 PR (producer, consumer,
  sink) can rely on it being present.

**Configuration:**
- Image: `confluentinc/cp-schema-registry:7.8.0` (paired with Apache Kafka 3.9.x)
- Port: `8081` (exposed on host for tooling access)
- Storage backend: the Kafka cluster itself via `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka-1:9092,kafka-2:9092,kafka-3:9092`
- `_schemas` topic replication factor: `3` (production-grade, matches the cluster's
  default replication factor)
- Default compatibility level: left at Kafka's default (`BACKWARD`) — the Week 1 schema
  evolution PR will explicitly re-assert this and run compatibility experiments against
  it
- Depends on all 3 Kafka brokers being healthy (`depends_on` with `condition: service_healthy`)

**Trade-off:** Schema Registry adds ~512 MB RAM and one more JVM to the stack. On a
16 GB+ laptop this is comfortably within budget (see §8.1). The alternative — compiled
Protobuf objects without a registry — was considered but rejected because the revised
Week 1 plan explicitly calls for schema evolution experiments using a registry's
compatibility modes.

> **Industry Note — Schema Registry Best Practice:**
> At scale, the standard approach combines **schema-as-code** with the **Confluent Schema
> Registry**. Schemas live in a Git repo (source of truth, PR-reviewed), CI checks
> compatibility against the Registry, and CD publishes approved schemas. At runtime, the
> Registry enforces contracts on every produce/consume call. Neither approach alone
> suffices: the Registry without Git loses auditability; Git without the Registry has no
> centralized cross-service compatibility enforcement.
>
> **Alternative — Compiled Schema Objects (Protobuf Model):**
> Schemas defined in Git can be compiled into language-native typed objects (e.g., via
> `protoc`), giving each service local runtime enforcement without a remote registry.
> This is widely used and well-suited for: single-team / single-codebase projects (the
> staleness problem disappears when everyone builds together), Protobuf-heavy ecosystems
> such as gRPC (Protobuf's wire format is inherently forward/backward compatible by
> design), and latency-critical or air-gapped environments.
>
> **Trade-offs between the two approaches:**
>
> |                                                   | Compiled schema objects | Remote registry                          |
> |---------------------------------------------------|------------------------|------------------------------------------|
> | Enforces "message matches **my** schema"          | Yes                    | Yes                                      |
> | Enforces cross-service schema compatibility       | No — each service only knows its own build | Yes — central compatibility check |
> | Catches stale producers                           | No — runs fine until consumer breaks       | Yes — rejects incompatible schema at registration |

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Docker Compose                            │
│                                                                  │
│   ┌───────────┐   ┌───────────┐   ┌───────────┐                │
│   │  kafka-1   │   │  kafka-2   │   │  kafka-3   │                │
│   │  (broker   │   │  (broker   │   │  (broker   │                │
│   │   + ctrl)  │◄─►│   + ctrl)  │◄─►│   + ctrl)  │                │
│   │ :9092/:9093│   │ :9092/:9093│   │ :9092/:9093│                │
│   │ ext:19092  │   │ ext:19093  │   │ ext:19094  │                │
│   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘                │
│         │ KRaft Raft      │                │                      │
│         └────────────────┼────────────────┘                      │
│                          │                                        │
│   ┌──────────────────────┴───────────────────────┐               │
│   │               feature-store-net               │               │
│   └──┬───────────────────┬───────────────────┬───┘               │
│      │                   │                   │                    │
│ ┌────┴────────┐   ┌──────┴────────┐   ┌──────┴──────────┐        │
│ │ PostgreSQL   │   │ schema-registry│   │  (future:       │        │
│ │ :5432        │   │ :8081          │   │   redis, etc.)  │        │
│ │ ext:5432     │   │ ext:8081       │   │                 │        │
│ └─────────────┘   └────────────────┘   └─────────────────┘        │
│                          │                                        │
│                          └── stores schemas in `_schemas`         │
│                              topic on the Kafka cluster           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

Host machine (Python producer/consumer) connects via:
  - Kafka:           localhost:19092, localhost:19093, localhost:19094
  - PostgreSQL:      localhost:5432
  - Schema Registry: localhost:8081  (HTTP REST API)
```

---

## 4. Detailed Implementation

### 4.1 Project Directory Structure

```
streaming-feature-store/
├── docker/
│   ├── docker-compose.yml
│   └── postgres/
│       └── init.sql                  # DDL for raw_events table + indexes
├── src/
│   └── streaming_feature_store/
│       ├── __init__.py
│       └── config.py                 # Pydantic settings models
├── tests/
│   ├── conftest.py                   # Shared fixtures
│   ├── unit/
│   │   ├── __init__.py
│   │   └── test_config.py            # Unit tests for config models
│   └── integration/
│       ├── __init__.py
│       └── test_docker_infrastructure.py  # Integration tests for cluster
├── Makefile                          # Convenience commands
├── pyproject.toml                    # Project metadata + dependencies
└── ...
```

### 4.2 `docker-compose.yml`

The Compose file defines 5 services: `kafka-1`, `kafka-2`, `kafka-3`, `postgres`, and
`schema-registry`.

**Key configuration per Kafka broker:**

| Environment Variable | Value | Purpose |
|----------------------|-------|---------|
| `KAFKA_NODE_ID` | 1, 2, 3 | Unique broker/controller identity |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Combined mode |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093` | Raft voter list |
| `KAFKA_LISTENERS` | `INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:19092` | Three listener types |
| `KAFKA_ADVERTISED_LISTENERS` | `INTERNAL://kafka-<id>:9092,EXTERNAL://localhost:1909<x>` | Client-resolvable addresses |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT` | No TLS for local dev |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `INTERNAL` | Brokers talk over INTERNAL |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` | Controller traffic isolation |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `3` | Internal topic replication |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | `3` | Default for new topics |
| `KAFKA_MIN_INSYNC_REPLICAS` | `2` | Ack safety: 2-of-3 must confirm |
| `KAFKA_NUM_PARTITIONS` | `12` | Default partitions for new topics |
| `KAFKA_LOG_RETENTION_HOURS` | `168` | 7-day retention |
| `KAFKA_HEAP_OPTS` | `-Xmx512m -Xms512m` | Constrain JVM heap for laptop |
| `CLUSTER_ID` | (pre-generated) | Fixed cluster UUID for reproducibility |

**Resource limits per Kafka broker (Docker Compose `deploy.resources`):**
- Memory: 768 MB limit (512 MB JVM heap + ~256 MB off-heap/OS overhead)
- CPUs: 2.0

**PostgreSQL service configuration:**

| Setting | Value | Purpose |
|---------|-------|---------|
| Image | `postgres:17` | Latest stable |
| `POSTGRES_DB` | `feature_store` | Database name |
| `POSTGRES_USER` | `featurestore` | Service account |
| `POSTGRES_PASSWORD` | `featurestore_dev` | Dev-only password |
| Port mapping | `5432:5432` | Host access |
| Init script | `./postgres/init.sql` mounted to `/docker-entrypoint-initdb.d/` | Auto-creates schema on first start |
| `shared_buffers` | `256MB` | Via command-line override |
| `work_mem` | `16MB` | Via command-line override |
| `max_connections` | `50` | Via command-line override |

**Resource limits for PostgreSQL:**
- Memory: 512 MB limit
- CPUs: 1.0

**Schema Registry service configuration:**

| Setting | Value | Purpose |
|---------|-------|---------|
| Image | `confluentinc/cp-schema-registry:7.8.0` | Confluent Community edition; paired with Kafka 3.9.x |
| `SCHEMA_REGISTRY_HOST_NAME` | `schema-registry` | Advertised hostname on the Docker network |
| `SCHEMA_REGISTRY_LISTENERS` | `http://0.0.0.0:8081` | HTTP REST listener |
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | `PLAINTEXT://kafka-1:9092,kafka-2:9092,kafka-3:9092` | All 3 brokers via the internal listener |
| `SCHEMA_REGISTRY_KAFKASTORE_TOPIC` | `_schemas` | Default storage topic name |
| `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR` | `3` | Match cluster replication factor |
| `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` | `backward` | Default compatibility (explicit for clarity; Kafka's default is also `backward`) |
| Port mapping | `8081:8081` | Host access for tooling / curl |
| `depends_on` | kafka-1, kafka-2, kafka-3 (all `service_healthy`) | Registry requires a reachable Kafka quorum on startup |

**Resource limits for Schema Registry:**
- Memory: 512 MB limit (~256 MB JVM heap + overhead)
- CPUs: 0.5

**Health checks:**

| Service | Health Check Command | Interval | Retries |
|---------|---------------------|----------|---------|
| kafka-* | `/opt/kafka/bin/kafka-metadata.sh --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log --cluster-id <id>` or use a TCP check on port 9092 | 10s | 10 |
| postgres | `pg_isready -U featurestore -d feature_store` | 5s | 5 |
| schema-registry | `curl -fsS http://localhost:8081/subjects` (returns HTTP 200 once the Kafka-backed store is initialized) | 10s | 10 |

**Note on Kafka health checks:** The simplest reliable health check for KRaft-mode Kafka
is to use `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` or a basic TCP
socket check. We will use a TCP-based approach (`nc -z localhost 9092`) combined with
`kafka-metadata.sh` status check to verify both network readiness and Raft quorum.

### 4.3 `init.sql` — PostgreSQL Initialization

```sql
-- Create the raw_events table for the Kafka-to-PostgreSQL sink
CREATE TABLE IF NOT EXISTS raw_events (
    event_id        UUID            PRIMARY KEY,
    event_type      VARCHAR(50)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    session_id      VARCHAR(64)     NOT NULL,
    event_timestamp TIMESTAMPTZ     NOT NULL,
    properties      JSONB           NOT NULL DEFAULT '{}',
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp
    ON raw_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id
    ON raw_events (user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type
    ON raw_events (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_timestamp
    ON raw_events (user_id, event_timestamp);
```

### 4.4 `config.py` — Pydantic Configuration Models

```python
"""Pydantic configuration models for Kafka and PostgreSQL connections."""

import logging
from pathlib import Path
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class KafkaConfig(BaseSettings):
    """Configuration for connecting to the Kafka cluster.

    Parameters
    ----------
    bootstrap_servers : str
        Comma-separated list of broker addresses.
    security_protocol : str
        Security protocol for broker connections.
    default_topic : str
        Default topic name for event ingestion.
    default_num_partitions : int
        Default number of partitions for new topics.
    default_replication_factor : int
        Default replication factor for new topics.

    Notes
    -----
    Values can be overridden via environment variables
    prefixed with ``KAFKA_`` (e.g. ``KAFKA_BOOTSTRAP_SERVERS``).
    """

    bootstrap_servers: str = Field(
        default="localhost:19092,localhost:19093,localhost:19094",
        description="Comma-separated list of broker addresses",
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol for broker connections",
    )
    default_topic: str = Field(
        default="e-commerce-events",
        description="Default topic name for event ingestion",
    )
    default_num_partitions: int = Field(
        default=12,
        ge=1,
        description="Default number of partitions for new topics",
    )
    default_replication_factor: int = Field(
        default=3,
        ge=1,
        le=3,
        description="Default replication factor for new topics",
    )

    model_config = {"env_prefix": "KAFKA_"}

    @property
    def bootstrap_servers_list(self) -> list[str]:
        """Return bootstrap servers as a list."""
        return [s.strip() for s in self.bootstrap_servers.split(",")]


class PostgresConfig(BaseSettings):
    """Configuration for connecting to PostgreSQL.

    Parameters
    ----------
    host : str
        PostgreSQL server hostname.
    port : int
        PostgreSQL server port.
    database : str
        Database name.
    user : str
        Database user.
    password : SecretStr
        Database password (masked in logs).

    Notes
    -----
    Values can be overridden via environment variables
    prefixed with ``POSTGRES_`` (e.g. ``POSTGRES_HOST``).
    """

    host: str = Field(default="localhost", description="PostgreSQL server hostname")
    port: int = Field(default=5432, ge=1, le=65535, description="PostgreSQL server port")
    database: str = Field(default="feature_store", description="Database name")
    user: str = Field(default="featurestore", description="Database user")
    password: SecretStr = Field(
        default=SecretStr("featurestore_dev"),
        description="Database password",
    )

    model_config = {"env_prefix": "POSTGRES_"}

    @property
    def dsn(self) -> str:
        """Return a PostgreSQL DSN connection string (password masked)."""
        return (
            f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.database}"
        )

    def dsn_with_password(self) -> str:
        """Return a PostgreSQL DSN with the actual password.

        Returns
        -------
        str
            Full DSN connection string including the password.
        """
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class SchemaRegistryConfig(BaseSettings):
    """Configuration for connecting to Confluent Schema Registry.

    Parameters
    ----------
    url : str
        Base HTTP URL for the Schema Registry REST API.
    default_compatibility : str
        Default subject compatibility level used when registering new schemas.
    request_timeout_s : float
        HTTP request timeout in seconds for registry calls.

    Notes
    -----
    Values can be overridden via environment variables
    prefixed with ``SCHEMA_REGISTRY_`` (e.g. ``SCHEMA_REGISTRY_URL``).
    """

    url: str = Field(
        default="http://localhost:8081",
        description="Base HTTP URL for the Schema Registry REST API",
    )
    default_compatibility: str = Field(
        default="BACKWARD",
        pattern=r"^(BACKWARD|BACKWARD_TRANSITIVE|FORWARD|FORWARD_TRANSITIVE|FULL|FULL_TRANSITIVE|NONE)$",
        description="Default subject compatibility level",
    )
    request_timeout_s: float = Field(
        default=5.0,
        gt=0,
        description="HTTP request timeout in seconds",
    )

    model_config = {"env_prefix": "SCHEMA_REGISTRY_"}
```

### 4.5 `Makefile`

```makefile
COMPOSE_FILE := docker/docker-compose.yml

.PHONY: infra-up infra-down infra-status infra-logs infra-clean

infra-up:                ## Start Kafka + PostgreSQL
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for services to become healthy..."
	docker compose -f $(COMPOSE_FILE) ps

infra-down:              ## Stop services (preserve data)
	docker compose -f $(COMPOSE_FILE) down

infra-status:            ## Show service status
	docker compose -f $(COMPOSE_FILE) ps

infra-logs:              ## Tail service logs
	docker compose -f $(COMPOSE_FILE) logs -f

infra-clean:             ## Stop services AND delete all volumes (data loss!)
	docker compose -f $(COMPOSE_FILE) down -v

kafka-topics:            ## List Kafka topics
	docker compose -f $(COMPOSE_FILE) exec kafka-1 \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list

kafka-describe:          ## Describe all topics
	docker compose -f $(COMPOSE_FILE) exec kafka-1 \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe

psql:                    ## Open psql shell
	docker compose -f $(COMPOSE_FILE) exec postgres \
		psql -U featurestore -d feature_store

schema-subjects:         ## List registered schema subjects
	curl -fsS http://localhost:8081/subjects | jq .

schema-compat:           ## Show default compatibility level
	curl -fsS http://localhost:8081/config | jq .
```

---

## 5. Unit Tests

Unit tests run without Docker and validate configuration logic, schema correctness, and
model constraints. These use `pytest` with no external dependencies.

### 5.1 `tests/unit/test_config.py`

| Test Function | What It Tests |
|---------------|---------------|
| `test_kafka_config_defaults` | Default `KafkaConfig` values are correct (bootstrap servers, topic name, partitions, replication factor, protocol) |
| `test_kafka_config_bootstrap_servers_list` | `bootstrap_servers_list` property correctly splits a comma-separated string into a list |
| `test_kafka_config_custom_values` | `KafkaConfig` accepts and stores custom overrides |
| `test_kafka_config_invalid_partitions` | `KafkaConfig` rejects `default_num_partitions < 1` (Pydantic `ge=1` validation) |
| `test_kafka_config_invalid_replication_factor` | `KafkaConfig` rejects `default_replication_factor > 3` or `< 1` |
| `test_kafka_config_env_override` | `KafkaConfig` reads from `KAFKA_BOOTSTRAP_SERVERS` environment variable (use `monkeypatch`) |
| `test_postgres_config_defaults` | Default `PostgresConfig` values are correct |
| `test_postgres_config_dsn_masks_password` | `dsn` property contains `***` instead of the actual password |
| `test_postgres_config_dsn_with_password` | `dsn_with_password()` returns the real password |
| `test_postgres_config_password_is_secret` | `password` field is a `SecretStr` instance, and `str(config.password)` does not reveal the value |
| `test_postgres_config_invalid_port` | `PostgresConfig` rejects port `0` and port `70000` (outside valid range) |
| `test_postgres_config_env_override` | `PostgresConfig` reads from `POSTGRES_HOST`, `POSTGRES_PORT`, etc. environment variables |
| `test_schema_registry_config_defaults` | Default `SchemaRegistryConfig` values are correct (`url=http://localhost:8081`, `default_compatibility=BACKWARD`, `request_timeout_s=5.0`) |
| `test_schema_registry_config_invalid_compatibility` | `SchemaRegistryConfig` rejects an unknown compatibility level (e.g. `"SIDEWAYS"`) via the regex `pattern` |
| `test_schema_registry_config_invalid_timeout` | `SchemaRegistryConfig` rejects non-positive `request_timeout_s` (`0`, negative values) |
| `test_schema_registry_config_env_override` | `SchemaRegistryConfig` reads from `SCHEMA_REGISTRY_URL` and `SCHEMA_REGISTRY_DEFAULT_COMPATIBILITY` environment variables |

### 5.2 `tests/unit/test_init_sql.py`

| Test Function | What It Tests |
|---------------|---------------|
| `test_init_sql_file_exists` | `docker/postgres/init.sql` exists on disk |
| `test_init_sql_creates_raw_events_table` | SQL file contains `CREATE TABLE IF NOT EXISTS raw_events` |
| `test_init_sql_has_required_columns` | SQL file references all expected columns: `event_id`, `event_type`, `user_id`, `session_id`, `event_timestamp`, `properties`, `ingested_at` |
| `test_init_sql_has_required_indexes` | SQL file contains all 4 `CREATE INDEX` statements |
| `test_init_sql_is_valid_syntax` | Parse the SQL file with `sqlparse` (or a simple regex check) to verify there are no obvious syntax errors (unclosed parentheses, etc.) |

---

## 6. Integration Tests

Integration tests require the Docker Compose infrastructure to be running. They verify
that the cluster is healthy, services are reachable, and basic operations succeed.

### 6.1 Prerequisites

- Docker Compose services must be up and healthy (`make infra-up`).
- Tests are marked with `@pytest.mark.integration` so they can be excluded from fast
  CI runs.
- A `conftest.py` fixture provides a `docker_services_up` session-scoped fixture that
  checks service health before any integration test runs (and optionally starts
  services if not running).

### 6.2 `tests/integration/test_docker_infrastructure.py`

| Test Function | What It Tests | Expected Outcome |
|---------------|---------------|-----------------|
| `test_kafka_cluster_has_three_brokers` | Connect to any bootstrap server, fetch cluster metadata, verify 3 broker nodes are present | Metadata contains broker IDs 1, 2, 3 |
| `test_kafka_all_brokers_reachable` | Attempt to connect to each external listener (`localhost:19092`, `:19093`, `:19094`) independently | All 3 connections succeed |
| `test_kafka_create_topic` | Create a test topic with 3 partitions and replication factor 3 | Topic is created and described successfully; all partitions have 3 replicas |
| `test_kafka_produce_consume_roundtrip` | Produce a small batch of messages to a test topic, then consume them | All produced messages are received with correct content |
| `test_kafka_partition_assignment` | Produce messages keyed by a known set of keys, verify that messages with the same key land in the same partition | Partition is deterministic per key |
| `test_kafka_leader_election_on_broker_stop` | Stop one Kafka broker container, verify the cluster still operates (produce + consume succeeds on a topic with `min.insync.replicas=2`) | Produce/consume succeeds; leadership moves to a surviving broker |
| `test_kafka_broker_rejoin` | Restart the stopped broker, verify it rejoins the cluster and ISR is restored | Broker count returns to 3; ISR includes the rejoined broker |
| `test_postgres_connection` | Connect to PostgreSQL using `PostgresConfig` and execute a simple query (`SELECT 1`) | Query returns successfully |
| `test_postgres_raw_events_table_exists` | Query `information_schema.tables` for the `raw_events` table | Table exists in `public` schema |
| `test_postgres_raw_events_columns` | Query `information_schema.columns` and verify all expected columns and types exist | Columns match the schema definition |
| `test_postgres_raw_events_indexes` | Query `pg_indexes` for the `raw_events` table | All 4 indexes + primary key index exist |
| `test_postgres_insert_and_query` | Insert a sample event row into `raw_events`, then query it back | Inserted row matches on all fields |
| `test_postgres_idempotent_insert` | Insert the same event (same `event_id`) twice using `ON CONFLICT DO NOTHING` | No error; only 1 row exists |
| `test_schema_registry_reachable` | `GET http://localhost:8081/subjects` returns HTTP 200 and a JSON array | Status 200; response is a (possibly empty) list |
| `test_schema_registry_default_compatibility` | `GET http://localhost:8081/config` reports the global compatibility level | Returns `{"compatibilityLevel": "BACKWARD"}` |
| `test_schema_registry_backed_by_kafka` | Verify the `_schemas` topic exists on the Kafka cluster (registered by Schema Registry on first start) | Topic `_schemas` is present in cluster metadata with replication factor 3 |

### 6.3 Resource-Conscious Test Design

Since this runs on a single laptop (16 cores, limited RAM/disk):

- **Small message volumes:** Integration tests produce at most 100 messages per test
  function. No throughput stress tests in this PR — those come with the producer/
  consumer benchmarks in a later Week 1 PR.
- **Short timeouts:** Kafka consumer polls use a 10-second timeout. If a message is not
  received within 10 seconds, the test fails rather than hanging.
- **Cleanup:** Each test that creates topics or inserts data deletes them in a fixture
  teardown (`yield` + cleanup) to avoid disk accumulation across test runs.
- **No parallel integration tests:** Integration tests run sequentially
  (`-p no:xdist` for this test directory) to avoid race conditions when stopping/
  starting brokers.
- **Broker stop/start tests are isolated:** The leader election and rejoin tests are
  placed last (via `pytest` ordering) since stopping a broker affects other tests if
  they happen concurrently.

---

## 7. How to Run

> **WSL 2 users:** All commands below are run inside your WSL terminal (e.g. Ubuntu).
> Docker Desktop must be running on Windows with **WSL 2 integration enabled** for the
> distro you are using (`Docker Desktop → Settings → Resources → WSL Integration`).
> Confirm it works with `docker info` before continuing.

### 7.0 Prerequisites

| Tool | Minimum version | Install |
|------|----------------|---------|
| Docker Desktop (Windows) | 4.x | [docs.docker.com](https://docs.docker.com/desktop/) |
| Docker Compose plugin | v2 (bundled with Desktop) | included with Docker Desktop |
| Python | 3.11+ | `sudo apt install python3.11` or [python.org](https://python.org) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| make | any | `sudo apt install make` |

Verify your setup:

```bash
docker info          # must not error
docker compose version   # must show v2.x
python3 --version    # must show 3.11+
uv --version
make --version
```

### 7.1 Install Python Dependencies

All subsequent Python commands require the project package and its test extras
to be installed in your active virtual environment.

```bash
# Create a virtual environment and activate it (one-time setup)
uv venv .venv
source .venv/bin/activate

# Install the project + all test dependencies
make install
# Equivalent: uv pip install -e ".[test]"
```

> **Note:** Run all `pytest` and `make test*` commands with the virtual environment
> activated (`source .venv/bin/activate`).

### 7.2 Start the Infrastructure

```bash
# Start all services in the background
make infra-up

# Verify all services are healthy
make infra-status

# Expected output: all 5 services (kafka-1, kafka-2, kafka-3, postgres, schema-registry) show "healthy"
```

Typical startup time on WSL 2: **45–120 seconds** — the Kafka brokers first form the
KRaft quorum (~30–90s), and then `schema-registry` starts once the brokers are healthy
and initializes its `_schemas` topic (adds ~15–30s). If health checks keep showing
`starting`, wait a bit longer and re-run `make infra-status`.

> **`deploy.resources` compatibility:** The `docker-compose.yml` uses the
> `deploy.resources.limits` syntax (Compose v2 file format). Docker Desktop 4.x with
> the Compose v2 plugin supports this without Docker Swarm. If you see a warning like
> *"deploy.resources ignored"*, upgrade Docker Desktop to 4.x+.

### 7.3 Run Unit Tests

Unit tests require no running Docker services — they test config models and the SQL
file only.

```bash
# Run unit tests (no Docker required)
make test-unit
# Equivalent: pytest tests/unit/ -v
```

### 7.4 Run Integration Tests

```bash
# Ensure infrastructure is running first
make infra-up

# Run integration tests only (sequential, as required)
make test-integration
# Equivalent: pytest tests/integration/ -v -m integration -p no:xdist

# Run with verbose Kafka client logging (useful for debugging)
pytest tests/integration/ -v -m integration -p no:xdist --log-cli-level=DEBUG
```

> **Important:** Always run integration tests from the **project root directory**
> (`~/streaming-feature-store`). The `docker compose` calls inside the tests use a
> relative path (`docker/docker-compose.yml`) and will fail if run from a subdirectory.

### 7.5 Run All Tests

```bash
make test
# Equivalent: pytest tests/ -v --cov=streaming_feature_store --cov-report=term-missing
```

### 7.6 Inspect the Cluster Manually

```bash
# List topics
make kafka-topics

# Describe topics (partitions, replicas, ISR)
make kafka-describe

# Open a psql shell to inspect raw_events
make psql

# List registered Schema Registry subjects
make schema-subjects

# Show default compatibility level
make schema-compat

# Tail logs for debugging
make infra-logs
```

### 7.7 Tear Down

```bash
# Stop services, keep data volumes (cluster state preserved across sessions)
make infra-down

# Stop services AND delete all data volumes (complete reset)
make infra-clean
```

---

## 8. Resource Budget & Constraints

### 8.1 Memory Budget

| Component | RAM Allocation | Notes |
|-----------|---------------|-------|
| kafka-1 | 768 MB (512 MB heap) | Docker memory limit |
| kafka-2 | 768 MB (512 MB heap) | Docker memory limit |
| kafka-3 | 768 MB (512 MB heap) | Docker memory limit |
| PostgreSQL | 512 MB | Docker memory limit |
| schema-registry | 512 MB (~256 MB heap) | Docker memory limit |
| **Total infrastructure** | **~3.3 GB** | |

This leaves ample room on a machine with 16 GB+ RAM for the OS, Docker daemon, and
Python processes.

### 8.2 CPU Budget

| Component | CPU Limit | Notes |
|-----------|-----------|-------|
| kafka-* (each) | 2.0 CPUs | Sufficient for moderate throughput |
| PostgreSQL | 1.0 CPU | Mostly idle until sink starts |
| schema-registry | 0.5 CPU | Light REST API traffic |
| **Total** | **7.5 CPUs** | 8.5 remaining for host OS + apps |

### 8.3 Disk Budget

- **Kafka logs:** With `log.retention.hours=168` (7 days) and no active producers yet,
  disk usage is negligible (<100 MB for cluster metadata and internal topics).
- **PostgreSQL data:** The `raw_events` table is empty until the Kafka-to-PostgreSQL
  sink is built in a later PR.
- **Docker images:** ~2.3 GB total (Kafka image ~800 MB, PostgreSQL image ~400 MB,
  Schema Registry image ~800 MB, overhead ~300 MB).

### 8.4 Port Allocations

| Port | Service | Purpose |
|------|---------|---------|
| 19092 | kafka-1 | External Kafka client access |
| 19093 | kafka-2 | External Kafka client access |
| 19094 | kafka-3 | External Kafka client access |
| 5432 | postgres | PostgreSQL client access |
| 8081 | schema-registry | Schema Registry HTTP REST API |

---

## 9. Future Considerations

These items are explicitly **out of scope** for this PR but are noted here for awareness
as they affect decisions made in this design:

1. **Avro/Protobuf schema definitions & evolution experiments (later Week 1 PR):**
   The Schema Registry service is provisioned here, but the actual event schemas
   (`.avsc` or `.proto` files), schema registration code, and `BACKWARD` compatibility
   experiments (adding an optional `device_type` field, removing a deprecated field,
   promoting `int` → `long`) are deferred to the Week 1 schema-registry PR.

2. **Kafka-to-PostgreSQL sink (later Week 1 PR):** The `raw_events` schema is designed
   to support batch inserts from a sink consumer. The `event_id` UUID primary key with
   `ON CONFLICT DO NOTHING` supports idempotent writes.

3. **Topic creation automation:** The `e-commerce-events` topic with 12 partitions and
   replication factor 3 will be created programmatically by the producer (later PR),
   not pre-created in Docker Compose. This keeps the Compose file infrastructure-only.
   (The `_schemas` topic used by Schema Registry is the sole exception — it is
   auto-created by Schema Registry on first start with replication factor 3.)

   **Why producer-side for this project (not production-standard):** This is a learning
   project running on a laptop. Putting 12 partitions into the producer script is fine
   because (a) single developer, no review process needed, (b) no operator/Terraform
   overhead is appropriate at this scale, and (c) it teaches the `AdminClient` API,
   which is also used in integration tests.

   **Rule of thumb for production:** If more than one service reads or writes a topic,
   or if it has non-default config (compaction, retention, RF), it should be
   declaratively managed — not producer-created. Producer-created topics only make
   sense for truly private, single-owner topics, and even then most organizations
   forbid it for consistency. In production, topic definitions live in a Git repo and
   are applied by a dedicated **"infrastructure-as-code pipeline for Kafka"** — parallel
   to how Terraform manages AWS resources or Helm charts manage Kubernetes services.
   This pipeline is **not** part of the application's deploy path: it has its own repo,
   its own reviewers (platform/data-infra team), and cluster-admin credentials that the
   application services never hold. Common implementations: Strimzi `KafkaTopic` CRDs
   reconciled by an operator, Terraform's Kafka provider, or GitOps tools like
   `kafka-gitops` / Julie Ops.

4. **Redis (Week 2–3):** Redis will be added to the Docker Compose file in a later PR
   for online feature serving. The network and Compose structure are designed to
   accommodate additional services.

5. **TLS/SASL:** Not needed for local development. If this project is ever deployed
   beyond localhost, Kafka listeners and Schema Registry HTTP should be reconfigured
   with TLS (and Schema Registry with Basic Auth or mTLS).

6. **Monitoring (Week 5):** Prometheus JMX exporter for Kafka metrics,
   `pg_stat_statements` for PostgreSQL, and Schema Registry's built-in JMX/HTTP
   metrics endpoint can be added later. The Compose file structure supports adding
   sidecar containers.

---

## 10. Open Questions

1. **Kafka image version pinning:** Should we pin to a specific Kafka version (e.g.,
   `apache/kafka:3.9.0`) or use `latest`? Pinning is more reproducible; `latest`
   avoids manual bumps. **Recommendation:** Pin to a specific version for
   reproducibility.

2. **Docker Compose profiles:** Should we use Compose profiles to optionally include
   services (e.g., `--profile monitoring` for Prometheus/Grafana in Week 5)? This would
   keep the base `docker compose up` lean. **Recommendation:** Yes, use profiles for
   optional services added in later weeks.

3. **Python dependency management:** `pyproject.toml` with `pip` vs. `uv` vs. `poetry`.
   **Recommendation:** Use `pyproject.toml` with `uv` for fast dependency resolution.

4. **`CLUSTER_ID` generation:** KRaft requires a pre-generated cluster ID. We will
   generate one with `kafka-storage.sh random-uuid` and hardcode it in the Compose
   file for reproducibility. This is standard practice for Docker-based Kafka setups.

5. **Schema Registry image version pinning:** We pin to
   `confluentinc/cp-schema-registry:7.8.0` because Confluent Platform 7.8.x is paired
   with Apache Kafka 3.9.x (which matches `apache/kafka:3.9.0`). Upgrading Kafka in a
   future PR will require bumping Schema Registry to a compatible CP release.
   **Recommendation:** Pin both images together in lockstep to avoid client/broker
   protocol mismatches.
