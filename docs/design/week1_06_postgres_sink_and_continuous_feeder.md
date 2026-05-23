# Design Doc: Kafka-to-PostgreSQL Sink Consumer + Low-Rate Continuous Feeder

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** Sixth bulletpoint + Week 1 deliverable — Continuously populate the `raw_events` table in PostgreSQL so that by Week 4 there is a substantial historical dataset for offline feature computation and point-in-time joins. Two components in one PR: (1) a **Kafka-to-PostgreSQL sink consumer** (batched inserts) and (2) a **separate low-rate background-feeder producer** (daemon) that runs from Week 1 onward — lines 74–75 + the deliverable line of `gap_project_plan.md`.
**Author:** Auto-generated design document
**Date:** 2026-05-20

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

PR #1 ([`week1_01_docker_compose_infra.md`](week1_01_docker_compose_infra.md))
created the `raw_events` table in PostgreSQL — `event_id UUID PRIMARY KEY`,
typed columns for `event_type` / `user_id` / `session_id` /
`event_timestamp`, a JSONB `properties` column, and four supporting indexes —
and explicitly noted that the table would stay empty *until the
Kafka-to-PostgreSQL sink is built in a later PR*. PR #2 shipped the
`AvroEventConsumer` (registry-bound deserialization +
`avro_dict_to_event` adapter into the Pydantic `EcommerceEvent`). PR #4
shipped the `AvroEventProducer`, the vectorized `SyntheticEventGenerator`
(Zipfian `user_id` distribution), the `TokenBucketPacer`, and the
multi-process burst benchmark (`make load-test-mp`). PR #5 shipped the
single- and multi-process consumer-group harness for end-to-end latency
measurement.

**This is the PR that finally writes data into `raw_events`.** It is split
into two cleanly-separated components, because conflating them was already
the source of one documentation defect (see line 75 of the gap plan: *"Two
distinct producers — do not conflate them"*):

- **Sink consumer** (`src/streaming_feature_store/sink/`) — a
  single-process Python daemon that subscribes to a Kafka topic, batches
  messages, and bulk-inserts them into `raw_events` with idempotent
  semantics (`ON CONFLICT (event_id) DO NOTHING`). Offsets are committed
  **only after** the PostgreSQL `COMMIT` succeeds, giving an at-least-once
  read + idempotent-write combination that is functionally
  exactly-once at the row level without needing Kafka transactions.
- **Continuous feeder** (`src/streaming_feature_store/feeder/`) — a
  single-process, low-rate (≈200 evt/s by default), long-running variant of
  the existing `LoadRunner`. It produces to its **own topic**
  (`e-commerce-events-feed`), so a benchmark run against the original
  `e-commerce-events` topic never pollutes the historical dataset and a
  feeder daemon never competes with a benchmark run for partition leaders.

Both daemons are intended to run side-by-side under `docker compose`
(eventually) or as `nohup`-style background processes on the dev box, from
the moment this PR merges until the end of the project, so that by Week 4
the `raw_events` table contains tens of millions of rows for offline feature
computation and point-in-time joins.

A small **observability bonus** is folded into the sink: the
`SinkAccountant` records *per-partition message counts*, which doubles as
a sanity check that the Zipfian-skewed `user_id` distribution does not
concentrate heavy-hitter users into a single hot partition. The throughput
investigation (§B.5) already confirmed broker-side leadership balance
(4 partitions per broker), but per-key skew at the message level has
never been measured — this PR closes that gap as a free side effect.

### Out of Scope (Deferred to Later PRs)

- **Consume-process-produce EOS / transactional producer.** Moved to Week 2
  ([`gap_project_plan.md`](gap_project_plan.md) — Week 2 EOS bullet), because
  Kafka transactions only earn their keep when there is a real
  "process" step (feature compute + Redis write) to wrap atomically. A
  Week 1 transactional-producer PR with no transform would be a config
  flip on top of the already-shipped idempotent producer — not enough
  substance for its own PR.
- **Kafka Connect JDBC Sink Connector.** The gap plan lists this as an
  *or* alternative to a "simple Python consumer script". This PR chooses
  the Python script — see §2.1 for the trade-off.
- **Schema-evolution drills against the sink.** PR #3 already exercised
  Registry compatibility checks; replaying old-writer events through the
  sink is a §9 follow-up.
- **Multi-process sink consumer.** The feeder produces at ~200 evt/s; the
  GIL ceiling kicks in around 11–14k evt/s. A single-process sink has 50×
  headroom and the orchestration cost of `multiprocessing` is not
  justified here. See §2.3.
- **Offline feature backfills.** The `raw_events` table is the *input* to
  Week 4's offline feature computation; the backfill jobs themselves are
  out of scope for Week 1.

### Deliverables

- `src/streaming_feature_store/sink/__init__.py` — package init.
- `src/streaming_feature_store/sink/postgres_writer.py` — `PostgresWriter`
  wrapping a `psycopg` connection with bulk-`execute_values` idempotent
  inserts.
- `src/streaming_feature_store/sink/sink_runner.py` — `SinkRunner`: drives
  the consume → accumulate-batch → write-Postgres → commit-Kafka loop.
- `src/streaming_feature_store/sink/accountant.py` — `SinkAccountant`:
  counters (`consumed`, `inserted`, `conflict_skipped`,
  `deserialize_failed`), batch-size histogram, flush-latency reservoir,
  per-partition message counts (the Zipfian skew sanity-check).
- `src/streaming_feature_store/sink/report.py` — `SinkRunReport`
  Pydantic model + Markdown renderer.
- `src/streaming_feature_store/feeder/__init__.py` — package init.
- `src/streaming_feature_store/feeder/feeder_runner.py` — `FeederRunner`:
  single-process, rate-paced, long-running variant of `LoadRunner` with
  signal-handler-driven graceful shutdown.
- `scripts/run_postgres_sink.py` — CLI driver for the sink.
- `scripts/run_background_feeder.py` — CLI driver for the feeder.
- `docs/results/week1_postgres_sink_results.md` — generated artifact: 24 h
  smoke-run results including row counts, sustained throughput,
  per-partition skew table.
- `tests/unit/test_postgres_writer.py`, `test_sink_runner_unit.py`,
  `test_sink_accountant.py`, `test_sink_report.py`,
  `test_feeder_runner_unit.py`.
- `tests/integration/test_postgres_writer_end_to_end.py`,
  `tests/integration/test_sink_runner_end_to_end.py`,
  `tests/integration/test_feeder_runner_end_to_end.py`,
  `tests/integration/test_feeder_to_sink_pipeline.py`.
- `Makefile` targets: `sink-run`, `feeder-run`, `pipeline-up`,
  `pipeline-down`, `sink-report`.

---

## 2. Critical Design Decisions

### 2.1 Python Sink vs. Kafka Connect JDBC Sink Connector

**Decision:** Implement the sink as a **plain Python consumer script**,
not as a Kafka Connect JDBC Sink Connector.

**Rationale:**

- **Single-language stack.** The producer (PR #4) and the consumer-group
  E2E latency harness (PR #5) are Python; the validation and feature-compute
  stages in Week 2 will also be Python (or PyFlink). Adding a JVM-based
  Connect cluster purely for this sink is operational overhead that buys
  nothing for laptop-scale.
- **Reusability.** The existing `AvroEventConsumer` already handles
  registry-bound deserialization and the `avro_dict_to_event` adapter
  into the Pydantic `EcommerceEvent`. A Python sink composes that with a
  `psycopg` bulk insert in <200 lines of substantive code. A Connect
  setup requires a Connect cluster, a worker config, a connector config,
  a transformation chain, and DLT-handling — significant accidental
  complexity.
- **Pedagogical value.** Writing the sink by hand teaches the
  read→batch→write→commit ordering that *is* the whole point of
  "exactly-once at the row level without Kafka transactions". A
  declarative Connect config hides that.
- **Acknowledged tradeoff.** Kafka Connect is the production-correct choice
  for sinks at scale: SMT chain, auto-scaling, DLQ support, schema
  evolution handling, REST control plane. This is called out in §9
  (Future Considerations) so that the portfolio narrative is honest about
  why a real deployment would use Connect.

### 2.2 Idempotent Inserts via `ON CONFLICT (event_id) DO NOTHING`

**Decision:** Every batch insert uses `INSERT ... ON CONFLICT (event_id)
DO NOTHING`. The `event_id` UUID primary key is the idempotency key.

**Rationale:**

- Kafka delivery semantics from the consumer's perspective are
  **at-least-once** by default (and remain so even after the Week 2 EOS PR,
  because Kafka transactions only cover Kafka topics + offsets, not the
  external PostgreSQL write — see the Week 2 EOS bullet's
  "cross-store atomicity" caveat).
- A replay therefore must not produce duplicate rows. The
  `ON CONFLICT DO NOTHING` clause makes the second-write a no-op without
  raising, so the read-batch-write-commit loop can safely re-process the
  *same* messages after a crash without manual de-duplication logic.
- This was anticipated in PR #1's `raw_events` schema decision
  ([`week1_01_docker_compose_infra.md`](week1_01_docker_compose_infra.md) §2.6):
  *"A UUID primary key with ON CONFLICT DO NOTHING provides idempotent
  inserts, which is part of the exactly-once semantics strategy described
  in the Week 1 plan."*
- The `conflict_skipped` counter in `SinkAccountant` records how often a
  conflict actually fires; under normal steady-state this is `0`, and a
  non-zero number after a crash recovery is the *expected* signal that
  the at-least-once replay was correctly absorbed.

### 2.3 Single-Process Sink (No Multi-Process Orchestration)

**Decision:** The sink is a single-process daemon. No `multiprocessing`,
no consumer-group-of-processes orchestration.

**Rationale:**

- The feeder produces at ≈200 evt/s by default. The GIL throughput
  investigation ([`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md))
  showed a single Python process caps at ~11–14k evt/s on producer-side
  encode and similarly on consumer-side decode. The sink therefore has
  **~50× headroom** over the design rate.
- The orchestration cost of `multiprocessing` (spawn cost, IPC for
  snapshot aggregation, the careful child-entry top-level-function rule —
  see PR #5 §2.6) is real, and it would dwarf the throughput problem this
  sink is trying to solve.
- If the feeder rate is ever raised to >5k evt/s (e.g., to populate
  `raw_events` faster for a larger Week 4 dataset), the existing
  `MultiprocessConsumeRunner` from PR #5 can be lifted into the sink
  package as a §9 follow-up. Until then, the simpler design wins.
- **Symmetric reasoning to PR #5.** The plan-line ("single-process is
  fine — the GIL ceiling is irrelevant below ~10k/s") is preserved as
  a code-level decision and re-anchored to the measured GIL ceiling, not
  to vibes.

### 2.4 Separate Topic (`e-commerce-events-feed`) for the Feeder

**Decision:** The feeder produces to a topic **distinct from the
benchmark topic** — `e-commerce-events-feed` — and the sink subscribes
**only** to that feed topic.

**Rationale:**

- The gap plan (line 75) explicitly carves this out: *"ideally on its
  own topic so a benchmark run never pollutes the historical dataset."*
- A burst benchmark on `e-commerce-events` would otherwise inject a
  10 s spike of ~60k evt/s into `raw_events`, corrupting any
  point-in-time-join analysis that assumes a roughly-uniform arrival
  rate.
- Symmetric reasoning the other direction: the latency-benchmark
  consumer in PR #5 must not be contaminated by the feeder's steady
  trickle, because the latency-benchmark headline number is a
  *clean-room* measurement on a controlled burst.
- The feeder topic uses the **same partition count (12) and RF (3)** as
  the benchmark topic, so partition-skew measurement remains comparable.
- Topic creation reuses the existing `TopicAdmin.ensure_topic()` from
  PR #4; the feeder calls it at startup with `name="e-commerce-events-feed",
  num_partitions=12, replication_factor=3`. Idempotent: no-op if it
  exists.

### 2.5 Default Feeder Rate (200 evt/s) and Why

**Decision:** The feeder runs at **200 evt/s by default**, configurable
via `--rate-evt-per-sec`.

**Rationale:**

- **Daily volume:** 200 × 86400 ≈ **17.3 M events/day**, ~120 M/week. By
  the start of Week 4 (≈18 days after this PR merges), the table holds
  >200 M rows — substantial enough to make point-in-time joins
  observably non-trivial without becoming so large that a `EXPLAIN
  ANALYZE` on a laptop becomes a multi-minute affair.
- **Storage:** Each `raw_events` row is roughly 250–400 B on disk
  (event_id UUID + small typed columns + a modest JSONB `properties`).
  At 200 evt/s, that is ~5 GB/day, ~90 GB by Week 4. PostgreSQL
  on the dev box can hold this comfortably; if it ever becomes a
  problem, the feeder can be paused or the table partitioned by month
  (a §9 follow-up).
- **CPU:** The feeder is single-process Python; the
  `TokenBucketPacer` keeps it idle ≈99% of the time at 200 evt/s.
  PostgreSQL bulk-insert at 200 evt/s is negligible on a laptop.
- **Headroom for benchmarks.** The feeder + sink combined consume
  ~0.5 CPU; the multi-process producer benchmark + consumer-group
  benchmark each use ~8 CPU during their ~10 s burst. The two
  workloads never collide on the same partitions (separate topics
  per §2.4) and never collide on CPU more than transiently.

### 2.6 Batch Policy: 1000 Events OR 10 s, Whichever Comes First

**Decision:** The sink flushes a batch to PostgreSQL when **either**:

- the batch reaches **1000 events**, or
- **10 seconds** have elapsed since the *first* event in the current batch.

**Rationale:**

- **1000 events** matches the gap plan ("e.g., every 1,000 events or every
  10 seconds"). It is a balance point: large enough that `execute_values`
  amortizes round-trip and parse cost (a single batched INSERT of 1000
  rows is ~10× faster than 1000 individual INSERTs on PostgreSQL), small
  enough that a crash loses ≤1000 events of uncommitted-to-Postgres data
  — and those 1000 events are still in Kafka, so they are not *lost*,
  only *re-read* after restart.
- **10 s timeout** ensures that at the feeder's 200 evt/s rate, a batch
  fills naturally in 5 s but never *waits* more than 10 s. This bounds
  staleness: a row in `raw_events` is never more than ~10 s older than its
  arrival in Kafka, which matters if any debugging session inspects the
  table while events are flowing.
- The flush-trigger is checked **inside the `poll()` loop**, not on a
  separate timer thread, so there is no concurrency primitive to get
  wrong and no GIL-contender thread.

### 2.7 Read-Batch-Write-Commit Ordering (At-Least-Once Read + Idempotent Write)

**Decision:** The sink's main loop is strictly ordered:

```
1. poll()                   # read at-least-once from Kafka
2. deserialize + accumulate # build the current batch
3. if flush trigger:
4.     postgres_writer.flush(batch)   # idempotent INSERT
5.     consumer.commit(...)           # commit Kafka offsets
6.     clear batch
```

**Rationale:**

- **The order is the whole point.** If Kafka offsets were committed
  *before* the Postgres write, a crash between those two steps would
  drop the batch from Kafka without it ever reaching Postgres — silent
  data loss.
- With this ordering, a crash between steps 4 and 5 means the Postgres
  write *succeeded* but Kafka still thinks the messages are unconsumed.
  Restart re-reads the same messages, re-attempts the bulk insert,
  every row hits `ON CONFLICT DO NOTHING`, the `conflict_skipped`
  counter increments by the batch size, and step 5 finally commits.
  No duplicates, no loss. This is the "at-least-once read + idempotent
  write = effectively-exactly-once at the row level" pattern that the
  Week 1 plan promised.
- The consumer is configured with `enable.auto.commit=false` to make
  this ordering enforceable from the consumer side.
- Note: this is **independent of** the Week 2 EOS transactional-producer
  PR. The Week 2 PR adds atomicity for the *consume-process-produce*
  cycle (where the "produce" step is feature writes back to a downstream
  Kafka topic / Redis); it does **not** retroactively change the sink's
  contract here. The sink remains idempotent-insert + manual-commit
  forever, because Kafka transactions cannot span Postgres anyway
  (the "Kafka transactions ≠ cross-store atomicity" caveat that lives in
  the Week 2 EOS bullet).

### 2.8 Per-Partition Message Counts as a Zipfian Skew Sanity-Check

**Decision:** `SinkAccountant` keeps a `dict[int, int]` of per-partition
message counts (`partition_counts`). The Markdown report emits a sorted
table; the integration test asserts no single partition holds more
than `2 × mean` of the total.

**Rationale:**

- The synthetic generator
  ([`synthetic.py`](../../src/streaming_feature_store/load/synthetic.py)) draws
  `user_id` with **Zipfian skew** (`user_zipf_alpha=1.1` default). Heavy-hitter
  users emit disproportionately many events.
- The producer (PR #4) keys messages by `user_id`, so heavy-hitter users
  concentrate into the partitions that hash-map to their IDs. A single
  user generating, say, 5% of all events would lend ~5% load to one
  partition on top of the uniform background — at 12 partitions
  (~8.3% per partition under uniform), a ~13% partition is well within
  the `< 2× mean` threshold.
- The throughput investigation §B.5 already confirmed *broker-side
  leadership* balance (4 partitions per broker), but never measured
  *message-level* key-distribution skew. This is the free sanity-check
  that closes that gap.
- The check fires every batch flush. If it ever trips, the
  `report.md` will surface it and a `WARN` log line is emitted; the
  pipeline does not abort because skew is a tuning signal, not a
  correctness violation.

### 2.9 Graceful Shutdown via Signal Handlers

**Decision:** Both the sink and the feeder install handlers for
`SIGTERM` and `SIGINT` that set an `asyncio.Event`-like flag (actually a
`threading.Event` for sync code) so the main poll loop exits cleanly at
the next iteration. On exit:

- The sink **flushes the in-flight batch** (if any) and **commits
  offsets** before calling `consumer.close()`. This is the standard
  rebalance-friendly close pattern from PR #5.
- The feeder **flushes the producer's queue** (`producer.flush(timeout)`)
  and emits a final report row.

**Rationale:**

- These are long-running daemons. A SIGKILL would leak in-flight Kafka
  state; a SIGTERM during shell shutdown (`docker compose down`,
  `Ctrl-C` in a `tmux` pane) must shut down gracefully.
- The signal handler intentionally does **only** set-flag work, not Kafka
  operations, because Kafka client objects are not re-entrant from
  signal handlers (well-documented `librdkafka` constraint).

---

## 3. Architecture

### 3.1 Component Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Continuous Pipeline (this PR)                  │
│                                                                      │
│   ┌────────────────┐    ┌────────────────────────┐    ┌──────────┐   │
│   │ FeederRunner   │───▶│  e-commerce-events-feed│───▶│ SinkRunner│  │
│   │ (single proc,  │    │   (12 partitions, RF=3)│    │ (single   │  │
│   │  ~200 evt/s,   │    └────────────────────────┘    │  proc,    │  │
│   │  daemon)       │                                  │  daemon)  │  │
│   └────────────────┘                                  └─────┬─────┘  │
│                                                             │        │
│                                                             ▼        │
│                                                  ┌──────────────────┐│
│                                                  │ PostgresWriter   ││
│                                                  │ (psycopg,        ││
│                                                  │  execute_values, ││
│                                                  │  ON CONFLICT)    ││
│                                                  └────────┬─────────┘│
│                                                           │          │
│                                                           ▼          │
│                                                  ┌──────────────────┐│
│                                                  │ raw_events table ││
│                                                  │ (PR #1 schema)   ││
│                                                  └──────────────────┘│
└──────────────────────────────────────────────────────────────────────┘

  ── independent from ──

┌──────────────────────────────────────────────────────────────────────┐
│                Burst Benchmark Pipeline (existing PRs)               │
│                                                                      │
│   load-test-mp ──▶ e-commerce-events ──▶ consume-test-mp             │
│   (PR #4, ~10 s) (12 partitions, RF=3)  (PR #5, ~10 s)               │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Main Loop (Sink)

```
┌─── SinkRunner.run() ──────────────────────────────────────────────┐
│                                                                   │
│  while not shutdown_flag.is_set():                                │
│      msgs = consumer.poll_batch(timeout=1.0, max_records=500)     │
│                                                                   │
│      for raw in msgs:                                             │
│          try:                                                     │
│              event = avro_dict_to_event(decode(raw))              │
│          except (DeserializeError, ValidationError):              │
│              accountant.record_failure()                          │
│              continue                                             │
│          batch.append(event, partition=raw.partition())           │
│                                                                   │
│      if batch.should_flush():        # ≥1000 OR ≥10 s old         │
│          n_inserted, n_skipped = postgres_writer.flush(batch)     │
│          consumer.commit(asynchronous=False)                      │
│          accountant.record_flush(n_inserted, n_skipped, batch)    │
│          batch.clear()                                            │
│                                                                   │
│  # graceful shutdown path                                         │
│  if batch:                                                        │
│      postgres_writer.flush(batch)                                 │
│      consumer.commit(asynchronous=False)                          │
│  consumer.close()                                                 │
│  postgres_writer.close()                                          │
└───────────────────────────────────────────────────────────────────┘
```

### 3.3 Main Loop (Feeder)

```
┌─── FeederRunner.run() ────────────────────────────────────────────┐
│                                                                   │
│  while not shutdown_flag.is_set():                                │
│      n_to_emit = pacer.acquire(target_batch=200)   # ≤200/iter    │
│      events = generator.generate(n_to_emit)                       │
│      for ev in events:                                            │
│          producer.produce(topic="e-commerce-events-feed",         │
│                           key=ev.user_id, value=ev,               │
│                           on_delivery=accountant.on_delivery)     │
│      producer.poll(0)              # drain delivery callbacks     │
│                                                                   │
│  # graceful shutdown                                              │
│  producer.flush(timeout=10.0)                                     │
└───────────────────────────────────────────────────────────────────┘
```

### 3.4 Module Layout

```
src/streaming_feature_store/
├── sink/
│   ├── __init__.py
│   ├── postgres_writer.py     # PostgresWriter, BatchInsertResult
│   ├── sink_runner.py         # SinkRunner, SinkRunConfig, Batch
│   ├── accountant.py          # SinkAccountant, SinkSnapshot
│   └── report.py              # SinkRunReport (Pydantic) + renderer
├── feeder/
│   ├── __init__.py
│   └── feeder_runner.py       # FeederRunner, FeederRunConfig
├── consumer/                  # PR #2 — AvroEventConsumer (reused)
├── producer/                  # PR #2 — AvroEventProducer  (reused)
├── load/                      # PR #4 — SyntheticEventGenerator, Pacer (reused)
└── admin/                     # PR #4 — TopicAdmin (reused)

scripts/
├── run_postgres_sink.py
└── run_background_feeder.py

docs/results/
└── week1_postgres_sink_results.md   # generated artifact
```

---

## 4. Detailed Implementation

### 4.1 `PostgresWriter`

```
class PostgresWriter:
    """Bulk idempotent inserts into ``raw_events``.

    Parameters
    ----------
    dsn : str
        psycopg connection string (``host=... user=... ...``).
    statement_timeout_ms : int, optional
        PostgreSQL ``statement_timeout`` applied per session.  Defaults to
        ``30_000`` (30 s) — well above the worst-case 1000-row batch.

    Notes
    -----
    The connection is opened lazily on first ``flush`` and held for the
    lifetime of the writer (one persistent connection per sink process).
    Re-connection on transient failure (``OperationalError``) is the
    caller's responsibility; the sink runner wraps ``flush`` in a single
    retry with exponential backoff (§4.2).
    """

    def flush(self, events: list[EcommerceEvent]) -> BatchInsertResult:
        """Insert ``events`` with ``ON CONFLICT (event_id) DO NOTHING``.

        Returns a ``BatchInsertResult(inserted=N, skipped=M)`` where
        ``N + M == len(events)``.  Skipped rows are the count returned by
        ``cur.rowcount`` subtracted from ``len(events)`` after the
        ``execute_values`` call commits.
        """

    def close(self) -> None:
        """Close the underlying connection.  Safe to call twice."""
```

- Uses `psycopg.extras.execute_values` for a single round-trip
  insert of the entire batch.
- The `properties` JSONB column is built from the event's `payload`
  Pydantic submodel via `model_dump_json()`.
- A single `BEGIN; INSERT ...; COMMIT;` per batch — autocommit OFF so
  the commit is explicit and rollback on failure leaves the connection
  clean for the next attempt.

### 4.2 `SinkRunner`

```
class SinkRunner:
    """Single-process consume → batch → write-Postgres → commit-Kafka loop.

    Parameters
    ----------
    config : SinkRunConfig
        Pydantic config (topic, bootstrap, batch_max_rows, batch_max_age_s,
        consumer_group_id, dsn).
    consumer : AvroEventConsumer
        Reused from PR #2.
    writer : PostgresWriter
        See §4.1.
    accountant : SinkAccountant
        See §4.3.
    """

    def run(self) -> SinkSnapshot:
        """Run until ``self._shutdown.is_set()``.  Returns the final
        accountant snapshot.

        Implements the read-batch-write-commit ordering from §2.7.
        On flush failure, retries once with 1 s backoff; on second
        failure, raises and the caller's signal handler triggers
        graceful shutdown of the *unfailed* in-flight batch.
        """

    def request_shutdown(self) -> None:
        """Signal-handler-safe shutdown request."""
```

- The `Batch` helper holds `list[EcommerceEvent]` plus the wall-clock
  timestamp of the first appended event so `should_flush()` can be a
  cheap boolean.
- `enable.auto.commit=false` is enforced in `consumer` config; if a
  caller overrides it the constructor raises (defense in depth).

### 4.3 `SinkAccountant`

```
class SinkAccountant:
    """Counters + per-partition message tallies for the sink.

    Tracks
    ------
    consumed : int
        Total messages read from Kafka (including ones that fail to
        deserialize).
    inserted : int
        Sum of ``BatchInsertResult.inserted`` across all flushes.
    conflict_skipped : int
        Sum of ``BatchInsertResult.skipped`` — non-zero only after a
        crash-replay scenario (the idempotency signal, §2.2).
    deserialize_failed : int
        Avro decode or Pydantic validation failures.  Messages are
        dropped after one log line; no DLQ wiring (deferred to Week 2's
        validation layer).
    batch_size_hist : Histogram
        Number of rows per flush.  Surfaced in the report; useful to
        confirm the 1000-cap is the dominant trigger, not the 10 s
        timeout, at the default rate.
    flush_latency_ms_reservoir : Reservoir[float]
        Wall-clock duration of each ``PostgresWriter.flush`` call.
        Reservoir-sampled, fixed-size; p50/p95/p99 in the report.
    partition_counts : dict[int, int]
        Per-partition message counts.  Used for the Zipfian skew sanity
        check (§2.8).
    """
```

### 4.4 `FeederRunner`

```
class FeederRunner:
    """Single-process, rate-paced, long-running event feeder.

    Parameters
    ----------
    config : FeederRunConfig
        Pydantic config (topic, rate_evt_per_sec, bootstrap, registry_url,
        seed, user_population, num_skus).
    producer : AvroEventProducer
        Reused from PR #2 (default profile is throughput; the feeder does
        *not* need EOS — its writes are not the source of training-serving
        skew).
    generator : SyntheticEventGenerator
        Reused from PR #4.
    pacer : TokenBucketPacer
        Reused from PR #4, configured for the target rate.
    accountant : DeliveryAccountant
        Reused from PR #4.
    """

    def run(self) -> FeederSnapshot:
        """Run until ``self._shutdown.is_set()``."""
```

- The feeder is a thin wrapper. ~80% of its functionality is delegated to
  the already-shipped `LoadRunner` building blocks; the daemon-specific
  pieces are the signal handler, the long-run-friendly accountant snapshot
  cadence (every 60 s instead of every batch), and the periodic flush of
  intermediate metrics to `stdout` for `docker compose logs -f`-style
  observability.

### 4.5 `SinkRunReport` (Pydantic)

```
class SinkRunReport(BaseModel):
    started_at: datetime
    ended_at: datetime
    duration_s: float
    consumed: int
    inserted: int
    conflict_skipped: int
    deserialize_failed: int
    batches_flushed: int
    batch_size_p50: float
    batch_size_p99: float
    flush_latency_ms_p50: float
    flush_latency_ms_p95: float
    flush_latency_ms_p99: float
    partition_counts: dict[int, int]
    partition_skew_ratio: float   # max(count) / mean(count)
    partition_skew_pass: bool      # partition_skew_ratio < 2.0

    def render_markdown(self) -> str: ...
```

### 4.6 Topic Bootstrap

The sink and feeder both call `TopicAdmin.ensure_topic()` on startup:

- Feeder: ensures `e-commerce-events-feed` with 12 partitions, RF=3.
- Sink: also calls `ensure_topic` defensively (same args). If the feeder
  has not yet been started, the sink still creates the topic so it can
  subscribe; the topic will be empty until the feeder runs.

This is idempotent (per PR #4 §2.1), so duplicate calls are no-ops.

---

## 5. Unit Tests

All unit tests use `pytest` fixtures, no real Kafka, no real PostgreSQL.
`psycopg.Connection` is mocked via `unittest.mock.MagicMock` (`pytest`
fixtures wrapping `MagicMock` — `unittest` is **not** used as a test
framework, only its `mock` helpers).

| Test | Assertion |
|---|---|
| `test_postgres_writer_builds_correct_sql` | `execute_values` is called with the `ON CONFLICT (event_id) DO NOTHING` clause |
| `test_postgres_writer_flush_returns_inserted_and_skipped_counts` | Mocked `cur.rowcount` of `7` on a batch of `10` yields `BatchInsertResult(inserted=7, skipped=3)` |
| `test_postgres_writer_flush_empty_batch_is_noop` | `flush([])` does not call `execute_values`, returns `BatchInsertResult(0, 0)` |
| `test_postgres_writer_close_is_idempotent` | Two `close()` calls do not raise |
| `test_postgres_writer_flush_rolls_back_on_failure` | If `execute_values` raises, `conn.rollback()` is called and the exception propagates |
| `test_sink_runner_flushes_on_batch_full` | Append 1000 events → `writer.flush` called, `consumer.commit` called *after* `flush` |
| `test_sink_runner_flushes_on_age_timeout` | Append 5 events, advance fake clock by 11 s, next iteration → `flush` called |
| `test_sink_runner_commits_after_flush_not_before` | Order asserted via `MagicMock.mock_calls` — `flush` index < `commit` index |
| `test_sink_runner_deserialize_failure_does_not_break_batch` | One bad message in 100 → 99 inserted, `deserialize_failed == 1`, no exception |
| `test_sink_runner_shutdown_flushes_in_flight_batch` | Set shutdown flag mid-loop → final partial batch is flushed + committed before `consumer.close()` |
| `test_sink_runner_rejects_auto_commit_true_config` | Construct with `enable_auto_commit=True` → `ValueError` |
| `test_sink_accountant_partition_counts_tracked` | Record messages from partitions {0, 0, 1, 3, 3, 3} → `partition_counts == {0:2, 1:1, 3:3}` |
| `test_sink_accountant_skew_ratio_uniform` | 1200 messages evenly across 12 partitions → skew_ratio == 1.0, pass=True |
| `test_sink_accountant_skew_ratio_pathological` | 1100 in partition 0, 100 spread across the rest → skew_ratio ≈ 11, pass=False |
| `test_sink_accountant_conflict_skip_increments` | Two flushes returning skipped=5 and skipped=3 → `conflict_skipped == 8` |
| `test_sink_report_render_markdown_includes_partition_table` | Rendered Markdown contains a row for every partition in `partition_counts` |
| `test_sink_report_render_markdown_flags_failed_skew_check` | If `partition_skew_pass=False`, rendered Markdown contains a `⚠ skew check failed` marker |
| `test_feeder_runner_paces_to_target_rate` | Fake-clock 1 s window at 200 evt/s → `producer.produce` called between 180 and 220 times (allowing token-bucket jitter) |
| `test_feeder_runner_shutdown_flushes_producer` | Set shutdown → `producer.flush(timeout)` called exactly once |
| `test_feeder_runner_uses_feed_topic_default` | `producer.produce` called with `topic="e-commerce-events-feed"` |
| `test_feeder_run_config_validates_positive_rate` | `FeederRunConfig(rate_evt_per_sec=0)` raises `ValidationError` |
| `test_feeder_run_config_validates_topic_name_not_benchmark` | `FeederRunConfig(topic="e-commerce-events")` → `ValidationError` ("refusing to feed into the benchmark topic") |

Coverage target: **100% line + branch** for everything in `sink/` and
`feeder/`. Branches with no plausible runtime trigger
(e.g., `if not events:` in `flush()` after the runner's own guard) are
still exercised via direct unit calls.

---

## 6. Integration Tests

Integration tests use real Kafka + real PostgreSQL via the `docker
compose` infra and the `infra-up` fixture from PR #1. Marked
`@pytest.mark.integration`; skipped if `docker compose ps` reports no
running services.

| Test | Setup → Assertion |
|---|---|
| `test_postgres_writer_end_to_end_inserts_rows` | Real PG → `flush(10 events)` → `SELECT count(*) == 10` |
| `test_postgres_writer_end_to_end_on_conflict_skips` | Insert same batch twice → second call returns `skipped=N`, table row count unchanged |
| `test_sink_runner_end_to_end_consumes_and_inserts` | Produce 5000 events to `e-commerce-events-feed`, run sink for 30 s, expect ≥5000 rows in `raw_events` |
| `test_sink_runner_end_to_end_crash_replay_no_duplicates` | Run sink, kill after first flush (Postgres committed) but **before** Kafka commit (use a fault-injecting wrapper), restart, assert: `inserted` rows match the produced count and `conflict_skipped > 0` |
| `test_sink_runner_end_to_end_partition_skew_under_zipfian` | Run feeder at 500 evt/s for 60 s, then read `partition_counts` from sink accountant — assert `partition_skew_ratio < 2.0` |
| `test_feeder_runner_end_to_end_produces_at_target_rate` | Run feeder at 200 evt/s for 30 s → expect 6000±300 messages on `e-commerce-events-feed` |
| `test_feeder_to_sink_pipeline_24h_smoke` | (Marked `slow`, opt-in via `-m slow`) Run feeder + sink for ≥10 min, expect `inserted > 100_000`, `conflict_skipped == 0`, `deserialize_failed == 0` |
| `test_pipeline_independence_from_benchmark` | Start feeder + sink; concurrently run `load-test-mp` against the *benchmark* topic. After both finish: `raw_events` count exactly matches feeder's produced count (no contamination from benchmark) |

---

## 7. How to Run

### 7.1 One-time bootstrap

```
make infra-up                  # PR #1: Kafka + Postgres + Registry
make topic-ensure              # PR #4: ensures e-commerce-events
                               # (the feeder will ensure -feed itself)
```

### 7.2 Start the continuous pipeline

```
make feeder-run                # starts feeder daemon in foreground
                               # (200 evt/s → e-commerce-events-feed)
                               # Ctrl-C to stop gracefully
```

In a second terminal:

```
make sink-run                  # starts sink daemon in foreground
                               # (subscribes to e-commerce-events-feed)
                               # Ctrl-C to stop gracefully
```

Or daemonised:

```
make pipeline-up               # nohup both, write PIDs to .pids/
make pipeline-down             # SIGTERM both, wait for graceful shutdown
```

### 7.3 Generate the result artifact

```
make sink-report               # snapshot accountant, write
                               # docs/results/week1_postgres_sink_results.md
```

### 7.4 Inspect the table

```
make psql
feature_store=# SELECT count(*) FROM raw_events;
feature_store=# SELECT event_type, count(*) FROM raw_events GROUP BY 1;
feature_store=# SELECT user_id, count(*) FROM raw_events
                  GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
                                               -- Top 10 Zipfian users
```

### 7.5 CLI

```
python scripts/run_background_feeder.py \
    --rate-evt-per-sec 200 \
    --topic e-commerce-events-feed \
    --bootstrap kafka-1:9092 \
    --registry http://schema-registry:8081 \
    --seed 42

python scripts/run_postgres_sink.py \
    --topic e-commerce-events-feed \
    --group-id postgres-sink \
    --bootstrap kafka-1:9092 \
    --registry http://schema-registry:8081 \
    --dsn "host=postgres user=featurestore dbname=feature_store password=…" \
    --batch-max-rows 1000 \
    --batch-max-age-s 10
```

---

## 8. Resource Budget & Constraints

| Component | CPU (steady state) | RAM | Disk |
|---|---|---|---|
| FeederRunner @ 200 evt/s | <0.05 core | ~60 MB | 0 |
| SinkRunner @ 200 evt/s | <0.1 core | ~80 MB | 0 |
| PostgreSQL bulk-insert @ 200 evt/s | <0.1 core | (existing infra) | ~5 GB/day |

Total: <0.3 CPU continuously, with all 8+ CPUs free for benchmark bursts.

Constraints:

- **Disk growth.** At 200 evt/s the table grows ~5 GB/day. Allow ~150 GB
  by end of Phase 1. If short on disk: lower `--rate-evt-per-sec`, or
  add a §9 follow-up to partition the table by month and drop oldest
  partitions.
- **Kafka log retention.** The feed topic's `retention.ms` default
  (7 days) is fine — the sink commits offsets every batch, so messages
  older than the broker's retention horizon would only be re-read on a
  multi-day outage, which is out of scope.
- **PostgreSQL `statement_timeout`.** Set to 30 s in the writer's
  session — well above the worst-case 1000-row batch insert latency
  (typically <50 ms on a laptop).

---

## 9. Future Considerations

1. **Kafka Connect JDBC Sink Connector** — the production-correct
   replacement for the Python sink. Decision recorded in §2.1; revisit
   if/when the project ever needs SMTs, multi-cluster replication, or a
   REST control plane.
2. **Multi-process sink** — only needed if the feeder rate is ever raised
   above ~5k evt/s. The existing `MultiprocessConsumeRunner` from PR #5
   ports cleanly: one consumer-group-member process per partition, each
   with its own `PostgresWriter`, and each writing to the same
   `raw_events` table (the `ON CONFLICT` clause keeps it safe even if a
   message is ever delivered to two members during rebalance).
3. **Monthly partitioning of `raw_events`** — if the table exceeds ~100 GB
   or query performance degrades, partition by `event_timestamp` month.
   `pg_partman` is the conventional choice.
4. **Dead-letter topic for deserialize failures** — currently the sink
   logs and drops; Week 2's validation layer adds a `dead-letter-queue`
   topic with error metadata. The sink can subscribe to *that* topic too,
   so even invalid messages land in PG for forensics.
5. **Outbox pattern for cross-store atomicity** — when feature writes
   land in Week 2 (Kafka topic + Redis + PostgreSQL all written together),
   the sink's idempotent-write contract here serves as a reference for
   the Redis side. Outbox itself is a Week 2 design topic, not a sink
   change.
6. **Schema-evolution replay** — PR #3 exercised Registry compatibility;
   a §9 follow-up runs a synthetic *old-writer-schema* message stream
   through the sink to assert backward-compat reads land in the JSONB
   `properties` column without information loss.

---

## 10. Open Questions

1. **Should the feeder pause itself during a benchmark run?** Currently
   the topic separation (§2.4) makes this unnecessary — feeder and
   benchmark cannot collide. But if the host is RAM-constrained on a
   smaller dev box, a `--pause-on-benchmark` flag (watching for the
   benchmark's `make` target via a lockfile) might be worth adding.
   Default answer: no, topic separation is sufficient.
2. **Should the sink expose a `/metrics` endpoint** (Prometheus
   text-format) for the `SinkAccountant` snapshot? Useful in Week 5
   (freshness monitoring). Deferred to a Week 5 task — the sink only
   needs to *expose* a snapshot method here; the HTTP endpoint can wrap
   it later.
3. **`partition_skew_ratio` threshold.** Default is `2.0` (a partition
   may hold up to 2× the mean before failing the check). This is a
   conservative guess; the integration test
   `test_sink_runner_end_to_end_partition_skew_under_zipfian` will
   either confirm it or motivate raising/lowering it. If the Zipfian
   `alpha=1.1` distribution naturally produces ~1.3× skew in steady
   state, the threshold may need to be ~1.6 to leave headroom; if it
   produces ~1.05 skew, the threshold can be tightened to ~1.3.
