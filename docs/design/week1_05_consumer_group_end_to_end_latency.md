# Design Doc: Multi-Process Consumer Group with End-to-End Latency Measurement (GIL-Symmetric to the Producer)

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 1 — Kafka Fundamentals & Event Ingestion
**Scope:** Fifth bulletpoint — Build a consumer that reads events and measures end-to-end latency, implemented as a **consumer group of processes, not threads** (GIL symmetry — lines 70–71 of `gap_project_plan.md`)
**Author:** Auto-generated design document
**Date:** 2026-05-18

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
`EcommerceEvent` schema and shipped a *correctness-first* `AvroEventProducer`
**and** a matching `AvroEventConsumer`
(`src/streaming_feature_store/consumer/avro_consumer.py`) that handles
schema-bound Avro **de**serialization and a Pydantic-validating
`avro_dict_to_event` adapter. PR #3
(`week1_03_schema_evolution_experiments.md`) exercised the Registry's
compatibility checker. PR #4
(`week1_04_synthetic_event_producer.md`) added the synthetic generator + the
multi-worker load-runner, and — through the throughput investigation it
triggered ([`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md))
— established the project's central performance fact: **one CPython process
runs Python bytecode on one core; a Pydantic + Avro + Kafka workload caps at
~11–14k evt/s per process, and the only escape is more *processes* (the
`load_mp` harness, ~5× to ~60k).**

This PR is the **consumer-side counterpart** of PR #4. It builds a consumer
that reads `e-commerce-events`, deserializes each record, and measures
**end-to-end (produce → consumed-and-deserialized) latency** — implemented
**as a consumer group of processes**, because the GIL ceiling is symmetric:
the consumer's `fastavro` decode + Pydantic validation is exactly as
Python-bytecode-heavy as the producer's encode path, so a single-process
consumer hits the *same* ~11–14k wall and cannot drain a 50–60k producer.
The Kafka-idiomatic escape is the symmetric one: a consumer group with **one
member process per partition-subset** (≤ 12, the partition count). This PR
encodes that as a first-class architecture that deliberately mirrors
`load_mp`.

> **Supersession note.** PR #4 §9 item 6 speculated that PR #5 would be a
> *JVM* (Kafka Streams) consumer. The 2026-05-18 gap-plan revision (lines
> 70–71), written after the GIL investigation, redefines PR #5 as a **Python
> consumer group of processes** specifically so the project *demonstrates*
> the symmetric GIL ceiling on the consume side — a stronger portfolio
> narrative ("found the ceiling on produce, proved the same reasoning on
> consume"). The JVM cross-language consumer is demoted to a future item
> (§9).

Concretely, this PR:

1. Adds a single-member **consume runner**
   (`src/streaming_feature_store/consume/consume_runner.py`) that wraps the
   PR #2 `AvroEventConsumer` with a subscribe → poll → deserialize → account
   → manual-commit loop, an end-to-end-latency **`ConsumeAccountant`**, and a
   lag probe. This is the consumer analog of PR #4's `LoadRunner`.
2. Adds a **multi-process orchestrator**
   (`src/streaming_feature_store/consume_mp/`) — `process_planner`,
   `worker_entry`, `mp_runner`, `aggregator`, `report` — that spawns *N*
   member processes sharing one `group.id`, lets the broker assign each a
   disjoint partition subset, and aggregates per-process snapshots by
   re-percentiling the union of their latency reservoirs. This is the
   structural twin of `load_mp` (same `spawn`-pool + aggregate pattern).
3. Adds a CLI driver (`scripts/run_event_consume_mp.py`) and `Makefile`
   targets (`consume-test`, `consume-test-mp`, `consume-test-report`) that
   write `docs/results/week1_consume_results_mp.md`.
4. Adds the **symmetric-ceiling demonstration** as a first-class integration
   test: a 1-member group cannot drain a 50k backlog (consumer lag and
   end-to-end latency ramp without bound); an *N*-member group can.
5. Adds full unit coverage on the accountant, planner, runner (mocked
   consumer), report renderer, and CLI.

### What "End-to-End Latency" Means Here (and What It Does Not)

End-to-end latency is measured as **consumer-receive wall-clock minus the
record's produce timestamp**, where the produce timestamp is the Kafka
message timestamp (`msg.timestamp()`, `CreateTime` — set by librdkafka at
`produce()`), with the event's own `event_timestamp` field used as a
cross-check. On the single-host dev box the producer and consumer share one
monotonic wall clock, so there is **no clock-skew term** — the number is the
true queueing + transport + deserialize delay. It is explicitly **not**:

- A consumer *throughput* benchmark in isolation (throughput is reported, but
  the headline is latency and lag).
- A feature-computation or Redis-write benchmark (Week 2 layers those on;
  gap-plan line 76 says the Week 1 consumer "only reads and benchmarks").
- An *exactly-once read* test — see §2.7; `read_committed` is wired as a
  config knob but is a no-op until the EOS PR ships a transactional producer.
- A distributed clock-skew study (single host → shared clock; the
  distributed caveat is documented in §2.2, not measured).

### Out of Scope (Deferred to Later PRs)

- **Feature computation + Redis writes** (Week 2). This PR consumes,
  deserializes, accounts, and commits offsets — nothing downstream.
- **Exactly-once read (`read_committed`) end-to-end.** The default producer
  profile is non-transactional (`acks=1`); `isolation.level` is a config
  flag here so the EOS PR flips one switch (mirrors how `--eos` was added
  producer-side — investigation doc §4.4).
- **Consume-process-produce EOS** (transactional offset commits via
  `send_offsets_to_transaction`) — final Week 1 EOS PR.
- **JVM / Kafka-Streams cross-language consumer** — future item (§9), demoted
  from PR #4 §9.6 per the supersession note above.
- **Schema-evolution-on-read drills** beyond what PR #3 covered (old reader
  schema vs new writer schema) — a §9 follow-up.

### Deliverables

- `src/streaming_feature_store/consume/__init__.py`
- `src/streaming_feature_store/consume/consume_runner.py` — `ConsumeRunner`
  (one consumer-group member) + `ConsumeRunConfig`.
- `src/streaming_feature_store/consume/accountant.py` —
  `ConsumeAccountant` (consumed / deserialize-failed counters, end-to-end
  latency reservoir, per-error-class histogram, lag probe) +
  `ConsumeSnapshot`.
- `src/streaming_feature_store/consume/report.py` — `ConsumeRunReport`
  Pydantic model + Markdown renderer.
- `src/streaming_feature_store/consume_mp/__init__.py`
- `src/streaming_feature_store/consume_mp/process_planner.py` — member-count
  planner (`min(partitions, cpu_budget)`; **1 consumer per process** — see
  §2.6).
- `src/streaming_feature_store/consume_mp/worker_entry.py` — top-level
  `spawn` child entry point.
- `src/streaming_feature_store/consume_mp/mp_runner.py` —
  `MultiprocessConsumeRunner` (parent orchestration + aggregation).
- `src/streaming_feature_store/consume_mp/aggregator.py` — merge per-process
  snapshots; re-percentile the union of latency reservoirs.
- `src/streaming_feature_store/consume_mp/report.py` —
  `MultiprocessConsumeConfig` / `MultiprocessConsumeReport` + renderer.
- `scripts/run_event_consume_mp.py` — CLI driver.
- `docs/results/week1_consume_results_mp.md` — generated artifact.
- `tests/unit/test_consume_accountant.py`,
  `tests/unit/test_consume_runner_unit.py`,
  `tests/unit/test_consume_process_planner.py`,
  `tests/unit/test_consume_report.py`,
  `tests/unit/test_consume_mp_aggregator.py`,
  `tests/unit/test_run_event_consume_mp_cli.py`.
- `tests/integration/test_consume_runner_end_to_end.py`,
  `tests/integration/test_consume_mp_end_to_end.py`.
- `Makefile` targets: `consume-test`, `consume-test-mp`,
  `consume-test-mp-quick`, `consume-test-report`.

---

## 2. Critical Design Decisions

### 2.1 Consumer Group of *Processes*, Not Threads (the headline; GIL symmetry)

**Decision:** The drain path is a Kafka **consumer group** whose members are
**separate OS processes** (one `AvroEventConsumer` per process), spawned via
`multiprocessing.get_context("spawn")` exactly as `load_mp` spawns producers.
Member count is resolved by a planner (§2.6); workers-*within*-a-process is
fixed at **1** (one poll loop per process).

**Rationale:**
- The investigation proved the per-process Python ceiling is workload-
  agnostic (~11–14k evt/s). Consumer-side work — `fastavro` decode +
  `avro_dict_to_event` Pydantic construction — is the same kind of
  GIL-held bytecode as producer-side encode, so a single-process consumer
  has the **same** ceiling and **cannot** drain a 50–60k producer. This is
  not a hypothesis; it is the symmetric corollary of investigation §2.2.
- The Kafka-idiomatic unit of consumer parallelism is the **partition**.
  Scaling out the group with more member processes (each owning a disjoint
  partition subset) gives more cores of Python decode in parallel — the
  consume-side mirror of the producer's `load_mp` escape.
- Spawning processes (not `fork`) avoids librdkafka background-thread
  duplication, identical to the `load_mp` rationale.

**Trade-off:** *N*× memory and IPC/aggregation complexity, and the consumer
count is hard-capped at the partition count (12) — beyond that, extra members
sit idle. Accepted: it is the only design that escapes the GIL, and the cap
is a true Kafka property worth demonstrating.

### 2.2 End-to-End Latency Definition and Clock Source

**Decision:** `e2e_latency = consumer_receive_wallclock − msg.timestamp()`,
where `msg.timestamp()` is the `CreateTime` librdkafka stamped at
`produce()`. The event's `event_timestamp` field (generation time) is
recorded as a secondary cross-check. Percentiles come from a fixed-size
(4096) reservoir sampler, reusing PR #4's accountant pattern.

**Rationale:**
- `CreateTime` is the closest in-band proxy for "when the producer sent
  it"; subtracting it from consumer wall-clock yields queueing + transport +
  deserialize delay — the number Week 2's `<100 ms` budget cares about.
- On the single-host dev box the producer and consumer share the same
  monotonic clock, so there is **zero clock-skew error** — the measurement is
  clean by construction here.
- A bounded reservoir gives O(1) memory p50/p95/p99, consistent with
  `DeliveryAccountant` (PR #4 §2.4), so the renderer/aggregator code is
  symmetric.

**Trade-off:** In a *distributed* deployment producer/consumer clocks differ;
`CreateTime` − consumer-clock then includes skew. Documented, not corrected
here (single host). The §9 list notes the production options (broker
`LogAppendTime`, NTP discipline, or one-sided OWD estimation).

### 2.3 Manual Offset Commit — At-Least-Once Read

**Decision:** Keep `AvroEventConsumer`'s `enable.auto.commit=False`. The
runner commits offsets **synchronously after each fully-processed poll
batch** (`commit(asynchronous=False)`), never before deserialize+account.

**Rationale:**
- Commit-after-process is the "success marker" semantics the gap plan calls
  out (line 76): on crash/rebalance the group resumes from the last
  committed offset, so no record is *missed* (at-least-once read).
- A latency benchmark tolerates the at-least-once duplicate-on-replay window
  (it only inflates `consumed` slightly after a forced rebalance, which the
  report notes).

**Trade-off:** At-least-once means duplicates after an unclean stop.
Exactly-once read needs `read_committed` **and** a transactional producer
(§2.7) — deferred. Accepted; correctness of the *count* is asserted modulo a
logged rebalance.

### 2.4 Symmetric Package Layout (`consume/` ⇄ `load/`, `consume_mp/` ⇄ `load_mp/`)

**Decision:** Mirror the producer harness one-for-one:
`consume/{consume_runner,accountant,report}.py` mirrors
`load/{load_runner,accountant,report}.py`; `consume_mp/{process_planner,
worker_entry,mp_runner,aggregator,report}.py` mirrors `load_mp/`.

**Rationale:**
- Maximum code-pattern reuse (the `spawn`-pool, the
  `model_dump`-across-the-pickling-boundary trick, the
  re-percentile-the-union aggregation) — all proven in `load_mp`.
- The structural symmetry *is* the portfolio story: produce and consume hit
  the same wall and take the same escape. Reviewers can diff the two trees.

**Trade-off:** Some duplicated scaffolding rather than a shared generic
"MP harness" abstraction. Accepted for now — the investigation doc explicitly
keeps the harnesses "side by side so either can be deleted without disturbing
the other"; a later refactor can extract the common `spawn`+aggregate core.

### 2.5 Lag-Aware Termination and the Latency-Ramp Signature

**Decision:** A run ends at `--duration-s` **or** when consumer lag reaches
≈0 (`--until-caught-up`). The accountant samples **consumer lag** =
`high_water_mark − position` per assigned partition each poll, and the report
records lag-over-time plus whether end-to-end latency was **flat** (drained
in steady state) or **ramping** (fell behind — the
`latency ≈ t·(1 − C/P)` signature from the investigation discussion).

**Rationale:**
- Lag is the consumer's primary health metric. A flat e2e-latency series
  means the group keeps up; a monotonically rising series is the
  textbook "consumer slower than producer" collapse.
- This makes the **symmetric-GIL demonstration** measurable: a 1-member
  group against a 50k-rate backlog shows lag and latency ramping; the
  *N*-member group shows both flat. That contrast is the deliverable's
  headline (and §6 asserts it).

**Trade-off:** Lag sampling adds a metadata call per poll cycle; negligible
(`get_watermark_offsets` is cached/cheap and called per batch, not per
record).

### 2.6 One Consumer Per Process — Workers-Per-Process is **1**, Not `round(1/s)`

**Decision:** Unlike the producer (where the optimum was `W ≈ round(1/s) =
2` workers/process), each consumer process runs **exactly one** poll loop /
one `Consumer`. Scale only by adding processes.

**Rationale:**
- `confluent_kafka.Consumer.poll()` is **not safe for concurrent calls** on
  one consumer instance — multi-threading one consumer is a correctness bug,
  not just a perf question.
- Even with a multi-consumer-per-process design, a second poll loop would
  only add a GIL contender: the single poll loop already alternates between
  a **GIL-free** wait (network `poll`) and **GIL-held** decode, so the GIL
  is already kept busy through the natural blocking gap by *one* thread. The
  producer needed `W=2` because its worker parked in *GIL-yielding waits*
  (pacer / in-flight backpressure) ~half the time; the consumer's poll loop
  has no such large idle GIL window. So `round(1/s)` → `s ≈ 1` → `W = 1`.
- The unit of consumer scaling is the partition anyway; member count is
  `min(num_partitions, cpu_budget)`. The planner mirrors
  `load_mp.process_planner` but with `workers_per_process` removed.

**Trade-off:** None material — this is strictly simpler than the producer.
The design-decision write-up itself is interview-grade (it shows the
`W ≈ round(1/s)` model applied a second time and *correctly yielding a
different answer*).

### 2.7 `isolation.level` as a Config Knob (Read-Side EOS, Deferred)

**Decision:** Expose `--isolation-level {read_uncommitted,read_committed}`,
defaulting to `read_uncommitted`. Plumb it into the `DeserializingConsumer`
config but treat `read_committed` as inert until the EOS PR.

**Rationale:**
- The default producer profile is non-transactional (`acks=1`), so there
  are no aborted/uncommitted records to filter — `read_committed` would be a
  no-op that only adds LSO-wait latency. Wiring the knob now means the EOS
  PR flips one flag (symmetric to the producer's `--eos` switch added in the
  investigation work).

**Trade-off:** A config surface with no behavioral effect yet. Accepted —
it documents the seam and keeps the EOS PR a one-line change.

### 2.8 Deserialize-to-Pydantic vs. Raw-Dict Fast Path

**Decision:** `--deserialize-mode {pydantic,raw}`. `pydantic` (default) runs
the full `avro_dict_to_event` correctness path; `raw` stops at the decoded
dict (skips Pydantic construction).

**Rationale:**
- Symmetric to PR #4's `generate_avro_dicts` bypass. Running both modes
  splits the per-event cost into **decode** vs **validation**, the
  consumer-side analog of the producer's serialization-CPU-share metric —
  and shows *which* half of the GIL-held work dominates the consume ceiling.

**Trade-off:** `raw` mode is not the production path (features need typed
events). It exists only as a measurement control; the report labels it.

### 2.9 Reproducible CLI + Makefile, Not a Notebook

**Decision:** A regular module (`scripts/run_event_consume_mp.py`) invoked by
`make consume-test-mp`, writing a diffable Markdown report. Mirrors PR #4
§2.7 verbatim in spirit.

**Rationale:** Same as PR #4 — diffable artifact in review, and `pytest`
calls `MultiprocessConsumeRunner.run()` directly (no subprocess).

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    scripts/run_event_consume_mp.py                        │
│                                                                           │
│  1. assert subject e-commerce-events-value registered (fail fast)         │
│  2. plan = plan_consume_processes(partitions=12, cpu_budget)              │
│     → members = min(12, cpu_budget)   (1 consumer per process)            │
│  3. MultiprocessConsumeRunner(group_id, members).run()                    │
│                                                                           │
│      spawn Pool(members) ──┬── worker 0 ─┐                                 │
│                            ├── worker 1 ─┤  all share group.id=G          │
│                            └── worker N ─┘  broker assigns disjoint        │
│                                             partition subsets             │
│   each worker process:                                                    │
│     AvroEventConsumer.subscribe([topic])      # group-managed assignment  │
│     while not deadline and lag>0:                                         │
│       msgs = consumer.poll_batch()            # GIL-free network wait     │
│       for m in msgs:                                                      │
│         ev  = avro_dict_to_event(decode(m))   # GIL-held decode+validate  │
│         acct.record(e2e = now - m.timestamp())                            │
│       consumer.commit(asynchronous=False)     # success marker            │
│       acct.sample_lag(high_water - position)                              │
│     return ConsumeOutcome(snapshot, latency_samples)                      │
│                                                                           │
│  4. aggregate_outcomes() → MultiprocessConsumeReport                       │
│       sum counters · max wallclock · re-percentile ∪ reservoirs · Σ lag   │
│  5. render → docs/results/week1_consume_results_mp.md                      │
└───────────────┬───────────────────────────────────────────────────────────┘
                │ reuses
        ┌───────┴──────────┬───────────────────────┐
        ▼                  ▼                       ▼
┌────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ AvroEventConsumer│ │ SchemaRegistry   │ │ Kafka brokers (PR #1)│
│ (PR #2)         │ │ (PR #2 wrapper)  │ │ e-commerce-events    │
│ decode+Pydantic │ │ reader schema    │ │ 12 partitions, RF=3  │
└────────────────┘ └──────────────────┘ └──────────────────────┘
```

Structurally identical to the `load_mp` diagram with produce↔consume
mirrored: a parent spawns *N* single-purpose processes, the broker (not the
app) shards work across them, and the parent re-percentiles the union of
per-process latency reservoirs.

---

## 4. Detailed Implementation

### 4.1 Directory Structure Additions

```
streaming-feature-store/
├── scripts/
│   └── run_event_consume_mp.py
├── src/
│   └── streaming_feature_store/
│       ├── consume/
│       │   ├── __init__.py
│       │   ├── accountant.py
│       │   ├── consume_runner.py
│       │   └── report.py
│       └── consume_mp/
│           ├── __init__.py
│           ├── aggregator.py
│           ├── mp_runner.py
│           ├── process_planner.py
│           ├── report.py
│           └── worker_entry.py
├── docs/
│   └── results/
│       └── week1_consume_results_mp.md      # generated artifact
└── tests/
    ├── unit/
    │   ├── test_consume_accountant.py
    │   ├── test_consume_mp_aggregator.py
    │   ├── test_consume_process_planner.py
    │   ├── test_consume_report.py
    │   ├── test_consume_runner_unit.py
    │   └── test_run_event_consume_mp_cli.py
    └── integration/
        ├── test_consume_mp_end_to_end.py
        └── test_consume_runner_end_to_end.py
```

### 4.2 `consume/consume_runner.py` — `ConsumeRunner`

```python
class ConsumeRunConfig(BaseModel):
    duration_s: float
    group_id: str
    topic: str = "e-commerce-events"
    poll_timeout_s: float = 1.0
    max_batch: int = 1024
    until_caught_up: bool = False
    isolation_level: str = "read_uncommitted"   # see §2.7
    deserialize_mode: str = "pydantic"          # or "raw" (§2.8)

class ConsumeRunner:
    def __init__(self, kafka_config, registry_config,
                 run_config: ConsumeRunConfig, *,
                 consumer: AvroEventConsumer | None = None,
                 accountant: ConsumeAccountant | None = None): ...
    def run(self) -> ConsumeRunReport: ...
```

`run()` orchestration (one group member):

1. Fail-fast assert `"<topic>-value"` is registered (reuse PR #2
   `SchemaRegistry.get_latest`; same guard as `LoadRunner`).
2. Build/inject `AvroEventConsumer` (passes `group.id`,
   `isolation.level`, `enable.auto.commit=False`).
3. `subscribe([topic])` — group-managed assignment (broker hands this
   member its partition subset).
4. Loop until `deadline` (or lag≈0 if `until_caught_up`):
   poll a batch → for each msg: `now - msg.timestamp()` →
   `accountant.record(...)`; in `pydantic` mode also run
   `avro_dict_to_event` → on failure `accountant.record_deserialize_error`.
5. `commit(asynchronous=False)` once per processed batch.
6. `accountant.sample_lag(...)` from `get_watermark_offsets` vs `position`.
7. On exit: `consumer.close()` (triggers a clean group rebalance),
   assemble `ConsumeRunReport`.

### 4.3 `consume/accountant.py` — `ConsumeAccountant`

```python
class ConsumeAccountant:
    def record(self, *, e2e_latency_s: float) -> None: ...
    def record_deserialize_error(self, err_class: str) -> None: ...
    def sample_lag(self, lag: int) -> None: ...
    def snapshot(self) -> ConsumeSnapshot: ...
```

`ConsumeSnapshot` (Pydantic, `frozen=True`): `consumed`,
`deserialize_failed`, `errors_by_class`, `e2e_p50/p95/p99_ms`,
`max_lag`, `end_lag`, `lag_ramped: bool`, `wallclock_s`. Reservoir size 4096
(matches `DeliveryAccountant`). `lag_ramped` is set when the linear fit of
the lag series has a significantly positive slope (the "falling behind"
signature, §2.5).

### 4.4 `consume/report.py`

`ConsumeRunReport(BaseModel)` = `config + started_at + snapshot +
sustained_consume_eps + verdict`. Pure-function `render_markdown(report)`
(template strings, no Jinja — diffable, mirrors PR #4 §4.7).

### 4.5 `consume_mp/process_planner.py`

```python
def plan_consume_processes(*, partitions: int, cpu_budget: int,
                           requested: int | None) -> ConsumePlan: ...
# members = requested or min(partitions, cpu_budget)
# workers_per_process is intentionally absent — always 1 (§2.6)
```

Mirrors `load_mp.process_planner.plan_processes` minus the
`workers_per_process` axis; `rationale` string records the binding term
(`partition_cap` vs `cpu_budget`) for the report, exactly like `load_mp`.

### 4.6 `consume_mp/{worker_entry,mp_runner,aggregator,report}.py`

- `worker_entry.run_consume_worker(args: WorkerProcessArgs)` — top-level
  (so `spawn` can import it); rebuilds configs from `model_dump` dicts,
  runs a `ConsumeRunner`, returns `ConsumeOutcome(snapshot,
  e2e_samples, per_partition_lag)`. One-for-one with
  `load_mp.worker_entry`.
- `mp_runner.MultiprocessConsumeRunner` — asserts the subject once
  (shared), spawns `ctx.Pool(members)`, `pool.map`, aggregates. All members
  pass the **same `group.id`** so the broker performs the partition split.
- `aggregator.aggregate_outcomes` — Σ counters, `max` wallclock,
  re-percentile the **union** of e2e reservoirs, Σ lag (identical technique
  to `load_mp.aggregator`).
- `report.MultiprocessConsumeConfig/Report` + `render_markdown` — adds a
  **"Consumer profile"** row (`isolation.level`, `deserialize_mode`) so the
  artifact self-documents, exactly as the load report self-documents the
  producer profile.

### 4.7 `scripts/run_event_consume_mp.py` — Driver

```
usage: run_event_consume_mp.py [--duration-s S] [--members N]
       [--group-id G] [--topic T] [--until-caught-up]
       [--isolation-level {read_uncommitted,read_committed}]
       [--deserialize-mode {pydantic,raw}]
       [--report-path PATH] [--floor-eps EPS]
```

Logic: assert subject registered → `plan_consume_processes(...)` →
`MultiprocessConsumeRunner(...).run()` → `render_markdown` →
`docs/results/week1_consume_results_mp.md` → exit `0` if drained without a
ramping-lag verdict (and ≥ `--floor-eps` if set), else `1`. Same shape as
`run_event_load_mp.py`.

### 4.8 `docs/results/week1_consume_results_mp.md` — Output Artifact

```markdown
# Multi-Process Consumer Group — End-to-End Latency Results

**Generated:** <ISO-8601>
**Topic:** e-commerce-events   **Group:** wk1-consume

## Configuration
| Field | Value |
|---|---|
| Members (processes) | 6 |
| Workers per process | 1 |
| Isolation level | read_uncommitted |
| Deserialize mode | pydantic |
| Duration | 10.0 s |

## Aggregate results
| Metric | Value |
|---|---|
| Consumed | 612_300 |
| Deserialize failed | 0 |
| Sustained consume rate | 60_900 evt/s |
| End-to-end p50 / p95 / p99 | 7.1 / 22.4 / 38.9 ms |
| Max lag / End lag | 18_220 / 0 |
| Lag ramped? | No (steady-state drain) |

## Per-process breakdown
| # | Partitions | Consumed | e2e p99 ms | End lag |
|---|---|---|---|---|

## Verdict
✅ Group drained at producer rate; end-to-end latency flat.
```

### 4.9 `Makefile` Additions

```makefile
consume-test:            ## 1-member consumer (control: shows the GIL ceiling)
	uv run python scripts/run_event_consume_mp.py --duration-s 10 --members 1

consume-test-mp:         ## N-member consumer group (drains the producer)
	uv run python scripts/run_event_consume_mp.py --duration-s 10

consume-test-mp-quick:   ## Smoke: 2s, 1 member, no verdict
	uv run python scripts/run_event_consume_mp.py --duration-s 2 --members 1 \
	  --report-path /tmp/_consume_quick.md

consume-test-report:     ## Open the generated report
	@xdg-open docs/results/week1_consume_results_mp.md 2>/dev/null \
	  || echo "Report at docs/results/week1_consume_results_mp.md"
```

`consume-test` (1 member) vs `consume-test-mp` (planned *N*) are deliberately
the two halves of the symmetric-ceiling demo, mirroring how `make
load-test` (single-process threading) and `make load-test-mp` frame the
producer story.

---

## 5. Unit Tests

Run without Docker. Mock `AvroEventConsumer` / broker metadata.

### 5.1 `tests/unit/test_consume_accountant.py`

| Test | Assertion |
|---|---|
| `test_record_increments_consumed` | `record(e2e=…)` → `consumed += 1` |
| `test_deserialize_error_classified` | `record_deserialize_error("ValueError")` → `errors_by_class["ValueError"]==1`, `deserialize_failed==1` |
| `test_latency_reservoir_bounded` | 100K records → reservoir size == 4096 |
| `test_latency_percentiles_monotonic` | `p50 ≤ p95 ≤ p99` |
| `test_lag_ramped_true_on_rising_series` | strictly increasing lag samples → `lag_ramped is True` |
| `test_lag_ramped_false_on_flat_series` | flat/zero lag samples → `lag_ramped is False` |
| `test_snapshot_is_frozen` | mutating returned `ConsumeSnapshot` raises |
| `test_thread_safety_under_contention` | 8 threads × 10K `record` → counters exact |

### 5.2 `tests/unit/test_consume_runner_unit.py`

Mocks `AvroEventConsumer`, `SchemaRegistry`.

| Test | Assertion |
|---|---|
| `test_runner_aborts_when_subject_missing` | registry raises → `consumer.poll` never called |
| `test_runner_subscribes_to_topic` | `subscribe([topic])` called once before first poll |
| `test_runner_commits_after_each_batch` | `commit(asynchronous=False)` called once per processed batch, after `record` |
| `test_runner_never_commits_before_processing` | injected processing error → no `commit` for that batch |
| `test_runner_records_e2e_from_msg_timestamp` | e2e == `recv_clock - msg.timestamp()` (fake clock) |
| `test_runner_raw_mode_skips_pydantic` | `deserialize_mode="raw"` → `avro_dict_to_event` not called |
| `test_runner_until_caught_up_exits_on_zero_lag` | lag→0 ends the loop before `duration_s` |
| `test_runner_closes_consumer_on_exit` | `consumer.close()` called exactly once (clean rebalance) |
| `test_runner_returns_consume_report` | returns `ConsumeRunReport` with populated snapshot |

### 5.3 `tests/unit/test_consume_process_planner.py`

| Test | Assertion |
|---|---|
| `test_members_capped_by_partitions` | `partitions=12, cpu_budget=32` → 12, rationale=`partition_cap` |
| `test_members_capped_by_cpu_budget` | `partitions=12, cpu_budget=4` → 4, rationale=`cpu_budget` |
| `test_requested_overrides_plan` | `requested=3` → 3 |
| `test_requested_above_partitions_rejected` | `requested=20, partitions=12` → `ValueError` |
| `test_no_workers_per_process_axis` | plan object has no `workers_per_process` attribute (§2.6 invariant) |

### 5.4 `tests/unit/test_consume_report.py`

| Test | Assertion |
|---|---|
| `test_render_includes_config_and_profile_rows` | output has Members, Isolation level, Deserialize mode |
| `test_render_flat_latency_passes` | `lag_ramped=False` → ✅ verdict |
| `test_render_ramped_latency_fails` | `lag_ramped=True` → ❌ verdict + "fell behind" note |
| `test_render_lists_deserialize_errors` | `errors_by_class={"ValueError":3}` present |
| `test_config_rejects_bad_isolation_level` | `isolation_level="weird"` → `ValidationError` |

### 5.5 `tests/unit/test_consume_mp_aggregator.py`

| Test | Assertion |
|---|---|
| `test_counters_summed` | 6 outcomes → `consumed` is the sum |
| `test_wallclock_is_max_not_sum` | aggregate `wallclock_s == max` child |
| `test_latency_repercentiled_from_union` | aggregate p99 == percentile of concatenated reservoirs, not mean-of-p99 |
| `test_lag_summed_across_partitions` | aggregate `end_lag == Σ` per-process end lag |
| `test_lag_ramped_true_if_any_member_ramped` | one ramping member → aggregate `lag_ramped True` |

### 5.6 `tests/unit/test_run_event_consume_mp_cli.py`

Imports the non-package script via `importlib` (mirrors
`test_run_event_load_mp_cli.py`).

| Test | Assertion |
|---|---|
| `test_defaults` | `--members` default None, `isolation_level` default `read_uncommitted`, `deserialize_mode` default `pydantic` |
| `test_isolation_flag_parsed` | `--isolation-level read_committed` → namespace value |
| `test_members_one_parsed` | `--members 1` → 1 (the control-case path) |
| `test_invalid_deserialize_mode_rejected` | `--deserialize-mode bogus` → argparse error (exit 2) |

---

## 6. Integration Tests

Require PR #1 infra, PR #2 schemas, and a data source on the topic. Tests
use a **per-test topic** (`e-commerce-events-consumetest-<uuid4>`) and seed
it via the PR #4 load runner (small smoke produce) so consume tests are
self-contained.

### 6.1 Prerequisites / Fixtures

- Reuse PR #1 `docker_services_up`, PR #2 `registered_*_schema`, PR #4
  `TopicAdmin` per-test topic creation + a `seed_topic(n, rate)` helper that
  runs a short `load_mp` produce into the per-test topic.
- `@pytest.mark.integration`, `-p no:xdist` (project convention).

### 6.2 `tests/integration/test_consume_runner_end_to_end.py`

| Test | What It Verifies |
|---|---|
| `test_single_member_smoke_consumes_and_commits` | seed 5K, 1 member → `consumed==5000`, `deserialize_failed==0`, offsets committed |
| `test_e2e_latency_is_sane` | seed at 5K/s → e2e p99 < 250 ms (healthy laptop) |
| `test_until_caught_up_terminates` | seed fixed N, `until_caught_up` → run ends with `end_lag==0` before `duration_s` |
| `test_offsets_resume_after_restart` | consume half, kill, restart same `group.id` → resumes from committed offset, no gap |
| `test_raw_mode_decode_only` | `deserialize_mode="raw"` → `consumed==N`, `avro_dict_to_event` uninvoked (spy) |
| `test_fails_fast_on_missing_subject` | fresh topic, unregistered `-value` → `run()` raises before poll |

### 6.3 `tests/integration/test_consume_mp_end_to_end.py`

| Test | What It Verifies |
|---|---|
| `test_group_splits_partitions_across_members` | 6 members on 12 partitions → each member's assignment disjoint, union == all 12 |
| **`test_single_member_cannot_drain_50k_backlog`** | seed a 50K-evt backlog; **1 member** → lag **ramps**, `lag_ramped is True` (the symmetric-GIL proof — the headline) |
| **`test_member_group_drains_50k_backlog`** | same backlog; **N members** (planned) → lag returns to 0, `lag_ramped is False`, sustained ≥ producer rate |
| `test_aggregate_latency_is_union_percentile` | aggregate p99 equals percentile over the merged reservoir, within ε |
| `test_rebalance_on_member_crash_no_loss` | kill one member mid-run → survivors pick up its partitions; total `consumed` ≥ seeded (at-least-once) |
| `test_report_file_written_and_self_documents_profile` | report exists, contains Members / Isolation / Deserialize rows |

The paired `test_single_member_cannot_drain…` /
`test_member_group_drains…` is the consumer-side equivalent of PR #4's
50K-floor benchmark and the investigation's single-vs-multi-process
contrast. Tagged `@pytest.mark.benchmark`; excluded from default
`make test-integration`, run via `make test-benchmark`.

---

## 7. How to Run

> Prereqs: `make infra-up` → `make infra-status` → `make register-schemas`.
> Generate something to consume first (PR #4): `make load-test-mp` (or seed a
> per-test topic in integration fixtures).

```bash
source .venv/bin/activate

# Control case — 1 member: demonstrates the single-process GIL ceiling.
make consume-test
#   INFO  Subject e-commerce-events-value: registered (id=1, v=1)
#   INFO  ConsumeRunner member=0 partitions={0..11} group=wk1-consume
#   WARN  lag ramping: 18_220 → 41_900 → 68_400  (single process can't keep up)
#   INFO  ❌ Fell behind: lag ramped (single-process GIL ceiling)

# Escape — planned N-member group: drains at producer rate.
make consume-test-mp
#   INFO  ConsumePlan: members=min(12, cpu_budget)=6 (binding=partition_cap)
#   INFO  ✅ Group drained; e2e p50/p95/p99 = 7.1 / 22.4 / 38.9 ms; end lag 0

make consume-test-report      # open docs/results/week1_consume_results_mp.md
make test-unit                # no Docker
make test-integration         # needs infra; per-test topics
make test-benchmark           # the 1-vs-N 50K backlog contrast
```

---

## 8. Resource Budget & Constraints

| Item | Incremental cost | Notes |
|---|---|---|
| New Python modules | ~9 files, ~700 SLoC | mirrors `consume/` ⇄ `load/`, `consume_mp/` ⇄ `load_mp/` |
| Memory per member process | ~40–90 MB | one interpreter + librdkafka consumer + 4096-sample reservoir |
| Members (default) | `min(12, cpu_budget)` | hard-capped at partition count = 12 |
| Topic storage | none added | consumes existing data; no produce |
| Test runtime (unit) | < 4 s | no Docker |
| Test runtime (integration, default) | < 90 s | per-test topics, small seeds |
| Test runtime (`make test-benchmark`) | < 40 s | 1-vs-N 50K backlog contrast |

### 8.1 Acceptance Criteria

For `test_member_group_drains_50k_backlog` (the headline) to pass:

- ≥ 4 logical CPUs, ≥ 8 GB RAM (PR #1 CI runner config).
- Planned *N*-member group reaches `end_lag == 0` within `duration_s`.
- `lag_ramped is False` for the group; **`lag_ramped is True` for the
  1-member control** (the contrast is the deliverable — a non-ramping
  control would mean the test is not exercising the ceiling).
- `deserialize_failed == 0`; aggregate e2e p99 ≤ 250 ms sanity ceiling.

`LOADTEST_*`-style env overrides (`CONSUMETEST_*`) document how to relax
thresholds on weaker machines, mirroring PR #4 §8.1.

---

## 9. Future Considerations

1. **Read-side EOS (`read_committed`).** Once the EOS PR ships a
   transactional producer, default `--isolation-level read_committed` and add
   an integration test that aborted-transaction records are filtered (LSO
   semantics). The knob (§2.7) already exists so this is a one-line flip,
   symmetric to the producer's `--eos`.
2. **Consume-process-produce EOS.** Replace manual `commit` with
   `send_offsets_to_transaction` inside the producer transaction so the
   read→feature→write cycle (Week 2) is exactly-once end to end.
3. **JVM / Kafka-Streams cross-language consumer.** The PR #4 §9.6
   speculation: a JVM consumer (no GIL) as a contrast point — quantify the
   single-JVM-process vs single-Python-process drain gap, the cross-language
   counterpart to this PR's intra-Python multi-process result.
4. **Schema-evolution on read.** Pin an *old* reader schema while the writer
   emits a PR #3 evolved schema; assert the consumer still deserializes
   (BACKWARD compat) and record the column-default behavior — closes the
   read-side of the PR #3 story.
5. **Distributed clock-skew handling.** When producer/consumer are on
   different hosts, add a broker-`LogAppendTime` mode and/or an NTP-offset
   correction term to the e2e metric (§2.2 caveat).
6. **Cooperative-sticky rebalance tuning.** Measure stop-the-world vs
   incremental rebalance pause during the member-crash test; relevant to
   Week 5 freshness-SLA monitoring.
7. **Backpressure into Week 2.** When feature computation + Redis writes are
   added, the consume loop gains a downstream-bound cost; the
   `ConsumeAccountant` should split e2e into transport vs
   decode vs downstream so Week 2's `<100 ms` budget is attributable.

---

## 10. Open Questions

1. **Group-managed assignment vs. explicit `assign()` for the benchmark?**
   Group-managed is realistic (demonstrates rebalances) but makes per-member
   partition sets non-deterministic across runs.
   **Recommendation:** default group-managed; add `--static-assignment` for
   deterministic per-partition latency attribution when debugging skew.
2. **Should the verdict fail on `lag_ramped` or on an absolute e2e p99
   ceiling?** Ramp detection is robust to host speed; an absolute ceiling
   flakes on weak CI.
   **Recommendation:** verdict on `lag_ramped` (relative, host-independent);
   record absolute p99 as informational — same philosophy as the
   investigation's "ratio not absolute" stance.
3. **Commit cadence: per-batch or every K batches?** Per-batch is the safest
   success-marker but adds a synchronous round-trip per ~1024 records.
   **Recommendation:** per-batch for Week 1 (correctness clarity); revisit
   with async commit + periodic flush if commit RTT shows up in the e2e
   tail.
4. **Should `consume-test` (1 member) be in CI or benchmark-only?** It is
   *expected to fail its drain verdict by design* (the control case),
   exactly like the threading harness's 50K test fails by design
   (investigation §4.3).
   **Recommendation:** benchmark-only, asserted as a *positive* test that
   `lag_ramped is True` — i.e., CI verifies the ceiling still exists, not
   that it is absent.
5. **One reader schema version pinned, or always-latest?** PR #2's consumer
   uses the registry reader schema.
   **Recommendation:** always-latest for Week 1; the pinned-old-reader drill
   is the §9.4 schema-evolution-on-read follow-up.
```
