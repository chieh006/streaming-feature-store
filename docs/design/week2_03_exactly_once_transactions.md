# Design Doc: Exactly-Once Semantics — Transactional Consume-Process-Produce

**Phase:** 1 — Real-Time Feature Store & Streaming Pipeline
**Week:** 2 — Validation & Feature Computation
**Scope:** The Week 2 **Exactly-once semantics (EOS) — transactions layer**
bullet of [`gap_project_plan.md`](gap_project_plan.md) (lines 84–86). Now that
the pipeline has a real **consume → process → produce** cycle (the validator of
[`week2_01`](week2_01_validation_layer_and_dlq.md) and the sliding-window
consumer of
[`week2_02`](week2_02_sliding_window_features_plain_consumer.md)), wrap that
cycle in a **transactional producer** so the input-offset commit and the
Kafka-side feature/route writes are **atomic across Kafka topics + consumer
offsets**. Shipping this also flips the consumer default `--isolation-level` to
`read_committed`. The idempotent-producer foundation is already shipped
(Week 1's `eos` profile — [`week1_04`](week1_04_synthetic_event_producer.md),
[`config.py`](../../src/streaming_feature_store/config.py) `ProducerTuning.enable_idempotence`);
only the **transactional wrapping** is new.
**Supersedes / supersedes-by:** none — strictly additive on top of PR #1 / PR #2.
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
11. [Top 5 Concepts Worth Understanding (Interview Prep)](#11-top-5-concepts-worth-understanding-interview-prep)

---

## 1. Overview

By the end of Week 2 PR #2 the pipeline runs two **consume-process-produce**
daemons, both built on the same `consume → process → produce → flush → commit`
loop with `enable.auto.commit=false`:

- **Validator** ([`validate/runner.py`](../../src/streaming_feature_store/validate/runner.py))
  — consumes `e-commerce-events-feed`, routes each message to
  `validated-events` (on `Valid`) or `dead-letter-queue` (on `Invalid`).
- **Sliding-window consumer**
  ([`sliding/consumer.py`](../../src/streaming_feature_store/sliding/consumer.py))
  — consumes `validated-events`, computes windowed features, produces
  `sliding-features` (+ `sliding-features-late`) and writes Redis.

Both are **at-least-once today**: the produce is flushed *before* the offset is
committed, so a crash in the gap replays the batch and emits duplicates that
downstream absorbs via idempotency keys ([`week2_01`](week2_01_validation_layer_and_dlq.md)
§2.7, [`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.7). That
is correct but not *exactly-once*: a `read_uncommitted` consumer **sees the
duplicates**.

This PR closes the gap on the **Kafka side** of the cycle. It wraps the
produce + offset-commit in a Kafka transaction so that, from the point of view
of a `read_committed` consumer, **each input record produces its output records
exactly once** — duplicates from a replayed batch belong to an *aborted*
transaction and are filtered below the broker's Last Stable Offset (LSO).

### What ships

- **`eos/` package** — a transactional producer wrapper
  (`TransactionalAvroProducer`) exposing
  `init_transactions()` / `begin_transaction()` /
  `send_offsets_to_transaction()` / `commit_transaction()` /
  `abort_transaction()`, plus a **`CommitStrategy`** seam so the existing
  validator and sliding runners switch between *at-least-once* and
  *transactional* commit with a one-line construction change (§2.2, §4.3).
- **`transactional.id` derivation** — a stable, unique-per-process id
  (§2.3), threaded from the CLI/Make layer into each consumer-group member.
- **Default `--isolation-level` flip to `read_committed`** on every downstream
  consumer (validator's source is upstream of the txn, so its own default is
  unchanged; the *sliding* consumer, the Week 1 latency/ sink consumers, and
  the Week 3 serving reads now default to `read_committed`) — §2.5.
- **Single multi-topic transactional producer per process** (§2.4) — because a
  Kafka transaction is **producer-scoped**, the validator's two output topics
  (`validated-events` + `dead-letter-queue`) and the sliding consumer's two
  (`sliding-features` + `sliding-features-late`) must each be produced by **one**
  transactional producer instance, replacing the current two-producer
  arrangement when `--eos` is on.
- `docs/results/week2_eos_results.md` — generated smoke-run report (txn
  commit/abort counts, commit-marker rate, p50/p95/p99 with vs without EOS, a
  crash-replay exactly-once audit read at `read_committed`).
- Unit + integration tests (§5, §6) and `Makefile` targets (§7).

### What this PR does **not** do (the two load-bearing caveats)

Both caveats are quoted verbatim from the plan's EOS bullet
([`gap_project_plan.md`](gap_project_plan.md) lines 85–86) and are the spine of
the design:

1. **`transactional.id` is per-process.** A transactional producer needs a
   *stable, unique* `transactional.id`. The multi-process consumer-group design
   ([`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.11)
   therefore means **N transactional ids and N independent per-process
   transaction scopes**, *not* one shared distributed transaction. §2.3 designs
   the id; §2.8 designs zombie fencing around it.

2. **Kafka transactions ≠ cross-store atomicity.** A Kafka transaction spans
   *only* Kafka topics + consumer offsets. It does **not** make the external
   **Redis** and **PostgreSQL** writes atomic with the Kafka commit. Those stay
   on the **idempotent-write** contract already shipped (Redis latest-wins —
   [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.8/§2.9;
   Postgres `ON CONFLICT DO NOTHING` —
   [`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.2/§2.7),
   which carries forward unchanged. True cross-store atomicity needs an
   **outbox** pattern, designed-but-deferred in §2.6 / §9.

### Out of scope (deferred)

- **The Postgres sink's contract.** The sink
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md)) is *not*
  wrapped: its "produce" is a Postgres `INSERT`, which a Kafka transaction
  cannot span (§2.6). It remains at-least-once-read + idempotent-write
  **forever** — this PR does not touch it.
- **The load/benchmark producers** (`load_mp`, the burst harness). They have no
  *consume* side, so there is nothing to make atomic; their idempotent-`eos`
  profile (Week 1) is already the right and only EOS they need.
- **The outbox pattern** for genuine Kafka↔Redis↔Postgres atomicity (§9.1).
- **Per-input-partition `transactional.id`** (the Kafka-Streams task model),
  which would make EOS survive *dynamic* rebalances; we pin to static
  membership instead (§2.3, §10.1).

### Deliverables

- `src/streaming_feature_store/eos/__init__.py` — package init.
- `src/streaming_feature_store/eos/transactional_producer.py` —
  `TransactionalAvroProducer` (multi-topic, txn API) + `TransactionalConfig`.
- `src/streaming_feature_store/eos/transactional_id.py` —
  `derive_transactional_id()` and validation.
- `src/streaming_feature_store/eos/commit_strategy.py` —
  `CommitStrategy` protocol, `AtLeastOnceCommit`, `TransactionalCommit`.
- Surgical edits to `validate/runner.py` and `sliding/consumer.py`: their
  `_flush_and_commit` delegates to the injected `CommitStrategy`.
- A `transactional_id` / `enable_idempotence` field on `ProducerTuning`
  ([`config.py`](../../src/streaming_feature_store/config.py)) and
  `consumer_group_metadata()` / `position()` accessors on
  [`AvroEventConsumer`](../../src/streaming_feature_store/consumer/avro_consumer.py).
- CLI `--eos` / `--transactional-id` flags on `run_validator.py`,
  `run_validator_mp.py`, `run_sliding_features_consumer.py`.
- `Makefile` targets `validator-run-eos`, `sliding-run-eos`, `eos-report`,
  `eos-verify` (read at `read_committed`).
- `docs/results/week2_eos_results.md` (generated).

---

## 2. Critical Design Decisions

### 2.1 Wrap the Consume-Process-Produce Cycles, Not the Sink or the Loader

**Decision:** Apply transactional EOS to exactly the two loops that *read* from
Kafka **and** *write back* to Kafka — the validator and the sliding-window
consumer. Leave the Postgres sink and the load/benchmark producers untouched.

**Rationale:**

- **A Kafka transaction is only meaningful for a read-process-write cycle.** The
  unit of atomicity it offers is "{these produced records} **and** {these input
  offsets} either all commit or all abort." That shape exactly matches the
  validator and the sliding consumer. It does **not** match the load harness
  (write-only — no offsets to bind) or the sink (its write is to Postgres, which
  is outside Kafka's transaction boundary — §2.6).
- **The foundation is already paid for.** Both target loops already run
  `enable.auto.commit=false` with the produce-before-commit ordering
  ([`week2_01`](week2_01_validation_layer_and_dlq.md) §2.7). Transactional
  wrapping is a *strict additive layer* on that ordering — replace the manual
  `flush(); consumer.commit()` with `send_offsets_to_transaction();
  commit_transaction()` — not a rewrite.
- **The narrative is symmetric with Week 1.** Week 1 measured the idempotent
  *producer* tax; this PR completes the picture by adding the *transactional*
  half and measuring its incremental cost (§2.9), closing the EOS story the
  Phase 5 system-design problem "1B events/day" needs
  ([`gap_project_plan.md`](gap_project_plan.md) line 351).

### 2.2 A `CommitStrategy` Seam (At-Least-Once vs Transactional)

**Decision:** Introduce a `CommitStrategy` protocol with two implementations.
The runner's `_flush_and_commit` calls `strategy.commit(producer, consumer,
offsets)`; everything else in the loop is unchanged.

```python
class CommitStrategy(Protocol):
    def begin(self) -> None: ...
    def commit(self, *, consumer: AvroEventConsumer) -> None: ...
    def abort(self) -> None: ...

class AtLeastOnceCommit:        # today's behavior, the default
    def begin(self): pass
    def commit(self, *, consumer):
        self._producer.flush(self._timeout_s)   # produce ack'd first…
        consumer.commit()                         # …then offsets (§2.7 w1_06)
    def abort(self): pass

class TransactionalCommit:       # new, --eos
    def begin(self):
        self._producer.begin_transaction()
    def commit(self, *, consumer):
        self._producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata(),
        )
        self._producer.commit_transaction(self._timeout_s)
    def abort(self):
        self._producer.abort_transaction(self._timeout_s)
```

**Rationale:**

- **One loop, two contracts, zero duplication.** The validator and the sliding
  consumer keep their single loop. Injecting the strategy at construction is the
  minimal, reviewable change and keeps the at-least-once path — which the sink
  also depends on conceptually — first-class and tested, not bit-rotted.
- **`begin` at the top of each batch, `commit`/`abort` at the bottom.** The
  strategy owns the transaction lifecycle so the runner never learns the txn
  vocabulary. `AtLeastOnceCommit.begin/abort` are no-ops, so the loop body reads
  identically in both modes.
- **Single Responsibility (project guideline §1).** The transaction mechanics
  live in `eos/`, isolated from validation logic and windowing logic.

### 2.3 `transactional.id` — Stable, Unique, Per-Process (Caveat #1)

**Decision:** Each consumer-group member process gets its **own**
`transactional.id` of the form `f"{group_id}-{member_ordinal}"` (e.g.
`sliding-features-job-0 … sliding-features-job-11`), pinned to a **static
member identity** via librdkafka `group.instance.id` (static group membership)
so that the id ⇄ partition-subset mapping is stable across restarts. `init_transactions()`
runs once at startup, before `subscribe()`.

**Rationale:**

- **Two non-negotiable properties, in tension.** A `transactional.id` must be
  **stable across restarts** (so the broker recognizes a restarted process as
  the *same* producer and fences its zombie predecessor — the whole point of
  transactional EOS) **and unique across live processes** (two live producers
  sharing an id will fence each other in a loop). `f"{group_id}-{ordinal}"`
  with a CLI/env-pinned ordinal satisfies both: deterministic per process,
  disjoint across the group.
- **This is *N* transactions, not one.** Per the plan's caveat #1, there is **no
  shared distributed transaction** across the group — each process commits only
  *its own* assigned partitions' work. That is correct and sufficient because
  each input partition is owned by exactly one member
  ([`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.2), so no
  two transactions ever cover the same input record.
- **Static membership keeps the id honest under restart.** With dynamic
  membership, a bounce reshuffles partition assignments and a process's
  `transactional.id` could end up bound to *different* input partitions than the
  offsets it fences — a correctness hazard. `group.instance.id` (static
  membership) makes a restarted member reclaim the *same* partitions, so its
  fixed `transactional.id` stays matched to its offsets. The rebalance-proof
  alternative (one `transactional.id` **per input partition**, à la Kafka
  Streams) is heavier (recreate producers on every assignment) and deferred —
  §10.1.
- **`init_transactions()` is once, at startup.** It registers the id with the
  transaction coordinator, fences any prior epoch, and recovers/aborts a
  dangling transaction from a previous crash. It must precede the first
  `begin_transaction()` and the first `poll()`.

### 2.4 One Multi-Topic Transactional Producer Per Process

**Decision:** Each process uses a **single** `TransactionalAvroProducer` that
can produce to **all** of its cycle's output topics. The validator's
`validated-events` + `dead-letter-queue`, and the sliding consumer's
`sliding-features` + `sliding-features-late`, are written through one producer
instance. The current two-producer arrangements (separate
`AvroEventProducer` + `DlqProducer`; separate feature + late sinks) **collapse
to one producer** when `--eos` is on.

**Rationale:**

- **A transaction is producer-scoped — you cannot commit two producers
  atomically.** `begin/commit_transaction()` operate on a *single* producer
  instance with a single `transactional.id`. If `validated-events` were written
  by producer A and `dead-letter-queue` by producer B, a crash could commit A
  and abort B — exactly the partial-write the transaction is meant to prevent.
  Atomicity across both topics **requires** one producer.
- **Per-topic serialization stays correct.** The transactional producer holds a
  small **serializer registry keyed by topic** (`EcommerceEvent`-Avro for
  `validated-events`, `DlqRecord`-Avro for `dead-letter-queue`,
  `SlidingFeatureRecord`-Avro for `sliding-features`, raw-event-Avro for the
  late topic). `produce(topic, key, value)` selects the serializer by topic, so
  one producer multiplexes heterogeneous value schemas without losing
  registry-backed Avro encoding.
- **Keying is preserved.** Each call keeps its existing key (validated:
  `user_id`; dlq: `(topic,partition,offset)`; features: `user_id:resolution`),
  so partitioning and downstream per-user locality are unchanged.

### 2.5 Flip the Default `--isolation-level` to `read_committed`

**Decision:** With a transactional producer in the pipeline, change the
**default** `isolation.level` of consumers *downstream of a transaction* from
`read_uncommitted` to `read_committed`. The wired-but-inert knob from Week 1
([`avro_consumer.py`](../../src/streaming_feature_store/consumer/avro_consumer.py)
§ docstring; [`gap_project_plan.md`](gap_project_plan.md) line 73) becomes
**functional**.

**Rationale:**

- **`read_committed` is what makes EOS observable.** It tells the consumer to
  deliver only records **below the LSO** (Last Stable Offset) — i.e. records
  whose transaction has committed — and to **filter out aborted** records
  entirely. Without it, a downstream reader still sees the duplicate emissions
  from a replayed/aborted batch, and the whole transactional apparatus is
  invisible. The flip is therefore part of *shipping* EOS, not a follow-up.
- **Which consumers flip:** the sliding consumer (reads the validator's
  transactional `validated-events`), the Week 3 serving reads, the Week 1
  latency/sink consumers when pointed at a transactional topic, and any
  `eos-verify` reader. The **validator's own source** is `e-commerce-events-feed`
  (the non-transactional background feeder), so its consumer default is left at
  `read_uncommitted` — there is nothing transactional upstream of it to filter.
- **Cost: consumer-side latency, not throughput.** `read_committed` holds
  records back until their transaction commits, so end-to-end latency now
  includes the producer's transaction-commit cadence (§2.7). This is the
  consumer-side half of the EOS tax measured in §2.9.

### 2.6 Kafka Transactions ≠ Cross-Store Atomicity (Caveat #2)

**Decision:** The Redis and Postgres writes stay **outside** the Kafka
transaction and rely on **idempotent writes**, unchanged from PR #2 / Week 1.
The transaction covers only `{Kafka output records} + {input offsets}`.

**Rationale:**

- **The boundary is a hard property of Kafka, not a choice.** A Kafka
  transaction's atoms are Kafka partitions (topic-partitions + the
  `__consumer_offsets` partition). Redis and Postgres are different systems with
  their own commit protocols; no Kafka API can enlist them. Pretending otherwise
  is the classic distributed-transactions trap.
- **Idempotent writes give "effectively-once" per store.** Redis `HSET` is
  latest-wins and TTL-bounded
  ([`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.8/§2.9);
  Postgres is `ON CONFLICT (event_id) DO NOTHING`
  ([`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md) §2.2). A
  replayed batch re-applies the *same* keyed write → no corruption. This is the
  same contract that already absorbs at-least-once replay; transactions do not
  change it.
- **Ordering within the cycle, made explicit.** For the sliding consumer the
  Redis write happens **after** `commit_transaction()` succeeds (so Redis never
  reflects an aborted feature value). The window is then: *Kafka committed but
  process dies before the Redis write* → Redis is briefly stale → next
  emission for that user overwrites it (latest-wins) → convergent. The inverse
  order (Redis before commit) would surface aborted features in the online
  store, which is worse; hence commit-then-Redis.
- **The "proper" fix is an outbox, and it is deferred.** True Kafka↔Redis↔
  Postgres atomicity needs an outbox/idempotent-consumer pattern (write the
  intent into a transactional Kafka topic, a separate idempotent applier mirrors
  it to Redis/Postgres). Designed in §9.1; out of scope here because idempotent
  writes already meet the project's correctness bar at laptop scale.

### 2.7 Transaction Boundary = One Poll-Batch

**Decision:** One transaction per consumed **poll-batch** (the existing
`poll_max_records`-bounded batch), not per message and not per fixed wall-clock
interval. `begin_transaction()` opens when a non-empty batch arrives;
`commit_transaction()` closes after the batch's produces + `send_offsets`.

**Rationale:**

- **Per-message transactions are pathological.** Every commit writes transaction
  **markers** to each touched partition and an offset record; doing that
  per-message multiplies broker write amplification and collapses throughput.
  Batching amortizes the marker cost over the whole batch.
- **The batch is already the unit of work.** PR #1/#2 already flush + commit per
  batch; reusing that boundary means the transaction scope equals the
  at-least-once scope, so the only change is *how* the boundary commits.
- **The tradeoff is latency vs marker overhead.** A larger batch → fewer commit
  markers → better throughput, but higher worst-case latency (records wait for
  the batch's commit before a `read_committed` consumer sees them) and a larger
  replay on abort. `poll_max_records` / `poll_timeout_s` remain the tuning
  knobs; defaults (500 / 1.0 s) are inherited and revisited against the §8
  budget.

### 2.8 Fencing, Aborts, and Error Classification

**Decision:** Classify every transactional API error via
`KafkaError.txn_requires_abort()` / `.retriable()` / `.fatal()` and act
accordingly: retriable → retry the call; abortable → `abort_transaction()` then
re-process the batch; fatal/fenced → log and **exit the process** (let the
supervisor restart it, which re-runs `init_transactions()` and fences the
zombie).

**Rationale:**

- **Zombie fencing is the safety property we are buying.** If a process stalls
  (GC pause, SIGSTOP) past `transaction.timeout.ms` and a restarted instance
  with the same `transactional.id` takes over, the coordinator bumps the
  producer **epoch**; the zombie's next `commit_transaction()` fails fenced.
  Treating fenced as fatal-and-exit is correct: the new epoch owns the work.
- **Abort must re-process, not skip.** On an abortable error the produced
  records are discarded by the broker and offsets are *not* advanced, so the
  batch is re-consumed on the next loop — at-least-once delivery preserved,
  exactly-once *observation* preserved (the aborted copy is filtered by
  `read_committed`).
- **Stateless vs stateful abort handling differ.** The *validator* is stateless,
  so it re-consumes the aborted batch **in-process** (`abort → continue`). The
  *sliding consumer* is **stateful** — its panes already folded the batch's
  events before the produce/commit — so re-folding the same batch in-process
  would double-count. It therefore treats an abort as a **mini-restart**:
  `abort_transaction()` then re-raise so the process exits and the §2.10
  cold-start warm-up rebuilds pane state from the (un-advanced) committed
  offsets. Aborts are rare, so the warm-up cost is acceptable; full
  rollback-able state is the "exactly-once with large state" trigger that
  §2.1 reserves for Flink.
- **`transaction.timeout.ms` must exceed the batch processing time.** Set it
  above the worst-case `poll → process → produce` span (well under the broker's
  `transaction.max.timeout.ms`), so a slow-but-healthy batch is not mistaken for
  a zombie. Recorded as a tuning constraint in §8.

### 2.9 EOS Cost — Recalibrating the Week 1 Tax

**Decision:** Treat the transactional layer's cost as the **incremental** tax on
top of the already-measured idempotent-producer tax, and measure it in the
generated report (§7) rather than asserting a number up front.

**Rationale:**

- **Week 1 already quantified the idempotent half.** The `eos` profile costs a
  *conserved* tax: ~15% throughput when capacity-bound, or ~2.5–3× p95/p99 tail
  latency when rate-paced
  ([`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md)
  §4.4 / §4.4.1; [`gap_project_plan.md`](gap_project_plan.md) line 68/130). This
  PR adds two *new* costs on top: (a) transaction commit **markers** per batch
  (producer + broker), and (b) `read_committed` **consumer-side latency** (§2.5).
- **At feeder scale the tax is latency, not throughput.** The pipeline runs at
  ~200 evt/s, far below any GIL/throughput ceiling, so EOS here manifests as
  **added tail latency** (commit-marker round-trips + LSO wait), exactly the
  rate-paced regime Week 1 identified. The §7 report measures p50/p95/p99 with
  and without `--eos` to make that concrete.
- **It recalibrates the Week 2 <100 ms budget.** The
  [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §8 note that
  `acks=all` already shifts the producer tail upward compounds here with the
  commit-marker + `read_committed` wait; the report documents the new end-to-end
  distribution.

### 2.10 Reuse, Not Replace, the At-Least-Once Contract

**Decision:** The idempotent-write contracts on the **non-Kafka** stores are
unchanged and remain the system's correctness floor even with EOS on. The
transactional layer is purely *additive* to the Kafka side.

**Rationale:**

- **Defense in depth.** Even with perfect Kafka EOS, the Redis/Postgres writes
  can still be replayed (the §2.6 commit-then-Redis window). Keeping them
  idempotent means EOS-on and EOS-off behave identically at those stores — no
  store's correctness *depends* on transactions being enabled, so `--eos`
  stays a safe, reversible flag.
- **The sink is the proof.** [`week1_06`](week1_06_postgres_sink_and_continuous_feeder.md)
  §2.7 explicitly states the sink "remains idempotent-insert + manual-commit
  forever, because Kafka transactions cannot span Postgres anyway." This PR
  honors that — it never reaches into the sink.

---

## 3. Architecture

### 3.1 Transactional Consume-Process-Produce Loop (one process)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  TransactionalRunner  (one per process; consumer group of N ≤ 12)              │
│  transactional.id = f"{group_id}-{ordinal}"   group.instance.id pinned (§2.3)  │
│                                                                                │
│  init_transactions()   ← once, before subscribe()  (fences prior epoch)        │
│        │                                                                       │
│        ▼                                                                       │
│   ┌────────────── loop, per non-empty poll-batch (§2.7) ────────────────────┐ │
│   │  msgs = consumer.poll_batch()        (read_committed upstream filtered)  │ │
│   │  strategy.begin()  → producer.begin_transaction()                        │ │
│   │  for msg in msgs:                                                        │ │
│   │      out = process(msg)              (validate │ window-compute)         │ │
│   │      producer.produce(out.topic, out.key, out.value)   (multi-topic §2.4)│ │
│   │  strategy.commit(consumer):                                             │ │
│   │      producer.send_offsets_to_transaction(positions, group_metadata)    │ │
│   │      producer.commit_transaction()   ← atomic: {records}+{offsets}       │ │
│   │  # only AFTER commit succeeds:                                           │ │
│   │      redis_sink.write(out)           (idempotent, OUTSIDE txn §2.6)      │ │
│   └─────────────────────────────────────────────────────────────────────────┘ │
│   on abortable error → strategy.abort() → re-process batch  (§2.8)            │
│   on fenced/fatal    → log + exit(nonzero) → supervisor restart (§2.8)        │
└──────────────────────────────────────────────────────────────────────────────┘
                                   │ commits transaction markers + offsets
                                   ▼
        validated-events / sliding-features  (records below LSO only)
                                   │
                                   ▼
        downstream consumer  isolation.level = read_committed  (§2.5)
```

### 3.2 Where EOS sits in the end-to-end pipeline

```
feeder ─▶ e-commerce-events-feed ─▶ VALIDATOR  (txn: validated + dlq + offsets)
   (non-txn upstream;                    │  transactional.id = validator-feed-{k}
    validator reads                      ▼
    read_uncommitted)            validated-events  ──▶ SLIDING CONSUMER
                                 (read_committed)        (txn: sliding-features +
                                                          late + offsets;
                                                          Redis idempotent, outside)
                                                              │
                                              ┌───────────────┴───────────────┐
                                              ▼                               ▼
                                   sliding-features (read_committed)   Redis feat:user:*
                                              │                        (idempotent §2.6)
                                              ▼
                                   Week 4 offline/online consistency
```

The Postgres **sink** still hangs off `e-commerce-events-feed` (or
`validated-events`) on the **at-least-once** contract — deliberately *not* in any
transaction (§2.1, §2.6).

### 3.3 Module Layout

```
src/streaming_feature_store/eos/
├── __init__.py
├── transactional_producer.py   # TransactionalAvroProducer (multi-topic, txn API)
│                               # + TransactionalConfig (Pydantic)
├── transactional_id.py         # derive_transactional_id(group_id, ordinal)
└── commit_strategy.py          # CommitStrategy, AtLeastOnceCommit, TransactionalCommit

# edited, not new:
src/streaming_feature_store/config.py                 # ProducerTuning.transactional_id
src/streaming_feature_store/consumer/avro_consumer.py # + consumer_group_metadata(),
│                                                     #   position(); default flip
src/streaming_feature_store/validate/runner.py        # inject CommitStrategy
src/streaming_feature_store/sliding/consumer.py       # inject CommitStrategy

scripts/run_validator.py, run_validator_mp.py, run_sliding_features_consumer.py
                                                      # --eos / --transactional-id

docs/results/
└── week2_eos_results.md         # generated
```

### 3.4 Config Model

`TransactionalConfig` is the per-process EOS knob bag (Pydantic,
`extra="forbid"`), constructed from CLI/env and handed to the producer wrapper:

```python
class TransactionalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False                     # the --eos master switch
    transactional_id: str | None = None       # required when enabled (§2.3)
    group_instance_id: str | None = None       # static membership (§2.3)
    transaction_timeout_ms: int = 60_000       # > worst-case batch span (§2.8)
    commit_timeout_s: float = 30.0             # commit_transaction() budget
    output_topics: tuple[str, ...] = ()        # validated by the producer (§2.4)

    @model_validator(mode="after")
    def _id_required_when_enabled(self) -> "TransactionalConfig":
        if self.enabled and not self.transactional_id:
            raise ValueError("transactional_id is required when EOS is enabled")
        return self
```

---

## 4. Detailed Implementation

### 4.1 `transactional.id` Derivation

```python
def derive_transactional_id(group_id: str, ordinal: int) -> str:
    """Build a stable, unique-per-process transactional.id (§2.3).

    Parameters
    ----------
    group_id : str
        The Kafka consumer group id (e.g. ``"sliding-features-job"``).
    ordinal : int
        This process's fixed member ordinal in ``[0, num_workers)``.

    Returns
    -------
    str
        ``f"{group_id}-{ordinal}"`` — deterministic per process, disjoint
        across the group, stable across restarts.

    Raises
    ------
    ValueError
        If *ordinal* is negative.
    """
    if ordinal < 0:
        raise ValueError(f"ordinal must be >= 0, got {ordinal}")
    return f"{group_id}-{ordinal}"
```

The ordinal is supplied by the MP supervisor (the same place
[`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §10.4 assigns
`--num-workers` indices) and pinned to `group.instance.id` so a restarted member
reclaims the same partitions.

### 4.2 Transactional Producer Wrapper

```python
class TransactionalAvroProducer:
    """Single multi-topic producer that owns a Kafka transaction (§2.4).

    Holds one ``SerializingProducer`` with ``transactional.id`` set and a
    per-topic Avro serializer registry, so heterogeneous value schemas
    (validated events, DLQ records, feature records) commit atomically.
    """

    def __init__(self, kafka_config, registry_config,
                 txn_config: TransactionalConfig,
                 serializers: dict[str, AvroSerializer]) -> None:
        conf = {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "enable.idempotence": True,                       # required for txns
            "acks": "all",
            "transactional.id": txn_config.transactional_id,
            "transaction.timeout.ms": txn_config.transaction_timeout_ms,
            "key.serializer": StringSerializer("utf_8"),
        }
        if txn_config.group_instance_id:                       # static membership
            conf["group.instance.id"] = txn_config.group_instance_id
        self._producer = SerializingProducer(conf)             # value ser per-topic
        self._serializers = serializers
        self._initialised = False

    def init_transactions(self, timeout_s: float = 30.0) -> None:
        """Register the txn id, fence prior epoch, recover dangling txn (§2.3)."""
        self._producer.init_transactions(timeout_s)
        self._initialised = True

    def begin_transaction(self) -> None:
        self._producer.begin_transaction()

    def produce(self, topic: str, key: str, value: object) -> None:
        """Serialize *value* with the topic's registered Avro serializer (§2.4)."""
        serializer = self._serializers[topic]
        self._producer.produce(topic=topic, key=key,
                               value=serializer(value, _ctx(topic)))

    def send_offsets_to_transaction(self, offsets, group_metadata) -> None:
        self._producer.send_offsets_to_transaction(offsets, group_metadata)

    def commit_transaction(self, timeout_s: float = 30.0) -> None:
        self._producer.commit_transaction(timeout_s)

    def abort_transaction(self, timeout_s: float = 30.0) -> None:
        self._producer.abort_transaction(timeout_s)
```

> **Note.** The real `produce` passes a `to_dict`-style serializer; the sketch
> elides the `SerializationContext`. The wrapper is intentionally thin — each
> method delegates one librdkafka transaction call (project guideline §1,
> single responsibility), mirroring the structure of the existing
> [`AvroEventProducer`](../../src/streaming_feature_store/producer/avro_producer.py).

### 4.3 Runner Integration (validator shown; sliding is identical in shape)

The only edits to [`runner.py`](../../src/streaming_feature_store/validate/runner.py)
are: construct with a `CommitStrategy`, call `strategy.begin()` before the batch
loop, and replace `_flush_and_commit` with `strategy.commit(consumer=...)`.

```python
def run(self) -> ValidatorRunReport:
    self._strategy.init()                 # init_transactions() if transactional
    self._consumer.subscribe()
    try:
        while not self._shutdown.is_set():
            messages = self._consumer.poll_batch(cfg.poll_timeout_s,
                                                 cfg.poll_max_records)
            if not messages:
                continue
            self._strategy.begin()        # begin_transaction() | no-op
            try:
                for msg in messages:
                    self._handle_msg(msg) # produces via the txn producer
                self._strategy.commit(consumer=self._consumer)
            except KafkaException as exc:
                if exc.args[0].txn_requires_abort():
                    self._strategy.abort()        # re-consume next loop (§2.8)
                    continue
                raise                              # fenced/fatal → exit (§2.8)
    finally:
        self._consumer.close()
        self._producer.close()
```

`AtLeastOnceCommit` makes `init/begin/abort` no-ops and `commit` the existing
`flush(); consumer.commit()` — so the default path is byte-for-byte today's
behavior, and the `except` branch is simply never exercised (no
`txn_requires_abort` without a transaction).

### 4.4 Consumer Accessors + Default Flip

[`AvroEventConsumer`](../../src/streaming_feature_store/consumer/avro_consumer.py)
gains two thin passthroughs needed by `send_offsets_to_transaction`:

```python
def consumer_group_metadata(self):
    """Opaque group metadata required to bind offsets into a txn (§2.4)."""
    return self._consumer.consumer_group_metadata()

def position(self, partitions):
    """Current consume positions for the assigned partitions."""
    return self._consumer.position(partitions)
```

and its `isolation_level` default changes `"read_uncommitted" →
"read_committed"` (§2.5). The CLI keeps `--isolation-level` so a benchmark run
can opt back to `read_uncommitted` explicitly.

### 4.5 Avro Schemas

Unchanged. EOS adds **no new schema** — it transports the *same*
`validated-events` / `dead-letter-queue` / `sliding-features` records, only
inside a transaction. The per-topic serializer registry (§4.2) reuses the
already-registered subjects.

---

## 5. Unit Tests

All `pytest`, pure-Python; the Kafka producer/consumer are faked so the
transaction *protocol* is asserted without a broker.

| Test | Assertion |
|---|---|
| `test_txn_config_requires_id_when_enabled` | `enabled=True, transactional_id=None` → `ValidationError` |
| `test_txn_config_allows_no_id_when_disabled` | `enabled=False` → valid, id may be `None` |
| `test_derive_txn_id_format` | `("sliding-features-job", 3)` → `"sliding-features-job-3"` |
| `test_derive_txn_id_unique_across_ordinals` | ordinals `0..11` → 12 distinct ids |
| `test_derive_txn_id_rejects_negative_ordinal` | `ordinal=-1` → `ValueError` |
| `test_derive_txn_id_stable_for_same_inputs` | same `(group, ordinal)` twice → identical id (restart stability) |
| `test_transactional_commit_calls_in_order` | fake producer records call order: `begin → produce* → send_offsets → commit` |
| `test_transactional_commit_sends_group_metadata_and_positions` | `send_offsets_to_transaction` receives `consumer.position()` + `consumer_group_metadata()` |
| `test_transactional_abort_on_abortable_error` | producer raises `txn_requires_abort()=True` → `abort_transaction()` called, no commit |
| `test_fenced_error_is_reraised_not_aborted` | error with `txn_requires_abort()=False, fatal()=True` → re-raised (process exits) |
| `test_at_least_once_commit_flushes_then_commits` | `AtLeastOnceCommit.commit` → `producer.flush()` precedes `consumer.commit()` |
| `test_at_least_once_begin_abort_are_noops` | `begin()/abort()/init()` make no producer calls |
| `test_multi_topic_producer_selects_serializer_by_topic` | produce to validated vs dlq → correct serializer invoked per topic |
| `test_multi_topic_producer_unknown_topic_raises` | produce to an unregistered topic → `KeyError`/`ValueError` |
| `test_multi_topic_producer_preserves_keys` | validated keyed `user_id`; dlq keyed `topic:partition:offset` |
| `test_init_transactions_called_once_before_subscribe` | runner with transactional strategy → `init_transactions` precedes `subscribe` |
| `test_consumer_default_isolation_is_read_committed` | `AvroEventConsumer(...)` default → `isolation.level=read_committed` |
| `test_consumer_isolation_override_still_honored` | explicit `read_uncommitted` → passed through |
| `test_redis_write_happens_after_commit` | sliding strategy: Redis `write` not called until `commit_transaction` returns (§2.6) |
| `test_redis_write_skipped_when_transaction_aborts` | aborted batch → no Redis write for that batch |
| `test_empty_batch_opens_no_transaction` | `poll_batch` returns `[]` → `begin_transaction` never called (§2.7) |

Coverage target: **100% line + branch** for `src/streaming_feature_store/eos/`
and the edited `_flush_and_commit` / `run` branches.

## 6. Integration Tests

Real 3-broker Kafka (via `make infra-up`); marked `@pytest.mark.integration`,
skipped when `docker compose ps` reports no running services. EOS needs RF=3
and the `__transaction_state` topic, which the dev cluster already provides.

| Test | Setup → Assertion |
|---|---|
| `test_eos_validator_exactly_once_under_replay` | run validator `--eos`; kill between `commit_transaction` ack and process exit; restart → a `read_committed` reader of `validated-events` sees **each input event once** (no duplicate `event_id`) |
| `test_aborted_batch_invisible_to_read_committed` | force an abort mid-batch (inject a process error after `produce`, before commit) → `read_committed` reader sees **zero** records from that batch; `read_uncommitted` reader sees them (proves filtering) |
| `test_offsets_committed_atomically_with_output` | after `--eos` run, `__consumer_offsets` position == count of records on `validated-events` (no offset ahead of output) |
| `test_validated_and_dlq_commit_atomically` | a batch with both valid + invalid events → both topics advance together; abort → **neither** advances (§2.4) |
| `test_fencing_zombie_producer` | two processes with the **same** `transactional.id` → the second's `init_transactions` fences the first; first's next `commit_transaction` fails fenced (§2.8) |
| `test_mp_group_each_member_own_txn_id` | 4-worker `--eos` group → 4 distinct `transactional.id`s active; no member fences another; all partitions covered |
| `test_read_committed_default_on_downstream` | sliding consumer started without `--isolation-level` → its consumer reports `read_committed` (§2.5) |
| `test_sliding_kafka_eos_redis_idempotent` | sliding `--eos`: `sliding-features` exactly-once at `read_committed`; Redis values converge after a forced replay (§2.6) |
| `test_eos_end_to_end_with_validator_and_sliding` | feeder → validator `--eos` → sliding `--eos`, 5 min → feature counts at `read_committed` match the feeder log exactly (no duplicate-inflated counts) |
| `test_eos_tax_latency_recorded` | `--eos` vs default run → report shows higher p95/p99, equal correctness (§2.9) |
| `test_transaction_timeout_below_broker_max` | `transaction.timeout.ms` > batch span and < broker `transaction.max.timeout.ms` → no spurious fencing on a slow batch |

## 7. How to Run

### 7.1 Bootstrap (EOS needs the transaction-state topic, already present)

```
make infra-up                  # 3-broker Kafka + Postgres + Registry + Redis
make topic-ensure              # e-commerce-events, -feed, validated-events
make register-schemas-feed     # feeder subject; validated/sliding/dlq self-register
```

### 7.2 Run the pipeline with EOS on

```
make feeder-run                       # ~200 evt/s non-txn feeder (upstream)
make validator-run-eos                # single-proc validator with --eos (1 txn scope: validated+dlq)
# — OR, to demonstrate the multi-member EOS group (each member its own
#   transactional.id → N txn scopes, §2.3), run this INSTEAD of the line above.
#   SOURCE=feed points it at the feeder's topic (default is the bench topic):
make validator-run-mp EOS=1 N=4 SOURCE=feed
make sliding-run-eos                  # sliding consumer with --eos (txn: features)
```

### 7.3 Verify exactly-once (read at the LSO)

```
make eos-verify                       # console-consume read_committed, dedupe-check
# equivalently, raw:
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic validated-events \
  --from-beginning --timeout-ms 5000 \
  --isolation-level read_committed \
  --property print.key=true --property key.separator=' | '
```

### 7.4 CLI

```
python scripts/run_validator.py \
    --eos \
    --transactional-id validator-feed-0 \
    --group-instance-id validator-feed-0 \
    --transaction-timeout-ms 60000

python scripts/run_sliding_features_consumer.py \
    --eos \
    --transactional-id sliding-features-job-0 \
    --group-instance-id sliding-features-job-0 \
    --isolation-level read_committed \
    --redis-host localhost
```

### 7.5 Report / tear down

```
make eos-report                # open docs/results/week2_eos_results.md
make infra-down
```

## 8. Resource Budget & Constraints

| Component | Added cost under EOS |
|---|---|
| Transactional producer (×N≤12) | 2 transaction markers per touched partition per **batch** commit; one `__consumer_offsets` write per commit |
| Broker | `__transaction_state` writes per `init/begin/commit`; marker fan-out across produced partitions |
| `read_committed` consumer | buffers records until their txn commits → adds the commit-cadence to end-to-end latency (§2.5) |

Constraints:

- **`transaction.timeout.ms` > worst-case batch span, < broker
  `transaction.max.timeout.ms`** (default 15 min). Default 60 s comfortably
  brackets a 500-record batch at feeder rate (§2.8).
- **EOS tax is latency-shaped here, not throughput-shaped.** At ~200 evt/s the
  pipeline is far below the GIL/throughput ceiling, so the cost is added
  p95/p99 latency (commit markers + LSO wait), the rate-paced regime of
  [`week1_load_test_throughput_investigation.md`](../results/week1_load_test_throughput_investigation.md)
  §4.4.1 — measured, not assumed (§2.9).
- **Static membership recommended.** `group.instance.id` avoids a rebalance
  storm on rolling restart and keeps `transactional.id` ⇄ partition stable
  (§2.3); without it, EOS still works but a bounce risks the id/partition
  mismatch noted in §10.1.
- **RF ≥ 3 / `min.insync.replicas ≥ 2`** for the transactional topics — already
  the dev-cluster default; EOS durability assumes it.

## 9. Future Considerations

1. **Outbox pattern for true cross-store atomicity.** Write the feature *intent*
   into the transactional `sliding-features` topic only (Kafka-atomic), then a
   separate **idempotent applier** mirrors committed records to Redis/Postgres.
   This removes the §2.6 commit-then-Redis staleness window entirely. Deferred
   because idempotent writes already meet the correctness bar at this scale, and
   the applier is a second daemon to operate.
2. **Per-input-partition `transactional.id` (Kafka-Streams task model).** Bind
   one `transactional.id` to each *input partition* rather than to the process,
   recreating producers on assignment. This makes EOS survive **dynamic**
   rebalances (no static-membership requirement) at the cost of more producers
   and assignment-time churn. The §2.3 static-membership choice is the simpler
   laptop-scale answer; this is the production answer.
3. **`transactional.id` registry / lease.** At higher worker counts, hand out
   ordinals from a coordinator (or derive from a stable pod ordinal under K8s in
   Phase 4) instead of a CLI flag, eliminating the manual-uniqueness hazard.
4. **EOS for the offline path.** The Week 4 DuckDB recompute reads
   `read_committed`, so its online/offline divergence study can now *exclude*
   duplicate-emission as a source — recorded for the Week 4 consistency report.
5. **Exactly-once with large state → Flink.** If per-user windowed state ever
   exceeds process memory, the "exactly-once with large state" trigger from
   [`week2_02`](week2_02_sliding_window_features_plain_consumer.md) §2.1 fires
   and Flink's checkpointed EOS becomes the right tool. This PR is the
   laptop-scale equivalent; the migration trigger is documented, not taken.

## 10. Open Questions

1. **Static membership vs. per-partition `transactional.id` under rebalance.**
   The §2.3 design pins `transactional.id` to a static member ordinal, correct
   only while assignments are stable. A genuine dynamic rebalance (member added
   mid-run) can leave a process's fixed id bound to *different* partitions than
   its committed offsets. **Now wired:** every EOS consumer (validator single +
   MP, sliding single + MP) sets `group.instance.id` =
   `derive_transactional_id(group, ordinal)`, so a restart within
   `session.timeout.ms` reclaims the same partitions without a rebalance —
   keeping the id ⇄ partition mapping stable across restarts. The remaining
   open question is *mid-run membership changes* (a new member joining): is
   static membership sufficient for the demo, or is the per-partition model
   (§9.2) worth the extra producers now? Leaning: static membership for Week 2;
   document the production answer.
2. **Transaction batch size.** Larger batches amortize commit-marker cost but
   raise `read_committed` latency and abort-replay size (§2.7). Tune
   `poll_max_records` against the §8 latency budget in the smoke run — keep 500,
   or drop it for tighter tails?
3. **Should the validator's two topics really share one transaction?** §2.4 says
   yes (atomic valid+dlq routing). But a poison message that *aborts* the batch
   then re-routes its sibling valid events on replay — is the re-emission +
   `read_committed` filtering fully transparent, or worth a per-topic split with
   weaker atomicity? Settle at code-review with the abort integration test.
4. **`commit_transaction` timeout vs. `max.poll.interval.ms`.** A long commit
   must not let the consumer miss its poll deadline and trigger a rebalance.
   Confirm `commit_timeout_s` < `max.poll.interval.ms` headroom, or move the
   commit off the poll thread.
5. **Sink under EOS upstream.** With `validated-events` now transactional, should
   a Postgres sink pointed at it read `read_committed` (cleaner offline dataset)
   even though its own write stays at-least-once? Likely yes — note for the
   Week 4 batch source choice.

## 11. Top 5 Concepts Worth Understanding (Interview Prep)

These are the five load-bearing ideas in this PR — the ones an interviewer
probing "you said you built exactly-once, explain it" will dig into. Each is
tied to the concrete code that implements it, with the question it tends to
unlock.

### 11.1 The atomic unit is `{output records} + {input offsets}`, not just the writes

This is the single most important idea, and the one most candidates get wrong.
Exactly-once on a consume-process-produce loop is **not** "produce idempotently."
It is: *the output records and the consumer offset that says "I read these
inputs" commit or abort together, as one Kafka transaction.* The offset advance
is bound into the transaction via
[`send_offsets_to_transaction`](../../src/streaming_feature_store/sliding/consumer.py#L393-L396)
— which writes the offsets to the internal `__consumer_offsets` topic *as part
of the producer's transaction*, not via a separate `consumer.commit()`. So a
crash can never leave outputs committed but offsets behind (duplicate on replay)
or offsets committed but outputs missing (data loss). The ordering in
[`_commit_batch_txn`](../../src/streaming_feature_store/sliding/consumer.py#L363-L405)
is the whole pattern: `begin → produce(all topics) → send_offsets → commit`.

**Mental model in one line:**

> read (non-atomic) → process → **[ produce outputs + write source-offset to
> `__consumer_offsets` ]** atomic via one producer transaction → only then Redis
> (idempotent, outside).

The read/`poll()` is *not* in the transaction — it just fetches and moves the
in-memory cursor. What is atomic is the **produce + the source-offset commit**:
both are partition writes (`sliding-features` / `sliding-features-late` and the
`__consumer_offsets` partition for the group), so the *same* transaction
coordinator commits them together with the same commit markers. The offset is
only advanced if the produce committed — abort leaves it untouched and the batch
re-reads from the last committed source offset.

- **Likely question:** *"How is consume-process-produce exactly-once different
  from an idempotent producer?"* Answer: idempotence dedupes retries of a single
  producer session; it says nothing about the consumer offset. EOS makes the
  offset commit part of the same atomic write as the output, closing the
  read-process-write loop.

### 11.2 `transactional.id`, producer epoch, and zombie fencing

A transactional producer needs a `transactional.id` that is **stable across
restarts** (so the coordinator recognizes a restarted process as the *same*
producer) yet **unique across live processes** (two live producers sharing an id
fence each other in a loop). `f"{group_id}-{ordinal}"` —
[`derive_transactional_id`](../../src/streaming_feature_store/eos/transactional_id.py#L18-L52)
— satisfies both. At startup
[`init_transactions()`](../../src/streaming_feature_store/sliding/consumer.py#L416)
registers the id with the transaction coordinator, **bumps the producer epoch**,
and aborts any dangling transaction from a crashed predecessor. If a stalled
"zombie" (GC pause, SIGSTOP past `transaction.timeout.ms`) later tries to commit,
its now-stale epoch is **fenced** and the commit fails. Note the load-bearing
caveat: with N workers this is **N independent transactions, not one distributed
transaction** — correct because each input partition is owned by exactly one
member, so no two transactions ever cover the same record. Static membership
(`group.instance.id`) keeps the id ⇄ partition mapping stable across restarts.

- **Likely question:** *"Two instances of the same job are running — how does
  Kafka prevent the dead one from corrupting state?"* Answer: producer epoch
  fencing, established by `init_transactions`.

### 11.3 EOS is invisible without `read_committed` (the LSO half)

The producer side is only half the system. A transaction's outputs are
unobservable as "exactly once" unless the *downstream consumer* reads with
[`isolation.level=read_committed`](../../src/streaming_feature_store/sliding/consumer.py#L225).
That tells the broker to deliver only records **below the Last Stable Offset
(LSO)** — records whose transaction has committed — and to **filter out aborted
records entirely**. With `read_uncommitted`, a reader still sees the duplicate
emissions from a replayed/aborted batch and the whole transactional apparatus is
pointless. That is why this PR *flips the default* to `read_committed` (§2.5):
shipping EOS means shipping both halves. The cost is **consumer-side latency**,
not throughput — records are held back until their transaction commits, so
end-to-end latency now includes the commit cadence.

- **Likely question:** *"You enabled transactions but still see duplicates
  downstream — why?"* Answer: the reader is `read_uncommitted`; aborted/replayed
  records below the LSO are only filtered for a `read_committed` consumer.

### 11.4 Kafka transactions ≠ cross-store atomicity (the Redis/Postgres boundary)

A Kafka transaction's atoms are **Kafka partitions** (topic-partitions + the
`__consumer_offsets` partition). It **cannot** enlist Redis or Postgres — no
Kafka API spans them. So those writes stay **outside** the transaction on an
**idempotent-write** contract (Redis latest-wins `HSET`, Postgres `ON CONFLICT
DO NOTHING`). The concrete consequence is the **commit-then-Redis ordering** in
[`_poll_batch_eos`](../../src/streaming_feature_store/sliding/consumer.py#L356-L361):
Redis is written *only after* `commit_transaction()` returns, so the online
store never reflects an *aborted* feature value. The residual window — Kafka
committed but the process dies before the Redis write — leaves Redis briefly
stale and is reconciled by the next latest-wins overwrite. The "proper" fix
(genuine Kafka↔Redis↔Postgres atomicity) is the **outbox pattern**, designed but
deferred (§9.1).

- **Likely question:** *"Does your exactly-once cover the feature you write to
  Redis?"* Answer: no — Kafka transactions stop at Kafka's boundary; Redis is
  effectively-once via idempotent latest-wins writes, ordered after the commit.

### 11.5 Stateful abort = mini-restart, not in-process retry

This is the subtle, codebase-specific deep cut that separates a memorized answer
from real understanding. On an *abortable* error
([`requires_abort`](../../src/streaming_feature_store/eos/commit_strategy.py#L27-L53)
checks the native `KafkaError.txn_requires_abort()`), a **stateless** loop (the
validator) can simply `abort_transaction()` and re-consume the batch in-process.
But the **stateful** sliding consumer has *already folded the batch's events into
its in-memory panes* before the produce/commit. Re-folding the same batch
in-process would **double-count**. So it treats an abort as a **mini-restart**:
abort, then **re-raise so the process exits**
([`_commit_batch_txn`](../../src/streaming_feature_store/sliding/consumer.py#L398-L405)),
and the §2.10 cold-start warm-up rebuilds pane state from the (un-advanced)
committed offsets. This is the price of holding mutable state outside the
transaction — and the exact point where the design notes that *truly*
rollback-able large state is the trigger to reach for Flink's checkpointed EOS.
(Related: the transaction boundary is **one poll-batch** capped at
[`_EOS_MAX_RECORDS = 500`](../../src/streaming_feature_store/sliding/consumer.py#L60),
never per message — per-message commits would multiply transaction-marker write
amplification and collapse throughput.)

- **Likely question:** *"On a transaction abort, why not just retry the batch in
  memory?"* Answer: because in-memory aggregation state already absorbed the
  batch; replaying it would double-count, so a stateful consumer must discard and
  rebuild from the committed offset.
