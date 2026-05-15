---
title: "Why My Kafka Producer Couldn't Break 14k events/sec — and the GIL Fingerprint That Explained It"
subtitle: "One fact about CPython explained three mysteries: the plateau I couldn't tune away, why five smart fixes all failed, and the thread count that finally worked."
tags: python, kafka, performance, concurrency, gil
---

> **TL;DR.** A Kafka load test needed to sustain 50,000+ events/sec. It
> plateaued at ~14k no matter what I tuned. The whole story collapses to
> one fact — *CPython runs one process's Python on one core at a time* —
> and that single fact explains all three puzzles: why tuning didn't
> help, why five independent architecture changes all failed, and why
> the eventual multiprocessing fix had a **counter-intuitive optimal
> thread count** (2 per process beat both 1 and 3). The fix got ~5× to
> ~60k. The post includes the part where I was confidently wrong.

---

## The setup

I was building the ingestion path for a streaming feature store:
synthetic events, Avro-serialized, into a 3-broker Kafka cluster via
`confluent-kafka`'s `SerializingProducer`, Pydantic for validation. The
bar was concrete: **sustain 60,000 evt/s, fail under 50,000**, 10-second
run, no dropped messages.

It sustained ~6,000. After tuning, ~14,500 — 3–4× short of the floor,
and crucially with **zero delivery failures**. Nothing was breaking. It
was hitting a *ceiling*. This is the hunt for that ceiling, and the one
fact at the bottom of it.

*Stack: Python 3.12, `confluent-kafka` (librdkafka), `pydantic` v2, Avro
+ Schema Registry, Kafka KRaft on Docker Compose, a custom multi-threaded
load runner. WSL2 dev box — relevant later.*

---

## Act 1: the plateau, and the fix I got wrong

First move: producer tuning — `linger.ms=20`, `lz4`, `acks=1` (dev
only), 2 MB batches, generous queue caps. It worked *exactly as
designed*: **+18% throughput, p95/p99 latency roughly halved.** And
still 3.4× below the floor.

That's where a performance investigation gets interesting: the tuning
did its job, but the bottleneck was somewhere else. So I kept going,
one change per run (otherwise you can't attribute anything):

| Change | Hypothesis | Result |
|---|---|---|
| librdkafka tuning | broker round-trips dominate | ✅ +18%, kept |
| One producer **per worker thread** | shared serializer lock is the bottleneck | ❌ flat throughput, latency 2–6× *worse* |
| Verify ≥12 partitions | broker-side serialization | ✅ no-op (already optimal) |
| Poll once per batch, not per event | per-event poll lock contention | ❌ −34% |
| Dedicated callback-pump thread | workers waste time polling | ❌ −32% (best p50 ever, though) |

Five smart, well-targeted hypotheses. Every one regressed or did
nothing. **That pattern is itself the clue** — but first, the one I got
wrong, because it's the most instructive data point in the whole
investigation.

The library docstring says *"Not thread-safe — one instance per
producing thread."* The serializer holds a schema-cache lock. Twelve
threads, one lock — obviously the bottleneck. I shipped it without
profiling. **Throughput went flat; every latency percentile got 2–6×
worse.** Post-mortem: splitting one shared producer into twelve
fragmented the broker-side batches ~12× (tiny frequent requests instead
of one fat batched one), and the broker's *per-request* fixed cost
swamped any lock saving. The lock was a microsecond dict lookup — 1–5%
of per-event time, never the bottleneck. *"Not thread-safe" is a
correctness statement, not a performance one.* A 5-minute `py-spy` run
would have killed the idea before I wrote a line. I'm keeping this in
because the engineer who hides their wrong turns is hiding the part
where the reasoning actually happens.

---

## Act 2: the fingerprint

Step back and look at every configuration measured:

| Configuration | Sustained evt/s |
|---|---|
| Baseline | ~12,250 |
| + librdkafka tuning | ~14,500 |
| + per-worker producers | ~14,400 |
| + per-batch poll | ~9,600 |
| + dedicated pump thread | ~9,800 |
| tuning only (revert) | ~14,800 |

**Six independent architectures — different locks, topologies, polling
strategies — all clustered in 9.5–14.8k.** Each change moved *which
inner constraint was binding* without ever moving the *outer* ceiling.

That is consistent with exactly one thing: a **process-wide
serialization point no in-process change can escape**. In CPython
there is precisely one — the Global Interpreter Lock. One thread runs
Python bytecode at a time, regardless of thread or core count.

### Each failure is positive evidence

The regressions aren't disappointments — each is a *falsification test*
the GIL hypothesis survived:

- **Per-worker producers** removed 12-way contention on *two* locks at
  once. If either bound throughput, it should have jumped. **0% change**
  — freeing inner locks does nothing when threads queue on the GIL
  anyway.
- **Per-batch polling** cut a lock's acquire frequency ~16,800×. If
  frequency were the cost, this recovers it. **−34%** — the dispatch
  *work* still happens; concentrating it just made GIL contention
  spikier.
- **The pump thread** is the *structurally correct* fix for the
  symptom (a profile showed 93% of worker time in `poll()`). It did its
  job — best p50 I ever measured. But **throughput fell 32%**. "The
  architecture is right" and "throughput regressed" are only both true
  if the work moved to a thread competing for the *same GIL*. With real
  parallelism the pump runs on its own core and throughput climbs. It
  didn't.

Three changes that *should* have moved throughput if anything but the
GIL were binding. None did.

### The arithmetic that closed it

At ~14,500 evt/s, per-event Python work (Pydantic + serializer adapter
+ bookkeeping + poll callbacks) is ~70 µs of GIL-held time:

```
14,500 evt/s × 70 µs/evt ≈ 1,015 ms of GIL-held time per second
1 CPU core                = 1,000 ms of execution per second
```

We were saturating ~100% of **one core's worth of Python** — the
ceiling, from first principles, matching the measured plateau. No
in-process multi-threaded design beats one core of Python, because the
GIL serializes bytecode across all threads. Every architecture I tried
just rearranged which thread held the one lock that matters.

---

## Act 3: the same fact, billed twice more

Here's the through-line. The GIL didn't only explain the plateau and
the failed fixes — *the same one-core-per-process fact dictates how to
escape it, and how to tune the escape.*

**Escape:** if the limit is one GIL per process, run more processes.
Each gets its own interpreter, its own GIL, its own core. I built a
separate multiprocessing harness (kept beside the threaded one — they
serve different jobs): N producer processes, the proven single-process
config in each, the parent aggregating results.

**The tuning law — billed a third time.** The projection said
4 × ~14.5k ≈ ~58k. The first auto-layout, **4 processes × 3 workers**,
gave only **34.5k** — each process *slower* than one running alone.
**6 × 2** (same 12 total threads, redistributed) gave **~60k** — same
thread count, **+76%**. **8 × 1** ≈ 62k (tied, but 33% more processes
for it). **2 × 6**: a dismal 15k.

Workers-per-process has an *optimum*, and it's the GIL again. Treat each
process's GIL as a single-server queue you want ~100% utilized with no
queue forming for it:

- **Too few (1/proc):** the lone worker spends ~half its time parked in
  GIL-yielding waits (rate limiter, backpressure). The GIL sits **idle**
  — half the process's Python capacity wasted. *Why 8×1 ≯ 6×2.*
- **Too many (3+/proc):** more threads than the GIL can serve; the
  surplus block in the handoff path and you pay serialization +
  handoff overhead. *Why 4×3 < 6×2.*

The optimum is the fewest workers that keep the GIL continuously busy
through the natural blocking gaps: empirically `W ≈ round(1/s)`, where
`s` is the fraction of a worker's wall time *holding* the GIL. Here
`s ≈ 0.5`, so **W = 2**. Two workers tag-team the GIL — one runs while
the other is parked — while librdkafka's C sender threads (no GIL) do
the network in parallel. That turns "grid-search every combination"
into "profile once for `s`, compute the layout, confirm with one run."

| | Sustained evt/s | vs. 50k floor |
|---|---|---|
| Single process (best, tuned) | ~14,500 | ❌ 3.4× under |
| 4 × 3 multiprocess | ~34,500 | ❌ under |
| **6 × 2 multiprocess (kept)** | **~59–62k** | ✅ **clears it** |

~5× the single-process number, latency *also* better (p50 16 vs 28 ms;
p95 47 vs 170 ms), zero failures.

One honest caveat, because error bars are part of the result: this was
a WSL2 dev box, numbers drifted as broker/OS state warmed. The
load-bearing claims aren't "60,000" — they're the **~5× ratio**, the
**mechanism**, and the **`W ≈ round(1/s)` model**. Those reproduce; a
laptop benchmark number doesn't.

---

## What I'd tell another engineer

1. **Profile before optimizing.** My one regression came from shipping
   a plausible hypothesis without the 5-minute profile that would have
   killed it. "Obviously slow" is not a measurement.
2. **% of time ≠ % wasted.** 93% in `poll()` wasn't waste — it was
   *useful* callback work. Removing it relocated the work, not deleted it.
3. **"Not thread-safe" ≠ "this lock is hot."** One is a correctness
   claim, the other an empirical one. Only measurement answers the second.
4. **Architecture changes are conservation games.** Moving work between
   threads doesn't reduce it; it changes who waits. Real gains need less
   total work or escaping the serialization point.
5. **Out of in-process options? The next move is multiprocessing or a
   no-GIL build — not more thread tuning.** And size it with
   `W ≈ round(1/s)`, not intuition.

The point of writing this up isn't the 5×. It's that the entire
investigation — a plateau, five dead ends, and a surprising thread
count — reduces to *one* fact about CPython, provable with a
falsification chain and a back-of-envelope. Shipping the speedup is the
deliverable. Knowing *why*, with evidence, is the job.

---

*Full investigation — every run, the lock-stack diagram, the fix-by-fix
forensics, the queueing-theory derivation of the worker law — is in the
project's engineering log. This is the narrative version. Pushback
welcome.*
