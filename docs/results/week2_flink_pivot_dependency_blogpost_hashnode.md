> **TL;DR.** The canonical streaming-interview feature — *"clicks in the
> last 5 minutes"* — over a 3-broker Kafka cluster. I built it on PyFlink
> first: a JobManager, two TaskManagers, an Apache Beam portability
> bridge, hand-pinned connector JARs, RocksDB, a checkpoint volume. The
> first smoke run died **inside a Beam worker on a one-line Redis hostname
> typo** — before a single window ever emitted. I threw the cluster away
> and rewrote it as *one Python process* with in-memory windows. It
> shipped. The lesson isn't "Flink bad." It's that every dependency is a
> liability you pay whether or not you ever collect its benefit — and at
> ~200 evt/s I was paying for six failure domains to get none of their
> payoff.

---

## The setup

Same project as [last time](https://github.com/chieh006/streaming-feature-store):
a streaming feature store. This PR is the headline feature — sliding-window
aggregations per user: `clicks_5m`, `purchases_24h`, twelve features across
three resolutions (5 min / 1 h / 24 h), each emitting on its own slide
cadence. The "route" was never in doubt; sliding windows were the chosen
approach from day one. The only open question was **what computes them.**

The textbook answer is Apache Flink. So I started there.

*Stack: Python 3.12, PyFlink DataStream API, a local Flink cluster on Docker
Compose, Avro + Schema Registry, Redis as the online store. WSL2 dev box.*

---

## Act 1: the cluster that never emitted a window

The Flink design was *correct*. Pane-based pre-aggregation for constant-state
windows, bounded-out-of-orderness watermarks (5 s skew, 30 s idleness),
allowed-lateness re-firing with an `emission_seq`, a dual sink contract
(Redis hash + Kafka topic). I'm not knocking the semantics — they're standard
streaming theory and they were right.

Then I ran it. The first smoke test died like this:

```text
  File ".../apache_beam/runners/worker/sdk_worker.py", line ...
  File ".../apache_beam/runners/worker/operations.py", line ...
  ... eight more frames of Beam + JVM plumbing ...
ConnectionError: Error connecting to redis:6379
```

A **Redis hostname** was wrong. One line. The kind of bug you fix in
fifteen seconds — *if you can see it*. Here it surfaced ten frames deep
inside a Beam SDK worker, routed through the Python↔JVM portability bridge,
underneath the Flink operator that called it. No window had emitted. I
couldn't yet tell whether the *approach* worked, because the **scaffolding**
hadn't even finished standing up.

That's the moment that mattered. Not "Flink is slow" — I never got far
enough to measure speed. **The failure was opaque, and it was in the
foundation, not the feature.** A bug in the foundation makes you question
the whole approach. A bug in the glue is a one-line patch. I was staring at
the wrong kind.

---

## Act 2: count the failure domains

I rewrote it as a plain `confluent-kafka` consumer that keeps the window
state in ordinary Python dicts. Same windowing semantics — copied across
*verbatim*, because the semantics were never the problem. The only thing
that changed was the machine underneath them.

Here's the whole argument in one table — **what has to stand up correctly
before a single feature can be computed:**

<table>
  <thead>
    <tr><th>Moving part</th><th>PyFlink build</th><th>Plain consumer</th></tr>
  </thead>
  <tbody>
    <tr><td>JobManager</td><td>✅ required</td><td>—</td></tr>
    <tr><td>TaskManagers (×2)</td><td>✅ required</td><td>—</td></tr>
    <tr><td>JVM runtime</td><td>✅ required</td><td>—</td></tr>
    <tr><td>Apache Beam portability bridge (Python↔JVM)</td><td>✅ required</td><td>—</td></tr>
    <tr><td>Hand-pinned connector JARs</td><td>✅ required</td><td>—</td></tr>
    <tr><td>RocksDB state backend + checkpoint volume</td><td>✅ required</td><td>—</td></tr>
    <tr><td>Kafka client</td><td>(inside Flink)</td><td><code>confluent-kafka</code> — <em>proven in wk1</em></td></tr>
    <tr><td>Online store client</td><td><code>redis-py</code> in a UDF</td><td><code>redis-py</code> — <em>proven in wk1</em></td></tr>
    <tr><td>Window state</td><td>RocksDB</td><td>a Python <code>dict</code></td></tr>
    <tr><td><strong>New, unproven failure domains</strong></td><td><strong>6+</strong></td><td><strong>0</strong></td></tr>
  </tbody>
</table>

The right column is built entirely from parts that already worked end-to-end
the week before. The left column introduces *six independent things* that had
never run together in this environment — and a feature can't emit until **all
six** are healthy at once. That's not a throughput argument. It's a
probability argument: the more independent parts that must simultaneously be
correct, the more first runs you lose to scaffolding you can't see into.

---

## The honest part: the rewrite also broke. Twice.

If I stopped here it would read as "simple thing worked first try." It
didn't. The plain consumer failed on *its* first two runs too:

1. **A bootstrap hostname** pointed at the Docker-internal broker name from a
   host shell — `Failed to resolve 'kafka-1:9092'`.
2. **A deserializer call** passed `None` where the library now demands a
   `SerializationContext` — `TypeError` on the first message decoded.

Two bugs. *Same class* as the one that killed the Flink build — a wrong
address, a bad argument. The difference was the bill:

<table>
  <thead>
    <tr><th></th><th>PyFlink</th><th>Plain consumer</th></tr>
  </thead>
  <tbody>
    <tr><td>What failed</td><td>Redis hostname</td><td>Broker hostname; a <code>None</code> arg</td></tr>
    <tr><td>How it surfaced</td><td>~10 frames of Beam/JVM</td><td>a 3-line Python traceback</td></tr>
    <tr><td>Time to fix</td><td>(abandoned the approach)</td><td>one edit each, minutes</td></tr>
  </tbody>
</table>

**"It broke" is not the metric. "How much did the break cost" is.** Opaque
failures in a deep stack don't just take longer to fix — they make you
distrust the whole design, because you can't localize the fault. Transparent
failures in a shallow stack stay where they happen.

---

## "But isn't the simple version slower?"

The reasonable objection: surely you gave up performance. For *this* problem,
no — not on the axis that matters.

<table>
  <thead>
    <tr><th>Dimension</th><th>PyFlink (promised)</th><th>Plain consumer (measured)</th></tr>
  </thead>
  <tbody>
    <tr><td>Feature freshness (the sliding-window output)</td><td>5 s watermark + slide cadence</td><td><strong>identical</strong> — same 5 s + same slide</td></tr>
    <tr><td>Steady-state throughput / worker</td><td>~10k evt/s/slot, via a Beam crossing</td><td>~11–14k evt/s/process, no crossing</td></tr>
    <tr><td>Horizontal ceiling</td><td>12 (operator parallelism)</td><td>12 (consumer-group processes)</td></tr>
    <tr><td>Restart recovery</td><td>instant (RocksDB checkpoints)</td><td>cold-start warm-up — <strong>Flink wins</strong></td></tr>
    <tr><td>State beyond RAM</td><td>spills to disk — <strong>Flink wins</strong></td><td>heap; would OOM</td></tr>
  </tbody>
</table>

Feature freshness is governed by the watermark and the slide cadence, and the
consumer copies both exactly — so "the 5 minutes ending now" is equally fresh
either way. Throughput is a wash: both are GIL-bound in Python and both cap at
12 partitions, except the consumer *drops* the Python↔JVM crossing, so per
worker it's equal-or-better. (PyFlink's only road to genuinely higher
throughput was switching to **Java** — which the Flink design itself rejected.)

Flink genuinely wins two rows — restart recovery and state-beyond-RAM. But the
feeder is ~200 evt/s, the whole state is tens of MB, and restarts are rare.
**Neither winning row binds at this scale.** You don't pay six failure domains
for a benefit your workload never triggers.

And the asymmetry in that table's column headers is the real punchline:
PyFlink's numbers are **promised** — it never emitted a window, so they were
never measured. The consumer's are the ones I actually clocked.

---

## The correction I had to make (I was half-wrong)

My first writeup of this had a tidy story: *"the rewrite was easy because the
hard conceptual work — panes, watermarks, lateness — was already done in the
Flink design; the second doc just inherited it."* A reviewer pushed back, and
they were right.

That framing **conflates two different questions:**

- *Why was the rewrite cheap to produce?* → it reused already-specified,
  already-unit-tested semantics. True.
- *Why did the Flink build fail?* → **dependencies.** Full stop. Nothing to
  do with windowing semantics.

I had answered the first question and quietly passed it off as the answer to
the second. But the semantics were *never the risk* — they're standard streaming
theory and the sliding-window route was decided before either design existed.
If you had written Flink's exact semantics on the plain consumer's
dependency footprint, **it would have worked first try too.** The failure was
the six failure domains, not the math. Crediting the rewrite's *ease* for the
original's *failure* points at the wrong cause.

---

## What I'd tell another engineer

1. **Count failure domains before you count features.** A part that must be
   healthy for anything to work is a liability you pay up front and a benefit
   you only sometimes collect.
2. **Match the tool's weight to the problem's scale.** Flink earns its
   machinery at 100k+ evt/s and state beyond RAM. At 200 evt/s and tens of MB,
   that machinery is pure downside — cost with the benefit switched off.
3. **Opaqueness is a cost, and it's underrated.** The *same one-line bug* was
   ten Beam/JVM frames in one stack and a three-line traceback in the other.
   Depth of stack sets the price of every future mistake.
4. **"It also broke" isn't the comparison — "how expensive was the break" is.**
   The shallow version failed twice and shrugged it off in minutes.
5. **Don't confuse "why was the rewrite easy" with "why did the original
   fail."** They have different answers. Mixing them hides the real cause.

The headline is "I replaced a Flink cluster with one Python process." The
actual deliverable is knowing *why* that was the right call with evidence —
and being able to name the exact conditions (bigger-than-RAM state, sub-second
SLAs, exactly-once with large state) that would send me straight back to Flink.

---

## Coda: the same shape as last time

[Last post](https://github.com/chieh006/streaming-feature-store) ended on:
*a cost doesn't tell you where it lands — the binding constraint does.* This
one is the same shape, one level up. **A dependency is a cost you pay even
when its benefit isn't the binding constraint.** Flink's checkpointing,
RocksDB spill, and JVM speed are real benefits — for a workload that triggers
them. Mine didn't. So all that was left of them was the bill: six things that
had to stand up before I could see whether one Redis hostname was wrong.

Account for what actually binds. Then buy exactly that, and nothing heavier.

---

*Full design docs — the superseded Flink version kept as an artifact, the
plain-consumer version that shipped, and the fix-by-fix forensics — are in the
project's [engineering log on GitHub](https://github.com/chieh006/streaming-feature-store).
This is the narrative version. Pushback welcome — it's how the last section of
this very post got written.*
