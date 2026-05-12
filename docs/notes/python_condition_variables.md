# Python Condition Variables — Summary

## The primitives

- **`threading.Lock`** — a mutex. Equivalent to C++ `std::mutex`. Use via `with lock:`.
- **`threading.Condition(lock)`** — a condition variable bound to a lock. Equivalent to C++ `std::condition_variable`. Use via `with cond:`. Lets a thread sleep on a predicate without busy-waiting or hogging the lock.

## The canonical pattern

```python
with self._cond:
    while not predicate:
        self._cond.wait(timeout=...)
    # safely mutate shared state — lock is held
```

Always `while`, never `if` — see "spurious wake-ups" below.

## What `cond.wait(timeout=t)` does

It executes a **two-phase** wake-up cycle:

1. **Phase 1 — Sleep on the condition.** Atomically releases the lock and blocks the thread. The OS marks the thread as not-runnable, so it consumes **0% CPU** (similar to `time.sleep`, but signalable). Wakes up on: another thread calling `notify()`, the timeout firing, or a spurious wake-up.
2. **Phase 2 — Re-acquire the lock.** After being unblocked, the thread tries to grab the lock. If another thread holds it, the thread **blocks on the lock** until it's free. This phase has **no timeout** — it waits as long as necessary.

`wait()` does not return until phase 2 succeeds. **Invariant: when `wait()` returns, you hold the lock.** Always.

### Concrete example

Thread A is in `wait(timeout=0.01)`. Thread B is inside the same `with self._cond:` block doing its own refill.

- **t = 0.000s**: A enters `wait()`, releases the lock, sleeps.
- **t = 0.005s**: B enters `with self._cond:`, acquires the lock, starts refilling.
- **t = 0.010s**: A's timeout fires. A is woken and tries to acquire the lock — B still has it. A blocks on the lock (phase 2).
- **t = 0.012s**: B finishes; its `with` block exits and releases the lock.
- **t = 0.012s**: A acquires the lock. `wait()` returns. A resumes at the next statement.

A's total wait was 0.012s, not 0.010s — the timeout sets a **minimum** wait, not a maximum return time. But A always re-acquires the lock eventually.

## Key clarifications from our discussion

- **Suspended thread = zero CPU.** Unlike a busy-wait (`while not ready: pass`), a thread in `wait()` is removed from the scheduler entirely. No cycles burned while sleeping.
- **Why not just hold the lock and spin?** That deadlocks the system: the spinning thread waits for shared state to change, but holds the lock that any state-mutating thread would need. Refilling inside the spin loop technically works but wastes a full CPU core and serializes all workers.
- **Lock contention during phase 2 does not re-trigger the timeout.** If the lock is busy when the thread wakes, it just queues on the lock — it does not "go back to sleep on the timeout." The timeout only governs phase 1.
- **Total wait ≥ timeout.** The timeout sets a minimum sleep duration, not a maximum return time. Phase 2 may add extra delay if the lock is contended.
- **Spurious wake-ups are real.** `wait()` may return without notify or timeout. The thread still goes through phase 2 normally and re-acquires the lock — the wake-up reason is invisible to the caller. The `while`-loop predicate re-check turns spurious wakes into harmless no-ops: one extra refill/check, then back to sleep.
- **C++ parity.** `std::condition_variable::wait_for` follows the same two-phase semantics, the same lock-held-on-return guarantee, and the same spurious-wake-up rule. The C++ mental model transfers directly.

## In this project

The `TokenBucketPacer` at [pacer.py:117-125](../../src/streaming_feature_store/load/pacer.py#L117-L125) uses this pattern with a timeout computed from `deficit / target_rate` — the exact time until enough tokens should have accrued. It never calls `notify()`; timeouts alone drive the wake-ups.
