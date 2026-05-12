# Python (GIL) vs C++/Go/Rust: CPU- and I/O-bound Work

## GIL basics

- **Per-process**: exactly one Global Interpreter Lock per Python interpreter process (one PID = one GIL).
- **Protects**: every Python object's reference count and interpreter state.
- **Released** when a thread waits on I/O (`socket`, `file`, `time.sleep`) or enters a C extension that opts out (NumPy, Polars, PyArrow).
- **Held** during pure Python bytecode execution → only one thread runs Python at a time.

## CPU-bound: 4 threads, 4 cores

**C++ / Go / Rust** — no global lock, true parallelism:
```
Core 1: T1 T1 T1 T1 T1 ...   ← all 4 threads truly run
Core 2: T2 T2 T2 T2 T2 ...     in parallel on separate
Core 3: T3 T3 T3 T3 T3 ...     cores
Core 4: T4 T4 T4 T4 T4 ...
→ ~4× speedup
```

**Python `threading`** — GIL serializes all Python execution:
```
Core 1: T1 T2 T3 T4 T1 T2 ...  ← only one runs Python at a time
Core 2: (free for OS / other apps)
Core 3: (free for OS / other apps)
Core 4: (free for OS / other apps)
→ ~1× (no gain; small overhead from GIL handoffs)
```

## I/O-bound: 4 threads producing to Kafka (1ms CPU + 9ms wait per msg)

**C++ / Go / Rust and Python `threading` behave the same here** — threads suspend during I/O, freeing the CPU:
```
Time →   t1   t2   t3   t4   t5 ... t10  t11  t12  t13  t14
Core 1:  T1   T2   T3   T4   --      --   T1   T2   T3   T4
         PY   PY   PY   PY  (idle: all 4 suspended)  PY   PY   PY   PY

T1:      RUN  ←─── suspended, waiting for ACK ───→  RUN
T2:      --   RUN  ←─── suspended, waiting ────→    --   RUN
T3:      --   --   RUN  ←─── suspended ────→        --   --   RUN
T4:      --   --   --   RUN  ←─── suspended ──→     --   --   --   RUN

NIC HW:       [T1 bytes in flight ───── ACK]
              [T2 bytes in flight ───── ACK]
                   [T3 bytes in flight ───── ACK]
                        [T4 bytes in flight ───── ACK]
              ↑ all 4 messages traveling concurrently in hardware ↑
```
The GIL is released during each `send()` syscall, so other Python threads run while one waits. Throughput ~4× single-threaded — comparable to C++/Go/Rust for the same workload.

## CPU-bound + shared memory: language differences

| Language | Mechanism | Sharing | Parallelism |
|---|---|---|---|
| **C++ / Rust** | `std::thread` + `std::mutex` (Rust enforces safety at compile time) | Shared memory by default | ✅ true |
| **Go** | Goroutines + channels (CSP-style) | Channels preferred; shared memory + `sync.Mutex` allowed | ✅ true |
| **Python `threading`** | One GIL | Easy (just references + `Lock`) | ❌ serialized |
| **Python `multiprocessing`** | Multiple processes, each its own GIL | Hard — needs `Queue`/`Pipe`/`shared_memory` | ✅ true |

## How Python handles CPU-bound work with shared memory

Practical priority order:

1. **Restructure to avoid sharing** — partition input, return per-worker results, combine at end. Use `multiprocessing.Pool`.
2. **Use a GIL-releasing library** — Polars, NumPy, PyArrow handle large data and parallelize internally across cores from a single Python thread. Most idiomatic Python answer.
3. **`multiprocessing` + IPC** when sharing is unavoidable:
   - `Queue` / `Pipe` — message passing (simple, slower, pickled).
   - `Value` / `Array` — small shared primitives.
   - `shared_memory` — large arrays with no copy.
   - `Manager` — shared dict/list proxies (convenient, slow).
4. **Cython / Rust extensions / `numba`** — last resort for custom hot loops.

Avoid `threading` for pure-Python CPU work; the GIL guarantees no speedup.

## One-line summary

**C++/Go/Rust achieve true CPU parallelism by default; Python does not within one process.** For I/O-bound work, Python is competitive because the GIL releases during waits. For CPU-bound work, Python relies on `multiprocessing` (one GIL per process) or GIL-releasing libraries (Polars/NumPy/PyArrow) to match what other languages get from threads alone.
