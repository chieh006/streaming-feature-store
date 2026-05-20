# Multi-Process Consumer Group — End-to-End Latency Results

**Generated:** 2026-05-20T05:02:20.126713+00:00
**Topic:** e-commerce-events   **Group:** wk1-consume-1member

## Configuration

| Field | Value |
|---|---|
| Members (processes) | 1 |
| Workers per process | 1 |
| Isolation level | read_uncommitted |
| Deserialize mode | pydantic |
| Until caught up | False |
| Duration | 10.0 s |

## Aggregate results

| Metric | Value |
|---|---|
| Consumed | 225_280 |
| Deserialize failed | 0 |
| Sustained consume rate | 22,376 evt/s |
| End-to-end p50 / p95 / p99 | 73359.8 / 77009.4 / 77400.6 ms |
| Max lag / End lag | 434_176 / 407_552 |
| Lag ramped? | Yes (fell behind) |
| Errors by class | {} |
| Max member wallclock | 10.07 s |

## Per-process breakdown

| # | Partitions | Consumed | e2e p99 ms | End lag |
|---|---|---|---|---|
| 0 | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] | 225_280 | 77400.6 | 407_552 |

## Verdict

❌ Fell behind: consumer lag ramped — the symmetric single-process GIL ceiling (design doc §2.1).
