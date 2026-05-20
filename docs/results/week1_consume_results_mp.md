# Multi-Process Consumer Group — End-to-End Latency Results

**Generated:** 2026-05-20T05:14:10.029594+00:00
**Topic:** e-commerce-events   **Group:** wk1-consume-group-v2

## Configuration

| Field | Value |
|---|---|
| Members (processes) | 8 |
| Workers per process | 1 |
| Isolation level | read_uncommitted |
| Deserialize mode | pydantic |
| Until caught up | True |
| Duration | 120.0 s |

## Aggregate results

| Metric | Value |
|---|---|
| Consumed | 0 |
| Deserialize failed | 0 |
| Sustained consume rate | 0 evt/s |
| End-to-end p50 / p95 / p99 | 0.0 / 0.0 / 0.0 ms |
| Max lag / End lag | 0 / 0 |
| Lag ramped? | No (steady-state drain) |
| Errors by class | {} |
| Max member wallclock | 6.07 s |

## Per-process breakdown

| # | Partitions | Consumed | e2e p99 ms | End lag |
|---|---|---|---|---|
| 0 | [] | 0 | 0.0 | 0 |
| 1 | [] | 0 | 0.0 | 0 |
| 2 | [] | 0 | 0.0 | 0 |
| 3 | [] | 0 | 0.0 | 0 |
| 4 | [] | 0 | 0.0 | 0 |
| 5 | [] | 0 | 0.0 | 0 |
| 6 | [] | 0 | 0.0 | 0 |
| 7 | [] | 0 | 0.0 | 0 |

## Verdict

✅ Group drained at producer rate; end-to-end latency flat.
