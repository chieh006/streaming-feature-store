# Multi-Process Synthetic Event Load Test Results

**Generated:** 2026-05-18T03:26:55.915932+00:00
**Topic:** e-commerce-events

## Configuration

| Field | Value |
|---|---|
| Duration | 10.0 s |
| Target rate (aggregate) | 60_000 evt/s |
| Processes | 6 |
| Workers per process | 2 |
| Batch size | 1024 |
| Max in-flight (per process) | 50000 |
| Seed | 42 |
| Producer profile | EOS (idempotent, acks=all, max.in.flight=5) |

## Aggregate results

| Metric | Value |
|---|---|
| Produced | 632_832 |
| Acked | 632_832 |
| Failed | 0 |
| Sustained rate | 61,846 evt/s ✅ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 90.6 / 264.2 / 370.0 ms |
| Errors by class | {} |
| Max child wallclock | 10.23 s |

## Per-process breakdown

| # | Produced | Acked | Failed | Sustained evt/s | p50 ms | p95 ms | p99 ms | Wallclock s |
|---|---|---|---|---|---|---|---|---|
| 0 | 105_472 | 105_472 | 0 | 10,321 | 91.1 | 253.9 | 369.6 | 10.22 |
| 1 | 105_472 | 105_472 | 0 | 10,325 | 91.7 | 257.3 | 356.9 | 10.22 |
| 2 | 105_472 | 105_472 | 0 | 10,313 | 90.0 | 257.5 | 360.7 | 10.23 |
| 3 | 105_472 | 105_472 | 0 | 10,308 | 85.7 | 278.1 | 380.9 | 10.23 |
| 4 | 105_472 | 105_472 | 0 | 10,312 | 88.3 | 273.0 | 379.4 | 10.23 |
| 5 | 105_472 | 105_472 | 0 | 10,308 | 93.2 | 263.1 | 357.5 | 10.23 |

## Verdict

✅ PASSED: sustained 61,846 evt/s vs floor 50_000 evt/s.
