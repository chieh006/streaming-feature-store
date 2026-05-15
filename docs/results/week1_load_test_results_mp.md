# Multi-Process Synthetic Event Load Test Results

**Generated:** 2026-05-15T00:37:28.364636+00:00
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

## Aggregate results

| Metric | Value |
|---|---|
| Produced | 632_832 |
| Acked | 632_832 |
| Failed | 0 |
| Sustained rate | 61,735 evt/s ✅ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 88.9 / 106.3 / 124.6 ms |
| Errors by class | {} |
| Max child wallclock | 10.25 s |

## Per-process breakdown

| # | Produced | Acked | Failed | Sustained evt/s | p50 ms | p95 ms | p99 ms | Wallclock s |
|---|---|---|---|---|---|---|---|---|
| 0 | 105_472 | 105_472 | 0 | 10,292 | 84.2 | 104.4 | 127.7 | 10.25 |
| 1 | 105_472 | 105_472 | 0 | 10,318 | 89.8 | 103.0 | 125.9 | 10.22 |
| 2 | 105_472 | 105_472 | 0 | 10,307 | 87.5 | 105.5 | 122.0 | 10.23 |
| 3 | 105_472 | 105_472 | 0 | 10,298 | 88.8 | 110.6 | 120.5 | 10.24 |
| 4 | 105_472 | 105_472 | 0 | 10,290 | 90.6 | 103.4 | 119.9 | 10.25 |
| 5 | 105_472 | 105_472 | 0 | 10,289 | 89.3 | 109.7 | 127.6 | 10.25 |

## Verdict

✅ PASSED: sustained 61,735 evt/s vs floor 50_000 evt/s.
