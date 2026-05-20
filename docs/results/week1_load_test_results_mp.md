# Multi-Process Synthetic Event Load Test Results

**Generated:** 2026-05-20T05:01:11.743232+00:00
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
| Producer profile | throughput (acks=1, no idempotence) |

## Aggregate results

| Metric | Value |
|---|---|
| Produced | 632_832 |
| Acked | 632_832 |
| Failed | 0 |
| Sustained rate | 61,727 evt/s ✅ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 71.8 / 104.1 / 129.4 ms |
| Errors by class | {} |
| Max child wallclock | 10.25 s |

## Per-process breakdown

| # | Produced | Acked | Failed | Sustained evt/s | p50 ms | p95 ms | p99 ms | Wallclock s |
|---|---|---|---|---|---|---|---|---|
| 0 | 105_472 | 105_472 | 0 | 10,327 | 67.5 | 117.7 | 131.7 | 10.21 |
| 1 | 105_472 | 105_472 | 0 | 10,321 | 68.5 | 103.4 | 126.8 | 10.22 |
| 2 | 105_472 | 105_472 | 0 | 10,318 | 73.8 | 102.2 | 130.2 | 10.22 |
| 3 | 105_472 | 105_472 | 0 | 10,318 | 22.6 | 101.3 | 121.9 | 10.22 |
| 4 | 105_472 | 105_472 | 0 | 10,315 | 72.9 | 115.6 | 133.2 | 10.23 |
| 5 | 105_472 | 105_472 | 0 | 10,288 | 89.7 | 107.7 | 126.4 | 10.25 |

## Verdict

✅ PASSED: sustained 61,727 evt/s vs floor 50_000 evt/s.
