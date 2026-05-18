# Multi-Process Synthetic Event Load Test Results

**Generated:** 2026-05-18T03:25:18.384681+00:00
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
| Sustained rate | 61,693 evt/s ✅ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 88.1 / 104.5 / 124.4 ms |
| Errors by class | {} |
| Max child wallclock | 10.26 s |

## Per-process breakdown

| # | Produced | Acked | Failed | Sustained evt/s | p50 ms | p95 ms | p99 ms | Wallclock s |
|---|---|---|---|---|---|---|---|---|
| 0 | 105_472 | 105_472 | 0 | 10,327 | 88.0 | 106.7 | 121.1 | 10.21 |
| 1 | 105_472 | 105_472 | 0 | 10,322 | 87.6 | 110.9 | 126.6 | 10.22 |
| 2 | 105_472 | 105_472 | 0 | 10,320 | 88.8 | 114.6 | 125.9 | 10.22 |
| 3 | 105_472 | 105_472 | 0 | 10,313 | 84.7 | 100.7 | 116.0 | 10.23 |
| 4 | 105_472 | 105_472 | 0 | 10,282 | 89.5 | 104.0 | 124.5 | 10.26 |
| 5 | 105_472 | 105_472 | 0 | 10,290 | 89.5 | 106.1 | 125.5 | 10.25 |

## Verdict

✅ PASSED: sustained 61,693 evt/s vs floor 50_000 evt/s.
