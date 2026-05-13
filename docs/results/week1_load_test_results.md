# Week 1 — Synthetic Event Load Test Results

**Generated:** 2026-05-13T08:09:49.249381+00:00
**Topic:** e-commerce-events

## Configuration

| Field | Value |
|---|---|
| Duration | 10.0 s |
| Target rate | 60_000 evt/s |
| Workers | 12 |
| Batch size | 1024 |
| Max in-flight | 50000 |
| Seed | 42 |

## Results

| Metric | Value |
|---|---|
| Produced | 152_576 |
| Acked | 152_576 |
| Failed | 0 |
| Sustained rate | 14,814 evt/s ❌ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 29.7 / 192.0 / 742.3 ms |
| Errors by class | {} |
| Wallclock | 10.30 s |

## Verdict

❌ FAILED: sustained 14,814 evt/s vs floor 50_000 evt/s.
