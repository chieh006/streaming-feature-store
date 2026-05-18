# Week 1 — Synthetic Event Load Test Results

**Generated:** 2026-05-18T03:23:07.799416+00:00
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
| Produced | 123_904 |
| Acked | 123_904 |
| Failed | 0 |
| Sustained rate | 11,886 evt/s ❌ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 26.3 / 150.9 / 299.4 ms |
| Errors by class | {} |
| Wallclock | 10.42 s |

## Verdict

❌ FAILED: sustained 11,886 evt/s vs floor 50_000 evt/s.
