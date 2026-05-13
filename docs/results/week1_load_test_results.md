# Week 1 — Synthetic Event Load Test Results

**Generated:** 2026-05-13T05:57:59.955547+00:00
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
| Produced | 153_600 |
| Acked | 153_600 |
| Failed | 0 |
| Sustained rate | 14,755 evt/s ❌ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 180.2 / 405.9 / 487.8 ms |
| Errors by class | {} |
| Wallclock | 10.41 s |

## Verdict

❌ FAILED: sustained 14,755 evt/s vs floor 50_000 evt/s.
