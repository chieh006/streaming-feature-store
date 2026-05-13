# Week 1 — Synthetic Event Load Test Results

**Generated:** 2026-05-13T05:13:36.350411+00:00
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
| Produced | 148_480 |
| Acked | 148_480 |
| Failed | 0 |
| Sustained rate | 14,364 evt/s ❌ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 29.1 / 169.1 / 242.1 ms |
| Errors by class | {} |
| Wallclock | 10.34 s |

## Verdict

❌ FAILED: sustained 14,364 evt/s vs floor 50_000 evt/s.
