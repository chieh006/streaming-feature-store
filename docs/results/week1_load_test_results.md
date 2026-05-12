# Week 1 — Synthetic Event Load Test Results

**Generated:** 2026-05-12T05:15:23.990700+00:00
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
| Produced | 65_536 |
| Acked | 65_536 |
| Failed | 0 |
| Sustained rate | 5,949 evt/s ❌ (floor 50_000) |
| Ack latency p50 / p95 / p99 | 22.0 / 830.6 / 894.2 ms |
| Errors by class | {} |
| Wallclock | 11.02 s |

## Verdict

❌ FAILED: sustained 5,949 evt/s vs floor 50_000 evt/s.
