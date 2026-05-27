# Week 1 — PostgreSQL Sink Run Results

**Started:** 2026-05-26T05:27:01.222543+00:00
**Ended:** 2026-05-26T06:29:26.147387+00:00
**Topic:** e-commerce-events-feed
**Consumer group:** postgres-sink

## Counters

| Metric | Value |
|---|---|
| Duration | 3744.92 s |
| Consumed | 755_000 |
| Inserted | 755_000 |
| Conflict-skipped | 0 |
| Deserialize failed | 0 |
| Batches flushed | 754 |
| Sustained insert rate | 202 rows/s |

## Batch sizes

| Statistic | Value |
|---|---:|
| p50 | 1,000.0 |
| p99 | 1,001.9 |

## Flush latency (ms)

| Statistic | Value |
|---|---:|
| p50 | 34.53 |
| p95 | 66.71 |
| p99 | 77.18 |

## Partition skew sanity check

`partition_skew_ratio = max / mean = 1.902` (threshold `< 2.00`) — ✅

| Partition | Messages |
|---:|---:|
| 0 | 63_824 |
| 1 | 58_142 |
| 2 | 48_634 |
| 3 | 46_828 |
| 4 | 54_500 |
| 5 | 53_830 |
| 6 | 86_047 |
| 7 | 49_857 |
| 8 | 119_673 |
| 9 | 51_660 |
| 10 | 59_416 |
| 11 | 62_589 |
