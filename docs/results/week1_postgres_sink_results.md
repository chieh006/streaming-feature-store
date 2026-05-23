# Week 1 — PostgreSQL Sink Run Results

**Started:** 2026-05-23T05:35:00.426034+00:00
**Ended:** 2026-05-23T05:39:08.869259+00:00
**Topic:** e-commerce-events-feed
**Consumer group:** postgres-sink

## Counters

| Metric | Value |
|---|---|
| Duration | 248.44 s |
| Consumed | 111_600 |
| Inserted | 111_600 |
| Conflict-skipped | 0 |
| Deserialize failed | 0 |
| Batches flushed | 96 |
| Sustained insert rate | 449 rows/s |

## Batch sizes

| Statistic | Value |
|---|---:|
| p50 | 1,200.0 |
| p99 | 1,400.0 |

## Flush latency (ms)

| Statistic | Value |
|---|---:|
| p50 | 36.79 |
| p95 | 69.72 |
| p99 | 80.20 |

## Partition skew sanity check

`partition_skew_ratio = max / mean = 1.910` (threshold `< 2.00`) — ✅

| Partition | Messages |
|---:|---:|
| 0 | 9_419 |
| 1 | 8_628 |
| 2 | 7_039 |
| 3 | 6_920 |
| 4 | 8_030 |
| 5 | 7_910 |
| 6 | 12_457 |
| 7 | 7_607 |
| 8 | 17_760 |
| 9 | 7_599 |
| 10 | 8_890 |
| 11 | 9_341 |
