# Week 1 — PostgreSQL Sink Run Results

**Started:** 2026-06-08T00:26:34.768831+00:00
**Ended:** 2026-06-08T01:14:19.684948+00:00
**Topic:** e-commerce-events-feed
**Consumer group:** postgres-sink

## Counters

| Metric | Value |
|---|---|
| Duration | 2864.92 s |
| Consumed | 574_600 |
| Inserted | 574_600 |
| Conflict-skipped | 0 |
| Deserialize failed | 0 |
| Batches flushed | 574 |
| Sustained insert rate | 201 rows/s |

## Batch sizes

| Statistic | Value |
|---|---:|
| p50 | 1,000.0 |
| p99 | 1,096.3 |

## Flush latency (ms)

| Statistic | Value |
|---|---:|
| p50 | 29.70 |
| p95 | 60.37 |
| p99 | 72.58 |

## Partition skew sanity check

`partition_skew_ratio = max / mean = 1.903` (threshold `< 2.00`) — ✅

| Partition | Messages |
|---:|---:|
| 0 | 48_529 |
| 1 | 44_130 |
| 2 | 37_076 |
| 3 | 35_558 |
| 4 | 41_340 |
| 5 | 40_927 |
| 6 | 65_788 |
| 7 | 37_849 |
| 8 | 91_099 |
| 9 | 39_373 |
| 10 | 45_120 |
| 11 | 47_811 |
