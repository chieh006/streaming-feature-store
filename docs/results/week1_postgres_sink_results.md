# Week 1 — PostgreSQL Sink Run Results

**Started:** 2026-06-07T07:04:10.888990+00:00
**Ended:** 2026-06-07T07:11:00.805131+00:00
**Topic:** e-commerce-events-feed
**Consumer group:** postgres-sink

## Counters

| Metric | Value |
|---|---|
| Duration | 409.92 s |
| Consumed | 66_372 |
| Inserted | 66_352 |
| Conflict-skipped | 20 |
| Deserialize failed | 0 |
| Batches flushed | 68 |
| Sustained insert rate | 162 rows/s |

## Batch sizes

| Statistic | Value |
|---|---:|
| p50 | 1,000.0 |
| p99 | 1,192.8 |

## Flush latency (ms)

| Statistic | Value |
|---|---:|
| p50 | 55.00 |
| p95 | 282.77 |
| p99 | 27850.01 |

## Partition skew sanity check

`partition_skew_ratio = max / mean = 1.896` (threshold `< 2.00`) — ✅

| Partition | Messages |
|---:|---:|
| 0 | 5_626 |
| 1 | 5_138 |
| 2 | 4_310 |
| 3 | 4_078 |
| 4 | 4_783 |
| 5 | 4_658 |
| 6 | 7_619 |
| 7 | 4_428 |
| 8 | 10_486 |
| 9 | 4_410 |
| 10 | 5_283 |
| 11 | 5_553 |
