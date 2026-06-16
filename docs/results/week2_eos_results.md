# Week 2 — Validator Run Results

**Started:** 2026-06-12T04:41:24.851484+00:00
**Ended:** 2026-06-12T06:26:33.800886+00:00
**Source topic:** `e-commerce-events-feed`
**Validated topic:** `validated-events`
**DLQ topic:** `dead-letter-queue`
**Consumer group:** `validator-feed`

## Counters

| Metric | Value |
|---|---|
| Duration | 6308.95 s |
| Consumed | 1_266_400 |
| Validated | 1_266_400 |
| Invalid (total) | 0 |
| Invalid rate | 0.00% — ✅ |
| Sustained consume rate | 201 evt/s |

## Invalid by error class

| Error class | Count |
|---|---:|
| _none_ | 0 |

## Invalid by validator

| Validator | Count |
|---|---:|
| _none_ | 0 |

## Top failing fields

| Field path | Count |
|---|---:|
| _none_ | 0 |

## Validation latency (µs)

| Statistic | Value |
|---|---:|
| p50 | 25.50 |
| p95 | 90.72 |
| p99 | 200.91 |

## Partition skew sanity check

`partition_skew_ratio = max / mean = 1.897` (threshold `< 2.00`) — ✅

| Partition | Messages |
|---:|---:|
| 0 | 107_222 |
| 1 | 97_674 |
| 2 | 81_317 |
| 3 | 78_916 |
| 4 | 91_127 |
| 5 | 89_961 |
| 6 | 144_915 |
| 7 | 83_595 |
| 8 | 200_182 |
| 9 | 86_894 |
| 10 | 99_543 |
| 11 | 105_054 |
