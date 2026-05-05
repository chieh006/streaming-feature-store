# Week 1 — Schema Evolution Drill Results

**Generated:** 2026-05-05T06:21:25.615840+00:00
**Subject:** e-commerce-events-value
**Compatibility level:** BACKWARD
## drill1 — Add optional `device_type` field to EcommerceEvent

| Field | Value |
|---|---|
| Mutation | `{'kind': 'add_optional_field', 'record': 'EcommerceEvent', 'field': 'device_type', 'avro_type': 'string'}` |
| Registration | [OK] accepted (schema_id=2, version=2) |
| Serde producer=v1,consumer=v2 | ok (5/5) |
| Serde producer=v2,consumer=v1 | ok (5/5) |

## drill2 — Remove defaulted `referrer` field from PageViewPayload

| Field | Value |
|---|---|
| Mutation | `{'kind': 'remove_field', 'record': 'PageViewPayload', 'field': 'referrer'}` |
| Registration | [OK] accepted (schema_id=3, version=2) |
| Serde producer=v1,consumer=v2 | ok (5/5) |
| Serde producer=v2,consumer=v1 | ok (5/5) |

## drill3 — Promote `PurchasePayload.quantity` from int to long

| Field | Value |
|---|---|
| Mutation | `{'kind': 'promote_field_type', 'record': 'PurchasePayload', 'field': 'quantity', 'new_type': 'long'}` |
| Registration | [OK] accepted (schema_id=4, version=2) |
| Serde producer=v1,consumer=v2 | ok (5/5) |
| Serde producer=v2,consumer=v1 | error: KafkaError{code=_VALUE_DESERIALIZATION,val=-159,str="Schema mismatch: long is not int"} |
