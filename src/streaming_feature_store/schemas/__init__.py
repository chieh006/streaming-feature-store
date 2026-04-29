"""Avro schema loading, registration, and Pydantic event models."""

from streaming_feature_store.schemas.loader import (
    SCHEMAS_ROOT,
    SchemaLoadError,
    dump_schema,
    load_avro_file,
    load_schema_set,
)
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)
from streaming_feature_store.schemas.registry import (
    RegisteredSchema,
    SchemaRegistry,
)

__all__ = [
    "SCHEMAS_ROOT",
    "ClickPayload",
    "EcommerceEvent",
    "EventType",
    "PageViewPayload",
    "PurchasePayload",
    "RegisteredSchema",
    "SchemaLoadError",
    "SchemaRegistry",
    "dump_schema",
    "load_avro_file",
    "load_schema_set",
]
