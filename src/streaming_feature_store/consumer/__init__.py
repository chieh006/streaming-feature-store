"""Avro-deserializing Kafka consumer for ``EcommerceEvent`` messages."""

from streaming_feature_store.consumer.avro_consumer import (
    AvroEventConsumer,
    avro_dict_to_event,
)

__all__ = ["AvroEventConsumer", "avro_dict_to_event"]
