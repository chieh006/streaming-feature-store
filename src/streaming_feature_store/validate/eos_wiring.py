"""Build the validator's transactional EOS producer + routes + strategy.

Both the single-process CLI (``scripts/run_validator.py``) and each
multi-process worker (``validate_mp/worker_entry.py``) need the *same* EOS
wiring: one :class:`~streaming_feature_store.eos.TransactionalAvroProducer`
serving both ``validated-events`` (``EcommerceEvent`` Avro) and
``dead-letter-queue`` (``DlqRecord`` Avro), the two route adapters over it, and
a :class:`~streaming_feature_store.eos.TransactionalCommit` strategy bound to
it (design week2_03 §2.4).  Factoring it here keeps the single- and
multi-process paths identical and unit-testable.
"""

from __future__ import annotations

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.eos import (
    TransactionalAvroProducer,
    TransactionalCommit,
    TransactionalConfig,
    TransactionalDlqRoute,
    TransactionalValidatedRoute,
    transactional_producer_conf,
)
from streaming_feature_store.producer.avro_producer import _event_to_dict
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.validate.dlq import _dlq_to_dict, load_dlq_schema_str

# Subpath under ``schemas/`` for the composite EcommerceEvent schema bound to
# the ``validated-events`` topic.
VALIDATED_SCHEMA_VERSION_DIR: str = "ecommerce/v1"


def build_validator_txn_producer(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    *,
    validated_topic: str,
    dlq_topic: str,
    txn_config: TransactionalConfig,
) -> TransactionalAvroProducer:
    """Build the single multi-topic transactional producer (design §2.4).

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    validated_topic : str
        Topic for valid events (``EcommerceEvent`` Avro).
    dlq_topic : str
        Dead-letter-queue topic (``DlqRecord`` Avro).
    txn_config : TransactionalConfig
        EOS knobs supplying the ``transactional.id``.

    Returns
    -------
    TransactionalAvroProducer
        One producer whose per-topic serializer registry lets both topics
        commit inside a single transaction.
    """
    registry = SchemaRegistry(registry_config)
    event_serializer = AvroSerializer(
        schema_registry_client=registry.client,
        schema_str=dump_schema(
            load_schema_set(SCHEMAS_ROOT / VALIDATED_SCHEMA_VERSION_DIR)
        ),
        to_dict=_event_to_dict,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    dlq_serializer = AvroSerializer(
        schema_registry_client=registry.client,
        schema_str=load_dlq_schema_str(),
        to_dict=_dlq_to_dict,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )

    def _topic_serializer(serializer: AvroSerializer, topic: str):
        ctx = SerializationContext(topic, MessageField.VALUE)
        return lambda value: serializer(value, ctx)

    serializers = {
        validated_topic: _topic_serializer(event_serializer, validated_topic),
        dlq_topic: _topic_serializer(dlq_serializer, dlq_topic),
    }
    return TransactionalAvroProducer(
        serializers, conf=transactional_producer_conf(kafka_config, txn_config)
    )


def build_validator_eos(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    *,
    validated_topic: str,
    dlq_topic: str,
    txn_config: TransactionalConfig,
) -> tuple[
    TransactionalValidatedRoute, TransactionalDlqRoute, TransactionalCommit
]:
    """Build the EOS route producers + commit strategy for the validator.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    validated_topic, dlq_topic : str
        The two output topics produced atomically (design §2.4).
    txn_config : TransactionalConfig
        EOS knobs (transactional id + commit timeout).

    Returns
    -------
    tuple
        ``(validated_route, dlq_route, commit_strategy)`` — the two adapters
        share one transactional producer, and the strategy commits it.
    """
    producer = build_validator_txn_producer(
        kafka_config,
        registry_config,
        validated_topic=validated_topic,
        dlq_topic=dlq_topic,
        txn_config=txn_config,
    )
    validated_route = TransactionalValidatedRoute(producer, validated_topic)
    dlq_route = TransactionalDlqRoute(producer, dlq_topic)
    strategy = TransactionalCommit(producer, txn_config.commit_timeout_s)
    return validated_route, dlq_route, strategy
