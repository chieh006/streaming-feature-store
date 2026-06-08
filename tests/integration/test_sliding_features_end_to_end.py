"""End-to-end integration tests for the sliding-window features consumer.

Each test stands up a per-test source / sink / late topic set, registers the
source + late value subjects with the composite ``EcommerceEvent`` schema,
produces a small burst of events with crafted *event* timestamps, then runs
:class:`SlidingFeaturesConsumer` in a background thread (with the cold-start
seek-back rewinding to the pre-produced events) until the expected feature
records appear on ``sliding-features`` and the Redis hash — or a hard timeout
fires.  Requires a live Kafka + Schema Registry + Redis; skipped otherwise.
"""

from __future__ import annotations

import importlib.util
import logging
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import redis
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)
from streaming_feature_store.schemas.registry import SchemaRegistry
from streaming_feature_store.sliding.consumer import SlidingFeaturesConsumer
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    SlidingFeatureRecord,
)
from streaming_feature_store.sliding.sinks import load_sliding_schema_str

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_HOST = "localhost"
_BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
_REGISTRY_URL = "http://localhost:8081"
_REDIS_PORT = 6379
_MINUTE_MS = 60_000

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _tcp_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return ``True`` if a TCP connection to *host:port* succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def live_infra() -> None:
    """Skip the module unless Kafka, Schema Registry, and Redis are reachable."""
    checks = {
        "kafka": _tcp_open(_HOST, 19092),
        "schema-registry": _tcp_open(_HOST, 8081),
        "redis": _tcp_open(_HOST, _REDIS_PORT),
    }
    missing = [name for name, ok in checks.items() if not ok]
    if missing:
        pytest.skip(f"sliding integration needs live {missing}; run 'make infra-up'")


def _load_register_module():
    """Import the ``register_schemas`` script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_slidingtest", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def ids() -> dict[str, str]:
    """Per-test topic / group / user namespacing."""
    tag = uuid.uuid4().hex[:8]
    return {
        "source": f"sliding-src-{tag}",
        "sink": f"sliding-feat-{tag}",
        "late": f"sliding-late-{tag}",
        "group": f"sliding-grp-{tag}",
        "user": f"u-{tag}",
    }


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(bootstrap_servers=_BOOTSTRAP)


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(url=_REGISTRY_URL)


@pytest.fixture
def consumer_config(ids: dict[str, str]) -> SlidingConsumerConfig:
    return SlidingConsumerConfig(
        bootstrap=_BOOTSTRAP,
        registry_url=_REGISTRY_URL,
        source_topic=ids["source"],
        sink_topic=ids["sink"],
        late_sink_topic=ids["late"],
        consumer_group=ids["group"],
        redis_host=_HOST,
        redis_port=_REDIS_PORT,
        warmup_seek_back=True,
        poll_timeout_seconds=0.5,
    )


@pytest.fixture
def prepared(
    live_infra, kafka_config, registry_config, consumer_config, ids
):
    """Create topics, register the source + late ecommerce subjects, then clean up."""
    admin = TopicAdmin(kafka_config)
    admin.ensure_topic(ids["source"], num_partitions=3, replication_factor=3)
    admin.ensure_topic(ids["sink"], num_partitions=3, replication_factor=3)
    admin.ensure_topic(ids["late"], num_partitions=3, replication_factor=3)

    register_cli = _load_register_module()
    for subject in (f"{ids['source']}-value", f"{ids['late']}-value"):
        assert register_cli.main(["--subject", subject]) == 0
    registry = SchemaRegistry(registry_config)
    registry.register(f"{ids['sink']}-value", load_sliding_schema_str())

    yield ids

    for name in (ids["source"], ids["sink"], ids["late"]):
        try:
            admin.delete_topic(name)
        except Exception:  # pragma: no cover - best-effort teardown
            logger.warning(f"could not delete topic {name}")
    for subject in (
        f"{ids['source']}-value",
        f"{ids['sink']}-value",
        f"{ids['late']}-value",
    ):
        for permanent in (False, True):
            try:
                registry.delete_subject(subject, permanent=permanent)
            except Exception:  # pragma: no cover
                pass


@pytest.fixture
def redis_client():
    client = redis.Redis(host=_HOST, port=_REDIS_PORT)
    yield client
    client.close()


# ---------------------------------------------------------------------------
# Event + driver helpers
# ---------------------------------------------------------------------------


def _ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _click(user: str, ts_ms: int) -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=ClickPayload(element_id="b", page_url="/h"),
    )


def _page_view(user: str, ts_ms: int, product_marker: str) -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PAGE_VIEW,
        user_id=user,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=PageViewPayload(page_url=f"/p/{product_marker}", referrer=None),
    )


def _purchase(user: str, ts_ms: int, product_id: str, price_cents: int = 1000) -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id=user,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=PurchasePayload(product_id=product_id, quantity=1, price_cents=price_cents),
    )


def _aligned_base() -> int:
    """Return a minute-aligned event-time base a few minutes in the past."""
    now_ms = int(time.time() * 1000) - 10 * _MINUTE_MS
    return (now_ms // _MINUTE_MS) * _MINUTE_MS


def _produce(kafka_config, registry_config, topic, events) -> None:
    producer = AvroEventProducer(kafka_config, registry_config, topic=topic)
    try:
        for event in events:
            producer.produce(event)
        assert producer.flush(10.0) == 0
    finally:
        producer.close()


class _ConsumerThread:
    """Run a :class:`SlidingFeaturesConsumer` in a background thread."""

    def __init__(self, config: SlidingConsumerConfig) -> None:
        self._consumer = SlidingFeaturesConsumer(config)
        self._thread = threading.Thread(target=self._consumer.run, daemon=True)

    def __enter__(self) -> "_ConsumerThread":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._consumer.request_shutdown()
        self._thread.join(timeout=15.0)


def _drain_records(
    kafka_config, registry_config, topic, *, expected: int, timeout_s: float = 25.0
) -> list[SlidingFeatureRecord]:
    """Drain *topic* and decode :class:`SlidingFeatureRecord` values."""
    registry = SchemaRegistry(registry_config)
    deserializer = AvroDeserializer(schema_registry_client=registry.client)
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": f"drain-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    records: list[SlidingFeatureRecord] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and len(records) < expected:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error() is not None:
                continue
            ctx = SerializationContext(topic, MessageField.VALUE)
            records.append(
                SlidingFeatureRecord.from_avro_dict(deserializer(msg.value(), ctx))
            )
    finally:
        consumer.close()
    return records


def _records_for_resolution(records, resolution_value):
    return [r for r in records if r.window_resolution.value == resolution_value]


# ---------------------------------------------------------------------------
# Topic + schema bootstrap
# ---------------------------------------------------------------------------


def test_sink_topic_and_sliding_schema_registered(
    prepared, kafka_config, registry_config, consumer_config, ids
):
    """Constructing the consumer registers the sliding-feature subject BACKWARD."""
    consumer = SlidingFeaturesConsumer(consumer_config)
    try:
        registry = SchemaRegistry(registry_config)
        latest = registry.get_latest(f"{ids['sink']}-value")
        assert "SlidingFeatureRecord" in latest.schema_str
        assert "W_5M_SLIDE_1M" in latest.schema_str
    finally:
        consumer._shutdown_sinks()  # noqa: SLF001 - release resources without a poll loop


# ---------------------------------------------------------------------------
# 5 m resolution: emission cadence + counts
# ---------------------------------------------------------------------------


def test_5m_window_emits_and_counts_clicks(
    prepared, kafka_config, registry_config, consumer_config, ids
):
    """7 clicks in one minute → at least one 5 m record with click_count == 7."""
    base = _aligned_base()
    user = ids["user"]
    events = [_click(user, base + i * 100) for i in range(7)]
    # Drive event-time forward so the watermark crosses several slide ticks.
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(1, 7)]
    _produce(kafka_config, registry_config, ids["source"], events)

    with _ConsumerThread(consumer_config):
        records = _drain_records(
            kafka_config, registry_config, ids["sink"], expected=5
        )

    five_m = _records_for_resolution(records, "5m")
    assert len(five_m) >= 5  # one per slide tick (design §2.5)
    assert max(r.click_count for r in five_m) == 7


def test_1h_window_counts_distinct_products(
    prepared, kafka_config, registry_config, consumer_config, ids
):
    """Purchases of 3 distinct products within an hour → distinct_products == 3."""
    base = _aligned_base()
    user = ids["user"]
    events = [
        _purchase(user, base + 1000, "A"),
        _purchase(user, base + 2000, "B"),
        _purchase(user, base + 3000, "A"),
        _purchase(user, base + 4000, "C"),
    ]
    # Advance event-time ~11 min so a 1 h slide tick (5 min) fires.
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(1, 12)]
    _produce(kafka_config, registry_config, ids["source"], events)

    with _ConsumerThread(consumer_config):
        records = _drain_records(
            kafka_config, registry_config, ids["sink"], expected=3
        )

    one_h = _records_for_resolution(records, "1h")
    assert one_h, "expected at least one 1 h record"
    assert max(r.distinct_products for r in one_h) == 3


def test_24h_record_excludes_click_count(
    prepared, kafka_config, registry_config, consumer_config, ids
):
    """A 24 h record never carries click_count (design §2.14)."""
    base = _aligned_base() - 23 * 60 * _MINUTE_MS  # start ~23 h ago
    user = ids["user"]
    events = [_purchase(user, base + 1000, "A")]
    # Advance event-time by >1 h so a 24 h slide tick (1 h) fires.
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(1, 66)]
    _produce(kafka_config, registry_config, ids["source"], events)

    with _ConsumerThread(consumer_config):
        records = _drain_records(
            kafka_config, registry_config, ids["sink"], expected=1
        )

    twenty_four_h = _records_for_resolution(records, "24h")
    assert twenty_four_h, "expected at least one 24 h record"
    assert all(r.click_count is None for r in twenty_four_h)
    assert all(r.page_view_count is None for r in twenty_four_h)


# ---------------------------------------------------------------------------
# Redis online store
# ---------------------------------------------------------------------------


def test_redis_hash_carries_resolution_suffixed_fields(
    prepared, kafka_config, registry_config, consumer_config, redis_client, ids
):
    """After traffic the per-user Redis hash carries _5m / _1h fields with a TTL."""
    base = _aligned_base()
    user = ids["user"]
    events = [_purchase(user, base + 1000, "A")]
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(1, 12)]
    _produce(kafka_config, registry_config, ids["source"], events)

    with _ConsumerThread(consumer_config):
        _drain_records(kafka_config, registry_config, ids["sink"], expected=3)

    key = f"feat:user:{user}"
    fields = {k.decode(): v.decode() for k, v in redis_client.hgetall(key).items()}
    assert any(name.endswith("_5m") for name in fields)
    assert any(name.endswith("_1h") for name in fields)
    assert redis_client.ttl(key) > 0


# ---------------------------------------------------------------------------
# Allowed lateness re-emission
# ---------------------------------------------------------------------------


def test_late_event_re_emits_with_higher_seq(
    prepared, kafka_config, registry_config, consumer_config, ids
):
    """A within-lateness late click re-fires its window with emission_seq >= 1."""
    base = _aligned_base()
    user = ids["user"]
    target_minute = 5
    events = [_click(user, base + target_minute * _MINUTE_MS) for _ in range(3)]
    # Close the window covering minute 5, then add a within-lateness late click.
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(6, 9)]
    events.append(_click(user, base + target_minute * _MINUTE_MS + 20_000))
    events += [_click(user, base + minute * _MINUTE_MS) for minute in range(9, 11)]
    _produce(kafka_config, registry_config, ids["source"], events)

    with _ConsumerThread(consumer_config):
        records = _drain_records(
            kafka_config, registry_config, ids["sink"], expected=6
        )

    window_end = base + (target_minute + 1) * _MINUTE_MS
    for_window = [
        r
        for r in _records_for_resolution(records, "5m")
        if r.window_end_ms == window_end
    ]
    assert any(r.emission_seq >= 1 for r in for_window)
