"""Integration tests for the BACKWARD-compat schema-evolution drills.

These tests exercise the full path: pin compatibility, register the candidate
schema, produce events under the new writer schema, consume them through the
prior reader schema (and vice versa), and assert the documented Avro
resolution behaviour. They also verify that the documented negative-control
mutations are rejected by the Registry.

Requires the Docker compose stack from PR #1 to be running and the v1
baseline registered (PR #2).
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path

import pytest
import requests

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer import AvroEventConsumer
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.schemas.evolution import (
    add_optional_field,
    promote_field_type,
    remove_field,
)
from streaming_feature_store.schemas.registry import RegistryError

pytestmark = pytest.mark.integration

V1_DIR: Path = SCHEMAS_ROOT / "ecommerce" / "v1"
DRIVER_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "run_schema_evolution.py"
)
EVENTS_PER_DIRECTION = 5
POLL_TIMEOUT_S = 15.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def kafka_config() -> KafkaConfig:
    return KafkaConfig()


@pytest.fixture(scope="module")
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig()


@pytest.fixture(scope="module")
def subject(kafka_config: KafkaConfig) -> str:
    return f"{kafka_config.topic}-value"


@pytest.fixture(scope="module")
def registry(
    docker_services_up: None, registry_config: SchemaRegistryConfig
) -> SchemaRegistry:
    return SchemaRegistry(registry_config)


@pytest.fixture(scope="module")
def baseline_schema_str() -> str:
    return dump_schema(load_schema_set(V1_DIR))


@pytest.fixture(scope="module")
def baseline_composite() -> dict:
    return load_schema_set(V1_DIR)


@pytest.fixture
def clean_evolution_subject(
    registry: SchemaRegistry, subject: str, baseline_schema_str: str
) -> str:
    """Hard-reset the subject and re-register the v1 baseline.

    Ensures every test starts with version 1 = baseline and no leftover
    experiment versions.
    """
    _force_reset_subject(registry, subject)
    registry.register(subject, baseline_schema_str)
    registry.set_compatibility(subject, "BACKWARD")
    yield subject
    _force_reset_subject(registry, subject)
    registry.register(subject, baseline_schema_str)


def _force_reset_subject(registry: SchemaRegistry, subject: str) -> None:
    """Soft-delete then hard-delete a subject, swallowing 404s."""
    for permanent in (False, True):
        try:
            registry.delete_subject(subject, permanent=permanent)
        except RegistryError:
            pass


def _make_event(payload, event_type: EventType) -> EcommerceEvent:
    """Construct a minimal :class:`EcommerceEvent`."""
    from datetime import datetime, timezone

    return EcommerceEvent(
        event_id=uuid.uuid4(),
        event_type=event_type,
        user_id="u-int",
        session_id="s-int",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=payload,
    )


def _produce_events(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    events: list[EcommerceEvent],
) -> None:
    with AvroEventProducer(kafka_config, registry_config) as producer:
        for event in events:
            producer.produce(event)
        producer.flush(timeout_s=10.0)


def _consume_events(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    *,
    reader_schema_str: str | None,
    expected: int,
) -> list[EcommerceEvent]:
    group_id = f"int-test-{uuid.uuid4().hex[:8]}"
    with AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=group_id,
        reader_schema_str=reader_schema_str,
    ) as consumer:
        return consumer.consume(timeout_s=POLL_TIMEOUT_S, max_messages=expected)


# ---------------------------------------------------------------------------
# Compatibility setup
# ---------------------------------------------------------------------------


def test_subject_compatibility_is_backward_after_setup(
    clean_evolution_subject: str, registry_config: SchemaRegistryConfig
) -> None:
    response = requests.get(
        f"{registry_config.url}/config/{clean_evolution_subject}",
        timeout=registry_config.request_timeout_s,
    )
    response.raise_for_status()
    assert response.json()["compatibilityLevel"] == "BACKWARD"


# ---------------------------------------------------------------------------
# Drill 1 — add optional field
# ---------------------------------------------------------------------------


def test_drill1_add_optional_field_is_accepted(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
) -> None:
    candidate = add_optional_field(
        baseline_composite, name="device_type", avro_type="string"
    )
    schema_id = registry.register(
        clean_evolution_subject, dump_schema(candidate)
    )
    assert schema_id > 0
    latest = registry.get_latest(clean_evolution_subject)
    assert "device_type" in latest.schema_str
    assert latest.version >= 2


def test_drill1_round_trip_in_both_directions(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
    baseline_schema_str: str,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    candidate = add_optional_field(
        baseline_composite, name="device_type", avro_type="string"
    )
    registry.register(clean_evolution_subject, dump_schema(candidate))

    events = [
        _make_event(
            ClickPayload(element_id=f"btn-{i}", page_url="/x"), EventType.CLICK
        )
        for i in range(EVENTS_PER_DIRECTION)
    ]
    _produce_events(kafka_config, registry_config, events)

    new_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=None,
        expected=EVENTS_PER_DIRECTION,
    )
    assert len(new_consumed) == EVENTS_PER_DIRECTION

    old_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=baseline_schema_str,
        expected=EVENTS_PER_DIRECTION,
    )
    assert len(old_consumed) == EVENTS_PER_DIRECTION


# ---------------------------------------------------------------------------
# Drill 2 — remove defaulted field
# ---------------------------------------------------------------------------


def test_drill2_remove_defaulted_field_is_accepted(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
) -> None:
    candidate = remove_field(
        baseline_composite, record_name="PageViewPayload", field="referrer"
    )
    schema_id = registry.register(
        clean_evolution_subject, dump_schema(candidate)
    )
    assert schema_id > 0


def test_drill2_round_trip_in_both_directions(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
    baseline_schema_str: str,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    candidate = remove_field(
        baseline_composite, record_name="PageViewPayload", field="referrer"
    )
    registry.register(clean_evolution_subject, dump_schema(candidate))

    events = [
        _make_event(
            PageViewPayload(page_url=f"/p{i}", referrer=None),
            EventType.PAGE_VIEW,
        )
        for i in range(EVENTS_PER_DIRECTION)
    ]
    _produce_events(kafka_config, registry_config, events)

    new_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=None,
        expected=EVENTS_PER_DIRECTION,
    )
    old_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=baseline_schema_str,
        expected=EVENTS_PER_DIRECTION,
    )
    assert len(new_consumed) == EVENTS_PER_DIRECTION
    assert len(old_consumed) == EVENTS_PER_DIRECTION
    # Old reader should fill referrer from the field's default (null)
    assert all(e.payload.referrer is None for e in old_consumed)  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Drill 3 — promote int to long
# ---------------------------------------------------------------------------


def test_drill3_promote_int_to_long_is_accepted(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
) -> None:
    candidate = promote_field_type(
        baseline_composite,
        record_name="PurchasePayload",
        field="quantity",
        new_type="long",
    )
    schema_id = registry.register(
        clean_evolution_subject, dump_schema(candidate)
    )
    assert schema_id > 0


def test_drill3_in_range_round_trip_succeeds(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
    baseline_schema_str: str,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    candidate = promote_field_type(
        baseline_composite,
        record_name="PurchasePayload",
        field="quantity",
        new_type="long",
    )
    registry.register(clean_evolution_subject, dump_schema(candidate))

    events = [
        _make_event(
            PurchasePayload(
                product_id=f"sku-{i}", quantity=i + 1, price_cents=999
            ),
            EventType.PURCHASE,
        )
        for i in range(EVENTS_PER_DIRECTION)
    ]
    _produce_events(kafka_config, registry_config, events)

    new_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=None,
        expected=EVENTS_PER_DIRECTION,
    )
    old_consumed = _consume_events(
        kafka_config,
        registry_config,
        reader_schema_str=baseline_schema_str,
        expected=EVENTS_PER_DIRECTION,
    )
    assert len(new_consumed) == EVENTS_PER_DIRECTION
    assert len(old_consumed) == EVENTS_PER_DIRECTION


# ---------------------------------------------------------------------------
# Negative controls
# ---------------------------------------------------------------------------


def test_negative_control_add_required_field_is_rejected(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
) -> None:
    """A new required field with no default must be rejected by BACKWARD."""
    import copy

    bad = copy.deepcopy(baseline_composite)
    bad["fields"].append(
        {"name": "tenant_id", "type": "string"}  # no default
    )
    with pytest.raises(RegistryError) as exc_info:
        registry.register(clean_evolution_subject, dump_schema(bad))
    assert "incompatible" in str(exc_info.value).lower()


def test_negative_control_remove_no_default_field_is_rejected(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    baseline_composite: dict,
) -> None:
    bad = remove_field(
        baseline_composite,
        record_name="EcommerceEvent",
        field="event_id",
        force=True,
    )
    with pytest.raises(RegistryError) as exc_info:
        registry.register(clean_evolution_subject, dump_schema(bad))
    assert "incompatible" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Driver / report
# ---------------------------------------------------------------------------


def _load_driver_module():
    """Import the driver script as an importable module."""
    spec = importlib.util.spec_from_file_location(
        "run_schema_evolution_int", DRIVER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_schema_evolution_int"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_driver_writes_report_with_three_passing_drills(
    clean_evolution_subject: str,
    tmp_path: Path,
) -> None:
    driver = _load_driver_module()
    report_path = tmp_path / "report.md"
    rc = driver.main(
        [
            "--drill",
            "all",
            "--report-path",
            str(report_path),
            "--snapshot-root",
            str(tmp_path / "snapshots"),
        ]
    )
    assert rc == 0
    body = report_path.read_text(encoding="utf-8")
    assert "drill1" in body
    assert "drill2" in body
    assert "drill3" in body
    assert body.count("[OK]") >= 3


def test_driver_cleanup_restores_baseline(
    clean_evolution_subject: str,
    registry: SchemaRegistry,
    tmp_path: Path,
) -> None:
    driver = _load_driver_module()
    rc = driver.main(
        [
            "--drill",
            "1",
            "--report-path",
            str(tmp_path / "r.md"),
            "--snapshot-root",
            str(tmp_path / "s"),
        ]
    )
    assert rc == 0
    # After cleanup, the live latest version should be the baseline schema
    latest = registry.get_latest(clean_evolution_subject)
    assert "device_type" not in latest.schema_str
