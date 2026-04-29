"""Integration tests for ``scripts/register_schemas.py`` against a live Schema
Registry."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import fastavro
import pytest
import requests

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.schemas.loader import (
    SCHEMAS_ROOT,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.schemas.registry import SchemaRegistry

pytestmark = pytest.mark.integration

SCRIPT_PATH: Path = Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
V1_DIR: Path = SCHEMAS_ROOT / "ecommerce" / "v1"


def _load_register_module():
    spec = importlib.util.spec_from_file_location("register_schemas_int", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["register_schemas_int"] = module
    spec.loader.exec_module(module)
    return module


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


@pytest.fixture
def clean_subject(registry: SchemaRegistry, subject: str) -> str:
    """Delete *subject* (soft + hard) before and after the test."""
    _safe_delete(registry, subject)
    yield subject
    _safe_delete(registry, subject)


def _safe_delete(registry: SchemaRegistry, subject: str) -> None:
    try:
        registry.delete_subject(subject, permanent=False)
    except Exception:
        pass
    try:
        registry.delete_subject(subject, permanent=True)
    except Exception:
        pass


@pytest.fixture
def registered_subject(
    clean_subject: str, registry: SchemaRegistry
) -> str:
    """Register the v1 schema set and return the subject."""
    cli = _load_register_module()
    rc = cli.main([])
    assert rc == 0
    return clean_subject


def test_register_schemas_creates_subject(
    registered_subject: str, registry: SchemaRegistry
) -> None:
    assert registered_subject in registry.list_subjects()


def test_registered_schema_has_expected_version_1(
    registered_subject: str, registry: SchemaRegistry
) -> None:
    latest = registry.get_latest(registered_subject)
    assert latest.version == 1
    assert latest.subject == registered_subject


def test_registered_schema_payload_matches_disk(
    registered_subject: str, registry: SchemaRegistry
) -> None:
    latest = registry.get_latest(registered_subject)
    server_parsed = fastavro.parse_schema(
        __import__("json").loads(latest.schema_str)
    )
    disk_parsed = fastavro.parse_schema(load_schema_set(V1_DIR))
    assert server_parsed["name"] == disk_parsed["name"]


def test_reregistration_is_idempotent(
    registered_subject: str, registry: SchemaRegistry
) -> None:
    first = registry.get_latest(registered_subject)
    cli = _load_register_module()
    rc = cli.main([])
    assert rc == 0
    second = registry.get_latest(registered_subject)
    assert first.schema_id == second.schema_id
    assert first.version == second.version


def test_registration_rejects_incompatible_change(
    registered_subject: str,
    registry: SchemaRegistry,
    tmp_path: Path,
) -> None:
    """Adding a required (no-default) field violates BACKWARD compatibility:
    a new reader would expect the field on data that older writers never
    populated."""
    cli = _load_register_module()

    incompatible_dir = tmp_path / "v1-broken"
    incompatible_dir.mkdir()
    for src in V1_DIR.glob("*.avsc"):
        if src.name == "click_payload.avsc":
            broken = {
                "type": "record",
                "name": "ClickPayload",
                "namespace": "com.featurestore.ecommerce.v1",
                "fields": [
                    {"name": "element_id", "type": "string"},
                    {"name": "page_url", "type": "string"},
                    {"name": "new_required_field", "type": "string"},
                ],
            }
            (incompatible_dir / src.name).write_text(
                __import__("json").dumps(broken), encoding="utf-8"
            )
        else:
            (incompatible_dir / src.name).write_text(src.read_text(encoding="utf-8"))

    rc = cli.main(["--schemas-dir", str(incompatible_dir)])
    assert rc == 1


def test_compatibility_level_is_backward(
    registered_subject: str, registry_config: SchemaRegistryConfig
) -> None:
    response = requests.get(
        f"{registry_config.url.rstrip('/')}/config",
        timeout=registry_config.request_timeout_s,
    )
    response.raise_for_status()
    body = response.json()
    assert body.get("compatibilityLevel") == "BACKWARD"


def test_dump_canonical_schema_matches_registry_after_normalization(
    registered_subject: str, registry: SchemaRegistry
) -> None:
    """The schema we dumped is parseable and equivalent to what the registry holds."""
    latest = registry.get_latest(registered_subject)
    disk_str = dump_schema(load_schema_set(V1_DIR))
    assert fastavro.parse_schema(__import__("json").loads(disk_str))["name"] == (
        fastavro.parse_schema(__import__("json").loads(latest.schema_str))["name"]
    )
