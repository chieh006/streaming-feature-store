"""Unit tests for ``SchemaRegistry`` (mocked underlying client)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming_feature_store.config import SchemaRegistryConfig
from streaming_feature_store.schemas.registry import (
    RegisteredSchema,
    RegistryError,
    SchemaRegistry,
)


@pytest.fixture
def config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(url="http://registry:8081", request_timeout_s=2.5)


@pytest.fixture
def patched_client(config: SchemaRegistryConfig):
    with patch(
        "streaming_feature_store.schemas.registry.SchemaRegistryClient"
    ) as cls:
        instance = MagicMock(name="SchemaRegistryClient")
        cls.return_value = instance
        yield instance, cls


def test_construction_passes_url_and_timeout(
    config: SchemaRegistryConfig, patched_client
) -> None:
    _instance, cls = patched_client
    SchemaRegistry(config)
    cls.assert_called_once_with({"url": config.url, "timeout": config.request_timeout_s})


def test_register_returns_id(config: SchemaRegistryConfig, patched_client) -> None:
    instance, _cls = patched_client
    instance.register_schema.return_value = 42
    reg = SchemaRegistry(config)
    assert reg.register("subj", "{}") == 42


def test_register_wraps_registry_errors(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.register_schema.side_effect = SchemaRegistryError(
        http_status_code=409, error_code=42201, error_message="incompatible"
    )
    reg = SchemaRegistry(config)
    with pytest.raises(RegistryError):
        reg.register("subj", "{}")


def test_get_latest_returns_snapshot(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    rs = MagicMock()
    rs.subject = "subj"
    rs.schema_id = 7
    rs.version = 1
    rs.schema.schema_str = '{"x":1}'
    instance.get_latest_version.return_value = rs
    reg = SchemaRegistry(config)
    out = reg.get_latest("subj")
    assert isinstance(out, RegisteredSchema)
    assert out.schema_id == 7
    assert out.version == 1
    assert out.schema_str == '{"x":1}'


def test_get_latest_wraps_errors(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.get_latest_version.side_effect = SchemaRegistryError(
        http_status_code=404, error_code=40401, error_message="not found"
    )
    reg = SchemaRegistry(config)
    with pytest.raises(RegistryError):
        reg.get_latest("subj")


def test_set_compatibility_calls_client(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    reg = SchemaRegistry(config)
    reg.set_compatibility("subj", "BACKWARD")
    instance.set_compatibility.assert_called_once_with(
        subject_name="subj", level="BACKWARD"
    )


def test_set_compatibility_wraps_errors(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.set_compatibility.side_effect = SchemaRegistryError(
        http_status_code=422, error_code=42203, error_message="bad level"
    )
    reg = SchemaRegistry(config)
    with pytest.raises(RegistryError):
        reg.set_compatibility("subj", "INVALID")


def test_list_subjects_returns_list(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.get_subjects.return_value = ["a", "b"]
    reg = SchemaRegistry(config)
    assert reg.list_subjects() == ["a", "b"]


def test_list_subjects_wraps_errors(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.get_subjects.side_effect = SchemaRegistryError(
        http_status_code=500, error_code=50001, error_message="boom"
    )
    reg = SchemaRegistry(config)
    with pytest.raises(RegistryError):
        reg.list_subjects()


def test_delete_subject_returns_versions(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.delete_subject.return_value = [1, 2, 3]
    reg = SchemaRegistry(config)
    assert reg.delete_subject("subj", permanent=True) == [1, 2, 3]
    instance.delete_subject.assert_called_once_with("subj", permanent=True)


def test_delete_subject_wraps_errors(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    instance.delete_subject.side_effect = SchemaRegistryError(
        http_status_code=404, error_code=40401, error_message="missing"
    )
    reg = SchemaRegistry(config)
    with pytest.raises(RegistryError):
        reg.delete_subject("subj")


def test_client_property_returns_underlying(
    config: SchemaRegistryConfig, patched_client
) -> None:
    instance, _cls = patched_client
    reg = SchemaRegistry(config)
    assert reg.client is instance
