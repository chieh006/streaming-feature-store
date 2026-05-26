"""Unit tests for ``scripts/run_validator.py``.

Focused on the bootstrap helpers — topic ensure and the new
``_ensure_validated_schema_registered`` step that registers the
composite ``EcommerceEvent`` schema under ``f"{validated_topic}-value"``
before the validated-events :class:`AvroEventProducer` is constructed.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import SchemaRegistryConfig
from streaming_feature_store.schemas.loader import SCHEMAS_ROOT

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "run_validator.py"
)


@pytest.fixture(scope="module")
def cli_module():
    """Import ``scripts/run_validator.py`` as a module for direct testing.

    Returns
    -------
    module
        The freshly-imported ``run_validator`` module.
    """
    spec = importlib.util.spec_from_file_location(
        "run_validator_cli", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_validator_cli"] = module
    spec.loader.exec_module(module)
    return module


def test_ensure_validated_schema_registered_calls_registry(cli_module) -> None:
    """Helper should compute the subject and call ``SchemaRegistry.register``."""
    fake_registry = MagicMock()
    fake_registry.register.return_value = 42
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        schema_id = cli_module._ensure_validated_schema_registered(
            SchemaRegistryConfig(),
            validated_topic="validated-events",
        )
    assert schema_id == 42
    fake_registry.register.assert_called_once()
    subject_arg, schema_arg = fake_registry.register.call_args[0]
    assert subject_arg == "validated-events-value"
    assert isinstance(schema_arg, str) and len(schema_arg) > 0


def test_ensure_validated_schema_registered_honors_topic_override(
    cli_module,
) -> None:
    """Custom *validated_topic* must drive the subject name."""
    fake_registry = MagicMock()
    fake_registry.register.return_value = 7
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        cli_module._ensure_validated_schema_registered(
            SchemaRegistryConfig(),
            validated_topic="custom-validated",
        )
    subject_arg = fake_registry.register.call_args[0][0]
    assert subject_arg == "custom-validated-value"


def test_ensure_validated_schema_registered_uses_default_schema_dir(
    cli_module,
) -> None:
    """Default *schema_version_dir* resolves under :data:`SCHEMAS_ROOT`."""
    assert cli_module._VALIDATED_SCHEMA_VERSION_DIR == "ecommerce/v1"
    assert (SCHEMAS_ROOT / "ecommerce" / "v1").is_dir()


def test_ensure_validated_schema_registered_honors_schema_dir_override(
    cli_module, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An overridden *schema_version_dir* must be respected.

    The helper resolves the directory under :data:`SCHEMAS_ROOT`; we patch
    the module-level reference plus the loader / dumper so we can assert
    the override without writing real Avro files.
    """
    fake_registry = MagicMock()
    fake_registry.register.return_value = 1
    captured: dict[str, Path] = {}

    def _fake_load(schema_dir: Path) -> dict:
        captured["dir"] = schema_dir
        return {"type": "record", "name": "X", "fields": []}

    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry), \
        patch.object(cli_module, "load_schema_set", side_effect=_fake_load), \
        patch.object(cli_module, "dump_schema", return_value="{}"), \
        patch.object(cli_module, "SCHEMAS_ROOT", tmp_path):
        cli_module._ensure_validated_schema_registered(
            SchemaRegistryConfig(),
            validated_topic="validated-events",
            schema_version_dir="custom/v9",
        )
    assert captured["dir"] == tmp_path / "custom" / "v9"


def test_ensure_topics_calls_admin_with_retention(cli_module) -> None:
    """``_ensure_topics`` must drive both ensure-topic calls."""
    fake_admin = MagicMock()
    with patch.object(cli_module, "TopicAdmin", return_value=fake_admin):
        cli_module._ensure_topics(
            cli_module._resolve_kafka_config(_args_with_defaults()),
            validated_topic="validated-events",
            dlq_topic="dead-letter-queue",
        )
    topics = [call.args[0] for call in fake_admin.ensure_topic.call_args_list]
    assert topics == ["validated-events", "dead-letter-queue"]


def test_resolve_source_topic_and_group_defaults_feed(cli_module) -> None:
    args = _args_with_defaults(source="feed")
    topic, group = cli_module._resolve_source_topic_and_group(args)
    assert topic == cli_module.DEFAULT_SOURCE_TOPIC
    assert group == cli_module.DEFAULT_GROUP_ID


def test_resolve_source_topic_and_group_defaults_bench(cli_module) -> None:
    args = _args_with_defaults(source="bench")
    topic, group = cli_module._resolve_source_topic_and_group(args)
    assert topic == cli_module._BENCH_SOURCE_TOPIC
    assert group == cli_module._BENCH_GROUP_ID


def test_resolve_source_topic_and_group_honors_overrides(cli_module) -> None:
    args = _args_with_defaults(
        source="feed", source_topic="custom-src", group_id="custom-grp"
    )
    topic, group = cli_module._resolve_source_topic_and_group(args)
    assert topic == "custom-src"
    assert group == "custom-grp"


def test_resolve_kafka_config_honors_bootstrap_override(cli_module) -> None:
    args = _args_with_defaults(bootstrap="kafka.example:9092")
    cfg = cli_module._resolve_kafka_config(args)
    assert cfg.bootstrap_servers == "kafka.example:9092"


def test_resolve_kafka_config_no_override(cli_module) -> None:
    args = _args_with_defaults()
    cfg = cli_module._resolve_kafka_config(args)
    # Default lives on KafkaConfig; just assert the field is populated.
    assert cfg.bootstrap_servers


def test_resolve_registry_config_honors_override(cli_module) -> None:
    args = _args_with_defaults(registry="http://sr.example:8081")
    cfg = cli_module._resolve_registry_config(args)
    assert cfg.url == "http://sr.example:8081"


def test_resolve_registry_config_no_override(cli_module) -> None:
    args = _args_with_defaults()
    cfg = cli_module._resolve_registry_config(args)
    assert cfg.url


def _args_with_defaults(**overrides: object):
    """Build an ``argparse.Namespace`` matching the CLI defaults.

    Parameters
    ----------
    **overrides
        Per-field overrides; unspecified fields fall back to the defaults
        baked into ``scripts/run_validator.py``.
    """
    import argparse

    defaults: dict[str, object] = {
        "source": "feed",
        "source_topic": None,
        "validated_topic": "validated-events",
        "dlq_topic": "dead-letter-queue",
        "group_id": None,
        "poll_timeout_s": 1.0,
        "poll_max_records": 500,
        "flush_timeout_s": 5.0,
        "bootstrap": None,
        "registry": None,
        "ensure_topics": True,
        "report_path": Path("/tmp/report.md"),
        "validator_version": "1.0.0",
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)
