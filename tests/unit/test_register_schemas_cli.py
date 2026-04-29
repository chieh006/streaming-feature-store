"""Unit tests for ``scripts/register_schemas.py``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig
from streaming_feature_store.schemas.loader import SCHEMAS_ROOT
from streaming_feature_store.schemas.registry import RegistryError

SCRIPT_PATH: Path = Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"


@pytest.fixture(scope="module")
def cli_module():
    spec = importlib.util.spec_from_file_location("register_schemas_cli", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["register_schemas_cli"] = module
    spec.loader.exec_module(module)
    return module


def test_resolve_schemas_dir_uses_explicit(cli_module, tmp_path: Path) -> None:
    assert cli_module._resolve_schemas_dir(tmp_path) == tmp_path


def test_resolve_schemas_dir_picks_latest(cli_module) -> None:
    out = cli_module._resolve_schemas_dir(None)
    assert out == SCHEMAS_ROOT / "ecommerce" / "v1"


def test_resolve_schemas_dir_raises_when_missing(
    cli_module, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "SCHEMAS_ROOT", tmp_path)
    with pytest.raises(FileNotFoundError):
        cli_module._resolve_schemas_dir(None)


def test_resolve_subject_explicit(cli_module) -> None:
    assert cli_module._resolve_subject("custom-value", KafkaConfig()) == "custom-value"


def test_resolve_subject_default(cli_module) -> None:
    cfg = KafkaConfig(topic="my-topic")
    assert cli_module._resolve_subject(None, cfg) == "my-topic-value"


def test_main_dry_run_does_not_register(cli_module) -> None:
    with patch.object(cli_module, "SchemaRegistry") as registry_cls:
        rc = cli_module.main(["--dry-run"])
    assert rc == 0
    registry_cls.assert_not_called()


def test_main_full_run_registers_and_returns_zero(cli_module) -> None:
    fake_registry = MagicMock()
    fake_registry.register.return_value = 99
    fake_latest = MagicMock(version=1)
    fake_registry.get_latest.return_value = fake_latest
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        rc = cli_module.main([])
    assert rc == 0
    fake_registry.register.assert_called_once()


def test_main_returns_one_on_registry_error(cli_module) -> None:
    fake_registry = MagicMock()
    fake_registry.register.side_effect = RegistryError("incompatible")
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        rc = cli_module.main([])
    assert rc == 1


def test_main_returns_two_when_schemas_dir_missing(
    cli_module, tmp_path: Path
) -> None:
    rc = cli_module.main(["--schemas-dir", str(tmp_path / "absent")])
    assert rc == 2


def test_main_sets_compatibility_when_requested(cli_module) -> None:
    fake_registry = MagicMock()
    fake_registry.register.return_value = 1
    fake_registry.get_latest.return_value = MagicMock(version=1)
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        rc = cli_module.main(["--compatibility", "BACKWARD"])
    assert rc == 0
    fake_registry.set_compatibility.assert_called_once_with(
        "e-commerce-events-value", "BACKWARD"
    )


def test_main_returns_one_when_compatibility_set_fails(cli_module) -> None:
    fake_registry = MagicMock()
    fake_registry.register.return_value = 1
    fake_registry.get_latest.return_value = MagicMock(version=1)
    fake_registry.set_compatibility.side_effect = RegistryError("nope")
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        rc = cli_module.main(["--compatibility", "BACKWARD"])
    assert rc == 1


def test_main_tolerates_get_latest_failure(cli_module) -> None:
    fake_registry = MagicMock()
    fake_registry.register.return_value = 1
    fake_registry.get_latest.side_effect = RegistryError("flaky")
    with patch.object(cli_module, "SchemaRegistry", return_value=fake_registry):
        rc = cli_module.main([])
    assert rc == 0


def test_main_verbose_flag(cli_module) -> None:
    """--verbose just toggles logging level; should not affect exit code."""
    with patch.object(cli_module, "SchemaRegistry") as registry_cls:
        rc = cli_module.main(["--dry-run", "--verbose"])
    assert rc == 0
    registry_cls.assert_not_called()
