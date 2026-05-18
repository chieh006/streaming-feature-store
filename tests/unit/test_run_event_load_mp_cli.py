"""Unit tests for the ``--eos`` switch in ``scripts/run_event_load_mp.py``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "run_event_load_mp.py"
)

_ENV_KEY = "KAFKA_PRODUCER_ENABLE_IDEMPOTENCE"


@pytest.fixture(scope="module")
def cli_module():
    """Import the non-package script module via importlib.

    Returns
    -------
    module
        The loaded ``run_event_load_mp`` module object.
    """
    spec = importlib.util.spec_from_file_location(
        "run_event_load_mp_cli", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_event_load_mp_cli"] = module
    spec.loader.exec_module(module)
    return module


def test_eos_flag_defaults_to_false(cli_module) -> None:
    """The ``--eos`` flag must default to ``False``."""
    args = cli_module._build_parser().parse_args([])
    assert args.eos is False


def test_eos_flag_parses_true(cli_module) -> None:
    """Passing ``--eos`` must set the flag ``True``."""
    args = cli_module._build_parser().parse_args(["--eos"])
    assert args.eos is True


def test_apply_eos_profile_sets_env_when_enabled(
    cli_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_apply_eos_profile(True)`` must export the idempotence env var."""
    monkeypatch.delenv(_ENV_KEY, raising=False)
    cli_module._apply_eos_profile(True)
    assert cli_module.os.environ[_ENV_KEY] == "true"


def test_apply_eos_profile_noop_when_disabled(
    cli_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_apply_eos_profile(False)`` must not touch the environment."""
    monkeypatch.delenv(_ENV_KEY, raising=False)
    cli_module._apply_eos_profile(False)
    assert _ENV_KEY not in cli_module.os.environ
