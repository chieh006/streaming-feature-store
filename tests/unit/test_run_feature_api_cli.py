"""Unit tests for ``scripts/run_feature_api.py``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI

from streaming_feature_store.serving.models import ServingConfig

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "run_feature_api.py"
)


@pytest.fixture(scope="module")
def cli():
    """Import the CLI script as a module for direct testing."""
    spec = importlib.util.spec_from_file_location("feature_api_cli", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["feature_api_cli"] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# argument parsing / config building
# ---------------------------------------------------------------------------


def test_config_from_args_defaults(cli) -> None:
    args = cli._build_parser().parse_args([])
    config = cli._config_from_args(args)
    assert config.redis_host == "localhost"
    assert config.redis_port == 6379
    assert config.api_host == "0.0.0.0"
    assert config.api_port == 8000
    assert config.workers == 1


def test_config_from_args_overrides(cli) -> None:
    args = cli._build_parser().parse_args(
        [
            "--redis-host",
            "redis.internal",
            "--redis-port",
            "6380",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
            "--workers",
            "4",
        ]
    )
    config = cli._config_from_args(args)
    assert config.redis_host == "redis.internal"
    assert config.redis_port == 6380
    assert config.api_host == "127.0.0.1"
    assert config.api_port == 9000
    assert config.workers == 4


# ---------------------------------------------------------------------------
# environment round trip (worker config propagation, §2.6)
# ---------------------------------------------------------------------------


def test_export_then_load_env_round_trips(cli, monkeypatch) -> None:
    for var in (
        cli._ENV_REDIS_HOST,
        cli._ENV_REDIS_PORT,
        cli._ENV_POOL_MAX,
        cli._ENV_SOCKET_TIMEOUT,
        cli._ENV_KEY_PREFIX,
    ):
        monkeypatch.delenv(var, raising=False)
    source = ServingConfig(
        redis_host="r-host",
        redis_port=6390,
        redis_pool_max_connections=64,
        redis_socket_timeout_seconds=0.25,
        key_prefix="feat:u:",
    )
    cli._export_config_to_env(source)
    loaded = cli._config_from_env()
    assert loaded.redis_host == "r-host"
    assert loaded.redis_port == 6390
    assert loaded.redis_pool_max_connections == 64
    assert loaded.redis_socket_timeout_seconds == 0.25
    assert loaded.key_prefix == "feat:u:"


def test_config_from_env_uses_defaults_when_unset(cli, monkeypatch) -> None:
    for var in (
        cli._ENV_REDIS_HOST,
        cli._ENV_REDIS_PORT,
        cli._ENV_POOL_MAX,
        cli._ENV_SOCKET_TIMEOUT,
        cli._ENV_KEY_PREFIX,
    ):
        monkeypatch.delenv(var, raising=False)
    loaded = cli._config_from_env()
    assert loaded == ServingConfig()


def test_app_factory_builds_app_from_env(cli, monkeypatch) -> None:
    monkeypatch.setenv(cli._ENV_REDIS_HOST, "r-host")
    app = cli.app_factory()
    assert isinstance(app, FastAPI)


# ---------------------------------------------------------------------------
# launch dispatch
# ---------------------------------------------------------------------------


def test_run_single_worker_passes_app_object(cli) -> None:
    args = cli._build_parser().parse_args(["--log-level", "warning"])
    sentinel = object()
    with (
        patch.object(cli, "create_app", return_value=sentinel) as create_app,
        patch.object(cli.uvicorn, "run") as uvicorn_run,
    ):
        rc = cli._run(args)
    assert rc == 0
    create_app.assert_called_once()
    call = uvicorn_run.call_args
    assert call.args[0] is sentinel
    assert call.kwargs["host"] == "0.0.0.0"
    assert call.kwargs["port"] == 8000
    assert call.kwargs["log_level"] == "warning"
    assert "workers" not in call.kwargs


def test_run_multi_worker_uses_factory(cli) -> None:
    args = cli._build_parser().parse_args(["--workers", "3", "--redis-host", "r-host"])
    with (
        patch.object(cli.uvicorn, "run") as uvicorn_run,
        patch.object(cli, "_ensure_repo_importable") as ensure,
        patch.object(cli, "_export_config_to_env") as export,
    ):
        rc = cli._run(args)
    assert rc == 0
    ensure.assert_called_once()
    export.assert_called_once()
    call = uvicorn_run.call_args
    assert call.args[0] == cli._FACTORY_TARGET
    assert call.kwargs["factory"] is True
    assert call.kwargs["workers"] == 3


def test_ensure_repo_importable_adds_repo_root(cli, monkeypatch) -> None:
    monkeypatch.setenv("PYTHONPATH", "/pre/existing")
    monkeypatch.setattr(cli.sys, "path", list(cli.sys.path))  # isolate sys.path
    cli._ensure_repo_importable()
    repo_root = str(cli._REPO_ROOT)
    assert repo_root in cli.sys.path
    parts = cli.os.environ["PYTHONPATH"].split(cli.os.pathsep)
    assert repo_root in parts
    assert "/pre/existing" in parts


def test_main_parses_and_runs(cli) -> None:
    with patch.object(cli, "_run", return_value=0) as run:
        rc = cli.main(["--workers", "1"])
    assert rc == 0
    run.assert_called_once()
