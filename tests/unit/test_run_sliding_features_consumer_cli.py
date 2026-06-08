"""Unit tests for ``scripts/run_sliding_features_consumer.py``."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.sliding.consumer import SlidingRunSnapshot
from streaming_feature_store.sliding.models import SlidingConsumerConfig

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "run_sliding_features_consumer.py"
)


@pytest.fixture(scope="module")
def cli():
    """Import the CLI script as a module for direct testing."""
    spec = importlib.util.spec_from_file_location("sliding_cli", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["sliding_cli"] = module
    spec.loader.exec_module(module)
    return module


def _snapshot() -> SlidingRunSnapshot:
    return SlidingRunSnapshot(
        consumed=10,
        late=1,
        emitted_by_resolution={"5m": 5, "1h": 2, "24h": 1},
        active_users=3,
    )


# ---------------------------------------------------------------------------
# argument parsing / config building
# ---------------------------------------------------------------------------


def test_config_from_args_defaults(cli) -> None:
    args = cli._build_parser().parse_args([])
    config = cli._config_from_args(args)
    assert config.source_topic == "validated-events"
    assert config.num_workers == 1
    assert config.warmup_seek_back is True


def test_config_from_args_overrides(cli) -> None:
    args = cli._build_parser().parse_args(
        [
            "--bootstrap",
            "host:9092",
            "--registry",
            "http://reg:8081",
            "--num-workers",
            "4",
            "--no-warmup-seek-back",
            "--allowed-lateness-seconds",
            "10",
            "--redis-host",
            "localhost",
        ]
    )
    config = cli._config_from_args(args)
    assert config.bootstrap == "host:9092"
    assert config.registry_url == "http://reg:8081"
    assert config.num_workers == 4
    assert config.warmup_seek_back is False
    assert config.allowed_lateness_seconds == 10
    assert config.redis_host == "localhost"


def test_kafka_and_registry_config_bind_to_values(cli) -> None:
    config = SlidingConsumerConfig(bootstrap="b:9092", registry_url="http://r:8081")
    assert cli._kafka_config(config).bootstrap_servers == "b:9092"
    assert cli._registry_config(config).url == "http://r:8081"


# ---------------------------------------------------------------------------
# bootstrap helpers
# ---------------------------------------------------------------------------


def test_ensure_topics_creates_both_output_topics(cli) -> None:
    config = SlidingConsumerConfig()
    with patch.object(cli, "TopicAdmin") as admin_cls:
        cli._ensure_topics(config, cli._kafka_config(config))
    admin = admin_cls.return_value
    created = {call.args[0] for call in admin.ensure_topic.call_args_list}
    assert created == {"sliding-features", "sliding-features-late"}


def test_ensure_schemas_registers_both_subjects(cli) -> None:
    config = SlidingConsumerConfig()
    with patch.object(cli, "SchemaRegistry") as sr_cls:
        cli._ensure_schemas(config, cli._registry_config(config))
    subjects = {call.args[0] for call in sr_cls.return_value.register.call_args_list}
    assert subjects == {"sliding-features-value", "sliding-features-late-value"}


def test_install_signal_handlers_requests_shutdown(cli) -> None:
    consumer = MagicMock()
    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    with patch.object(cli.signal, "signal", _fake_signal):
        cli._install_signal_handlers(consumer)
    # Fire one captured handler and confirm it requests shutdown.
    handler = next(iter(captured.values()))
    handler(15, None)
    consumer.request_shutdown.assert_called_once()


# ---------------------------------------------------------------------------
# report rendering
# ---------------------------------------------------------------------------


def test_render_report_includes_counters(cli) -> None:
    started = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    ended = datetime(2026, 6, 7, 12, 1, tzinfo=UTC)
    report = cli.render_report(_snapshot(), SlidingConsumerConfig(), started, ended)
    assert "Events consumed: 10" in report
    assert "| 5m | 5 |" in report
    assert "Duration: 60.0 s" in report


# ---------------------------------------------------------------------------
# run paths
# ---------------------------------------------------------------------------


def test_run_single_writes_report(cli, tmp_path) -> None:
    report_path = tmp_path / "report.md"
    args = cli._build_parser().parse_args(["--report-path", str(report_path)])
    config = SlidingConsumerConfig()
    fake_consumer = MagicMock()
    fake_consumer.run.return_value = _snapshot()
    with (
        patch.object(cli, "SlidingFeaturesConsumer", return_value=fake_consumer),
        patch.object(cli, "_install_signal_handlers"),
    ):
        rc = cli._run_single(
            args, config, cli._kafka_config(config), cli._registry_config(config)
        )
    assert rc == 0
    assert "Sliding-Window Features Smoke Run" in report_path.read_text()


def test_worker_entry_runs_consumer(cli) -> None:
    args = cli._build_parser().parse_args([])
    fake_consumer = MagicMock()
    with (
        patch.object(cli, "SlidingFeaturesConsumer", return_value=fake_consumer) as cls,
        patch.object(cli, "_install_signal_handlers"),
    ):
        cli._worker_entry(args, SlidingConsumerConfig(), 0)
    cls.assert_called_once()
    fake_consumer.run.assert_called_once()


def test_run_group_starts_and_joins_workers(cli) -> None:
    args = cli._build_parser().parse_args([])
    config = SlidingConsumerConfig(num_workers=3)
    with patch.object(cli.multiprocessing, "Process") as proc_cls:
        rc = cli._run_group(args, config)
    assert rc == 0
    assert proc_cls.call_count == 3
    assert proc_cls.return_value.start.call_count == 3
    assert proc_cls.return_value.join.call_count == 3


def test_run_dispatches_to_single_with_ensure(cli, tmp_path) -> None:
    args = cli._build_parser().parse_args(
        ["--report-path", str(tmp_path / "r.md")]
    )
    with (
        patch.object(cli, "_ensure_topics") as ensure_topics,
        patch.object(cli, "_ensure_schemas") as ensure_schemas,
        patch.object(cli, "_run_single", return_value=0) as run_single,
        patch.object(cli, "_run_group") as run_group,
    ):
        rc = cli._run(args)
    assert rc == 0
    ensure_topics.assert_called_once()
    ensure_schemas.assert_called_once()
    run_single.assert_called_once()
    run_group.assert_not_called()


def test_run_dispatches_to_group_without_ensure(cli) -> None:
    args = cli._build_parser().parse_args(["--no-ensure-topics", "--num-workers", "2"])
    with (
        patch.object(cli, "_ensure_topics") as ensure_topics,
        patch.object(cli, "_run_group", return_value=0) as run_group,
        patch.object(cli, "_run_single") as run_single,
    ):
        rc = cli._run(args)
    assert rc == 0
    ensure_topics.assert_not_called()
    run_group.assert_called_once()
    run_single.assert_not_called()


def test_main_parses_and_runs(cli) -> None:
    with patch.object(cli, "_run", return_value=0) as run:
        rc = cli.main(["--no-ensure-topics"])
    assert rc == 0
    run.assert_called_once()
