"""Unit tests for ``scripts/run_event_consume_mp.py``."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "run_event_consume_mp.py"
)


@pytest.fixture(scope="module")
def cli_module():
    """Import the non-package script module via importlib."""
    spec = importlib.util.spec_from_file_location(
        "run_event_consume_mp_cli", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_event_consume_mp_cli"] = module
    spec.loader.exec_module(module)
    return module


# --- argument parsing -----------------------------------------------------


def test_defaults(cli_module) -> None:
    args = cli_module._build_parser().parse_args([])
    assert args.members is None
    assert args.isolation_level == "read_uncommitted"
    assert args.deserialize_mode == "pydantic"
    assert args.group_id == "wk1-consume"
    assert args.until_caught_up is False


def test_isolation_flag_parsed(cli_module) -> None:
    args = cli_module._build_parser().parse_args(
        ["--isolation-level", "read_committed"]
    )
    assert args.isolation_level == "read_committed"


def test_members_one_parsed(cli_module) -> None:
    args = cli_module._build_parser().parse_args(["--members", "1"])
    assert args.members == 1


def test_until_caught_up_flag(cli_module) -> None:
    args = cli_module._build_parser().parse_args(["--until-caught-up"])
    assert args.until_caught_up is True


def test_invalid_deserialize_mode_rejected(cli_module) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_module._build_parser().parse_args(["--deserialize-mode", "bogus"])
    assert exc.value.code == 2


# --- _run wiring (runner stubbed) -----------------------------------------


def _fake_report(*, passed: bool) -> SimpleNamespace:
    snap = SimpleNamespace(
        consumed=10,
        deserialize_failed=0 if passed else 1,
        e2e_p50_ms=5.0,
        e2e_p95_ms=20.0,
        e2e_p99_ms=40.0,
        end_lag=0 if passed else 999,
        lag_ramped=not passed,
    )
    return SimpleNamespace(
        passed=passed,
        aggregate_snapshot=snap,
        sustained_consume_eps=60_000.0 if passed else 100.0,
    )


def _patch_runner(cli_module, monkeypatch, report) -> None:
    class _FakeRunner:
        def __init__(self, *a, **kw) -> None:
            ...

        def run(self):
            return report

    monkeypatch.setattr(cli_module, "MultiprocessConsumeRunner", _FakeRunner)
    monkeypatch.setattr(cli_module, "render_markdown", lambda r: "REPORT")


def test_run_returns_zero_on_pass(cli_module, monkeypatch, tmp_path) -> None:
    _patch_runner(cli_module, monkeypatch, _fake_report(passed=True))
    args = cli_module._build_parser().parse_args(
        ["--members", "4", "--report-path", str(tmp_path / "r.md")]
    )
    assert cli_module._run(args) == 0
    assert (tmp_path / "r.md").read_text() == "REPORT"


def test_run_returns_one_on_fail(cli_module, monkeypatch, tmp_path) -> None:
    _patch_runner(cli_module, monkeypatch, _fake_report(passed=False))
    args = cli_module._build_parser().parse_args(
        ["--members", "1", "--report-path", str(tmp_path / "r.md")]
    )
    assert cli_module._run(args) == 1


def test_main_invokes_run(cli_module, monkeypatch, tmp_path) -> None:
    _patch_runner(cli_module, monkeypatch, _fake_report(passed=True))
    rc = cli_module.main(
        ["--members", "2", "--report-path", str(tmp_path / "r.md")]
    )
    assert rc == 0
