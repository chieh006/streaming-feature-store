"""Unit tests for :mod:`streaming_feature_store.consume.report`."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume.report import (
    ConsumeRunConfig,
    ConsumeRunReport,
    render_markdown,
)


def _snap(**over) -> ConsumeSnapshot:
    base = dict(
        consumed=1000,
        deserialize_failed=0,
        errors_by_class={},
        e2e_p50_ms=5.0,
        e2e_p95_ms=20.0,
        e2e_p99_ms=40.0,
        max_lag=1000,
        end_lag=0,
        lag_ramped=False,
        wallclock_s=10.0,
    )
    base.update(over)
    return ConsumeSnapshot(**base)


def _report(sustained: float = 5_000.0, *, floor: float = 0.0, **snap_over) -> ConsumeRunReport:
    return ConsumeRunReport(
        config=ConsumeRunConfig(duration_s=10.0, group_id="wk1-consume"),
        started_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
        snapshot=_snap(**snap_over),
        sustained_consume_eps=sustained,
        assigned_partitions=[0, 1, 2],
        floor_eps=floor,
    )


def test_render_includes_config_and_profile_rows() -> None:
    out = render_markdown(_report())
    assert "Configuration" in out
    assert "Isolation level" in out
    assert "Deserialize mode" in out
    assert "Assigned partitions" in out


def test_render_flat_latency_passes() -> None:
    out = render_markdown(_report(lag_ramped=False))
    assert "✅" in out
    assert "flat" in out


def test_render_ramped_latency_fails() -> None:
    out = render_markdown(_report(lag_ramped=True))
    assert "❌" in out
    assert "Fell behind" in out


def test_render_lists_deserialize_errors() -> None:
    out = render_markdown(_report(errors_by_class={"ValueError": 3}, deserialize_failed=3))
    assert "ValueError" in out


def test_config_rejects_bad_isolation_level() -> None:
    with pytest.raises(ValidationError):
        ConsumeRunConfig(
            duration_s=1.0, group_id="g", isolation_level="weird"
        )


def test_config_rejects_bad_deserialize_mode() -> None:
    with pytest.raises(ValidationError):
        ConsumeRunConfig(
            duration_s=1.0, group_id="g", deserialize_mode="bogus"
        )


def test_config_rejects_non_positive_duration() -> None:
    with pytest.raises(ValidationError):
        ConsumeRunConfig(duration_s=0.0, group_id="g")


def test_config_rejects_empty_group_id() -> None:
    with pytest.raises(ValidationError):
        ConsumeRunConfig(duration_s=1.0, group_id="")


def test_config_accepts_valid_levels_and_modes() -> None:
    cfg = ConsumeRunConfig(
        duration_s=1.0,
        group_id="g",
        isolation_level="read_committed",
        deserialize_mode="raw",
    )
    assert cfg.isolation_level == "read_committed"
    assert cfg.deserialize_mode == "raw"


def test_passed_false_when_lag_ramped() -> None:
    assert _report(lag_ramped=True).passed is False


def test_passed_false_when_deserialize_failed() -> None:
    assert _report(deserialize_failed=2).passed is False


def test_passed_false_when_below_floor() -> None:
    assert _report(sustained=100.0, floor=50_000.0).passed is False


def test_passed_true_when_flat_clean_and_above_floor() -> None:
    rep = _report(sustained=60_000.0, floor=50_000.0)
    assert rep.passed is True


def test_render_contains_metrics() -> None:
    out = render_markdown(_report())
    assert "Consumed" in out
    assert "End-to-end p50 / p95 / p99" in out
    assert "Max lag / End lag" in out
