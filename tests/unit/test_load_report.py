"""Unit tests for :class:`LoadRunReport` and :func:`render_markdown`."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import (
    LoadRunConfig,
    LoadRunReport,
    render_markdown,
)


def _snap(**over) -> AccountantSnapshot:
    base = dict(
        produced=10,
        acked=10,
        failed=0,
        in_flight=0,
        errors_by_class={},
        ack_latency_p50_ms=1.0,
        ack_latency_p95_ms=2.0,
        ack_latency_p99_ms=3.0,
        wallclock_s=10.0,
    )
    base.update(over)
    return AccountantSnapshot(**base)


def _report(sustained_eps: float, **snap_over) -> LoadRunReport:
    return LoadRunReport(
        config=LoadRunConfig(duration_s=10.0, target_rate=60_000.0, workers=4),
        started_at=datetime(2026, 5, 8, tzinfo=timezone.utc),
        snapshot=_snap(**snap_over),
        sustained_rate_eps=sustained_eps,
    )


def test_render_includes_config_table():
    out = render_markdown(_report(61_000.0))
    assert "Duration" in out
    assert "Target rate" in out
    assert "Workers" in out


def test_render_passes_floor_check_with_check_mark():
    out = render_markdown(_report(61_230.0))
    assert "✅" in out
    assert "PASSED" in out


def test_render_fails_floor_check_with_x_mark():
    out = render_markdown(_report(42_000.0))
    assert "❌" in out
    assert "FAILED" in out


def test_render_lists_errors_when_present():
    rep = _report(60_000.0, errors_by_class={"_TIMED_OUT": 17})
    out = render_markdown(rep)
    assert "_TIMED_OUT" in out


def test_render_empty_errors_renders_blank_dict():
    out = render_markdown(_report(60_000.0))
    assert "{}" in out


def test_load_run_config_rejects_negative_duration():
    with pytest.raises(ValidationError):
        LoadRunConfig(duration_s=-1.0, target_rate=1.0, workers=1)


def test_load_run_config_target_rate_positive_or_none():
    LoadRunConfig(duration_s=1.0, target_rate=None, workers=1)
    with pytest.raises(ValidationError):
        LoadRunConfig(duration_s=1.0, target_rate=0.0, workers=1)


def test_load_run_config_rejects_zero_workers():
    with pytest.raises(ValidationError):
        LoadRunConfig(duration_s=1.0, target_rate=1.0, workers=0)


def test_unpaced_target_rate_renders_unpaced():
    rep = LoadRunReport(
        config=LoadRunConfig(duration_s=1.0, target_rate=None, workers=1),
        started_at=datetime(2026, 5, 8, tzinfo=timezone.utc),
        snapshot=_snap(),
        sustained_rate_eps=80_000.0,
    )
    out = render_markdown(rep)
    assert "unpaced" in out


def test_passed_property_requires_zero_failures():
    rep = _report(60_000.0, failed=1)
    assert rep.passed is False


def test_notes_renders_when_present():
    rep = LoadRunReport(
        config=LoadRunConfig(duration_s=1.0, target_rate=1.0, workers=1),
        started_at=datetime(2026, 5, 8, tzinfo=timezone.utc),
        snapshot=_snap(),
        sustained_rate_eps=60_000.0,
        notes="run on M1",
    )
    out = render_markdown(rep)
    assert "run on M1" in out
