"""Unit tests for the multi-process report models and renderer."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    MultiprocessLoadReport,
    ProcessOutcome,
    render_markdown,
)


def _snapshot(**over) -> AccountantSnapshot:
    base = dict(
        produced=1000,
        acked=1000,
        failed=0,
        in_flight=0,
        errors_by_class={},
        ack_latency_p50_ms=1.0,
        ack_latency_p95_ms=5.0,
        ack_latency_p99_ms=10.0,
        wallclock_s=10.0,
    )
    base.update(over)
    return AccountantSnapshot(**base)


def _outcome(idx: int) -> ProcessOutcome:
    cfg = LoadRunConfig(duration_s=1.0, workers=1, topic="t")
    report = LoadRunReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        snapshot=_snapshot(),
        sustained_rate_eps=100.0,
        floor_eps=0.0,
    )
    return ProcessOutcome(process_index=idx, report=report)


def test_mp_config_validates_positive_target_rate():
    """``target_rate`` must be ``> 0`` or ``None``."""
    with pytest.raises(ValueError, match="target_rate must be > 0 or None"):
        MultiprocessLoadConfig(
            duration_s=1.0, target_rate=0.0, processes=2, workers_per_process=1
        )


def test_mp_config_accepts_none_target_rate():
    """``target_rate=None`` is valid (unpaced)."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, target_rate=None, processes=2, workers_per_process=1
    )
    assert cfg.target_rate is None


def test_to_per_process_run_config_splits_target_rate_evenly():
    """Per-process target = aggregate / processes."""
    cfg = MultiprocessLoadConfig(
        duration_s=10.0, target_rate=60_000.0, processes=4, workers_per_process=3
    )
    sub = cfg.to_per_process_run_config(0)
    assert sub.target_rate == pytest.approx(15_000.0)
    assert sub.workers == 3
    assert sub.duration_s == 10.0


def test_to_per_process_run_config_preserves_none_target_rate():
    """``target_rate=None`` propagates to each child unchanged."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, target_rate=None, processes=2, workers_per_process=1
    )
    sub = cfg.to_per_process_run_config(0)
    assert sub.target_rate is None


def test_to_per_process_run_config_decorrelates_seed_per_child():
    """Child seeds differ to keep synthetic streams independent."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, processes=3, workers_per_process=1, seed=42
    )
    seeds = {cfg.to_per_process_run_config(i).seed for i in range(3)}
    assert len(seeds) == 3


def test_to_per_process_run_config_rejects_out_of_range_index():
    """``process_index`` must lie in ``[0, processes)``."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, processes=2, workers_per_process=1
    )
    with pytest.raises(ValueError, match="process_index must be in"):
        cfg.to_per_process_run_config(2)
    with pytest.raises(ValueError, match="process_index must be in"):
        cfg.to_per_process_run_config(-1)


def test_mp_report_passed_true_when_floor_met_and_no_failures():
    """Pass = sustained ≥ floor AND failed == 0."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, processes=1, workers_per_process=1
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(produced=10_000, acked=10_000, failed=0),
        sustained_rate_eps=60_000.0,
        floor_eps=50_000.0,
    )
    assert report.passed is True


def test_mp_report_passed_false_when_below_floor():
    """Pass fails when sustained < floor."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, processes=1, workers_per_process=1
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(),
        sustained_rate_eps=10_000.0,
        floor_eps=50_000.0,
    )
    assert report.passed is False


def test_mp_report_passed_false_when_any_failures():
    """Pass fails when any delivery failed."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, processes=1, workers_per_process=1
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(produced=1000, acked=999, failed=1),
        sustained_rate_eps=60_000.0,
        floor_eps=50_000.0,
    )
    assert report.passed is False


def test_render_markdown_includes_aggregate_and_per_process_rows():
    """Rendered Markdown shows the aggregate table and one row per process."""
    cfg = MultiprocessLoadConfig(
        duration_s=10.0,
        target_rate=60_000.0,
        processes=2,
        workers_per_process=3,
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime(2026, 5, 14, tzinfo=timezone.utc),
        process_outcomes=[_outcome(0), _outcome(1)],
        aggregate_snapshot=_snapshot(produced=20_000, acked=20_000, failed=0),
        sustained_rate_eps=55_000.0,
        floor_eps=50_000.0,
    )
    md = render_markdown(report)
    assert "Multi-Process Synthetic Event Load Test Results" in md
    assert "Processes" in md
    assert "Workers per process" in md
    assert "✅ PASSED" in md
    # Two per-process table rows: "| 0 |" and "| 1 |".
    assert "| 0 |" in md
    assert "| 1 |" in md


def test_render_markdown_unpaced_label():
    """``target_rate=None`` renders as ``unpaced``."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, target_rate=None, processes=1, workers_per_process=1
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(),
        sustained_rate_eps=100.0,
        floor_eps=0.0,
    )
    md = render_markdown(report)
    assert "unpaced" in md


def test_render_markdown_labels_throughput_profile_by_default():
    """Default config (``eos=False``) labels the throughput profile."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0, target_rate=60_000.0, processes=1, workers_per_process=1
    )
    assert cfg.eos is False
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(),
        sustained_rate_eps=100.0,
        floor_eps=0.0,
    )
    md = render_markdown(report)
    assert "Producer profile" in md
    assert "throughput (acks=1, no idempotence)" in md
    assert "EOS (idempotent" not in md


def test_render_markdown_labels_eos_profile_when_eos_true():
    """``eos=True`` labels the EOS profile in the rendered report."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0,
        target_rate=60_000.0,
        processes=1,
        workers_per_process=1,
        eos=True,
    )
    report = MultiprocessLoadReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        process_outcomes=[_outcome(0)],
        aggregate_snapshot=_snapshot(),
        sustained_rate_eps=100.0,
        floor_eps=0.0,
    )
    md = render_markdown(report)
    assert "Producer profile | EOS (idempotent, acks=all, max.in.flight=5)" in md
