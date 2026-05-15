"""Unit tests for :mod:`streaming_feature_store.load_mp.aggregator`."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.load_mp.aggregator import (
    _aggregate_snapshot,
    _merge_errors,
    _merged_samples,
    _percentile_ms,
    aggregate_outcomes,
)
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    ProcessOutcome,
)


def _snapshot(
    *,
    produced: int = 100,
    acked: int = 100,
    failed: int = 0,
    p50: float = 1.0,
    p95: float = 5.0,
    p99: float = 10.0,
    wallclock_s: float = 10.0,
    errors: dict[str, int] | None = None,
) -> AccountantSnapshot:
    """Build an ``AccountantSnapshot`` for testing."""
    return AccountantSnapshot(
        produced=produced,
        acked=acked,
        failed=failed,
        in_flight=produced - acked - failed,
        errors_by_class=dict(errors) if errors else {},
        ack_latency_p50_ms=p50,
        ack_latency_p95_ms=p95,
        ack_latency_p99_ms=p99,
        wallclock_s=wallclock_s,
    )


def _outcome(
    process_index: int,
    *,
    snapshot: AccountantSnapshot | None = None,
    samples_s: list[float] | None = None,
    sustained: float = 100.0,
) -> ProcessOutcome:
    """Build a ``ProcessOutcome`` for testing."""
    snap = snapshot if snapshot is not None else _snapshot()
    cfg = LoadRunConfig(duration_s=1.0, workers=1, topic="t")
    report = LoadRunReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        snapshot=snap,
        sustained_rate_eps=sustained,
        floor_eps=0.0,
    )
    return ProcessOutcome(
        process_index=process_index,
        report=report,
        latency_samples_s=samples_s if samples_s is not None else [],
    )


def _mp_config(processes: int = 2) -> MultiprocessLoadConfig:
    return MultiprocessLoadConfig(
        duration_s=1.0,
        target_rate=None,
        processes=processes,
        workers_per_process=1,
    )


def test_merge_errors_sums_per_class():
    """``errors_by_class`` values sum across processes."""
    outs = [
        _outcome(0, snapshot=_snapshot(failed=2, errors={"_TIMED_OUT": 2})),
        _outcome(
            1,
            snapshot=_snapshot(
                failed=3, errors={"_TIMED_OUT": 1, "_MSG_TIMED_OUT": 2}
            ),
        ),
    ]
    merged = _merge_errors(outs)
    assert merged == {"_TIMED_OUT": 3, "_MSG_TIMED_OUT": 2}


def test_merge_errors_empty_when_no_failures():
    """No failures anywhere → empty dict."""
    outs = [_outcome(0), _outcome(1)]
    assert _merge_errors(outs) == {}


def test_percentile_ms_returns_zero_for_empty_samples():
    """Empty reservoir → ``0.0``."""
    assert _percentile_ms([], 50.0) == 0.0


def test_percentile_ms_converts_seconds_to_ms():
    """``np.percentile`` × 1000 conversion."""
    samples = [0.001, 0.002, 0.003, 0.004]
    p50 = _percentile_ms(samples, 50.0)
    # median of {1, 2, 3, 4} ms is 2.5 ms.
    assert p50 == pytest.approx(2.5)


def test_merged_samples_concatenates_in_order():
    """Per-process reservoirs concatenate into one list."""
    outs = [
        _outcome(0, samples_s=[0.001, 0.002]),
        _outcome(1, samples_s=[0.003]),
    ]
    assert _merged_samples(outs) == [0.001, 0.002, 0.003]


def test_aggregate_snapshot_sums_counters_and_takes_max_wallclock():
    """Counters summed; ``wallclock_s`` is the per-process max."""
    outs = [
        _outcome(
            0,
            snapshot=_snapshot(produced=100, acked=100, failed=0, wallclock_s=10.0),
            samples_s=[0.001, 0.002],
        ),
        _outcome(
            1,
            snapshot=_snapshot(produced=120, acked=119, failed=1, wallclock_s=10.5),
            samples_s=[0.003, 0.004],
        ),
    ]
    snap = _aggregate_snapshot(outs)
    assert snap.produced == 220
    assert snap.acked == 219
    assert snap.failed == 1
    assert snap.in_flight == 0
    assert snap.wallclock_s == pytest.approx(10.5)


def test_aggregate_snapshot_recomputes_percentiles_from_union():
    """Latency percentiles use the union of per-process reservoirs."""
    outs = [
        _outcome(0, samples_s=[0.001, 0.002]),
        _outcome(1, samples_s=[0.003, 0.004]),
    ]
    snap = _aggregate_snapshot(outs)
    # Union sample is {1, 2, 3, 4} ms → p50 = 2.5 ms.
    assert snap.ack_latency_p50_ms == pytest.approx(2.5)


def test_aggregate_snapshot_rejects_empty_outcomes():
    """Empty outcomes → :class:`ValueError`."""
    with pytest.raises(ValueError, match="outcomes must be non-empty"):
        _aggregate_snapshot([])


def test_aggregate_outcomes_returns_full_report():
    """End-to-end aggregation produces a :class:`MultiprocessLoadReport`."""
    cfg = _mp_config(processes=2)
    started = datetime.now(tz=timezone.utc)
    outs = [
        _outcome(
            0,
            snapshot=_snapshot(produced=1000, acked=1000, failed=0, wallclock_s=10.0),
            samples_s=[0.001, 0.002, 0.003],
        ),
        _outcome(
            1,
            snapshot=_snapshot(produced=1100, acked=1100, failed=0, wallclock_s=10.2),
            samples_s=[0.004, 0.005, 0.006],
        ),
    ]
    report = aggregate_outcomes(
        config=cfg, started_at=started, outcomes=outs, floor_eps=200.0
    )
    assert report.config is cfg
    assert report.aggregate_snapshot.produced == 2100
    assert report.aggregate_snapshot.acked == 2100
    expected_sustained = 2100 / 10.2
    assert report.sustained_rate_eps == pytest.approx(expected_sustained)
    # 2100 acked / 10.2 s ≈ 205 evt/s, above the 200 floor and no failures.
    assert report.passed is True


def test_aggregate_outcomes_marks_failed_when_under_floor():
    """Aggregate sustained < floor → ``passed == False``."""
    cfg = _mp_config(processes=2)
    started = datetime.now(tz=timezone.utc)
    outs = [
        _outcome(
            0,
            snapshot=_snapshot(produced=100, acked=100, failed=0, wallclock_s=10.0),
        ),
        _outcome(
            1,
            snapshot=_snapshot(produced=100, acked=100, failed=0, wallclock_s=10.0),
        ),
    ]
    report = aggregate_outcomes(
        config=cfg, started_at=started, outcomes=outs, floor_eps=10_000.0
    )
    assert report.passed is False


def test_aggregate_outcomes_marks_failed_when_any_delivery_failed():
    """Any non-zero ``failed`` count → ``passed == False`` regardless of rate."""
    cfg = _mp_config(processes=2)
    started = datetime.now(tz=timezone.utc)
    outs = [
        _outcome(
            0,
            snapshot=_snapshot(produced=1000, acked=999, failed=1, wallclock_s=1.0),
        ),
        _outcome(
            1,
            snapshot=_snapshot(produced=1000, acked=1000, failed=0, wallclock_s=1.0),
        ),
    ]
    report = aggregate_outcomes(
        config=cfg, started_at=started, outcomes=outs, floor_eps=100.0
    )
    assert report.passed is False


def test_aggregate_outcomes_orders_process_outcomes_by_index():
    """Per-process outcomes are sorted by ``process_index`` in the report."""
    cfg = _mp_config(processes=3)
    started = datetime.now(tz=timezone.utc)
    # Submit out of order.
    outs = [_outcome(2), _outcome(0), _outcome(1)]
    report = aggregate_outcomes(config=cfg, started_at=started, outcomes=outs)
    assert [o.process_index for o in report.process_outcomes] == [0, 1, 2]
