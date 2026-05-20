"""Unit tests for :mod:`streaming_feature_store.consume_mp` aggregation."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume.report import ConsumeRunConfig, ConsumeRunReport
from streaming_feature_store.consume_mp.aggregator import (
    _aggregate_snapshot,
    _merge_errors,
    _merged_e2e_samples,
    _percentile_ms,
    aggregate_outcomes,
)
from streaming_feature_store.consume_mp.report import (
    ConsumeOutcome,
    MultiprocessConsumeConfig,
    MultiprocessConsumeReport,
    render_markdown,
)


def _snap(
    *,
    consumed: int = 100,
    deserialize_failed: int = 0,
    p50: float = 1.0,
    p95: float = 5.0,
    p99: float = 10.0,
    max_lag: int = 0,
    end_lag: int = 0,
    lag_ramped: bool = False,
    wallclock_s: float = 10.0,
    errors: dict[str, int] | None = None,
) -> ConsumeSnapshot:
    return ConsumeSnapshot(
        consumed=consumed,
        deserialize_failed=deserialize_failed,
        errors_by_class=dict(errors) if errors else {},
        e2e_p50_ms=p50,
        e2e_p95_ms=p95,
        e2e_p99_ms=p99,
        max_lag=max_lag,
        end_lag=end_lag,
        lag_ramped=lag_ramped,
        wallclock_s=wallclock_s,
    )


def _outcome(
    idx: int,
    *,
    snapshot: ConsumeSnapshot | None = None,
    samples_s: list[float] | None = None,
    sustained: float = 100.0,
    partitions: list[int] | None = None,
) -> ConsumeOutcome:
    snap = snapshot if snapshot is not None else _snap()
    report = ConsumeRunReport(
        config=ConsumeRunConfig(duration_s=1.0, group_id="g"),
        started_at=datetime.now(tz=timezone.utc),
        snapshot=snap,
        sustained_consume_eps=sustained,
        assigned_partitions=partitions if partitions is not None else [idx],
        floor_eps=0.0,
    )
    return ConsumeOutcome(
        process_index=idx,
        report=report,
        e2e_samples_s=samples_s if samples_s is not None else [],
    )


def _cfg(members: int = 2) -> MultiprocessConsumeConfig:
    return MultiprocessConsumeConfig(
        duration_s=1.0, group_id="g", members=members
    )


# --- helper-level ---------------------------------------------------------


def test_merge_errors_sums_per_class() -> None:
    outs = [
        _outcome(0, snapshot=_snap(deserialize_failed=2, errors={"ValueError": 2})),
        _outcome(
            1,
            snapshot=_snap(
                deserialize_failed=3, errors={"ValueError": 1, "KeyError": 2}
            ),
        ),
    ]
    assert _merge_errors(outs) == {"ValueError": 3, "KeyError": 2}


def test_percentile_ms_empty_is_zero() -> None:
    assert _percentile_ms([], 99.0) == 0.0


def test_percentile_ms_seconds_to_ms() -> None:
    assert _percentile_ms([0.001, 0.002, 0.003, 0.004], 50.0) == pytest.approx(2.5)


def test_merged_e2e_samples_concatenates() -> None:
    outs = [_outcome(0, samples_s=[0.001, 0.002]), _outcome(1, samples_s=[0.003])]
    assert _merged_e2e_samples(outs) == [0.001, 0.002, 0.003]


# --- §5.5 table -----------------------------------------------------------


def test_counters_summed() -> None:
    outs = [
        _outcome(i, snapshot=_snap(consumed=100, deserialize_failed=1))
        for i in range(6)
    ]
    snap = _aggregate_snapshot(outs)
    assert snap.consumed == 600
    assert snap.deserialize_failed == 6


def test_wallclock_is_max_not_sum() -> None:
    outs = [
        _outcome(0, snapshot=_snap(wallclock_s=10.0)),
        _outcome(1, snapshot=_snap(wallclock_s=10.7)),
    ]
    assert _aggregate_snapshot(outs).wallclock_s == pytest.approx(10.7)


def test_latency_repercentiled_from_union() -> None:
    outs = [
        _outcome(0, samples_s=[0.001, 0.002]),
        _outcome(1, samples_s=[0.003, 0.004]),
    ]
    # Union {1,2,3,4} ms → p50 == 2.5 ms (not the mean of per-member p50s).
    assert _aggregate_snapshot(outs).e2e_p50_ms == pytest.approx(2.5)


def test_lag_summed_across_partitions() -> None:
    outs = [
        _outcome(0, snapshot=_snap(max_lag=300, end_lag=10)),
        _outcome(1, snapshot=_snap(max_lag=500, end_lag=25)),
    ]
    snap = _aggregate_snapshot(outs)
    assert snap.end_lag == 35
    assert snap.max_lag == 800


def test_lag_ramped_true_if_any_member_ramped() -> None:
    outs = [
        _outcome(0, snapshot=_snap(lag_ramped=False)),
        _outcome(1, snapshot=_snap(lag_ramped=True)),
    ]
    assert _aggregate_snapshot(outs).lag_ramped is True


def test_aggregate_snapshot_rejects_empty() -> None:
    with pytest.raises(ValueError, match="outcomes must be non-empty"):
        _aggregate_snapshot([])


# --- aggregate_outcomes + report ------------------------------------------


def test_aggregate_outcomes_orders_by_index_and_computes_sustained() -> None:
    outs = [
        _outcome(2, snapshot=_snap(consumed=100, wallclock_s=10.0)),
        _outcome(0, snapshot=_snap(consumed=100, wallclock_s=10.0)),
        _outcome(1, snapshot=_snap(consumed=100, wallclock_s=10.0)),
    ]
    report = aggregate_outcomes(
        config=_cfg(3), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    assert [o.process_index for o in report.process_outcomes] == [0, 1, 2]
    assert report.sustained_consume_eps == pytest.approx(30.0)


def test_report_passed_true_when_flat_clean() -> None:
    outs = [_outcome(0, snapshot=_snap(lag_ramped=False, deserialize_failed=0))]
    report = aggregate_outcomes(
        config=_cfg(1), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    assert report.passed is True


def test_report_passed_false_when_any_ramped() -> None:
    outs = [
        _outcome(0, snapshot=_snap(lag_ramped=False)),
        _outcome(1, snapshot=_snap(lag_ramped=True)),
    ]
    report = aggregate_outcomes(
        config=_cfg(2), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    assert report.passed is False


def test_report_passed_false_below_floor() -> None:
    outs = [_outcome(0, snapshot=_snap(consumed=10, wallclock_s=10.0))]
    report = aggregate_outcomes(
        config=_cfg(1),
        started_at=datetime.now(tz=timezone.utc),
        outcomes=outs,
        floor_eps=50_000.0,
    )
    assert report.passed is False


def test_render_markdown_has_breakdown_and_profile() -> None:
    outs = [
        _outcome(0, snapshot=_snap(consumed=300), partitions=[0, 1, 2]),
        _outcome(1, snapshot=_snap(consumed=300), partitions=[3, 4, 5]),
    ]
    report = aggregate_outcomes(
        config=_cfg(2), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    out = render_markdown(report)
    assert "Multi-Process Consumer Group" in out
    assert "Members (processes) | 2" in out
    assert "Workers per process | 1" in out
    assert "Isolation level" in out
    assert "Per-process breakdown" in out
    assert "[0, 1, 2]" in out


def test_render_markdown_not_ramped_but_not_passed_verdict() -> None:
    """Flat lag but deserialize failures → the fallback ❌ verdict line."""
    outs = [_outcome(0, snapshot=_snap(lag_ramped=False, deserialize_failed=4))]
    report = aggregate_outcomes(
        config=_cfg(1), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    out = render_markdown(report)
    assert "❌" in out
    assert "Did not pass" in out


def test_render_markdown_ramped_verdict() -> None:
    outs = [_outcome(0, snapshot=_snap(lag_ramped=True))]
    report = aggregate_outcomes(
        config=_cfg(1), started_at=datetime.now(tz=timezone.utc), outcomes=outs
    )
    out = render_markdown(report)
    assert "❌" in out
    assert "Fell behind" in out


# --- MultiprocessConsumeConfig --------------------------------------------


def test_mp_config_rejects_bad_isolation_level() -> None:
    with pytest.raises(ValidationError):
        MultiprocessConsumeConfig(
            duration_s=1.0, group_id="g", members=1, isolation_level="weird"
        )


def test_mp_config_rejects_bad_deserialize_mode() -> None:
    with pytest.raises(ValidationError):
        MultiprocessConsumeConfig(
            duration_s=1.0, group_id="g", members=1, deserialize_mode="x"
        )


def test_to_per_process_run_config_shares_group_id() -> None:
    cfg = MultiprocessConsumeConfig(
        duration_s=2.0,
        group_id="shared-grp",
        members=3,
        deserialize_mode="raw",
    )
    rc0 = cfg.to_per_process_run_config(0)
    rc2 = cfg.to_per_process_run_config(2)
    assert rc0.group_id == rc2.group_id == "shared-grp"
    assert rc0.deserialize_mode == "raw"
    assert isinstance(rc0, ConsumeRunConfig)


def test_to_per_process_run_config_rejects_out_of_range() -> None:
    cfg = MultiprocessConsumeConfig(duration_s=1.0, group_id="g", members=2)
    with pytest.raises(ValueError, match="process_index must be in"):
        cfg.to_per_process_run_config(2)


def test_mp_report_is_frozen() -> None:
    report = aggregate_outcomes(
        config=_cfg(1),
        started_at=datetime.now(tz=timezone.utc),
        outcomes=[_outcome(0)],
    )
    assert isinstance(report, MultiprocessConsumeReport)
    with pytest.raises((ValueError, TypeError)):
        report.sustained_consume_eps = 1.0  # type: ignore[misc]
