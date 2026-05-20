"""Merge :class:`ConsumeOutcome` objects into one report.

Counters (``consumed``, ``deserialize_failed``, error histogram) are
summed across members.  ``wallclock_s`` is the **max** across members
(they run in parallel; the slowest defines the harness wall time).
End-to-end-latency percentiles are recomputed from the **union** of
per-member reservoir samples — reservoir sampling stays unbiased under
concatenation when the per-member reservoirs are equally sized.  Lag is
**summed** because each member owns a disjoint partition subset, so the
system-wide backlog is the sum of per-member backlogs; ``lag_ramped`` is
the logical OR (if any member fell behind, the group did).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

import numpy as np

from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume_mp.report import (
    ConsumeOutcome,
    MultiprocessConsumeConfig,
    MultiprocessConsumeReport,
)

logger = logging.getLogger(__name__)


def _merge_errors(outcomes: Iterable[ConsumeOutcome]) -> dict[str, int]:
    """Sum ``errors_by_class`` histograms across members.

    Parameters
    ----------
    outcomes : iterable of ConsumeOutcome
        Per-member outcomes.

    Returns
    -------
    dict of str to int
        Merged histogram.
    """
    merged: dict[str, int] = {}
    for outcome in outcomes:
        for key, count in outcome.report.snapshot.errors_by_class.items():
            merged[key] = merged.get(key, 0) + count
    return merged


def _percentile_ms(samples: list[float], q: float) -> float:
    """Return the *q*-th percentile of *samples* in milliseconds.

    Parameters
    ----------
    samples : list of float
        Latency samples in seconds.
    q : float
        Percentile in ``[0, 100]``.

    Returns
    -------
    float
        Percentile value in milliseconds, or ``0.0`` if *samples* is empty.
    """
    if not samples:
        return 0.0
    return float(np.percentile(np.asarray(samples, dtype=np.float64), q) * 1000.0)


def _merged_e2e_samples(outcomes: Iterable[ConsumeOutcome]) -> list[float]:
    """Concatenate per-member end-to-end-latency reservoirs.

    Parameters
    ----------
    outcomes : iterable of ConsumeOutcome
        Per-member outcomes.

    Returns
    -------
    list of float
        Union of all per-member reservoir samples (seconds).
    """
    merged: list[float] = []
    for outcome in outcomes:
        merged.extend(outcome.e2e_samples_s)
    return merged


def _aggregate_snapshot(outcomes: list[ConsumeOutcome]) -> ConsumeSnapshot:
    """Combine per-member snapshots into one :class:`ConsumeSnapshot`.

    Parameters
    ----------
    outcomes : list of ConsumeOutcome
        Per-member outcomes.  Must be non-empty.

    Returns
    -------
    ConsumeSnapshot
        Aggregate snapshot with summed counters / lag, merged-reservoir
        percentiles, and the OR of per-member ramp verdicts.

    Raises
    ------
    ValueError
        If *outcomes* is empty.
    """
    if not outcomes:
        raise ValueError("outcomes must be non-empty")
    snaps = [o.report.snapshot for o in outcomes]
    consumed = sum(s.consumed for s in snaps)
    deserialize_failed = sum(s.deserialize_failed for s in snaps)
    wallclock = max(s.wallclock_s for s in snaps)
    samples = _merged_e2e_samples(outcomes)
    return ConsumeSnapshot(
        consumed=consumed,
        deserialize_failed=deserialize_failed,
        errors_by_class=_merge_errors(outcomes),
        e2e_p50_ms=_percentile_ms(samples, 50.0),
        e2e_p95_ms=_percentile_ms(samples, 95.0),
        e2e_p99_ms=_percentile_ms(samples, 99.0),
        max_lag=sum(s.max_lag for s in snaps),
        end_lag=sum(s.end_lag for s in snaps),
        lag_ramped=any(s.lag_ramped for s in snaps),
        wallclock_s=wallclock,
    )


def aggregate_outcomes(
    *,
    config: MultiprocessConsumeConfig,
    started_at: datetime,
    outcomes: list[ConsumeOutcome],
    floor_eps: float = 0.0,
) -> MultiprocessConsumeReport:
    """Aggregate per-member outcomes into a :class:`MultiprocessConsumeReport`.

    Parameters
    ----------
    config : MultiprocessConsumeConfig
        Parent-level config used for the run.
    started_at : datetime
        Wall-clock start (parent process, UTC).
    outcomes : list of ConsumeOutcome
        Per-member outcomes.  Must be non-empty.
    floor_eps : float, optional
        Sustained-rate floor.  Defaults to ``0.0``.

    Returns
    -------
    MultiprocessConsumeReport
        Aggregate report with per-member outcomes ordered by index.

    Raises
    ------
    ValueError
        If *outcomes* is empty.
    """
    aggregate = _aggregate_snapshot(outcomes)
    sustained = aggregate.consumed / max(aggregate.wallclock_s, 1e-9)
    ordered = sorted(outcomes, key=lambda o: o.process_index)
    return MultiprocessConsumeReport(
        config=config,
        started_at=started_at,
        process_outcomes=ordered,
        aggregate_snapshot=aggregate,
        sustained_consume_eps=sustained,
        floor_eps=floor_eps,
    )
