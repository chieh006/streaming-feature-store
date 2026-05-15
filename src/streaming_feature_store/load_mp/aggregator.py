"""Merge :class:`ProcessOutcome` objects into one :class:`MultiprocessLoadReport`.

Counters (``produced``, ``acked``, ``failed``, error histogram) are summed
across processes; ``wallclock_s`` is the **max** across children, since
all children run in parallel and the slowest defines the harness's
effective wall time.  Latency percentiles are recomputed from the union
of per-process reservoir samples — reservoir sampling stays unbiased
under concatenation when the per-process reservoirs are equally sized.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

import numpy as np

from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    MultiprocessLoadReport,
    ProcessOutcome,
)

logger = logging.getLogger(__name__)


def _merge_errors(outcomes: Iterable[ProcessOutcome]) -> dict[str, int]:
    """Sum ``errors_by_class`` histograms across processes.

    Parameters
    ----------
    outcomes : iterable of ProcessOutcome
        Per-process outcomes.

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


def _merged_samples(outcomes: Iterable[ProcessOutcome]) -> list[float]:
    """Concatenate per-process latency reservoirs.

    Parameters
    ----------
    outcomes : iterable of ProcessOutcome
        Per-process outcomes.

    Returns
    -------
    list of float
        Union of all per-process reservoir samples (seconds).
    """
    merged: list[float] = []
    for outcome in outcomes:
        merged.extend(outcome.latency_samples_s)
    return merged


def _aggregate_snapshot(outcomes: list[ProcessOutcome]) -> AccountantSnapshot:
    """Combine per-process snapshots into one :class:`AccountantSnapshot`.

    Parameters
    ----------
    outcomes : list of ProcessOutcome
        Per-process outcomes.  Must be non-empty.

    Returns
    -------
    AccountantSnapshot
        Aggregate snapshot with summed counters and merged-reservoir
        percentiles.

    Raises
    ------
    ValueError
        If *outcomes* is empty.
    """
    if not outcomes:
        raise ValueError("outcomes must be non-empty")
    produced = sum(o.report.snapshot.produced for o in outcomes)
    acked = sum(o.report.snapshot.acked for o in outcomes)
    failed = sum(o.report.snapshot.failed for o in outcomes)
    wallclock = max(o.report.snapshot.wallclock_s for o in outcomes)
    samples = _merged_samples(outcomes)
    return AccountantSnapshot(
        produced=produced,
        acked=acked,
        failed=failed,
        in_flight=produced - acked - failed,
        errors_by_class=_merge_errors(outcomes),
        ack_latency_p50_ms=_percentile_ms(samples, 50.0),
        ack_latency_p95_ms=_percentile_ms(samples, 95.0),
        ack_latency_p99_ms=_percentile_ms(samples, 99.0),
        wallclock_s=wallclock,
    )


def aggregate_outcomes(
    *,
    config: MultiprocessLoadConfig,
    started_at: datetime,
    outcomes: list[ProcessOutcome],
    floor_eps: float = 50_000.0,
) -> MultiprocessLoadReport:
    """Aggregate per-process outcomes into a :class:`MultiprocessLoadReport`.

    Parameters
    ----------
    config : MultiprocessLoadConfig
        Parent-level config used for the run.
    started_at : datetime
        Wall-clock start (parent process, UTC).
    outcomes : list of ProcessOutcome
        Per-process outcomes.  Must be non-empty.
    floor_eps : float, optional
        Throughput floor.  Defaults to ``50_000``.

    Returns
    -------
    MultiprocessLoadReport
        Aggregate report.

    Raises
    ------
    ValueError
        If *outcomes* is empty.
    """
    aggregate = _aggregate_snapshot(outcomes)
    sustained = aggregate.acked / max(aggregate.wallclock_s, 1e-9)
    # Sort outcomes by process_index so the per-process table renders in order.
    ordered = sorted(outcomes, key=lambda o: o.process_index)
    return MultiprocessLoadReport(
        config=config,
        started_at=started_at,
        process_outcomes=ordered,
        aggregate_snapshot=aggregate,
        sustained_rate_eps=sustained,
        floor_eps=floor_eps,
    )
