"""Merge :class:`ValidatorOutcome` objects into one report."""

from __future__ import annotations

import logging
from datetime import datetime

from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
    MultiprocessValidatorReport,
    ValidatorOutcome,
)

logger = logging.getLogger(__name__)


def aggregate_outcomes(
    *,
    config: MultiprocessValidatorConfig,
    started_at: datetime,
    outcomes: list[ValidatorOutcome],
) -> MultiprocessValidatorReport:
    """Aggregate per-member outcomes into a :class:`MultiprocessValidatorReport`.

    Parameters
    ----------
    config : MultiprocessValidatorConfig
        Parent-level config used for the run.
    started_at : datetime
        Wall-clock start (parent process, UTC).
    outcomes : list of ValidatorOutcome
        Per-member outcomes.  Must be non-empty.

    Returns
    -------
    MultiprocessValidatorReport
        Aggregate report with per-member outcomes ordered by index.

    Raises
    ------
    ValueError
        If *outcomes* is empty.
    """
    if not outcomes:
        raise ValueError("outcomes must be non-empty")
    ordered = sorted(outcomes, key=lambda o: o.process_index)
    total_consumed = sum(o.report.snapshot.consumed for o in ordered)
    total_validated = sum(o.report.snapshot.validated for o in ordered)
    total_invalid = sum(o.report.snapshot.invalid_total for o in ordered)
    max_wallclock = max(o.report.snapshot.wallclock_s for o in ordered)
    sustained = total_consumed / max(max_wallclock, 1e-9)
    return MultiprocessValidatorReport(
        config=config,
        started_at=started_at,
        process_outcomes=ordered,
        total_consumed=total_consumed,
        total_validated=total_validated,
        total_invalid=total_invalid,
        sustained_consume_eps=sustained,
    )
