"""Pydantic models + Markdown renderer for the multi-process consume run.

The single-member harness defines
:class:`~streaming_feature_store.consume.report.ConsumeRunConfig` /
:class:`~streaming_feature_store.consume.report.ConsumeRunReport`; this
module adds the *outer* multi-process wrappers, mirroring
:mod:`streaming_feature_store.load_mp.report`.

Unlike the producer side there is **no per-process seed decorrelation**:
every member is handed the *same* ``group.id`` so the broker performs the
partition split (design doc §2.1 / §4.6).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume.report import (
    _DESERIALIZE_MODES,
    _ISOLATION_LEVELS,
    ConsumeRunConfig,
    ConsumeRunReport,
)

logger = logging.getLogger(__name__)


class MultiprocessConsumeConfig(BaseModel):
    """Configuration for a multi-process consume run.

    Parameters
    ----------
    duration_s : float
        Wall-clock duration per member process.  Must be ``> 0``.
    group_id : str
        Shared consumer group id.  Every member uses this id; the broker
        assigns each a disjoint partition subset.
    members : int
        Number of member processes.  Must be ``>= 1``.
    topic : str, optional
        Target Kafka topic.  Defaults to ``"e-commerce-events"``.
    poll_timeout_s : float, optional
        Per-poll wall-clock budget.  Defaults to ``1.0``.
    max_batch : int, optional
        Maximum messages per poll cycle.  Defaults to ``1024``.
    until_caught_up : bool, optional
        End each member early once its lag reaches ``0``.  Defaults to
        ``False``.
    isolation_level : str, optional
        ``"read_uncommitted"`` (default) or ``"read_committed"``.
    deserialize_mode : str, optional
        ``"pydantic"`` (default) or ``"raw"``.
    """

    model_config = ConfigDict(frozen=True)

    duration_s: float = Field(..., gt=0)
    group_id: str = Field(..., min_length=1)
    members: int = Field(..., ge=1)
    topic: str = "e-commerce-events"
    poll_timeout_s: float = Field(default=1.0, gt=0)
    max_batch: int = Field(default=1024, ge=1)
    until_caught_up: bool = False
    isolation_level: str = "read_uncommitted"
    deserialize_mode: str = "pydantic"

    @field_validator("isolation_level")
    @classmethod
    def _validate_isolation_level(cls, value: str) -> str:
        """Reject unknown isolation levels.

        Parameters
        ----------
        value : str
            Candidate value.

        Returns
        -------
        str
            Validated value.
        """
        if value not in _ISOLATION_LEVELS:
            raise ValueError(
                f"isolation_level must be one of "
                f"{sorted(_ISOLATION_LEVELS)}, got {value!r}"
            )
        return value

    @field_validator("deserialize_mode")
    @classmethod
    def _validate_deserialize_mode(cls, value: str) -> str:
        """Reject unknown deserialize modes.

        Parameters
        ----------
        value : str
            Candidate value.

        Returns
        -------
        str
            Validated value.
        """
        if value not in _DESERIALIZE_MODES:
            raise ValueError(
                f"deserialize_mode must be one of "
                f"{sorted(_DESERIALIZE_MODES)}, got {value!r}"
            )
        return value

    def to_per_process_run_config(self, process_index: int) -> ConsumeRunConfig:
        """Derive the :class:`ConsumeRunConfig` for member ``process_index``.

        Parameters
        ----------
        process_index : int
            Zero-based member index.  Must be ``>= 0`` and ``< members``.

        Returns
        -------
        ConsumeRunConfig
            Per-member config.  Every member shares ``group_id`` (no seed
            decorrelation — the broker, not the app, shards the work).

        Raises
        ------
        ValueError
            If ``process_index`` is out of range.
        """
        if not (0 <= process_index < self.members):
            raise ValueError(
                f"process_index must be in [0, {self.members}), "
                f"got {process_index}"
            )
        return ConsumeRunConfig(
            duration_s=self.duration_s,
            group_id=self.group_id,
            topic=self.topic,
            poll_timeout_s=self.poll_timeout_s,
            max_batch=self.max_batch,
            until_caught_up=self.until_caught_up,
            isolation_level=self.isolation_level,
            deserialize_mode=self.deserialize_mode,
        )


class ConsumeOutcome(BaseModel):
    """One member process's contribution to the aggregate report.

    Parameters
    ----------
    process_index : int
        Zero-based member index.  ``>= 0``.
    report : ConsumeRunReport
        The per-member report returned by the child's
        :class:`~streaming_feature_store.consume.consume_runner.ConsumeRunner`.
    e2e_samples_s : list of float
        Raw end-to-end-latency reservoir samples (seconds) from the child's
        accountant.  The parent re-percentiles the **union** of these.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    report: ConsumeRunReport
    e2e_samples_s: list[float] = Field(default_factory=list)


class MultiprocessConsumeReport(BaseModel):
    """Aggregate result of a multi-process consume run.

    Parameters
    ----------
    config : MultiprocessConsumeConfig
        The parent-level config used for the run.
    started_at : datetime
        Wall-clock start of the run (parent process, UTC).
    process_outcomes : list of ConsumeOutcome
        Per-member outcomes.  Length equals ``config.members``.
    aggregate_snapshot : ConsumeSnapshot
        Counters summed across members; ``e2e_p*`` recomputed from the
        union of per-member reservoirs; lag summed; ``lag_ramped`` is the
        logical OR across members.
    sustained_consume_eps : float
        ``aggregate_snapshot.consumed / max_member_wallclock_s``.
    floor_eps : float
        Optional sustained-rate floor for the verdict.  Defaults to ``0.0``.
    """

    model_config = ConfigDict(frozen=True)

    config: MultiprocessConsumeConfig
    started_at: datetime
    process_outcomes: list[ConsumeOutcome]
    aggregate_snapshot: ConsumeSnapshot
    sustained_consume_eps: float
    floor_eps: float = 0.0

    @property
    def passed(self) -> bool:
        """Return ``True`` iff the group drained without falling behind.

        Returns
        -------
        bool
            ``True`` when aggregate lag did **not** ramp, there were no
            deserialize failures, and the sustained rate met ``floor_eps``.
        """
        return (
            not self.aggregate_snapshot.lag_ramped
            and self.aggregate_snapshot.deserialize_failed == 0
            and self.sustained_consume_eps >= self.floor_eps
        )


def _verdict_line(report: MultiprocessConsumeReport) -> str:
    """Return the human-readable verdict sentence.

    Parameters
    ----------
    report : MultiprocessConsumeReport
        Aggregate result.

    Returns
    -------
    str
        One-line verdict.
    """
    if report.aggregate_snapshot.lag_ramped:
        return (
            "❌ Fell behind: consumer lag ramped — the symmetric "
            "single-process GIL ceiling (design doc §2.1)."
        )
    if report.passed:
        return "✅ Group drained at producer rate; end-to-end latency flat."
    return (
        "❌ Did not pass: sustained rate below floor or deserialize "
        "failures present."
    )


def render_markdown(report: MultiprocessConsumeReport) -> str:
    """Render *report* as the Markdown artifact written to ``docs/results/``.

    Parameters
    ----------
    report : MultiprocessConsumeReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text.
    """
    cfg = report.config
    snap = report.aggregate_snapshot
    started = report.started_at.astimezone(timezone.utc).isoformat()
    lag_ramped = "Yes (fell behind)" if snap.lag_ramped else "No (steady-state drain)"
    lines: list[str] = []
    lines.append("# Multi-Process Consumer Group — End-to-End Latency Results")
    lines.append("")
    lines.append(f"**Generated:** {started}")
    lines.append(f"**Topic:** {cfg.topic}   **Group:** {cfg.group_id}")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Members (processes) | {cfg.members} |")
    lines.append("| Workers per process | 1 |")
    lines.append(f"| Isolation level | {cfg.isolation_level} |")
    lines.append(f"| Deserialize mode | {cfg.deserialize_mode} |")
    lines.append(f"| Until caught up | {cfg.until_caught_up} |")
    lines.append(f"| Duration | {cfg.duration_s} s |")
    lines.append("")
    lines.append("## Aggregate results")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Consumed | {snap.consumed:_} |")
    lines.append(f"| Deserialize failed | {snap.deserialize_failed:_} |")
    lines.append(
        f"| Sustained consume rate | "
        f"{report.sustained_consume_eps:,.0f} evt/s |"
    )
    lines.append(
        f"| End-to-end p50 / p95 / p99 | "
        f"{snap.e2e_p50_ms:.1f} / {snap.e2e_p95_ms:.1f} / "
        f"{snap.e2e_p99_ms:.1f} ms |"
    )
    lines.append(f"| Max lag / End lag | {snap.max_lag:_} / {snap.end_lag:_} |")
    lines.append(f"| Lag ramped? | {lag_ramped} |")
    lines.append(f"| Errors by class | {snap.errors_by_class} |")
    lines.append(f"| Max member wallclock | {snap.wallclock_s:.2f} s |")
    lines.append("")
    lines.append("## Per-process breakdown")
    lines.append("")
    lines.append("| # | Partitions | Consumed | e2e p99 ms | End lag |")
    lines.append("|---|---|---|---|---|")
    for outcome in report.process_outcomes:
        psnap = outcome.report.snapshot
        lines.append(
            f"| {outcome.process_index} | "
            f"{outcome.report.assigned_partitions} | "
            f"{psnap.consumed:_} | {psnap.e2e_p99_ms:.1f} | "
            f"{psnap.end_lag:_} |"
        )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(_verdict_line(report))
    lines.append("")
    return "\n".join(lines)
