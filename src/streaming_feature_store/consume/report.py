"""Pydantic ``ConsumeRunConfig`` / ``ConsumeRunReport`` and Markdown renderer.

Mirrors :mod:`streaming_feature_store.load.report` on the consume side.
The verdict is on the lag *signature* (did the member fall behind?), not an
absolute latency ceiling — the relative test is host-speed independent
(design doc §10 Q2).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.consume.accountant import ConsumeSnapshot

logger = logging.getLogger(__name__)

_ISOLATION_LEVELS: frozenset[str] = frozenset(
    {"read_uncommitted", "read_committed"}
)
_DESERIALIZE_MODES: frozenset[str] = frozenset({"pydantic", "raw"})


class ConsumeRunConfig(BaseModel):
    """Configuration for a single consumer-group-member run.

    Parameters
    ----------
    duration_s : float
        Wall-clock duration of the run.  Must be ``> 0``.
    group_id : str
        Consumer group id.  All members of a multi-process run share this
        id so the broker assigns each a disjoint partition subset.
    topic : str, optional
        Target Kafka topic.  Defaults to ``"e-commerce-events"``.
    poll_timeout_s : float, optional
        Per-poll wall-clock budget.  Defaults to ``1.0``.
    max_batch : int, optional
        Maximum messages collected per poll cycle.  Defaults to ``1024``.
    until_caught_up : bool, optional
        End the run early once consumer lag reaches ``0``.  Defaults to
        ``False`` (run for the full ``duration_s``).
    isolation_level : str, optional
        ``"read_uncommitted"`` (default) or ``"read_committed"`` — the
        read-side EOS seam (design doc §2.7).
    deserialize_mode : str, optional
        ``"pydantic"`` (default, full ``avro_dict_to_event`` path) or
        ``"raw"`` (decode only, skips Pydantic — measurement control,
        design doc §2.8).
    """

    model_config = ConfigDict(frozen=True)

    duration_s: float = Field(..., gt=0)
    group_id: str = Field(..., min_length=1)
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


class ConsumeRunReport(BaseModel):
    """Aggregate result of one consumer-group-member run.

    Parameters
    ----------
    config : ConsumeRunConfig
        The configuration used for the run.
    started_at : datetime
        Wall-clock start time (UTC).
    snapshot : ConsumeSnapshot
        Final accountant snapshot.
    sustained_consume_eps : float
        ``consumed / wallclock_s``.
    assigned_partitions : list of int
        Partition numbers this member owned (group-managed assignment).
    floor_eps : float
        Optional sustained-rate floor for the pass / fail verdict.
        Defaults to ``0.0`` (verdict driven solely by the lag signature).
    """

    model_config = ConfigDict(frozen=True)

    config: ConsumeRunConfig
    started_at: datetime
    snapshot: ConsumeSnapshot
    sustained_consume_eps: float
    assigned_partitions: list[int] = Field(default_factory=list)
    floor_eps: float = 0.0

    @property
    def passed(self) -> bool:
        """Return ``True`` iff the member drained without falling behind.

        Returns
        -------
        bool
            ``True`` when lag did **not** ramp, no deserialize failures
            occurred, and the sustained rate met ``floor_eps``.
        """
        return (
            not self.snapshot.lag_ramped
            and self.snapshot.deserialize_failed == 0
            and self.sustained_consume_eps >= self.floor_eps
        )


def _verdict_line(report: ConsumeRunReport) -> str:
    """Return the human-readable verdict sentence.

    Parameters
    ----------
    report : ConsumeRunReport
        Aggregate result.

    Returns
    -------
    str
        One-line verdict.
    """
    if report.snapshot.lag_ramped:
        return (
            "❌ Fell behind: consumer lag ramped — the single-process GIL "
            "ceiling (design doc §2.1)."
        )
    if report.passed:
        return "✅ Group member drained; end-to-end latency flat."
    return (
        "❌ Did not pass: sustained rate below floor or deserialize "
        "failures present."
    )


def render_markdown(report: ConsumeRunReport) -> str:
    """Render *report* as a Markdown artifact.

    Parameters
    ----------
    report : ConsumeRunReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text.
    """
    cfg = report.config
    snap = report.snapshot
    started = report.started_at.astimezone(timezone.utc).isoformat()
    lag_ramped = "Yes (fell behind)" if snap.lag_ramped else "No (steady-state drain)"
    lines: list[str] = []
    lines.append("# Consumer Group Member — End-to-End Latency Results")
    lines.append("")
    lines.append(f"**Generated:** {started}")
    lines.append(f"**Topic:** {cfg.topic}   **Group:** {cfg.group_id}")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Duration | {cfg.duration_s} s |")
    lines.append(f"| Isolation level | {cfg.isolation_level} |")
    lines.append(f"| Deserialize mode | {cfg.deserialize_mode} |")
    lines.append(f"| Until caught up | {cfg.until_caught_up} |")
    lines.append(
        f"| Assigned partitions | {report.assigned_partitions} |"
    )
    lines.append("")
    lines.append("## Results")
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
    lines.append(f"| Wallclock | {snap.wallclock_s:.2f} s |")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(_verdict_line(report))
    lines.append("")
    return "\n".join(lines)
