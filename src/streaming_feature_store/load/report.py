"""Pydantic ``LoadRunReport`` and Markdown renderer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.load.accountant import AccountantSnapshot

logger = logging.getLogger(__name__)


class LoadRunConfig(BaseModel):
    """Configuration for a single load-runner invocation.

    Parameters
    ----------
    duration_s : float
        Wall-clock duration of the run.  Must be ``> 0``.
    target_rate : float or None
        Desired events/sec; ``None`` disables pacing.
    workers : int
        Number of worker threads.  Must be ``>= 1``.
    batch_size : int, optional
        Synthetic generator batch size per loop iteration.  Defaults to ``1024``.
    max_in_flight : int, optional
        In-process backpressure cap.  Defaults to ``50_000``.
    seed : int, optional
        Synthetic generator seed.  Defaults to ``42``.
    topic : str, optional
        Target Kafka topic.  Defaults to ``"e-commerce-events"``.
    """

    model_config = ConfigDict(frozen=True)

    duration_s: float = Field(..., gt=0)
    target_rate: float | None = None
    workers: int = Field(..., ge=1)
    batch_size: int = Field(default=1024, ge=1)
    max_in_flight: int = Field(default=50_000, ge=1)
    seed: int = 42
    topic: str = "e-commerce-events"

    @field_validator("target_rate")
    @classmethod
    def _target_rate_positive_or_none(cls, value: float | None) -> float | None:
        """Reject ``target_rate <= 0`` (None is allowed).

        Parameters
        ----------
        value : float or None
            Candidate value.

        Returns
        -------
        float or None
            Validated value.
        """
        if value is not None and value <= 0:
            raise ValueError(f"target_rate must be > 0 or None, got {value}")
        return value


class LoadRunReport(BaseModel):
    """Aggregate load-test result.

    Parameters
    ----------
    config : LoadRunConfig
        The configuration used for the run.
    started_at : datetime
        Wall-clock start time (UTC).
    snapshot : AccountantSnapshot
        Final accountant snapshot.
    sustained_rate_eps : float
        ``acked / wallclock_s``.
    floor_eps : float
        Throughput floor that defines pass/fail.
    notes : str or None
        Free-form notes.
    """

    model_config = ConfigDict(frozen=True)

    config: LoadRunConfig
    started_at: datetime
    snapshot: AccountantSnapshot
    sustained_rate_eps: float
    floor_eps: float = 50_000.0
    notes: str | None = None

    @property
    def passed(self) -> bool:
        """Return ``True`` iff sustained rate meets the floor and no failures.

        Returns
        -------
        bool
            Pass/fail verdict.
        """
        return (
            self.sustained_rate_eps >= self.floor_eps and self.snapshot.failed == 0
        )


def _format_target(rate: float | None) -> str:
    """Format the target-rate field for the rendered report.

    Parameters
    ----------
    rate : float or None
        Target rate.

    Returns
    -------
    str
        Human-readable value.
    """
    if rate is None:
        return "unpaced"
    return f"{int(rate):_} evt/s"


def render_markdown(report: LoadRunReport) -> str:
    """Render *report* as the Markdown artifact written to ``docs/results/``.

    Parameters
    ----------
    report : LoadRunReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text.
    """
    cfg = report.config
    snap = report.snapshot
    verdict_mark = "✅" if report.passed else "❌"
    verdict_word = "PASSED" if report.passed else "FAILED"
    started = report.started_at.astimezone(timezone.utc).isoformat()
    lines: list[str] = []
    lines.append("# Week 1 — Synthetic Event Load Test Results")
    lines.append("")
    lines.append(f"**Generated:** {started}")
    lines.append(f"**Topic:** {cfg.topic}")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Duration | {cfg.duration_s} s |")
    lines.append(f"| Target rate | {_format_target(cfg.target_rate)} |")
    lines.append(f"| Workers | {cfg.workers} |")
    lines.append(f"| Batch size | {cfg.batch_size} |")
    lines.append(f"| Max in-flight | {cfg.max_in_flight} |")
    lines.append(f"| Seed | {cfg.seed} |")
    lines.append("")
    lines.append("## Results")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Produced | {snap.produced:_} |")
    lines.append(f"| Acked | {snap.acked:_} |")
    lines.append(f"| Failed | {snap.failed:_} |")
    lines.append(
        f"| Sustained rate | {snap.acked / max(snap.wallclock_s, 1e-9):,.0f} evt/s "
        f"{verdict_mark} (floor {int(report.floor_eps):_}) |"
    )
    lines.append(
        f"| Ack latency p50 / p95 / p99 | "
        f"{snap.ack_latency_p50_ms:.1f} / "
        f"{snap.ack_latency_p95_ms:.1f} / "
        f"{snap.ack_latency_p99_ms:.1f} ms |"
    )
    lines.append(f"| Errors by class | {snap.errors_by_class} |")
    lines.append(f"| Wallclock | {snap.wallclock_s:.2f} s |")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(
        f"{verdict_mark} {verdict_word}: sustained {report.sustained_rate_eps:,.0f} evt/s "
        f"vs floor {int(report.floor_eps):_} evt/s."
    )
    if report.notes:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        lines.append(report.notes)
    lines.append("")
    return "\n".join(lines)
