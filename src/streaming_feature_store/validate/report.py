"""Pydantic :class:`ValidatorRunReport` and Markdown renderer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.validate.accountant import ValidatorSnapshot

logger = logging.getLogger(__name__)

DEFAULT_HIGH_INVALID_RATE: float = 0.05


class ValidatorRunReport(BaseModel):
    """Aggregate :class:`ValidatorRunner` result.

    Parameters
    ----------
    source_topic : str
        Source Kafka topic the validator subscribed to.
    validated_topic : str
        Output topic for ``Valid`` events.
    dlq_topic : str
        Output topic for ``Invalid`` events.
    consumer_group : str
        Kafka consumer ``group.id`` used for the run.
    started_at : datetime
        Wall-clock start time (UTC).
    ended_at : datetime
        Wall-clock end time (UTC).
    snapshot : ValidatorSnapshot
        Final accountant snapshot.
    high_invalid_rate_threshold : float, optional
        Threshold above which the rendered Markdown surfaces an "elevated
        invalid rate" warning marker.  Defaults to ``0.05``.
    notes : str or None, optional
        Free-form notes recorded by the runner.
    """

    model_config = ConfigDict(frozen=True)

    source_topic: str
    validated_topic: str
    dlq_topic: str
    consumer_group: str
    started_at: datetime
    ended_at: datetime
    snapshot: ValidatorSnapshot
    high_invalid_rate_threshold: float = Field(
        default=DEFAULT_HIGH_INVALID_RATE, ge=0.0, le=1.0
    )
    notes: str | None = Field(default=None)

    @property
    def duration_s(self) -> float:
        """Wall-clock duration in seconds.

        Returns
        -------
        float
            ``(ended_at - started_at).total_seconds()``.
        """
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def sustained_consume_eps(self) -> float:
        """Sustained events/sec read from the source topic.

        Returns
        -------
        float
            ``consumed / max(wallclock_s, 1e-9)``.
        """
        wallclock = max(self.snapshot.wallclock_s, 1e-9)
        return self.snapshot.consumed / wallclock

    @property
    def high_invalid_rate(self) -> bool:
        """``True`` iff ``invalid_rate`` exceeds the configured threshold.

        Returns
        -------
        bool
            Trigger for the ``⚠ elevated invalid rate`` marker.
        """
        return self.snapshot.invalid_rate > self.high_invalid_rate_threshold


def _format_partition_table(counts: dict[int, int]) -> list[str]:
    """Render per-partition counts as a Markdown table.

    Parameters
    ----------
    counts : dict of int to int
        Per-partition message totals.

    Returns
    -------
    list of str
        Markdown lines.
    """
    lines = ["| Partition | Messages |", "|---:|---:|"]
    if not counts:
        lines.append("| _none_ | 0 |")
        return lines
    for partition_id in sorted(counts):
        lines.append(f"| {partition_id} | {counts[partition_id]:_} |")
    return lines


def _format_error_class_table(counts: dict) -> list[str]:
    """Render per-error-class counts as a Markdown table.

    Parameters
    ----------
    counts : dict of ErrorClass to int
        Per-class histogram.

    Returns
    -------
    list of str
        Markdown lines.
    """
    lines = ["| Error class | Count |", "|---|---:|"]
    if not counts:
        lines.append("| _none_ | 0 |")
        return lines
    for error_class, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        # Enum values render as the bare symbol name.
        name = getattr(error_class, "value", str(error_class))
        lines.append(f"| {name} | {n:_} |")
    return lines


def _format_validator_table(counts: dict[str, int]) -> list[str]:
    """Render per-validator counts as a Markdown table.

    Parameters
    ----------
    counts : dict of str to int
        Per-validator histogram.

    Returns
    -------
    list of str
        Markdown lines.
    """
    lines = ["| Validator | Count |", "|---|---:|"]
    if not counts:
        lines.append("| _none_ | 0 |")
        return lines
    for validator_name, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| {validator_name} | {n:_} |")
    return lines


def _format_top_fields_table(entries: list[tuple[str, int]]) -> list[str]:
    """Render top-N failing field paths as a Markdown table.

    Parameters
    ----------
    entries : list of (str, int)
        Pre-sorted top-N entries from
        :attr:`ValidatorSnapshot.top_failing_fields`.

    Returns
    -------
    list of str
        Markdown lines.
    """
    lines = ["| Field path | Count |", "|---|---:|"]
    if not entries:
        lines.append("| _none_ | 0 |")
        return lines
    for field_path, n in entries:
        lines.append(f"| `{field_path}` | {n:_} |")
    return lines


def render_markdown(report: ValidatorRunReport) -> str:
    """Render *report* as the Markdown artifact written to ``docs/results/``.

    Parameters
    ----------
    report : ValidatorRunReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text.
    """
    snap = report.snapshot
    started = report.started_at.astimezone(timezone.utc).isoformat()
    ended = report.ended_at.astimezone(timezone.utc).isoformat()
    skew_mark = "✅" if snap.partition_skew_pass else "⚠ skew check failed"
    invalid_mark = (
        "⚠ elevated invalid rate" if report.high_invalid_rate else "✅"
    )
    lines: list[str] = []
    lines.append("# Week 2 — Validator Run Results")
    lines.append("")
    lines.append(f"**Started:** {started}")
    lines.append(f"**Ended:** {ended}")
    lines.append(f"**Source topic:** `{report.source_topic}`")
    lines.append(f"**Validated topic:** `{report.validated_topic}`")
    lines.append(f"**DLQ topic:** `{report.dlq_topic}`")
    lines.append(f"**Consumer group:** `{report.consumer_group}`")
    lines.append("")
    lines.append("## Counters")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Duration | {report.duration_s:.2f} s |")
    lines.append(f"| Consumed | {snap.consumed:_} |")
    lines.append(f"| Validated | {snap.validated:_} |")
    lines.append(f"| Invalid (total) | {snap.invalid_total:_} |")
    lines.append(
        f"| Invalid rate | {snap.invalid_rate * 100.0:.2f}% — {invalid_mark} |"
    )
    lines.append(
        f"| Sustained consume rate | "
        f"{report.sustained_consume_eps:,.0f} evt/s |"
    )
    lines.append("")
    lines.append("## Invalid by error class")
    lines.append("")
    lines.extend(_format_error_class_table(snap.invalid_by_class))
    lines.append("")
    lines.append("## Invalid by validator")
    lines.append("")
    lines.extend(_format_validator_table(snap.invalid_by_validator))
    lines.append("")
    lines.append("## Top failing fields")
    lines.append("")
    lines.extend(_format_top_fields_table(snap.top_failing_fields))
    lines.append("")
    lines.append("## Validation latency (µs)")
    lines.append("")
    lines.append("| Statistic | Value |")
    lines.append("|---|---:|")
    lines.append(f"| p50 | {snap.validation_latency_us_p50:.2f} |")
    lines.append(f"| p95 | {snap.validation_latency_us_p95:.2f} |")
    lines.append(f"| p99 | {snap.validation_latency_us_p99:.2f} |")
    lines.append("")
    lines.append("## Partition skew sanity check")
    lines.append("")
    lines.append(
        f"`partition_skew_ratio = max / mean = {snap.partition_skew_ratio:.3f}` "
        f"(threshold `< {snap.skew_threshold:.2f}`) — {skew_mark}"
    )
    lines.append("")
    lines.extend(_format_partition_table(snap.partition_counts))
    lines.append("")
    if report.notes:
        lines.append("## Notes")
        lines.append("")
        lines.append(report.notes)
        lines.append("")
    return "\n".join(lines)
