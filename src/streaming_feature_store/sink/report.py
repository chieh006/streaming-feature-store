"""Pydantic :class:`SinkRunReport` and Markdown renderer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.sink.accountant import SinkSnapshot

logger = logging.getLogger(__name__)


class SinkRunReport(BaseModel):
    """Aggregate :class:`SinkRunner` result.

    Parameters
    ----------
    topic : str
        Source Kafka topic the sink subscribed to.
    consumer_group : str
        Kafka consumer ``group.id`` used for the run.
    started_at : datetime
        Wall-clock start time (UTC).
    ended_at : datetime
        Wall-clock end time (UTC).
    snapshot : SinkSnapshot
        Final accountant snapshot.
    notes : str or None
        Free-form notes recorded by the runner.
    """

    model_config = ConfigDict(frozen=True)

    topic: str
    consumer_group: str
    started_at: datetime
    ended_at: datetime
    snapshot: SinkSnapshot
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
    def sustained_insert_eps(self) -> float:
        """Sustained rows/sec actually written to ``raw_events``.

        Returns
        -------
        float
            ``inserted / max(wallclock_s, 1e-9)``.
        """
        wallclock = max(self.snapshot.wallclock_s, 1e-9)
        return self.snapshot.inserted / wallclock


def _format_partition_table(counts: dict[int, int]) -> list[str]:
    """Render per-partition counts as a Markdown table.

    Parameters
    ----------
    counts : dict of int to int
        Per-partition message totals.

    Returns
    -------
    list of str
        Markdown lines (header + body rows).
    """
    lines = ["| Partition | Messages |", "|---:|---:|"]
    if not counts:
        lines.append("| _none_ | 0 |")
        return lines
    for partition_id in sorted(counts):
        lines.append(f"| {partition_id} | {counts[partition_id]:_} |")
    return lines


def render_markdown(report: SinkRunReport) -> str:
    """Render *report* as the Markdown artifact written to ``docs/results/``.

    Parameters
    ----------
    report : SinkRunReport
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
    lines: list[str] = []
    lines.append("# Week 1 — PostgreSQL Sink Run Results")
    lines.append("")
    lines.append(f"**Started:** {started}")
    lines.append(f"**Ended:** {ended}")
    lines.append(f"**Topic:** {report.topic}")
    lines.append(f"**Consumer group:** {report.consumer_group}")
    lines.append("")
    lines.append("## Counters")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Duration | {report.duration_s:.2f} s |")
    lines.append(f"| Consumed | {snap.consumed:_} |")
    lines.append(f"| Inserted | {snap.inserted:_} |")
    lines.append(f"| Conflict-skipped | {snap.conflict_skipped:_} |")
    lines.append(f"| Deserialize failed | {snap.deserialize_failed:_} |")
    lines.append(f"| Batches flushed | {snap.batches_flushed:_} |")
    lines.append(
        f"| Sustained insert rate | {report.sustained_insert_eps:,.0f} rows/s |"
    )
    lines.append("")
    lines.append("## Batch sizes")
    lines.append("")
    lines.append("| Statistic | Value |")
    lines.append("|---|---:|")
    lines.append(f"| p50 | {snap.batch_size_p50:,.1f} |")
    lines.append(f"| p99 | {snap.batch_size_p99:,.1f} |")
    lines.append("")
    lines.append("## Flush latency (ms)")
    lines.append("")
    lines.append("| Statistic | Value |")
    lines.append("|---|---:|")
    lines.append(f"| p50 | {snap.flush_latency_ms_p50:.2f} |")
    lines.append(f"| p95 | {snap.flush_latency_ms_p95:.2f} |")
    lines.append(f"| p99 | {snap.flush_latency_ms_p99:.2f} |")
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
