"""Unit tests for :class:`SinkRunReport` rendering."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from streaming_feature_store.sink.accountant import SinkSnapshot
from streaming_feature_store.sink.report import SinkRunReport, render_markdown


def _snap(**over) -> SinkSnapshot:
    base = dict(
        consumed=100,
        inserted=100,
        conflict_skipped=0,
        deserialize_failed=0,
        batches_flushed=2,
        batch_size_p50=50.0,
        batch_size_p99=80.0,
        flush_latency_ms_p50=2.0,
        flush_latency_ms_p95=4.0,
        flush_latency_ms_p99=5.0,
        partition_counts={0: 30, 1: 20, 2: 25, 3: 25},
        partition_skew_ratio=1.2,
        partition_skew_pass=True,
        skew_threshold=2.0,
        wallclock_s=10.0,
    )
    base.update(over)
    return SinkSnapshot(**base)


def _report(**over) -> SinkRunReport:
    started = datetime(2026, 5, 20, tzinfo=timezone.utc)
    base = dict(
        topic="e-commerce-events-feed",
        consumer_group="postgres-sink",
        started_at=started,
        ended_at=started + timedelta(seconds=10),
        snapshot=_snap(),
    )
    base.update(over)
    return SinkRunReport(**base)


def test_sink_report_duration_property() -> None:
    report = _report()
    assert report.duration_s == 10.0


def test_sink_report_sustained_eps_property() -> None:
    report = _report(snapshot=_snap(inserted=2000, wallclock_s=10.0))
    assert report.sustained_insert_eps == 200.0


def test_sink_report_render_markdown_includes_partition_table() -> None:
    report = _report()
    md = render_markdown(report)
    for partition in report.snapshot.partition_counts:
        assert f"| {partition} |" in md


def test_sink_report_render_markdown_includes_header() -> None:
    report = _report()
    md = render_markdown(report)
    assert "# Week 1 — PostgreSQL Sink Run Results" in md
    assert "e-commerce-events-feed" in md
    assert "postgres-sink" in md


def test_sink_report_render_markdown_flags_failed_skew_check() -> None:
    snap = _snap(partition_skew_ratio=3.5, partition_skew_pass=False)
    md = render_markdown(_report(snapshot=snap))
    assert "⚠ skew check failed" in md


def test_sink_report_render_markdown_empty_partitions() -> None:
    snap = _snap(partition_counts={}, partition_skew_ratio=0.0)
    md = render_markdown(_report(snapshot=snap))
    assert "_none_" in md


def test_sink_report_render_markdown_includes_notes_section() -> None:
    md = render_markdown(_report(notes="ran during smoke test"))
    assert "## Notes" in md
    assert "ran during smoke test" in md


def test_sink_report_render_markdown_omits_notes_when_absent() -> None:
    md = render_markdown(_report())
    assert "## Notes" not in md


def test_sink_report_sustained_zero_wallclock_safe() -> None:
    report = _report(snapshot=_snap(inserted=10, wallclock_s=0.0))
    # No divide-by-zero; just yields a very large rate.
    assert report.sustained_insert_eps > 0
