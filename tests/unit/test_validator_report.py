"""Unit tests for :class:`ValidatorRunReport` and its Markdown renderer."""

from __future__ import annotations

from datetime import datetime, timezone

from streaming_feature_store.validate.accountant import ValidatorSnapshot
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.report import (
    ValidatorRunReport,
    render_markdown,
)


_SENTINEL = object()


def _snapshot(
    *,
    consumed: int = 1000,
    validated: int = 950,
    invalid_total: int = 50,
    invalid_rate: float = 0.05,
    invalid_by_class=_SENTINEL,
    invalid_by_validator=_SENTINEL,
    top_fields=_SENTINEL,
    partition_counts=_SENTINEL,
    skew_ratio: float = 1.0,
    skew_pass: bool = True,
) -> ValidatorSnapshot:
    if invalid_by_class is _SENTINEL:
        invalid_by_class = {ErrorClass.OUT_OF_RANGE: invalid_total}
    if invalid_by_validator is _SENTINEL:
        invalid_by_validator = {"PriceRangeValidator": invalid_total}
    if top_fields is _SENTINEL:
        top_fields = [("payload.price_cents", invalid_total)]
    if partition_counts is _SENTINEL:
        partition_counts = {0: 100, 1: 100, 2: 100}
    return ValidatorSnapshot(
        consumed=consumed,
        validated=validated,
        invalid_total=invalid_total,
        invalid_by_class=invalid_by_class,
        invalid_by_validator=invalid_by_validator,
        invalid_by_field_path={"payload.price_cents": invalid_total},
        deserialize_failed=0,
        schema_mismatches=0,
        pipeline_internal_errors=0,
        invalid_rate=invalid_rate,
        validation_latency_us_p50=12.5,
        validation_latency_us_p95=42.0,
        validation_latency_us_p99=100.0,
        partition_counts=partition_counts,
        partition_skew_ratio=skew_ratio,
        partition_skew_pass=skew_pass,
        skew_threshold=2.0,
        top_failing_fields=top_fields,
        wallclock_s=10.0,
    )


def _report(**override_snap) -> ValidatorRunReport:
    snap = _snapshot(**override_snap)
    return ValidatorRunReport(
        source_topic="e-commerce-events-feed",
        validated_topic="validated-events",
        dlq_topic="dead-letter-queue",
        consumer_group="validator-feed",
        started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 5, 25, 12, 0, 10, tzinfo=timezone.utc),
        snapshot=snap,
    )


def test_report_duration_seconds() -> None:
    r = _report()
    assert r.duration_s == 10.0


def test_report_sustained_consume_eps() -> None:
    r = _report()
    # consumed=1000, wallclock=10 → 100 evt/s.
    assert r.sustained_consume_eps == 100.0


def test_report_high_invalid_rate_threshold_default() -> None:
    r = _report(invalid_total=1, invalid_rate=0.01)
    assert r.high_invalid_rate is False


def test_report_high_invalid_rate_marker_when_exceeded() -> None:
    r = _report(invalid_total=300, invalid_rate=0.30)
    assert r.high_invalid_rate is True


def test_render_markdown_includes_invalid_rate_marker() -> None:
    md = render_markdown(_report(invalid_total=300, invalid_rate=0.30))
    assert "elevated invalid rate" in md


def test_render_markdown_includes_skew_pass_marker() -> None:
    md = render_markdown(_report())
    assert "✅" in md


def test_render_markdown_flags_failed_skew_check() -> None:
    md = render_markdown(_report(skew_ratio=5.0, skew_pass=False))
    assert "skew check failed" in md


def test_render_markdown_includes_partition_table() -> None:
    md = render_markdown(_report())
    assert "Partition" in md and "Messages" in md


def test_render_markdown_handles_empty_partition_counts() -> None:
    md = render_markdown(_report(partition_counts={}))
    assert "_none_" in md


def test_render_markdown_handles_empty_top_fields() -> None:
    md = render_markdown(_report(top_fields=[]))
    assert "_none_" in md


def test_render_markdown_handles_empty_error_class_table() -> None:
    md = render_markdown(_report(invalid_by_class={}))
    # Empty histograms still render the header but with a "_none_" row.
    assert "Error class" in md


def test_render_markdown_handles_empty_validator_table() -> None:
    md = render_markdown(_report(invalid_by_validator={}))
    assert "Validator" in md


def test_render_markdown_includes_source_and_dlq_topics() -> None:
    md = render_markdown(_report())
    assert "e-commerce-events-feed" in md
    assert "dead-letter-queue" in md
    assert "validated-events" in md


def test_render_markdown_includes_validation_latency_percentiles() -> None:
    md = render_markdown(_report())
    assert "p50" in md
    assert "p99" in md


def test_render_markdown_includes_notes_when_present() -> None:
    snap = _snapshot()
    report = ValidatorRunReport(
        source_topic="src",
        validated_topic="val",
        dlq_topic="dlq",
        consumer_group="g",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ended_at=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        snapshot=snap,
        notes="Smoke run after deploying validator v1.0.0.",
    )
    md = render_markdown(report)
    assert "Smoke run" in md
