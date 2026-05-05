"""Unit tests for ``scripts/run_schema_evolution.py``.

Tests the driver's pure logic with the Schema Registry, Kafka producer, and
Kafka consumer all mocked out.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# scripts/ is not a package; import via path manipulation.
_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import run_schema_evolution as driver  # noqa: E402
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig  # noqa: E402
from streaming_feature_store.schemas import (  # noqa: E402
    SCHEMAS_ROOT,
    load_schema_set,
)
from streaming_feature_store.schemas.evolution import (  # noqa: E402
    EvolutionDrillResult,
)
from streaming_feature_store.schemas.registry import RegistryError  # noqa: E402


@pytest.fixture
def baseline() -> dict:
    return load_schema_set(SCHEMAS_ROOT / "ecommerce" / "v1")


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers="b:9092",
        security_protocol="PLAINTEXT",
        topic="e-commerce-events",
    )


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(url="http://r:8081")


# ---------------------------------------------------------------------------
# Drill-spec construction
# ---------------------------------------------------------------------------


def test_build_drill_specs_returns_three() -> None:
    specs = driver.build_drill_specs()
    assert [s.drill_id for s in specs] == ["drill1", "drill2", "drill3"]


def test_select_specs_filters_by_choice() -> None:
    specs = driver.build_drill_specs()
    assert driver._select_specs("1", specs)[0].drill_id == "drill1"
    assert driver._select_specs("3", specs)[0].drill_id == "drill3"
    assert len(driver._select_specs("all", specs)) == 3


# ---------------------------------------------------------------------------
# Sample event factory
# ---------------------------------------------------------------------------


def test_build_sample_events_cycles_event_types() -> None:
    events = driver._build_sample_events(6)
    assert events[0].event_type.value == "CLICK"
    assert events[1].event_type.value == "PURCHASE"
    assert events[2].event_type.value == "PAGE_VIEW"
    assert events[3].event_type.value == "CLICK"


def test_build_sample_events_returns_requested_count() -> None:
    assert len(driver._build_sample_events(0)) == 0
    assert len(driver._build_sample_events(5)) == 5


# ---------------------------------------------------------------------------
# Registration / version helpers
# ---------------------------------------------------------------------------


def test_attempt_registration_success() -> None:
    registry = MagicMock()
    registry.register.return_value = 42
    accepted, error, schema_id = driver._attempt_registration(
        registry, "subject", "{}"
    )
    assert accepted is True
    assert error is None
    assert schema_id == 42


def test_attempt_registration_rejection() -> None:
    registry = MagicMock()
    registry.register.side_effect = RegistryError("incompatible")
    accepted, error, schema_id = driver._attempt_registration(
        registry, "subject", "{}"
    )
    assert accepted is False
    assert "incompatible" in (error or "")
    assert schema_id is None


def test_resolve_version_returns_value() -> None:
    registry = MagicMock()
    latest = MagicMock()
    latest.schema_id = 42
    latest.version = 2
    registry.get_latest.return_value = latest
    assert driver._resolve_version(registry, "subj", 42) == 2


def test_resolve_version_handles_registry_error() -> None:
    registry = MagicMock()
    registry.get_latest.side_effect = RegistryError("nope")
    assert driver._resolve_version(registry, "subj", 1) is None


# ---------------------------------------------------------------------------
# Snapshot-only path: writes files; never contacts the Registry
# ---------------------------------------------------------------------------


def test_run_drill_snapshot_only_writes_files_and_skips_registry(
    baseline: dict,
    tmp_path: Path,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    spec = driver.build_drill_specs()[0]
    registry = MagicMock()
    result = driver.run_drill(
        spec,
        baseline_composite=baseline,
        prior_schema_str="{}",
        snapshot_root=tmp_path,
        registry=registry,
        subject="subject",
        kafka_config=kafka_config,
        registry_config=registry_config,
        snapshot_only=True,
        poll_timeout_s=0.1,
    )
    assert (tmp_path / "v1.1" / "ecommerce_event.avsc").is_file()
    registry.register.assert_not_called()
    assert result.registration_accepted is False
    assert result.serde_matrix == {}


# ---------------------------------------------------------------------------
# Registration-failure path: serde matrix stays empty
# ---------------------------------------------------------------------------


def test_run_drill_skips_serde_matrix_on_registration_failure(
    baseline: dict,
    tmp_path: Path,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    spec = driver.build_drill_specs()[0]
    registry = MagicMock()
    registry.register.side_effect = RegistryError("incompatible")

    result = driver.run_drill(
        spec,
        baseline_composite=baseline,
        prior_schema_str="{}",
        snapshot_root=tmp_path,
        registry=registry,
        subject="subject",
        kafka_config=kafka_config,
        registry_config=registry_config,
        snapshot_only=False,
        poll_timeout_s=0.1,
    )
    assert result.registration_accepted is False
    assert "incompatible" in (result.registration_error or "")
    assert result.serde_matrix == {}


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def test_cleanup_soft_deletes_then_reregisters_baseline() -> None:
    registry = MagicMock()
    registry.delete_subject.return_value = [2, 3, 4]
    registry.register.return_value = 1
    driver.cleanup_experiment_versions(
        registry, "subject", baseline_version=1
    )
    registry.delete_subject.assert_called_once_with(
        "subject", permanent=False
    )
    registry.register.assert_called_once()


def test_cleanup_swallows_delete_error() -> None:
    registry = MagicMock()
    registry.delete_subject.side_effect = RegistryError("missing")
    # Must not raise
    driver.cleanup_experiment_versions(
        registry, "subject", baseline_version=1
    )
    registry.register.assert_not_called()


def test_cleanup_swallows_reregister_error() -> None:
    registry = MagicMock()
    registry.delete_subject.return_value = [2]
    registry.register.side_effect = RegistryError("nope")
    # Must not raise
    driver.cleanup_experiment_versions(
        registry, "subject", baseline_version=1
    )


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def _accepted_result() -> EvolutionDrillResult:
    return EvolutionDrillResult(
        drill_id="drill1",
        description="Add optional device_type",
        mutation={"kind": "add_optional_field"},
        registration_accepted=True,
        registered_schema_id=42,
        registered_version=2,
        serde_matrix={
            "producer=v2,consumer=v1": "ok (5/5)",
            "producer=v1,consumer=v2": "ok (5/5)",
        },
    )


def _rejected_result() -> EvolutionDrillResult:
    return EvolutionDrillResult(
        drill_id="drill_neg",
        description="Add required field",
        mutation={"kind": "add_required_field"},
        registration_accepted=False,
        registration_error="Schema is incompatible: ...",
    )


def test_render_report_includes_all_drills() -> None:
    report = driver.render_report(
        [_accepted_result(), _rejected_result()],
        subject="e-commerce-events-value",
        compatibility="BACKWARD",
        generated_at=datetime(2026, 5, 4, tzinfo=timezone.utc),
    )
    assert "drill1" in report
    assert "drill_neg" in report
    assert "BACKWARD" in report
    assert "[OK]" in report
    assert "[REJECTED]" in report


def test_render_report_omits_serde_matrix_for_rejection() -> None:
    report = driver.render_report(
        [_rejected_result()],
        subject="s",
        compatibility="BACKWARD",
        generated_at=datetime.now(tz=timezone.utc),
    )
    assert "Serde producer=" not in report
    assert "incompatible" in report


def test_render_drill_section_includes_notes() -> None:
    result = _accepted_result().model_copy(update={"notes": "interesting"})
    section = driver._render_drill_section(result)
    assert "interesting" in section


def test_verdict_icon_branches() -> None:
    assert driver._verdict_icon(True) == "[OK]"
    assert driver._verdict_icon(False) == "[REJECTED]"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_arg_parser_defaults() -> None:
    args = driver._build_arg_parser().parse_args([])
    assert args.drill == "all"
    assert args.snapshot_only is False
    assert args.keep_subject is False


def test_arg_parser_drill_choice_accepted() -> None:
    args = driver._build_arg_parser().parse_args(["--drill", "2"])
    assert args.drill == "2"


def test_arg_parser_rejects_invalid_drill() -> None:
    with pytest.raises(SystemExit):
        driver._build_arg_parser().parse_args(["--drill", "9"])


# ---------------------------------------------------------------------------
# main() orchestration with everything mocked
# ---------------------------------------------------------------------------


def test_main_snapshot_only_does_not_contact_registry(tmp_path: Path) -> None:
    report_path = tmp_path / "report.md"
    snapshot_root = tmp_path / "snapshots"
    with patch.object(driver, "SchemaRegistry") as registry_cls:
        rc = driver.main(
            [
                "--drill",
                "all",
                "--snapshot-only",
                "--snapshot-root",
                str(snapshot_root),
                "--report-path",
                str(report_path),
            ]
        )
    assert rc == 0
    registry_cls.assert_not_called()
    assert report_path.is_file()
    assert (snapshot_root / "v1.1").is_dir()


def test_main_pins_compatibility_and_runs_drills(tmp_path: Path) -> None:
    report_path = tmp_path / "report.md"
    snapshot_root = tmp_path / "snapshots"
    with patch.object(driver, "SchemaRegistry") as registry_cls, patch.object(
        driver, "_run_serde_matrix", return_value={}
    ) as matrix_mock:
        registry = MagicMock()
        registry.register.return_value = 42
        latest = MagicMock()
        latest.schema_id = 42
        latest.version = 2
        registry.get_latest.return_value = latest
        registry.delete_subject.return_value = [2]
        registry_cls.return_value = registry

        rc = driver.main(
            [
                "--drill",
                "1",
                "--snapshot-root",
                str(snapshot_root),
                "--report-path",
                str(report_path),
            ]
        )
    assert rc == 0
    registry.set_compatibility.assert_called_once_with(
        "e-commerce-events-value", "BACKWARD"
    )
    assert matrix_mock.called
    assert report_path.is_file()


def test_main_keep_subject_skips_cleanup(tmp_path: Path) -> None:
    with patch.object(driver, "SchemaRegistry") as registry_cls, patch.object(
        driver, "_run_serde_matrix", return_value={}
    ):
        registry = MagicMock()
        registry.register.return_value = 1
        latest = MagicMock()
        latest.schema_id = 1
        latest.version = 2
        registry.get_latest.return_value = latest
        registry_cls.return_value = registry

        driver.main(
            [
                "--drill",
                "1",
                "--keep-subject",
                "--snapshot-root",
                str(tmp_path / "s"),
                "--report-path",
                str(tmp_path / "r.md"),
            ]
        )
    registry.delete_subject.assert_not_called()


def test_main_returns_error_on_compatibility_failure(tmp_path: Path) -> None:
    with patch.object(driver, "SchemaRegistry") as registry_cls:
        registry = MagicMock()
        registry.set_compatibility.side_effect = RegistryError("denied")
        registry_cls.return_value = registry
        rc = driver.main(
            [
                "--drill",
                "1",
                "--snapshot-root",
                str(tmp_path / "s"),
                "--report-path",
                str(tmp_path / "r.md"),
            ]
        )
    assert rc == 1
