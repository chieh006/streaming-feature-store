"""Unit tests for :class:`ValidatorAccountant`."""

from __future__ import annotations

import pytest

from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import ErrorClass


class StepClock:
    """Monotonic stub: returns ``start, start+step, start+2*step, ...``."""

    def __init__(self, step: float = 0.5, start: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        v = self.t
        self.t += self.step
        return v


def test_accountant_rejects_zero_reservoir() -> None:
    with pytest.raises(ValueError):
        ValidatorAccountant(reservoir_size=0)


def test_accountant_rejects_non_positive_threshold() -> None:
    with pytest.raises(ValueError):
        ValidatorAccountant(skew_threshold=0.0)


def test_accountant_rejects_non_positive_top_k() -> None:
    with pytest.raises(ValueError):
        ValidatorAccountant(top_fields_k=0)


def test_accountant_threshold_property() -> None:
    acc = ValidatorAccountant(skew_threshold=1.5)
    assert acc.skew_threshold == 1.5


def test_accountant_records_consumed() -> None:
    acc = ValidatorAccountant()
    acc.record_consumed(7)
    acc.record_consumed()
    snap = acc.snapshot()
    assert snap.consumed == 8


def test_accountant_records_valid() -> None:
    acc = ValidatorAccountant()
    for _ in range(5):
        acc.record_valid()
    snap = acc.snapshot()
    assert snap.validated == 5


def test_accountant_records_invalid_by_class() -> None:
    acc = ValidatorAccountant()
    for _ in range(3):
        acc.record_invalid(
            error_class=ErrorClass.OUT_OF_RANGE,
            validator_name="PriceRangeValidator",
            error_field_path="payload.price_cents",
        )
    snap = acc.snapshot()
    assert snap.invalid_total == 3
    assert snap.invalid_by_class[ErrorClass.OUT_OF_RANGE] == 3


def test_accountant_top_failing_fields_sorted() -> None:
    acc = ValidatorAccountant()
    for _ in range(5):
        acc.record_invalid(
            error_class=ErrorClass.OUT_OF_RANGE,
            validator_name="P",
            error_field_path="price",
        )
    for _ in range(3):
        acc.record_invalid(
            error_class=ErrorClass.MALFORMED_RECORD,
            validator_name="U",
            error_field_path="user_id",
        )
    snap = acc.snapshot()
    assert snap.top_failing_fields[0] == ("price", 5)
    assert snap.top_failing_fields[1] == ("user_id", 3)


def test_accountant_top_failing_fields_caps_at_k() -> None:
    acc = ValidatorAccountant(top_fields_k=2)
    for path in ("a", "b", "c", "d"):
        acc.record_invalid(
            error_class=ErrorClass.OUT_OF_RANGE,
            validator_name="V",
            error_field_path=path,
        )
    snap = acc.snapshot()
    assert len(snap.top_failing_fields) == 2


def test_accountant_invalid_without_field_path_omits_from_top() -> None:
    acc = ValidatorAccountant()
    acc.record_invalid(
        error_class=ErrorClass.PIPELINE_INTERNAL_ERROR,
        validator_name="V",
        error_field_path=None,
    )
    snap = acc.snapshot()
    assert snap.invalid_total == 1
    assert snap.top_failing_fields == []


def test_accountant_records_partition_counts() -> None:
    acc = ValidatorAccountant()
    for p in [0, 0, 1, 3, 3, 3]:
        acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_counts == {0: 2, 1: 1, 3: 3}


def test_accountant_skew_ratio_uniform() -> None:
    acc = ValidatorAccountant()
    for p in range(12):
        for _ in range(100):
            acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_skew_ratio == pytest.approx(1.0)
    assert snap.partition_skew_pass is True


def test_accountant_skew_ratio_pathological() -> None:
    acc = ValidatorAccountant()
    for _ in range(1100):
        acc.record_partition(0)
    for p in range(1, 12):
        for _ in range(9):
            acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_skew_ratio > 5.0
    assert snap.partition_skew_pass is False


def test_accountant_records_validation_latency() -> None:
    acc = ValidatorAccountant()
    for us in (10.0, 20.0, 30.0, 40.0, 100.0):
        acc.record_validation_latency_us(us)
    snap = acc.snapshot()
    assert snap.validation_latency_us_p50 > 0.0
    assert snap.validation_latency_us_p99 >= snap.validation_latency_us_p50


def test_accountant_validation_latency_rejects_negative() -> None:
    acc = ValidatorAccountant()
    with pytest.raises(ValueError):
        acc.record_validation_latency_us(-1.0)


def test_accountant_invalid_rate_zero_when_consumed_zero() -> None:
    snap = ValidatorAccountant().snapshot()
    assert snap.invalid_rate == 0.0


def test_accountant_invalid_rate_calculated() -> None:
    acc = ValidatorAccountant()
    for _ in range(10):
        acc.record_consumed()
    for _ in range(3):
        acc.record_invalid(
            error_class=ErrorClass.OUT_OF_RANGE,
            validator_name="V",
            error_field_path="x",
        )
    snap = acc.snapshot()
    assert snap.invalid_rate == pytest.approx(0.3)


def test_accountant_convenience_class_counters() -> None:
    acc = ValidatorAccountant()
    acc.record_invalid(
        error_class=ErrorClass.DESERIALIZE_FAILURE,
        validator_name="X",
        error_field_path=None,
    )
    acc.record_invalid(
        error_class=ErrorClass.SCHEMA_MISMATCH,
        validator_name="X",
        error_field_path=None,
    )
    acc.record_invalid(
        error_class=ErrorClass.PIPELINE_INTERNAL_ERROR,
        validator_name="X",
        error_field_path=None,
    )
    snap = acc.snapshot()
    assert snap.deserialize_failed == 1
    assert snap.schema_mismatches == 1
    assert snap.pipeline_internal_errors == 1


def test_accountant_reservoir_replacement_path() -> None:
    acc = ValidatorAccountant(reservoir_size=4, seed=0)
    for us in (1.0, 2.0, 3.0, 4.0):
        acc.record_validation_latency_us(us)
    for us in (5.0, 6.0, 7.0, 8.0, 9.0):
        acc.record_validation_latency_us(us)
    snap = acc.snapshot()
    assert snap.validation_latency_us_p50 >= 0.0


def test_accountant_no_partitions_passes_skew() -> None:
    snap = ValidatorAccountant().snapshot()
    assert snap.partition_skew_ratio == 0.0
    assert snap.partition_skew_pass is True


def test_accountant_wallclock_advances() -> None:
    clock = StepClock(step=0.1)
    acc = ValidatorAccountant(clock=clock)
    snap = acc.snapshot()
    assert snap.wallclock_s > 0.0
