"""Unit tests for :func:`derive_transactional_id` (design week2_03 §2.3)."""

from __future__ import annotations

import pytest

from streaming_feature_store.eos import derive_transactional_id


def test_format_is_group_dash_ordinal() -> None:
    """The id is ``f"{group_id}-{ordinal}"``."""
    assert derive_transactional_id("sliding-features-job", 3) == (
        "sliding-features-job-3"
    )


def test_zero_ordinal_is_allowed() -> None:
    """A lone single process uses ordinal ``0``."""
    assert derive_transactional_id("validator-feed", 0) == "validator-feed-0"


def test_unique_across_ordinals() -> None:
    """Ordinals 0..11 yield 12 distinct ids (no two members collide)."""
    ids = {derive_transactional_id("g", n) for n in range(12)}
    assert len(ids) == 12


def test_stable_for_same_inputs() -> None:
    """Same ``(group, ordinal)`` is deterministic — restart stability."""
    assert derive_transactional_id("g", 7) == derive_transactional_id("g", 7)


def test_negative_ordinal_raises() -> None:
    """A negative ordinal is rejected."""
    with pytest.raises(ValueError, match="ordinal must be >= 0"):
        derive_transactional_id("g", -1)


@pytest.mark.parametrize("group", ["", "   "])
def test_empty_or_blank_group_raises(group: str) -> None:
    """An empty/blank group id is rejected."""
    with pytest.raises(ValueError, match="non-empty"):
        derive_transactional_id(group, 0)
