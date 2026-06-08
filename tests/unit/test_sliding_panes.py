"""Unit tests for :mod:`streaming_feature_store.sliding.panes`."""

from __future__ import annotations

from streaming_feature_store.sliding.aggregators import FiveMinuteAggregator
from streaming_feature_store.sliding.models import WindowResolution
from streaming_feature_store.sliding.panes import (
    PanedSlidingWindow,
    SlidingWindowManager,
)

R5M = WindowResolution.W_5M_SLIDE_1M
MINUTE = 60_000
# A minute-aligned base so window-ends land on slide boundaries.
BASE = (1_000_000_000_000 // MINUTE) * MINUTE


def _paned() -> PanedSlidingWindow:
    return PanedSlidingWindow(R5M, FiveMinuteAggregator())


# ---------------------------------------------------------------------------
# PanedSlidingWindow
# ---------------------------------------------------------------------------


def test_add_routes_event_to_correct_pane_index(sliding_events) -> None:
    paned = _paned()
    paned.add(sliding_events.click(ts_ms=130_000), 130_000)  # 130 s / 60 s slide
    assert set(paned.panes) == {2}


def test_add_reuses_existing_pane(sliding_events) -> None:
    paned = _paned()
    paned.add(sliding_events.click(ts_ms=130_000), 130_000)
    paned.add(sliding_events.click(ts_ms=135_000), 135_000)
    assert set(paned.panes) == {2}
    assert paned.panes[2].click_count == 2


def test_window_record_merges_panes_over_window(sliding_events) -> None:
    paned = _paned()
    # One click in each of panes 0..4 (minutes 0..4).
    for minute in range(5):
        paned.add(sliding_events.click(ts_ms=BASE + minute * MINUTE), BASE + minute * MINUTE)
    record = paned.window_record(BASE + 5 * MINUTE)
    assert record is not None
    assert record.click_count == 5
    assert record.window_start_ms == BASE
    assert record.window_end_ms == BASE + 5 * MINUTE


def test_window_record_none_when_all_panes_empty() -> None:
    assert _paned().window_record(BASE + 5 * MINUTE) is None


def test_window_record_decays_as_panes_age_out(sliding_events) -> None:
    paned = _paned()
    paned.add(sliding_events.click(ts_ms=BASE), BASE)  # pane in minute 0 only
    # Window ending at minute 6 covers panes [1..5] — minute 0 has slid out.
    assert paned.window_record(BASE + 6 * MINUTE) is None


def test_window_ends_including(sliding_events) -> None:
    paned = _paned()
    ends = paned.window_ends_including(130_000)  # pane index 2
    assert ends == [(2 + k) * MINUTE for k in range(1, 6)]


def test_gc_drops_panes_below_cutoff(sliding_events) -> None:
    paned = _paned()
    paned.add(sliding_events.click(ts_ms=BASE), BASE)  # pane index BASE/MINUTE
    # cutoff = (wm - size - lateness)//slide; choose wm so cutoff > pane index.
    paned.gc(watermark_ms=BASE + 7 * MINUTE, lateness_ms=30_000)
    assert paned.panes == {}


def test_gc_retains_lateness_tail(sliding_events) -> None:
    paned = _paned()
    paned.add(sliding_events.click(ts_ms=BASE), BASE)
    # cutoff = (BASE + 5min - 5min - 30s)//60s == BASE/60s - 1 < pane index.
    paned.gc(watermark_ms=BASE + 5 * MINUTE, lateness_ms=30_000)
    assert set(paned.panes) == {BASE // MINUTE}


# ---------------------------------------------------------------------------
# SlidingWindowManager — routing across resolutions
# ---------------------------------------------------------------------------


def test_manager_routes_event_to_all_three_resolutions(sliding_events) -> None:
    mgr = SlidingWindowManager(allowed_lateness_ms=30_000)
    mgr.add(sliding_events.click(ts_ms=BASE), BASE, watermark_ms=None, partition=0)
    users = mgr._users["u1"]
    rec_5m = users[WindowResolution.W_5M_SLIDE_1M].window_record(BASE + 5 * MINUTE)
    rec_1h = users[WindowResolution.W_1H_SLIDE_5M].window_record(BASE + 3_600_000)
    rec_24h = users[WindowResolution.W_24H_SLIDE_1H].window_record(BASE + 86_400_000)
    assert rec_5m.click_count == 1
    assert rec_1h.click_count == 1
    assert rec_24h.click_count is None  # 24 h excludes clicks (design §2.14)


def test_manager_active_user_count(sliding_events) -> None:
    mgr = SlidingWindowManager(allowed_lateness_ms=30_000)
    mgr.add(sliding_events.click(user_id="u1", ts_ms=BASE), BASE, None)
    mgr.add(sliding_events.click(user_id="u2", ts_ms=BASE), BASE, None)
    mgr.add(sliding_events.click(user_id="u1", ts_ms=BASE), BASE, None)  # reuse u1
    assert mgr.active_user_count == 2


# ---------------------------------------------------------------------------
# SlidingWindowManager — emission cursor
# ---------------------------------------------------------------------------


def _manager_5m() -> SlidingWindowManager:
    return SlidingWindowManager(allowed_lateness_ms=30_000, resolutions=[R5M])


def test_emit_cursor_fires_each_slide_once(sliding_events) -> None:
    mgr = _manager_5m()
    mgr.add(sliding_events.click(ts_ms=BASE), BASE, None)
    mgr.emit_due_windows(BASE)  # initialise cursor; window ending BASE has no pane
    records = mgr.emit_due_windows(BASE + 5 * MINUTE)
    ends = sorted(r.window_end_ms for r in records)
    assert ends == [BASE + k * MINUTE for k in range(1, 6)]
    assert all(r.emission_seq == 0 for r in records)


def test_emit_cursor_does_not_refire_emitted_window(sliding_events) -> None:
    mgr = _manager_5m()
    mgr.add(sliding_events.click(ts_ms=BASE), BASE, None)
    mgr.emit_due_windows(BASE + 5 * MINUTE)
    assert mgr.emit_due_windows(BASE + 5 * MINUTE) == []


def test_emit_skips_users_with_no_contributing_panes(sliding_events) -> None:
    mgr = _manager_5m()
    # u1 active in minute 0; u2 active only far in the future (no contributing pane yet).
    far = BASE + 100 * MINUTE
    mgr.add(sliding_events.click(user_id="u1", ts_ms=BASE), BASE, None)
    mgr.add(sliding_events.click(user_id="u2", ts_ms=far), far, None)
    mgr.emit_due_windows(BASE)
    records = mgr.emit_due_windows(BASE + 5 * MINUTE)
    assert {r.user_id for r in records} == {"u1"}


# ---------------------------------------------------------------------------
# SlidingWindowManager — allowed-lateness re-fire and very-late routing
# ---------------------------------------------------------------------------


def test_late_event_within_lateness_refires_with_higher_seq(sliding_events) -> None:
    mgr = _manager_5m()
    for _ in range(3):
        mgr.add(sliding_events.click(ts_ms=BASE + 5 * MINUTE), BASE + 5 * MINUTE, None)
    wm = BASE + 6 * MINUTE
    first = mgr.emit_due_windows(wm)
    target = next(r for r in first if r.window_end_ms == BASE + 6 * MINUTE)
    assert (target.click_count, target.emission_seq) == (3, 0)
    # Late click into the same pane, within the 30 s lateness tail.
    late = mgr.add(
        sliding_events.click(ts_ms=BASE + 5 * MINUTE + 40_000),
        BASE + 5 * MINUTE + 40_000,
        wm,
    )
    assert late is None
    refire = mgr.emit_due_windows(wm)
    assert len(refire) == 1
    assert (refire[0].click_count, refire[0].emission_seq) == (4, 1)


def test_very_late_event_returned_not_added(sliding_events) -> None:
    mgr = _manager_5m()
    wm = BASE + 6 * MINUTE
    # ts older than wm - lateness -> very late.
    event = sliding_events.click(ts_ms=BASE)
    late = mgr.add(event, BASE, wm)
    assert late is event
    assert mgr.active_user_count == 0  # not folded into any pane


def test_add_with_no_watermark_does_not_mark_late(sliding_events) -> None:
    mgr = _manager_5m()
    late = mgr.add(sliding_events.click(ts_ms=BASE), BASE, watermark_ms=None)
    assert late is None
    assert mgr._dirty == set()


# ---------------------------------------------------------------------------
# SlidingWindowManager — rebalance and GC bookkeeping
# ---------------------------------------------------------------------------


def test_drop_partitions_removes_owned_users(sliding_events) -> None:
    mgr = SlidingWindowManager(allowed_lateness_ms=30_000)
    mgr.add(sliding_events.click(user_id="u1", ts_ms=BASE), BASE, None, partition=0)
    mgr.add(sliding_events.click(user_id="u2", ts_ms=BASE), BASE, None, partition=1)
    dropped = mgr.drop_partitions({0})
    assert dropped == 1
    assert set(mgr._users) == {"u2"}


def test_drop_partitions_skips_users_without_recorded_partition(sliding_events) -> None:
    mgr = SlidingWindowManager(allowed_lateness_ms=30_000)
    mgr.add(sliding_events.click(user_id="u1", ts_ms=BASE), BASE, None)  # no partition
    assert mgr.drop_partitions({0}) == 0
    assert set(mgr._users) == {"u1"}


def test_gc_prunes_stale_emission_seq_entries() -> None:
    mgr = _manager_5m()
    mgr._seq[("u1", R5M, BASE)] = 5  # ancient window-end
    mgr.emit_due_windows(BASE + 1_000 * MINUTE)  # far-future watermark
    assert ("u1", R5M, BASE) not in mgr._seq
