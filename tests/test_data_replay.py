"""Tests for tools.data.replay: clocks, ordering, dedup, increments, replay."""

from __future__ import annotations

import warnings
from datetime import date, datetime, timezone
from typing import Any

import pytest

from tools.data import (
    DataContractError,
    DataGapError,
    DataGapEvent,
    DataSourceStateEvent,
    PointInTimeError,
    QuoteTick,
    RegisteredEvent,
    Session,
    TradeTick,
    TradingPhase,
)
from tools.data.replay import (
    LiveClock,
    ReplayClock,
    W_DATA_GAP,
    W_DUPLICATE_KEY,
    W_PHASE_SKIPPED,
    W_PIT_MISSING,
    W_SEQUENCE_MISSING,
    W_SESSION_RESET,
    deduplicate_events,
    normalize_trade_increment,
    replay_events,
    sequence_events,
    session_close,
    session_open,
)


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)


def make_session(
    *, start: datetime = utc(2024, 1, 2, 9, 30),
    end: datetime = utc(2024, 1, 2, 15, 0),
    accepts_trades: bool = True,
    accepts_quotes: bool = True,
) -> Session:
    phase = TradingPhase(
        name="open",
        start=start,
        end=end,
        accepts_trades=accepts_trades,
        accepts_quotes=accepts_quotes,
    )
    return Session(
        market="CN",
        trading_date=date(2024, 1, 2),
        timezone="Asia/Shanghai",
        phases=(phase,),
    )


def trade(
    instrument: str = "600000",
    event_time: datetime | None = None,
    sequence: int | str | None = 1,
    trading_date: date = date(2024, 1, 2),
    **over: Any,
) -> TradeTick:
    event_time = event_time if event_time is not None else utc(2024, 1, 2, 9, 30, 0)
    base: dict[str, Any] = dict(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=event_time,
        available_at=event_time,
        trading_date=trading_date,
        source="test",
        quality="valid",
        metadata={},
        event_type="trade",
        price=10.0,
        size=100.0,
        turnover=1000.0,
        side="BUY",
        sequence=sequence,
    )
    base.update(over)
    return TradeTick(**base)


def quote(instrument: str = "600000", event_time: datetime | None = None, sequence: int | str | None = None) -> QuoteTick:
    from tools.data import PriceLevel

    level = PriceLevel(price=10.0, size=100.0, level=1)
    event_time = event_time if event_time is not None else utc(2024, 1, 2, 9, 30, 1)
    return QuoteTick(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=event_time,
        available_at=event_time,
        trading_date=date(2024, 1, 2),
        source="test",
        quality="valid",
        metadata={},
        event_type="quote",
        bid_levels=(level,),
        ask_levels=(level,),
        last_price=10.0,
        last_size=100.0,
        sequence=sequence,
    )


def registered(instrument: str = "600000", event_time: datetime | None = None, event_type: str = "announcement") -> RegisteredEvent:
    event_time = event_time if event_time is not None else utc(2024, 1, 2, 9, 30, 2)
    return RegisteredEvent(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=event_time,
        available_at=event_time,
        trading_date=date(2024, 1, 2),
        source="test",
        quality="valid",
        metadata={},
        event_type=event_type,
        values={"k": 1},
    )


def gap(event_time: datetime | None = None) -> DataGapEvent:
    return DataGapEvent(
        dataset="market.trade",
        instrument_id="600000",
        detected_at=event_time if event_time is not None else utc(2024, 1, 2, 10, 0),
        from_position=None,
        to_position=None,
        recoverable=True,
        reason="stream gap",
    )


def state_event(event_time: datetime | None = None) -> DataSourceStateEvent:
    return DataSourceStateEvent(
        dataset="market.trade",
        source="mem",
        state="reconnecting",
        occurred_at=event_time if event_time is not None else utc(2024, 1, 2, 10, 0),
        error=None,
    )


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def __call__(self, event: Any) -> None:
        self.events.append(event)


class SubscribedSink:
    def __init__(self, event_types: set[str]) -> None:
        self.subscribed_event_types = event_types
        self.events: list[Any] = []

    def __call__(self, event: Any) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# ReplayClock
# ---------------------------------------------------------------------------


def test_replay_clock_now_starts_at_session_open():
    clock = ReplayClock(make_session())
    assert clock.now == utc(2024, 1, 2, 9, 30)


def test_replay_clock_advance_forward_and_as_of():
    clock = ReplayClock(make_session())
    clock.advance(utc(2024, 1, 2, 10, 0))
    assert clock.now == utc(2024, 1, 2, 10, 0)
    assert clock.as_of() == utc(2024, 1, 2, 10, 0)


def test_replay_clock_advance_to_same_time_ok():
    clock = ReplayClock(make_session())
    clock.advance(utc(2024, 1, 2, 9, 30))
    assert clock.now == utc(2024, 1, 2, 9, 30)


def test_replay_clock_rejects_backward():
    clock = ReplayClock(make_session())
    clock.advance(utc(2024, 1, 2, 10, 0))
    with pytest.raises(ValueError):
        clock.advance(utc(2024, 1, 2, 9, 45))


def test_replay_clock_close_idempotent():
    clock = ReplayClock(make_session())
    clock.close()
    clock.close()


# ---------------------------------------------------------------------------
# LiveClock
# ---------------------------------------------------------------------------


def test_live_clock_now_starts_at_session_open():
    clock = LiveClock(make_session())
    assert clock.now == utc(2024, 1, 2, 9, 30)


def test_live_clock_update_advances():
    clock = LiveClock(make_session())
    clock.update(utc(2024, 1, 2, 10, 30))
    assert clock.now == utc(2024, 1, 2, 10, 30)


def test_live_clock_update_clamps_backwards():
    clock = LiveClock(make_session())
    clock.update(utc(2024, 1, 2, 11, 0))
    clock.update(utc(2024, 1, 2, 10, 0))
    assert clock.now == utc(2024, 1, 2, 11, 0)


def test_live_clock_close_idempotent():
    clock = LiveClock(make_session())
    clock.close()
    clock.close()


# ---------------------------------------------------------------------------
# session_open / session_close
# ---------------------------------------------------------------------------


def test_session_open_close():
    ses = make_session(start=utc(2024, 1, 2, 9, 30), end=utc(2024, 1, 2, 15, 0))
    assert session_open(ses) == utc(2024, 1, 2, 9, 30)
    assert session_close(ses) == utc(2024, 1, 2, 15, 0)


# ---------------------------------------------------------------------------
# sequence_events
# ---------------------------------------------------------------------------


def test_sequence_none_yields_unchanged():
    events = [trade(sequence=2, event_time=utc(2024, 1, 2, 9, 31)),
              trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30))]
    assert [e.sequence for e in sequence_events(events, "none")] == [2, 1]


def test_sequence_per_instrument_orders_each_group():
    events = [
        quote("B", event_time=utc(2024, 1, 2, 9, 31), sequence=2),
        trade("A", event_time=utc(2024, 1, 2, 9, 31), sequence=2),
        trade("A", event_time=utc(2024, 1, 2, 9, 30), sequence=1),
        quote("B", event_time=utc(2024, 1, 2, 9, 30), sequence=1),
    ]
    result = list(sequence_events(events, "per_instrument"))
    a = [e for e in result if e.instrument_id == "A"]
    b = [e for e in result if e.instrument_id == "B"]
    assert [e.sequence for e in a] == [1, 2]
    assert [e.sequence for e in b] == [1, 2]


def test_sequence_per_instrument_group_tie_broken_by_sequence():
    events = [
        trade("A", event_time=utc(2024, 1, 2, 9, 30), sequence=5),
        trade("A", event_time=utc(2024, 1, 2, 9, 30), sequence=3),
    ]
    result = list(sequence_events(events, "per_instrument"))
    assert [e.sequence for e in result] == [3, 5]


def test_sequence_global_sorts_by_time_then_sequence():
    events = [
        trade("A", event_time=utc(2024, 1, 2, 9, 31), sequence=2),
        trade("B", event_time=utc(2024, 1, 2, 9, 30), sequence=9),
        trade("A", event_time=utc(2024, 1, 2, 9, 30), sequence=1),
    ]
    result = list(sequence_events(events, "global"))
    assert [e.sequence for e in result] == [1, 9, 2]


def test_sequence_global_control_events_exempt():
    control = gap(event_time=utc(2024, 1, 2, 9, 30))
    events = [control, trade(sequence=1, event_time=utc(2024, 1, 2, 9, 31))]
    result = list(sequence_events(events, "global"))
    assert any(isinstance(e, TradeTick) for e in result)


def test_sequence_global_missing_sequence_strict_raises():
    events = [trade(sequence=1), trade(sequence=None)]
    with pytest.raises(DataContractError):
        list(sequence_events(events, "global", strict=True))


def test_sequence_global_missing_sequence_non_strict_warns_and_yields():
    events = [trade(sequence=1), trade(sequence=None)]
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = list(sequence_events(events, "global", strict=False))
    assert len(result) == 2
    assert any(W_SEQUENCE_MISSING in str(w.message) for w in rec)


def test_sequence_global_negative_sequence_raises_value_error():
    events = [trade(sequence=1), trade(sequence=-1)]
    with pytest.raises(ValueError):
        list(sequence_events(events, "global"))


def test_sequence_invalid_ordering_rejected():
    with pytest.raises(ValueError):
        list(sequence_events([], "bogus"))


# ---------------------------------------------------------------------------
# deduplicate_events
# ---------------------------------------------------------------------------


def key(event):
    return (event.instrument_id, event.event_time, event.sequence)


def test_deduplicate_keeps_unique_and_removes_identical_duplicates():
    events = [trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30)),
              trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30)),
              trade(sequence=2, event_time=utc(2024, 1, 2, 9, 31))]
    result = list(deduplicate_events(events, key))
    assert len(result) == 2


def test_deduplicate_same_key_diff_content_strict_raises():
    events = [
        trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), price=10.0),
        trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), price=10.5),
    ]
    with pytest.raises(DataContractError):
        list(deduplicate_events(events, key, strict=True))


def test_deduplicate_same_key_diff_content_non_strict_warns_and_skips():
    events = [
        trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), price=10.0),
        trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), price=10.5),
    ]
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = list(deduplicate_events(events, key, strict=False))
    assert len(result) == 1
    assert any(W_DUPLICATE_KEY in str(w.message) for w in rec)


# ---------------------------------------------------------------------------
# normalize_trade_increment
# ---------------------------------------------------------------------------


def test_normalize_trade_increment_first_tick_resets():
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = normalize_trade_increment(
            None, trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), cumulative_volume=250.0, cumulative_turnover=2500.0)
        )
    assert result.size == 250.0
    assert result.turnover == 2500.0
    assert any(W_SESSION_RESET in str(w.message) for w in rec)


def test_normalize_trade_increment_delta():
    previous = trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30, 0), cumulative_volume=250.0, cumulative_turnover=2500.0)
    current = trade(sequence=2, event_time=utc(2024, 1, 2, 9, 30, 1), cumulative_volume=300.0, cumulative_turnover=3050.0)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = normalize_trade_increment(previous, current)
    assert result.size == 50.0
    assert result.turnover == 550.0
    assert not rec


def test_normalize_trade_increment_cross_session_resets():
    previous = trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30), trading_date=date(2024, 1, 2), cumulative_volume=250.0, cumulative_turnover=2500.0)
    current = trade(sequence=1, event_time=utc(2024, 1, 3, 9, 30), trading_date=date(2024, 1, 3), cumulative_volume=80.0, cumulative_turnover=800.0)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = normalize_trade_increment(previous, current)
    assert result.size == 80.0
    assert result.turnover == 800.0
    assert any(W_SESSION_RESET in str(w.message) for w in rec)


def test_normalize_trade_increment_cumulative_regression_resets():
    previous = trade(sequence=2, event_time=utc(2024, 1, 2, 9, 30, 1), cumulative_volume=300.0, cumulative_turnover=3050.0)
    current = trade(sequence=3, event_time=utc(2024, 1, 2, 9, 30, 2), cumulative_volume=200.0, cumulative_turnover=2000.0)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = normalize_trade_increment(previous, current)
    assert result.size == 200.0
    assert result.turnover == 2000.0
    assert any(W_SESSION_RESET in str(w.message) for w in rec)


def test_normalize_trade_increment_no_cumulative_returns_unchanged():
    current = trade(sequence=1, event_time=utc(2024, 1, 2, 9, 30))
    result = normalize_trade_increment(None, current)
    assert result.size == 100.0
    assert result.turnover == 1000.0


# ---------------------------------------------------------------------------
# replay_events
# ---------------------------------------------------------------------------


def test_replay_delivers_market_events_in_order():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    events = [quote(sequence=1, event_time=utc(2024, 1, 2, 9, 30)), trade(sequence=2, event_time=utc(2024, 1, 2, 9, 30, 1))]
    replay_events(events, clock, sink)
    assert len(sink.events) == 2
    assert sink.events[-1].sequence == 2


def test_replay_advances_clock():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    replay_events([trade(event_time=utc(2024, 1, 2, 10, 0), sequence=1)], clock, sink)
    assert clock.now == utc(2024, 1, 2, 10, 0)


def test_replay_control_state_event_not_delivered():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    replay_events([state_event(event_time=utc(2024, 1, 2, 10, 0)), trade(sequence=1, event_time=utc(2024, 1, 2, 10, 1))], clock, sink)
    assert len(sink.events) == 1
    assert isinstance(sink.events[0], TradeTick)
    assert clock.now == utc(2024, 1, 2, 10, 1)


def test_replay_gap_strict_raises():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    with pytest.raises(DataGapError):
        replay_events([gap(event_time=utc(2024, 1, 2, 10, 0))], clock, sink, strict=True)
    assert sink.events == []


def test_replay_gap_non_strict_warns_and_skips():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        replay_events([gap(event_time=utc(2024, 1, 2, 10, 0)), trade(sequence=1, event_time=utc(2024, 1, 2, 10, 1))], clock, sink, strict=False)
    assert len(sink.events) == 1
    assert any(W_DATA_GAP in str(w.message) for w in rec)


def test_replay_phase_gap_strict_raises():
    clock = ReplayClock(make_session(start=utc(2024, 1, 2, 9, 30)))
    sink = RecordingSink()
    with pytest.raises(DataContractError):
        replay_events([trade(sequence=1, event_time=utc(2024, 1, 2, 9, 0))], clock, sink, strict=True)


def test_replay_phase_gap_non_strict_skips_and_warns():
    clock = ReplayClock(make_session(start=utc(2024, 1, 2, 9, 30)))
    sink = RecordingSink()
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        replay_events([trade(sequence=1, event_time=utc(2024, 1, 2, 9, 0)), trade(sequence=2, event_time=utc(2024, 1, 2, 10, 0))], clock, sink, strict=False)
    assert len(sink.events) == 1
    assert sink.events[0].sequence == 2
    assert any(W_PHASE_SKIPPED in str(w.message) for w in rec)


def test_replay_accepts_trades_false_skips_trade():
    clock = ReplayClock(make_session(accepts_trades=False))
    sink = RecordingSink()
    replay_events([trade(sequence=1), quote(sequence=2, event_time=utc(2024, 1, 2, 9, 30, 1))], clock, sink)
    assert all(not isinstance(e, TradeTick) for e in sink.events)
    assert len(sink.events) == 1


def test_replay_accepts_quotes_false_skips_quote():
    clock = ReplayClock(make_session(accepts_quotes=False))
    sink = RecordingSink()
    replay_events([trade(sequence=1), quote(sequence=2, event_time=utc(2024, 1, 2, 9, 30, 1))], clock, sink)
    assert all(not isinstance(e, QuoteTick) for e in sink.events)
    assert len(sink.events) == 1


def test_replay_pit_strict_raises():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    event = trade(sequence=1, available_at=utc(2024, 1, 2, 12, 0))
    with pytest.raises(PointInTimeError):
        replay_events([event], clock, sink, strict=True)


def test_replay_pit_non_strict_skips_and_warns():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    event = trade(sequence=1, available_at=utc(2024, 1, 2, 12, 0))
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        replay_events([event, trade(sequence=2, event_time=utc(2024, 1, 2, 10, 0))], clock, sink, strict=False)
    assert len(sink.events) == 1
    assert any(W_PIT_MISSING in str(w.message) for w in rec)


def test_replay_negative_sequence_raises_value_error():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    with pytest.raises(ValueError):
        replay_events([trade(sequence=-1)], clock, sink)


def test_replay_registered_event_only_to_explicit_subscriber():
    clock = ReplayClock(make_session())
    subscribed = SubscribedSink({"announcement"})
    replay_events([registered(event_type="announcement")], clock, subscribed)
    assert len(subscribed.events) == 1

    clock2 = ReplayClock(make_session())
    unsubscribed = SubscribedSink({"earnings"})
    replay_events([registered(event_type="announcement")], clock2, unsubscribed)
    assert unsubscribed.events == []


def test_replay_plain_sink_receives_registered_event():
    clock = ReplayClock(make_session())
    sink = RecordingSink()
    replay_events([registered(event_type="announcement")], clock, sink)
    assert len(sink.events) == 1