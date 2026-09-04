from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timezone
from types import MappingProxyType
from typing import Mapping

import pytest

from tools.data import (
    Bar,
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataGapEvent,
    DataProvenance,
    DataRequest,
    DataSourceStateEvent,
    EventPosition,
    MarketEvent,
    PriceLevel,
    QualityReport,
    QualityWarning,
    QuoteTick,
    RecordEnvelope,
    RegisteredEvent,
    Session,
    StreamEvent,
    StreamRequest,
    TableBatch,
    TradingPhase,
    TradeTick,
)


def tz_now() -> datetime:
    return datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc)


def make_envelope(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "schema_version": "1.0",
        "event_id": "e1",
        "instrument_id": "600000",
        "asset_type": "equity",
        "effective_time": None,
        "event_time": tz_now(),
        "available_at": None,
        "trading_date": date(2024, 1, 1),
        "source": "test",
        "quality": "valid",
        "metadata": {},
    }
    base.update(over)
    return base


def make_provenance(adapter: str = "adapter") -> DataProvenance:
    return DataProvenance(
        adapter_name=adapter,
        source_revision="r1",
        request_fingerprint="fp",
        read_at=tz_now(),
    )


def make_quality() -> QualityReport:
    return QualityReport(status="ok")


# ---------------------------------------------------------------------------
# DataRequest
# ---------------------------------------------------------------------------


def test_datarequest_is_frozen_and_requires_dataset() -> None:
    request = DataRequest(dataset="market.bar")
    assert request.dataset == "market.bar"
    assert request.price_basis == "raw"
    with pytest.raises(FrozenInstanceError):
        request.dataset = "market.trade"  # type: ignore[misc]
    with pytest.raises(ValueError):
        DataRequest(dataset="")


def test_datarequest_deep_freezes_filters() -> None:
    nested = {"outer": {"inner": {"leaf": [1, 2, {3, 4}]}}}
    request = DataRequest(dataset="market.bar", filters=nested)
    assert isinstance(request.filters, MappingProxyType)
    assert isinstance(request.filters["outer"], MappingProxyType)
    assert isinstance(request.filters["outer"]["inner"], MappingProxyType)
    assert isinstance(request.filters["outer"]["inner"]["leaf"], tuple)
    assert isinstance(request.filters["outer"]["inner"]["leaf"][2], frozenset)
    with pytest.raises(TypeError):
        request.filters["other"] = 1  # type: ignore[index]
    with pytest.raises(TypeError):
        request.filters["outer"]["inner"]["other"] = 1  # type: ignore[index]


def test_datarequest_rejects_circular_reference_in_filters() -> None:
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError):
        DataRequest(dataset="market.bar", filters=cyclic)


def test_datarequest_start_end_must_be_paired() -> None:
    with pytest.raises(ValueError):
        DataRequest(dataset="x", start=tz_now())
    with pytest.raises(ValueError):
        DataRequest(dataset="x", end=tz_now())


def test_datarequest_start_end_mutually_exclusive_with_anchor_and_window() -> None:
    with pytest.raises(ValueError):
        DataRequest(dataset="x", start=tz_now(), end=tz_now(), anchor=tz_now())
    with pytest.raises(ValueError):
        DataRequest(dataset="x", start=tz_now(), end=tz_now(), session_window=(900, 1500))
    request = DataRequest(dataset="x", start=tz_now(), end=tz_now())
    assert request.start is not None and request.end is not None


def test_datarequest_session_window_requires_anchor() -> None:
    with pytest.raises(ValueError):
        DataRequest(dataset="x", session_window=(900, 1500))
    request = DataRequest(dataset="x", anchor=tz_now())
    assert request.anchor is not None
    request = DataRequest(dataset="x", anchor=tz_now(), session_window=(900, 1500))
    assert request.session_window == (900, 1500)


def test_datarequest_limit_must_be_positive_integer() -> None:
    assert DataRequest(dataset="x", limit=None).limit is None
    assert DataRequest(dataset="x", limit=10).limit == 10
    with pytest.raises(ValueError):
        DataRequest(dataset="x", limit=0)
    with pytest.raises(ValueError):
        DataRequest(dataset="x", limit=-1)
    with pytest.raises(ValueError):
        DataRequest(dataset="x", limit=True)


def test_datarequest_rejects_naive_datetime_fields() -> None:
    naive = datetime(2024, 1, 1, 9, 30)
    with pytest.raises(ValueError):
        DataRequest(dataset="x", anchor=naive)
    with pytest.raises(ValueError):
        DataRequest(dataset="x", start=naive, end=naive)
    with pytest.raises(ValueError):
        DataRequest(dataset="x", as_of=naive)
    DataRequest(dataset="x", anchor=date(2024, 1, 1))


def test_datarequest_validates_price_basis_and_frequency() -> None:
    with pytest.raises(ValueError):
        DataRequest(dataset="x", price_basis="bogus")
    with pytest.raises(ValueError):
        DataRequest(dataset="x", frequency="5w")
    DataRequest(dataset="x", price_basis="adjusted_forward", frequency="1d")


# ---------------------------------------------------------------------------
# StreamRequest
# ---------------------------------------------------------------------------


def test_streamrequest_validates_dataset_and_instruments() -> None:
    StreamRequest(dataset="market.trade", instruments=("600000",))
    StreamRequest(dataset="market.quote", instruments=("600000",))
    with pytest.raises(ValueError):
        StreamRequest(dataset="market.bar", instruments=("600000",))
    with pytest.raises(ValueError):
        StreamRequest(dataset="market.trade", instruments=())


def test_streamrequest_coerces_instruments_to_tuple() -> None:
    request = StreamRequest(dataset="market.trade", instruments=["600000"])
    assert request.instruments == ("600000",)


# ---------------------------------------------------------------------------
# Session / TradingPhase
# ---------------------------------------------------------------------------


def test_session_open_close_and_nonempty_phases() -> None:
    phase = TradingPhase(
        name="continuous",
        start=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
        end=datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc),
        accepts_trades=True,
        accepts_quotes=True,
    )
    session = Session(
        market="XSHG",
        trading_date=date(2024, 1, 1),
        timezone="Asia/Shanghai",
        phases=(phase,),
    )
    assert session.open == phase.start
    assert session.close == phase.end
    with pytest.raises(ValueError):
        Session(
            market="XSHG",
            trading_date=date(2024, 1, 1),
            timezone="Asia/Shanghai",
            phases=(),
        )


# ---------------------------------------------------------------------------
# Bar
# ---------------------------------------------------------------------------


def make_bar(**over: object) -> Bar:
    base: dict[str, object] = {
        "frequency": "1m",
        "interval_start": datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
        "interval_end": tz_now(),
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 100.0,
        "turnover": 1000.0,
        "is_complete": True,
        "price_basis": "raw",
    }
    values: dict[str, object] = {**make_envelope(), **base, **over}
    return Bar(**values)  # type: ignore[arg-type]


def test_bar_event_time_must_equal_interval_end() -> None:
    make_bar()
    with pytest.raises(ValueError):
        make_bar(event_time=None)
    with pytest.raises(ValueError):
        make_bar(event_time=datetime(2024, 1, 1, 9, 31, tzinfo=timezone.utc))


def test_bar_interval_order_and_finite_ohlc() -> None:
    with pytest.raises(ValueError):
        make_bar(interval_start=datetime(2024, 1, 1, 10, 1, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        make_bar(open=float("nan"))
    with pytest.raises(ValueError):
        make_bar(close=float("inf"))
    with pytest.raises(ValueError):
        make_bar(volume=float("nan"))
    with pytest.raises(ValueError):
        make_bar(open=True)


def test_bar_ohlc_consistency_and_frequency() -> None:
    with pytest.raises(ValueError):
        make_bar(low=12.0)
    with pytest.raises(ValueError):
        make_bar(high=8.0)
    with pytest.raises(ValueError):
        make_bar(frequency="5w")
    with pytest.raises(ValueError):
        make_bar(price_basis="bogus")


# ---------------------------------------------------------------------------
# TradeTick / QuoteTick
# ---------------------------------------------------------------------------


def make_tick(**over: object) -> TradeTick:
    base: dict[str, object] = {
        "event_type": "trade",
        "price": 10.5,
        "size": 100.0,
        "turnover": 1050.0,
        "side": "BUY",
        "sequence": 1,
    }
    values: dict[str, object] = {**make_envelope(), **base, **over}
    return TradeTick(**values)  # type: ignore[arg-type]


def test_trade_tick_validates_event_type_and_values() -> None:
    make_tick()
    with pytest.raises(ValueError):
        make_tick(event_type="quote")
    with pytest.raises(ValueError):
        make_tick(price=0.0)
    with pytest.raises(ValueError):
        make_tick(price=float("nan"))
    with pytest.raises(ValueError):
        make_tick(size=-1.0)
    with pytest.raises(ValueError):
        make_tick(turnover=-1.0)
    with pytest.raises(ValueError):
        make_tick(cumulative_volume=-5.0)
    with pytest.raises(ValueError):
        make_tick(cumulative_turnover=-5.0)
    with pytest.raises(ValueError):
        make_tick(side="HOLD")


def make_quote(**over: object) -> QuoteTick:
    base: dict[str, object] = {
        "event_type": "quote",
        "bid_levels": (
            PriceLevel(price=10.4, size=100.0, level=1),
            PriceLevel(price=10.3, size=200.0, level=2),
        ),
        "ask_levels": (
            PriceLevel(price=10.6, size=100.0, level=1),
            PriceLevel(price=10.7, size=200.0, level=2),
        ),
        "last_price": 10.5,
        "last_size": 50.0,
        "sequence": 2,
    }
    values: dict[str, object] = {**make_envelope(), **base, **over}
    return QuoteTick(**values)  # type: ignore[arg-type]


def test_quote_tick_validates_event_type_and_empty_book() -> None:
    make_quote()
    with pytest.raises(ValueError):
        make_quote(event_type="trade")
    empty = make_quote(bid_levels=(), ask_levels=())
    assert empty.bid_levels == ()
    assert empty.ask_levels == ()


def test_quote_tick_validates_level_ordering() -> None:
    with pytest.raises(ValueError):
        make_quote(bid_levels=(PriceLevel(price=10.4, size=1.0, level=2),))
    with pytest.raises(ValueError):
        make_quote(
            bid_levels=(
                PriceLevel(price=10.4, size=1.0, level=1),
                PriceLevel(price=10.3, size=1.0, level=1),
            )
        )
    with pytest.raises(ValueError):
        make_quote(
            bid_levels=(
                PriceLevel(price=10.4, size=1.0, level=1),
                PriceLevel(price=10.5, size=1.0, level=2),
            )
        )
    with pytest.raises(ValueError):
        make_quote(
            ask_levels=(
                PriceLevel(price=10.6, size=1.0, level=1),
                PriceLevel(price=10.5, size=1.0, level=2),
            )
        )
    with pytest.raises(ValueError):
        make_quote(
            ask_levels=(
                PriceLevel(price=10.6, size=1.0, level=1),
                PriceLevel(price=10.6, size=1.0, level=2),
            )
        )


def test_quote_tick_validates_last_price_and_size() -> None:
    with pytest.raises(ValueError):
        make_quote(last_price=0.0)
    with pytest.raises(ValueError):
        make_quote(last_price=-1.0)
    with pytest.raises(ValueError):
        make_quote(last_size=-1.0)
    make_quote(last_price=None, last_size=None)


def test_price_level_validates_price_size_level() -> None:
    with pytest.raises(ValueError):
        PriceLevel(price=0.0, size=1.0, level=1)
    with pytest.raises(ValueError):
        PriceLevel(price=1.0, size=-1.0, level=1)
    with pytest.raises(ValueError):
        PriceLevel(price=1.0, size=1.0, level=0)


# ---------------------------------------------------------------------------
# DataBatch
# ---------------------------------------------------------------------------


def make_batch(**over: object) -> DataBatch[object]:
    base: dict[str, object] = {
        "request_id": "r1",
        "dataset": "market.bar",
        "schema_version": "1.0",
        "correlation_id": None,
        "records": (),
        "complete": True,
        "next_cursor": None,
        "provenance": make_provenance(),
        "quality": make_quality(),
    }
    base.update(over)
    return DataBatch(**base)  # type: ignore[arg-type]


def test_databatch_complete_empty_requires_null_cursor() -> None:
    make_batch()
    with pytest.raises(ValueError):
        make_batch(complete=True, records=(), next_cursor="abc")


def test_databatch_incomplete_requires_cursor_or_data_gap() -> None:
    with pytest.raises(ValueError):
        make_batch(complete=False, next_cursor=None)
    make_batch(complete=False, next_cursor="abc")
    gap = QualityReport(
        status="warning",
        warnings=(QualityWarning(code="DATA_GAP", message="gap"),),
    )
    make_batch(complete=False, next_cursor=None, quality=gap)


def test_databatch_with_request_context() -> None:
    record = make_bar()
    batch = make_batch(records=(record,), complete=True)
    new_provenance = make_provenance(adapter="other")
    relabelled = batch.with_request_context("r2", "corr2", new_provenance)
    assert relabelled.request_id == "r2"
    assert relabelled.correlation_id == "corr2"
    assert relabelled.provenance is new_provenance
    assert relabelled.dataset == batch.dataset
    assert relabelled.records == batch.records
    assert relabelled.complete == batch.complete


def test_table_batch_and_calendar_batch_aliases() -> None:
    table = TableBatch(
        request_id="r",
        dataset="d",
        schema_version="1.0",
        correlation_id=None,
        records=({"a": 1},),
        complete=True,
        next_cursor=None,
        provenance=make_provenance(),
        quality=make_quality(),
    )
    assert isinstance(table, DataBatch)
    assert isinstance(table.records[0], Mapping)
    phase = TradingPhase(
        name="c",
        start=datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc),
        end=datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc),
        accepts_trades=True,
        accepts_quotes=True,
    )
    session = Session(
        market="XSHG",
        trading_date=date(2024, 1, 1),
        timezone="Asia/Shanghai",
        phases=(phase,),
    )
    calendar = CalendarBatch(
        request_id="r",
        dataset="calendar.session",
        schema_version="1.0",
        correlation_id=None,
        records=(session,),
        complete=True,
        next_cursor=None,
        provenance=make_provenance(),
        quality=make_quality(),
    )
    assert isinstance(calendar, DataBatch)


# ---------------------------------------------------------------------------
# Control events vs market events
# ---------------------------------------------------------------------------


def test_stream_events_and_market_events_differentiate() -> None:
    gap = DataGapEvent(
        dataset="market.trade",
        instrument_id="600000",
        detected_at=tz_now(),
        from_position=EventPosition(event_time=tz_now()),
        to_position=None,
        recoverable=True,
        reason="timeout",
    )
    state = DataSourceStateEvent(
        dataset="market.trade",
        source="feed",
        state="reconnecting",
        occurred_at=tz_now(),
        error=None,
    )
    tick = make_tick()
    registered = RegisteredEvent(
        **{
            **make_envelope(),
            "event_type": "announcement",
            "values": {"flag": True},
        }
    )
    assert isinstance(gap, StreamEvent)
    assert isinstance(state, StreamEvent)
    assert isinstance(tick, StreamEvent)
    assert isinstance(registered, StreamEvent)
    assert isinstance(tick, MarketEvent)
    assert isinstance(registered, MarketEvent)
    assert not isinstance(gap, MarketEvent)
    assert not isinstance(state, MarketEvent)


def test_record_envelope_freezes_metadata() -> None:
    tick = make_tick(metadata={"nested": {"a": [1, 2]}})
    assert isinstance(tick.metadata, MappingProxyType)
    assert isinstance(tick.metadata["nested"], MappingProxyType)
    assert isinstance(tick.metadata["nested"]["a"], tuple)
    with pytest.raises(TypeError):
        tick.metadata["other"] = 1  # type: ignore[index]
    with pytest.raises(ValueError):
        make_tick(quality="bogus")


def test_provenance_requires_tzaware_read_at() -> None:
    make_provenance()
    with pytest.raises(ValueError):
        DataProvenance(
            adapter_name="a",
            source_revision="r",
            request_fingerprint="f",
            read_at=datetime(2024, 1, 1),
        )


def test_calendar_request_validates_market_and_range() -> None:
    CalendarRequest(market="XSHG", start=date(2024, 1, 1), end=date(2024, 1, 5))
    with pytest.raises(ValueError):
        CalendarRequest(market="", start=date(2024, 1, 1), end=date(2024, 1, 5))
    with pytest.raises(ValueError):
        CalendarRequest(market="XSHG", start=date(2024, 1, 5), end=date(2024, 1, 1))
