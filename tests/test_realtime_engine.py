from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from engines.realtime import RealTimeTradeEngine
from tools.data import (
    DataGapError,
    DataGapEvent,
    DataPolicy,
    InMemoryGateway,
    LiveClock,
    QuoteTick,
    StreamRequest,
    Subscription,
    TradeTick,
    TradingPhase,
    UnsupportedDatasetError,
)
from trading.factors.base import TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.methods.selector import FixedStockPicking
from trading.minds.base import BaseMind
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream


def _day(offset: int = 0) -> str:
    return (datetime.now(timezone.utc).date() + timedelta(days=offset)).strftime("%Y%m%d")


def _instant(day: str, clock: str = "00:00:00") -> datetime:
    return datetime.strptime(f"{day} {clock}", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _session(day: str, start: str = "09:00:00", end: str = "15:00:00") -> object:
    phase = TradingPhase(
        name="regular",
        start=_instant(day, start),
        end=_instant(day, end),
        accepts_trades=True,
        accepts_quotes=True,
    )
    from tools.data import Session

    return Session(market="CN", trading_date=_instant(day).date(), timezone="UTC", phases=(phase,))


def _trade(day: str = _day(), clock: str = "09:31:00", price: float = 10.0, code: str = "000001.SZ") -> TradeTick:
    return TradeTick(
        schema_version="1",
        event_id=None,
        instrument_id=code,
        asset_type="equity",
        effective_time=_instant(day, clock),
        event_time=_instant(day, clock),
        available_at=None,
        trading_date=_instant(day).date(),
        source="test",
        quality="valid",
        metadata={},
        event_type="trade",
        price=price,
        size=100.0,
        turnover=price * 100,
        side="UNKNOWN",
        sequence=None,
    )


def _quote(day: str = _day(), clock: str = "09:31:00", last: float = 10.0, code: str = "000001.SZ") -> QuoteTick:
    from tools.data import PriceLevel

    return QuoteTick(
        schema_version="1",
        event_id=None,
        instrument_id=code,
        asset_type="equity",
        effective_time=_instant(day, clock),
        event_time=_instant(day, clock),
        available_at=None,
        trading_date=_instant(day).date(),
        source="test",
        quality="valid",
        metadata={},
        event_type="quote",
        bid_levels=(PriceLevel(price=last - 0.01, size=100.0, level=1),),
        ask_levels=(PriceLevel(price=last + 0.01, size=100.0, level=1),),
        last_price=last,
        last_size=100.0,
        sequence=None,
    )


def _gap(day: str = _day(), clock: str = "10:00:00", reason: str = "missing data") -> DataGapEvent:
    return DataGapEvent(
        dataset="market.trade",
        instrument_id=None,
        detected_at=_instant(day, clock),
        from_position=None,
        to_position=None,
        recoverable=True,
        reason=reason,
    )


def _state(day: str = _day(), clock: str = "10:00:00", state: str = "error") -> object:
    from tools.data import DataSourceStateEvent

    return DataSourceStateEvent(
        dataset="market.trade",
        source="test",
        state=state,
        occurred_at=_instant(day, clock),
        error=None,
    )


class DirectLiveBuyFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("live-buy")
        self.fired = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_tick(
        self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()
    ) -> list[ExecutionRequest]:
        if self.fired:
            return []
        self.fired = True
        return [
            ExecutionRequest(
                tick["code"],
                "BUY",
                tick["time"],
                price=tick["price"],
                volume=100,
                mode=ExecutionMode.MARKET,
            )
        ]


class LiveStrategy(BaseStrategy):
    def __init__(self, name: str = "live", code: str = "000001.SZ") -> None:
        super().__init__(
            name,
            selector=FixedStockPicking([code]),
            timer=BaseTimeSelection(f"{name}-timer", [], [DirectLiveBuyFactor()]),
        )


class QueryingTickFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("querying-tick")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": "20240102"}}]


class QueryingLiveStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__(
            "querying-live",
            selector=FixedStockPicking(["000001.SZ"]),
            timer=BaseTimeSelection("querying-timer", [], [QueryingTickFactor()]),
        )


class EqualMind(BaseMind):
    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        return {name: 1.0 for name in strategies_performance}


class MockTradeExecutor:
    def __init__(self, fail_orders: bool = False, reject_orders: bool = False) -> None:
        self.fail_orders = fail_orders
        self.reject_orders = reject_orders
        self.events: list[str] = []
        self.orders: list[tuple[str, str, int, float | None]] = []
        self.positions: dict[str, int] = {}

    def connect(self) -> None:
        self.events.append("connect")

    def disconnect(self) -> None:
        self.events.append("disconnect")

    def get_account(self) -> dict[str, float]:
        self.events.append("get_account")
        return {"total_assets": 100_000.0, "available_cash": 100_000.0}

    def get_positions(self) -> list[dict[str, Any]]:
        self.events.append("get_positions")
        return [
            {"symbol": code, "volume": volume, "cost_price": 10.0}
            for code, volume in self.positions.items()
            if volume > 0
        ]

    def buy(self, symbol: str, volume: int, **kwargs: Any) -> dict[str, Any]:
        self.events.append("buy")
        if self.fail_orders:
            raise RuntimeError("broker offline")
        if self.reject_orders:
            return {"success": False}
        self.orders.append(("BUY", symbol, volume, kwargs.get("price")))
        self.positions[symbol] = self.positions.get(symbol, 0) + volume
        return {"success": True}

    def sell(self, symbol: str, volume: int, **kwargs: Any) -> dict[str, Any]:
        self.events.append("sell")
        if self.fail_orders:
            raise RuntimeError("broker offline")
        if self.reject_orders:
            return {"success": False}
        self.orders.append(("SELL", symbol, volume, kwargs.get("price")))
        self.positions[symbol] = max(0, self.positions.get(symbol, 0) - volume)
        return {"success": True}


class RecordingGateway(InMemoryGateway):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.requests: list[StreamRequest] = []
        self.reads: list[Any] = []

    def subscribe(self, request: StreamRequest, sink: Any, control_sink: Any = None) -> Subscription:
        self.requests.append(request)
        return super().subscribe(request, sink, control_sink)

    def read(self, request: Any) -> Any:
        self.reads.append(request)
        return super().read(request)


def _engine(strategy: BaseStrategy | BaseStream, trader: MockTradeExecutor, gateway: RecordingGateway, **kwargs: Any) -> RealTimeTradeEngine:
    return RealTimeTradeEngine(strategy, data_gateway=gateway, trade_executor=trader, initial_capital=100_000, **kwargs)


def _day_gateway(day: str, events: list[Any] | None = None, gap_action: str = "continue") -> RecordingGateway:
    return RecordingGateway(
        events=events or [],
        sessions=[_session(day)],
        data_policy=DataPolicy(gap_action=gap_action),
    )


def test_realtime_engine_requires_gateway_and_trade_executor() -> None:
    with pytest.raises(ValueError, match="data_gateway"):
        RealTimeTradeEngine(LiveStrategy(), data_gateway=None, trade_executor=MockTradeExecutor())

    with pytest.raises(ValueError, match="trade_executor"):
        RealTimeTradeEngine(LiveStrategy(), data_gateway=RecordingGateway(), trade_executor=None)


def test_realtime_engine_subscribes_canonical_streams_and_syncs_account() -> None:
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    engine.stop()

    datasets = [request.dataset for request in gateway.requests]
    assert datasets == ["market.trade", "market.quote"]
    assert all(request.instruments == ("000001.SZ",) for request in gateway.requests)
    assert trader.events[:2] == ["connect", "get_account"]
    assert trader.events[-1] == "disconnect"


def test_realtime_engine_places_order_from_trade_tick() -> None:
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_trade())
    engine.stop()

    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]


def test_realtime_engine_ignores_events_outside_session_phases() -> None:
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_trade(clock="16:00:00"))
    engine.stop()

    assert trader.orders == []


def test_realtime_engine_tracks_last_prices_from_quote_ticks() -> None:
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_quote())
    engine.stop()

    assert engine._last_prices["000001.SZ"] == 10.0
    assert trader.orders == []


def test_realtime_engine_uses_optional_poll_path_into_shared_handler() -> None:
    gateway = RecordingGateway(
        events=[_trade()],
        sessions=[_session(_day())],
        data_policy=DataPolicy(gap_action="continue"),
    )
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    engine.drain_poll(StreamRequest(dataset="market.trade", instruments=("000001.SZ",), correlation_id="poll"))
    engine.stop()

    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]


def test_realtime_engine_fails_clearly_when_quote_capability_is_missing() -> None:
    class NoQuoteGateway(RecordingGateway):
        def subscribe(self, request: StreamRequest, sink: Any, control_sink: Any = None) -> Subscription:
            if request.dataset == "market.quote":
                raise UnsupportedDatasetError("no quote capability", dataset="market.quote")
            return super().subscribe(request, sink, control_sink)

    gateway = NoQuoteGateway(sessions=[_session(_day())], data_policy=DataPolicy())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    with pytest.raises(UnsupportedDatasetError):
        engine.start()

    assert engine._running is False
    assert "disconnect" in trader.events


def test_realtime_engine_builds_live_clock_from_session() -> None:
    day = _day()
    gateway = _day_gateway(day)
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    engine.stop()

    assert isinstance(engine._clock, LiveClock)
    assert engine._clock.session.trading_date == datetime.now(timezone.utc).date()
    assert engine._clock.now == _instant(day, "09:00:00")


def test_realtime_stream_submits_weighted_net_order() -> None:
    stream = BaseStream("live-stream", [LiveStrategy("child")], EqualMind())
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(stream, trader, gateway)

    engine.start()
    gateway.emit(_trade())
    engine.stop()

    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]


def test_realtime_engine_loads_daily_history_from_gateway() -> None:
    day = _day()
    gateway = _day_gateway(day)
    trader = MockTradeExecutor()
    engine = _engine(QueryingLiveStrategy(), trader, gateway)

    engine.start()
    engine.stop()

    assert any(getattr(request, "dataset", None) == "market.bar" for request in gateway.reads)


def test_realtime_engine_pause_and_resume_via_data_source_state() -> None:
    gateway = _day_gateway(_day(), gap_action="pause")
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_state(state="error"))
    assert engine._paused is True

    gateway.emit(_trade())
    assert trader.orders == []

    gateway.emit(_state(state="healthy"))
    assert engine._paused is False

    gateway.emit(_trade())
    engine.stop()

    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]


def test_realtime_engine_continues_on_gap_under_continue_policy() -> None:
    received: list[Any] = []
    gateway = _day_gateway(_day(), gap_action="continue")
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway, control_callback=received.append)

    engine.start()
    gateway.emit(_gap())
    assert engine._running is True
    assert engine._gap_error is None
    assert received and isinstance(received[0], DataGapEvent)
    engine.stop()


def test_realtime_engine_stops_on_gap_under_raise_policy() -> None:
    gateway = _day_gateway(_day(), gap_action="raise")
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_gap())

    assert engine._running is False
    assert isinstance(engine._gap_error, DataGapError)
    assert "disconnect" in trader.events
    assert all(subscription.state == "cancelled" for subscription in engine._subscriptions.values())

    engine.stop()


def test_realtime_engine_stop_cancels_subscriptions_and_ignores_after() -> None:
    gateway = _day_gateway(_day())
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    assert all(subscription.state == "active" for subscription in engine._subscriptions.values())

    engine.stop()

    assert all(subscription.state == "cancelled" for subscription in engine._subscriptions.values())
    assert trader.orders == []
    gateway.emit(_trade())
    assert trader.orders == []


def test_realtime_engine_rolls_day_with_settle_and_rebuilt_session() -> None:
    first = _day()
    second = _day(offset=1)
    gateway = RecordingGateway(
        events=[],
        sessions=[_session(first), _session(second)],
        data_policy=DataPolicy(gap_action="continue"),
    )
    trader = MockTradeExecutor()
    engine = _engine(LiveStrategy(), trader, gateway)

    engine.start()
    gateway.emit(_trade(day=first))
    gateway.emit(_trade(day=second))
    engine.stop()

    assert engine._current_date == datetime.now(timezone.utc).date() + timedelta(days=1)
    assert engine._clock.session.trading_date == datetime.now(timezone.utc).date() + timedelta(days=1)
    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]
