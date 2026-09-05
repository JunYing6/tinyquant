from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Callable

import pytest

from engines.realtime import RealTimeTradeEngine
from tools.data import DataRequest
from trading.factors.base import TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.minds.base import BaseMind
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream


class DirectLiveBuyFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("live-buy")
        self.fired = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
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
    def __init__(self, name: str = "live") -> None:
        super().__init__(name, timer=BaseTimeSelection(f"{name}-timer", [], [DirectLiveBuyFactor()]))


class QueryingTickFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("querying-tick")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": "20240102"}}]


class QueryingLiveStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("querying-live", timer=BaseTimeSelection("querying-timer", [], [QueryingTickFactor()]))


class EqualMind(BaseMind):
    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        return {name: 1.0 for name in strategies_performance}


class MockQuoteProvider:
    def __init__(self, ticks: list[dict[str, Any]]) -> None:
        self.ticks = ticks
        self.events: list[str] = []
        self.subscriptions: list[str] = []
        self.on_tick: Callable[[dict[str, Any]], None] | None = None

    def subscribe(
        self, codes: list[str], on_tick: Callable[[dict[str, Any]], None]
    ) -> None:
        self.events.append("subscribe")
        self.subscriptions = list(codes)
        self.on_tick = on_tick

    def start(self) -> None:
        self.events.append("start")
        assert self.on_tick is not None
        for tick in self.ticks:
            self.on_tick(tick)

    def stop(self) -> None:
        self.events.append("stop")


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


class RecordingMarketGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def read(self, request: DataRequest) -> object:
        self.calls.append((request.dataset, request.as_of.strftime("%Y%m%d") if request.as_of else ""))
        return None

    def sessions(self, request: object) -> object:
        return None


TICK = {
    "code": "000001.SZ",
    "trade_date": "20240102",
    "time": "09:31:00",
    "price": 10.0,
    "volume": 100,
    "amount": 1_000,
}


def test_realtime_engine_requires_quote_and_trade_providers() -> None:
    with pytest.raises(ValueError, match="quote_provider"):
        RealTimeTradeEngine(LiveStrategy(), None, MockTradeExecutor())

    with pytest.raises(ValueError, match="trade_executor"):
        RealTimeTradeEngine(LiveStrategy(), MockQuoteProvider([]), None)


def test_realtime_engine_connects_processes_tick_and_stops() -> None:
    quote = MockQuoteProvider([TICK])
    trader = MockTradeExecutor()
    engine = RealTimeTradeEngine(
        LiveStrategy(),
        quote_provider=quote,
        trade_executor=trader,
        initial_capital=100_000,
    )

    engine.start()
    engine.stop()

    assert trader.events[:2] == ["connect", "get_account"]
    assert quote.events == ["subscribe", "start", "stop"]
    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]
    assert trader.events[-1] == "disconnect"


def test_realtime_engine_logs_order_failure_without_retry(caplog: pytest.LogCaptureFixture) -> None:
    quote = MockQuoteProvider([TICK])
    trader = MockTradeExecutor(fail_orders=True)
    engine = RealTimeTradeEngine(LiveStrategy(), quote, trader)

    engine.start()
    engine.stop()

    assert trader.events.count("buy") == 1
    assert "broker offline" in caplog.text
    assert engine.account.positions == {}


def test_realtime_engine_treats_rejected_executor_result_as_failure(caplog: pytest.LogCaptureFixture) -> None:
    quote = MockQuoteProvider([TICK])
    trader = MockTradeExecutor(reject_orders=True)
    engine = RealTimeTradeEngine(LiveStrategy(), quote, trader)

    engine.start()
    engine.stop()

    assert trader.events.count("buy") == 1
    assert engine.account.positions == {}
    assert "rejected order" in caplog.text


def test_realtime_stream_submits_weighted_net_order() -> None:
    stream = BaseStream("live-stream", [LiveStrategy("child")], EqualMind())
    quote = MockQuoteProvider([TICK])
    trader = MockTradeExecutor()
    engine = RealTimeTradeEngine(stream, quote, trader)

    engine.start()
    engine.stop()

    assert trader.orders == [("BUY", "000001.SZ", 100, 10.0)]


def test_realtime_engine_uses_optional_market_provider_for_daily_requirements() -> None:
    provider = RecordingMarketGateway()
    engine = RealTimeTradeEngine(
        QueryingLiveStrategy(),
        MockQuoteProvider([]),
        MockTradeExecutor(),
        data_gateway=provider,
    )

    engine.start()
    engine.stop()

    assert len(provider.calls) == 1
    assert provider.calls[0][0] == "market.bar"
    assert len(provider.calls[0][1]) == 8


def test_realtime_engine_ignores_ticks_outside_trading_window() -> None:
    after_close = {**TICK, "time": "15:01:00"}
    quote = MockQuoteProvider([after_close])
    trader = MockTradeExecutor()
    engine = RealTimeTradeEngine(LiveStrategy(), quote, trader)

    engine.start()
    engine.stop()

    assert trader.orders == []
