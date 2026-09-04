from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from engines.core.pipeline import DataProviderError
from engines.fast import FastBacktestEngine
from tools.data_getter.market.schema import DataRequest
from trading.factors.base import KlineTimingFactor, TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.minds.base import BaseMind
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream


class MemoryCalendar:
    def __init__(self, dates: list[str]) -> None:
        self.dates = dates

    def get_trade_dates(self, start: str, end: str) -> list[str]:
        return [day for day in self.dates if start <= day <= end]


class MemoryProvider:
    def __init__(
        self,
        daily: dict[str, list[dict[str, Any]]],
        ticks: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.daily = daily
        self.ticks = ticks or {}
        self.calls: list[tuple[str, str]] = []

    def fetch(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
        self.calls.append((request.scope, date))
        if request.scope == "market/daily":
            return self.daily.get(date, [])
        if request.scope == "market/tick":
            return self.ticks.get(date, [])
        return []


class PassiveKlineFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("passive-kline")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class PassiveIntentExecutor(TickTimingFactor):
    execution_role = "intent_executor"
    accepted_intent_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("passive-executor")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


class BuyingFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, name: str = "fast") -> None:
        timer = BaseTimeSelection(
            f"{name}-timer", [PassiveKlineFactor()], [PassiveIntentExecutor()]
        )
        super().__init__(name, timer=timer)
        self._queued_once = False

    def _run_daily_pipeline(self) -> None:
        super()._run_daily_pipeline()
        if not self._queued_once:
            self._pending_orders.append(
                ExecutionRequest(
                    "000001.SZ",
                    "BUY",
                    "15:00:00",
                    price=0,
                    volume=100,
                    mode=ExecutionMode.MARKET,
                )
            )
            self._queued_once = True


class DirectBuyTickFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("direct-buy")
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


class TickStrategy(BaseStrategy):
    def __init__(self, name: str = "tick") -> None:
        super().__init__(name, timer=BaseTimeSelection(f"{name}-timer", [], [DirectBuyTickFactor()]))


class NextBarBuyFactor(KlineTimingFactor):
    emitted_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("next-bar-buy")
        self.emitted = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        if self.emitted:
            return []
        self.emitted = True
        return [
            SignalIntent(
                bar.code,
                "BUY",
                bar.end_time,
                "next bar entry",
                {"volume": 100},
            )
        ]


class NextBarFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        super().__init__(
            "next-bar",
            timer=BaseTimeSelection(
                "next-bar-timer", [NextBarBuyFactor()], [PassiveIntentExecutor()]
            ),
        )


class EqualMind(BaseMind):
    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        return {name: 1.0 for name in strategies_performance}


DAILY = {
    "20240102": [{"code": "000001.SZ", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}],
    "20240103": [{"code": "000001.SZ", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0}],
}


def test_backtest_requires_explicit_data_and_calendar_providers() -> None:
    with pytest.raises(ValueError, match="data_provider"):
        FastBacktestEngine(
            BuyingFastStrategy(),
            "20240102",
            "20240103",
            calendar_provider=MemoryCalendar(list(DAILY)),
        )

    with pytest.raises(ValueError, match="calendar_provider"):
        FastBacktestEngine(
            BuyingFastStrategy(),
            "20240102",
            "20240103",
            data_provider=MemoryProvider(DAILY),
        )


def test_fast_backtest_runs_with_injected_memory_providers() -> None:
    provider = MemoryProvider(DAILY)
    engine = FastBacktestEngine(
        BuyingFastStrategy(),
        "20240102",
        "20240103",
        initial_capital=100_000,
        mode="fast",
        data_provider=provider,
        calendar_provider=MemoryCalendar(list(DAILY)),
        progress_bar=False,
    )

    engine.run()

    assert [row["trade_date"] for row in engine.equity_curve] == ["20240102", "20240103"]
    assert engine.daily_positions[-1]["positions"] == {"000001.SZ": 100}
    assert ("market/daily", "20240102") in provider.calls
    assert engine.get_stats()["final_equity"] == engine.equity_curve[-1]["equity"]


def test_fast_backtest_applies_slippage_to_pending_daily_orders() -> None:
    engine = FastBacktestEngine(
        BuyingFastStrategy(),
        "20240102",
        "20240102",
        initial_capital=100_000,
        mode="fast",
        slippage={"buy": 0.01, "sell": 0.0, "model": "proportional"},
        data_provider=MemoryProvider(DAILY),
        calendar_provider=MemoryCalendar(["20240102"]),
        progress_bar=False,
    )

    engine.run()

    assert engine.account.cost_prices["000001.SZ"] == 10.1


def test_fast_backtest_executes_kline_intent_on_next_daily_bar() -> None:
    engine = FastBacktestEngine(
        NextBarFastStrategy(),
        "20240102",
        "20240103",
        mode="fast",
        data_provider=MemoryProvider(DAILY),
        calendar_provider=MemoryCalendar(list(DAILY)),
        progress_bar=False,
    )

    engine.run()

    assert engine.account.positions == {"000001.SZ": 100}
    assert engine.account.cost_prices["000001.SZ"] == 11.0


def test_tick_backtest_routes_provider_ticks_through_matching_runtime() -> None:
    provider = MemoryProvider(
        DAILY,
        {"20240102": [{"code": "000001.SZ", "time": "09:31:00", "price": 10.0, "volume": 100, "amount": 1_000}]},
    )
    engine = FastBacktestEngine(
        TickStrategy(),
        "20240102",
        "20240102",
        mode="tick",
        data_provider=provider,
        calendar_provider=MemoryCalendar(["20240102"]),
        progress_bar=False,
    )

    engine.run()

    assert engine.daily_positions[-1]["positions"] == {"000001.SZ": 100}
    assert ("market/tick", "20240102") in provider.calls


def test_stream_backtest_uses_shared_real_account_and_mind_weights() -> None:
    stream = BaseStream("stream", [BuyingFastStrategy("stream-child")], EqualMind())
    engine = FastBacktestEngine(
        stream,
        "20240102",
        "20240103",
        mode="fast",
        data_provider=MemoryProvider(DAILY),
        calendar_provider=MemoryCalendar(list(DAILY)),
        progress_bar=False,
    )

    engine.run()

    assert len(engine.equity_curve) == 2
    assert stream.real_account is engine.account
    assert stream.mind.current_weights == {"stream-child": 1.0}


def test_backtest_wraps_provider_errors_with_scope_and_date() -> None:
    class FailingProvider(MemoryProvider):
        def fetch(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
            raise RuntimeError("offline")

    engine = FastBacktestEngine(
        BuyingFastStrategy(),
        "20240102",
        "20240102",
        data_provider=FailingProvider(DAILY),
        calendar_provider=MemoryCalendar(["20240102"]),
        progress_bar=False,
    )

    with pytest.raises(DataProviderError, match="market/daily.*20240102.*offline"):
        engine.run()
