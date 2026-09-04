"""Small credential-free factories used by the CLI examples command."""

from __future__ import annotations

from typing import Any

from tools.data_getter.market.schema import DataRequest
from trading.factors.base import KlineTimingFactor, TickTimingFactor
from trading.factors.types import KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


class _DemoCalendar:
    def get_trade_dates(self, start: str, end: str) -> list[str]:
        return [date for date in ("20240102", "20240103") if start <= date <= end]


class _DemoMarketData:
    _daily = {
        "20240102": [{"code": "000001.SZ", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}],
        "20240103": [{"code": "000001.SZ", "open": 10.5, "high": 10.5, "low": 10.5, "close": 10.5}],
    }

    def fetch(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
        if request.scope == "market/daily":
            return list(self._daily.get(date, []))
        return []


class _DemoKlineFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("demo-kline")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class _DemoTickExecutor(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("demo-tick-executor")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


class _DemoStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        super().__init__(
            "demo-cli",
            timer=BaseTimeSelection(
                "demo-cli-timer", [_DemoKlineFactor()], [_DemoTickExecutor()]
            ),
        )


def build_demo_backtest() -> tuple[BaseStrategy, _DemoMarketData, _DemoCalendar]:
    """Return a deterministic in-memory strategy/provider/calendar triple."""
    return _DemoStrategy(), _DemoMarketData(), _DemoCalendar()


__all__ = ["build_demo_backtest"]
