"""Run a finite tinyquant live-engine session without external services."""

from __future__ import annotations

from collections.abc import Sequence
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from engines.realtime import RealTimeTradeEngine
from mock_quote_provider import InMemoryQuoteProvider  # type: ignore[import-not-found]
from mock_trade_executor import InMemoryTradeExecutor  # type: ignore[import-not-found]
from tools.data_getter.market.schema import DataRequest
from trading.factors.base import TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


class BuyOnceFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("buy-once")
        self.fired = False

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
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


class DemoLiveStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("demo-live", timer=BaseTimeSelection("live-timer", [], [BuyOnceFactor()]))


def main() -> None:
    executor = InMemoryTradeExecutor()
    quote = InMemoryQuoteProvider(
        [
            {
                "code": "000001.SZ",
                "trade_date": "20240102",
                "time": "09:31:00",
                "price": 10.0,
                "volume": 100,
                "amount": 1_000,
            }
        ]
    )
    engine = RealTimeTradeEngine(DemoLiveStrategy(), quote, executor)
    engine.start()
    engine.stop()
    print({"orders": executor.orders})


if __name__ == "__main__":
    main()
