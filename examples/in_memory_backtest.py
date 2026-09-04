"""Run a tinyquant fast backtest without credentials or local data files."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from engines.fast import FastBacktestEngine
from mock_market_data import InMemoryCalendar, InMemoryMarketDataProvider, SAMPLE_DAILY  # type: ignore[import-not-found]
from tools.data_getter.market.schema import DataRequest
from trading.factors.base import KlineTimingFactor, TickTimingFactor
from trading.factors.types import KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


class PassiveKlineFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("passive-kline")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class IntentExecutor(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("intent-executor")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


class DemoStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        super().__init__(
            "demo-fast",
            timer=BaseTimeSelection("demo-timer", [PassiveKlineFactor()], [IntentExecutor()]),
        )


def main() -> None:
    engine = FastBacktestEngine(
        DemoStrategy(),
        "20240102",
        "20240103",
        mode="fast",
        data_provider=InMemoryMarketDataProvider(SAMPLE_DAILY),
        calendar_provider=InMemoryCalendar(list(SAMPLE_DAILY)),
        progress_bar=False,
    )
    engine.run()
    print(engine.get_stats())


if __name__ == "__main__":
    main()
