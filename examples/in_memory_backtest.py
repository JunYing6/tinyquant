"""Run a tinyquant fast backtest without credentials or local data files."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from engines.fast import FastBacktestEngine
from tools.data import Bar, DataRequest, Session, TradingPhase
from adapters.memory_adapters import make_gateway

SAMPLE_DAILY = {
    "20240102": 10.0,
    "20240103": 10.5,
}
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


def _demo_gateway() -> Any:
    from datetime import datetime, timezone

    bars, sessions = [], []
    for day, price in SAMPLE_DAILY.items():
        close = datetime.strptime(day, "%Y%m%d").replace(hour=15, tzinfo=timezone.utc)
        bars.append(Bar(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=close, event_time=close, available_at=close, trading_date=close.date(), source="demo", quality="valid", metadata={}, frequency="1d", interval_start=close, interval_end=close, open=price, high=price, low=price, close=price, volume=0.0, turnover=0.0, is_complete=True, price_basis="raw"))
        phase = TradingPhase(name="regular", start=close.replace(hour=9), end=close, accepts_trades=True, accepts_quotes=True)
        sessions.append(Session(market="CN", trading_date=close.date(), timezone="UTC", phases=(phase,)))
    return make_gateway(bars, sessions)


def main() -> None:
    engine = FastBacktestEngine(
        DemoStrategy(),
        "20240102",
        "20240103",
        mode="fast",
        data_gateway=_demo_gateway(),
        progress_bar=False,
    )
    engine.run()
    print(engine.get_stats())


if __name__ == "__main__":
    main()
