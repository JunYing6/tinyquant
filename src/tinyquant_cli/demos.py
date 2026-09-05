"""Small credential-free factories used by the CLI examples command."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from tools.data import Bar, DataRequest, InMemoryGateway, Session, TradingPhase
from trading_nodes_base.factors.base import KlineTimingFactor, TickTimingFactor
from trading_nodes_base.factors.types import KlineBar, SignalIntent
from trading_nodes_base.methods.base import BaseTimeSelection
from trading_nodes_base.strategies.base import BaseStrategy


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
        super().__init__("demo-cli", timer=BaseTimeSelection("demo-cli-timer", [_DemoKlineFactor()], [_DemoTickExecutor()]))


def build_demo_backtest() -> tuple[BaseStrategy, InMemoryGateway]:
    bars = []
    for day, price in (("20240102", 10.0), ("20240103", 10.5)):
        instant = datetime.strptime(day, "%Y%m%d").replace(hour=15, tzinfo=timezone.utc)
        bars.append(Bar(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=instant, event_time=instant, available_at=instant, trading_date=instant.date(), source="demo", quality="valid", metadata={}, frequency="1d", interval_start=instant, interval_end=instant, open=price, high=price, low=price, close=price, volume=0, turnover=0, is_complete=True, price_basis="raw"))
    phases = tuple(TradingPhase(name="regular", start=datetime.strptime(day, "%Y%m%d").replace(hour=9, tzinfo=timezone.utc), end=datetime.strptime(day, "%Y%m%d").replace(hour=15, tzinfo=timezone.utc), accepts_trades=True, accepts_quotes=True) for day in ("20240102", "20240103"))
    sessions = tuple(Session(market="CN", trading_date=datetime.strptime(day, "%Y%m%d").date(), timezone="UTC", phases=(phase,)) for day, phase in zip(("20240102", "20240103"), phases))
    return _DemoStrategy(), InMemoryGateway(bars=bars, sessions=sessions)


__all__ = ["build_demo_backtest"]
