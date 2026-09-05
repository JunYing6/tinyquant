"""Run a finite tinyquant live-engine session without external services.

Drives :class:`RealTimeTradeEngine` from canonical ``market.trade`` /
``market.quote`` events pushed through an in-memory gateway.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
import sys
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from engines.realtime import RealTimeTradeEngine
from mock_trade_executor import InMemoryTradeExecutor  # type: ignore[import-not-found]
from tools.data import (
    DataPolicy,
    InMemoryGateway,
    Session,
    TradeTick,
    TradingPhase,
)
from trading_nodes_base.factors import TickTimingFactor
from trading_nodes_base.types import ExecutionMode, ExecutionRequest, SignalIntent
from trading_nodes_base.methods import BaseStockPicking, BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy


class DemoFixedPicking(BaseStockPicking):
    """Credential-free mock selector that passes the pool through unchanged."""

    def __init__(self, stock_pool: Sequence[str]) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool))
        super().__init__("demo-fixed-picking", [], [], [])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        if not self.stock_pool:
            return pd.DataFrame(columns=["asset", "weight"])
        weight = 1.0 / len(self.stock_pool)
        return pd.DataFrame({"asset": self.stock_pool, "weight": [weight] * len(self.stock_pool)})


class BuyOnceFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("buy-once")
        self.fired = False

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
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
        super().__init__(
            "demo-live",
            selector=DemoFixedPicking(["000001.SZ"]),
            timer=BaseTimeSelection("live-timer", [], [BuyOnceFactor()]),
        )


def main() -> None:
    executor = InMemoryTradeExecutor()
    now = datetime.now(timezone.utc)
    day = now.date()
    phase = TradingPhase(
        name="regular",
        start=now.replace(hour=9, minute=0, second=0, microsecond=0),
        end=now.replace(hour=15, minute=0, second=0, microsecond=0),
        accepts_trades=True,
        accepts_quotes=True,
    )
    session = Session(market="CN", trading_date=day, timezone="UTC", phases=(phase,))
    tick_time = now.replace(hour=9, minute=31, second=0, microsecond=0)
    tick = TradeTick(
        schema_version="1",
        event_id=None,
        instrument_id="000001.SZ",
        asset_type="equity",
        effective_time=tick_time,
        event_time=tick_time,
        available_at=None,
        trading_date=day,
        source="demo",
        quality="valid",
        metadata={},
        event_type="trade",
        price=10.0,
        size=100.0,
        turnover=1_000.0,
        side="UNKNOWN",
        sequence=None,
    )
    gateway = InMemoryGateway(events=[], sessions=[session], data_policy=DataPolicy())
    engine = RealTimeTradeEngine(DemoLiveStrategy(), gateway, executor)
    engine.start()
    gateway.emit(tick)
    engine.stop()
    print({"orders": executor.orders})


if __name__ == "__main__":
    main()
