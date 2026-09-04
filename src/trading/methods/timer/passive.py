from __future__ import annotations

from typing import Any

from trading.factors.base import KlineTimingFactor
from trading.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading.factors.types import KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection


class NoTradeFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("no-trade")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class NoTradeTiming(BaseTimeSelection):
    def __init__(self) -> None:
        super().__init__("no-trade-timing", [NoTradeFactor()], [IntentExecutorFactor()])


__all__ = ["NoTradeFactor", "NoTradeTiming"]
