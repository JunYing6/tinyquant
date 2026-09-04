from __future__ import annotations

from collections import deque
from typing import Any

from trading.factors.base import RiskKlineFactor
from trading.factors.types import KlineBar, RiskSignal


class AtrStopRiskFactor(RiskKlineFactor):
    def __init__(self, window: int = 14, multiple: float = 2.0) -> None:
        super().__init__("atr-stop")
        if window <= 0 or multiple <= 0:
            raise ValueError("window and multiple must be positive")
        self.window = window
        self.multiple = multiple
        self._bars: dict[str, deque[KlineBar]] = {}
        self._entry: dict[str, float] = {}

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[RiskSignal]:
        bars = self._bars.setdefault(bar.code, deque(maxlen=self.window + 1))
        bars.append(bar)
        if len(bars) < self.window:
            return []
        atr = sum(item.high - item.low for item in bars) / len(bars)
        stop = bar.close - self.multiple * atr
        entry = self._entry.setdefault(bar.code, bar.close)
        if entry > 0 and bar.close < stop:
            return [RiskSignal(True, "float", value=0.0, code=bar.code, time=bar.end_time, reason="ATR stop")]
        return []


__all__ = ["AtrStopRiskFactor"]
