from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from typing import Any

from trading.factors.base import KlineTimingFactor
from trading.factors.types import KlineBar, SignalIntent


class _DailyTimingFactor(KlineTimingFactor):
    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def _intent(self, bar: KlineBar, action: str, reason: str) -> SignalIntent:
        return SignalIntent(bar.code, action, bar.end_time, reason, {"volume": 100})


class DualMaTimingFactor(_DailyTimingFactor):
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, slow_window: int = 20) -> None:
        super().__init__("dual-ma")
        if fast_window <= 0 or fast_window >= slow_window:
            raise ValueError("fast_window must be positive and below slow_window")
        self.fast_window, self.slow_window = fast_window, slow_window
        self.history: dict[str, deque[float]] = {}
        self.previous: dict[str, bool] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.slow_window))
        values.append(bar.close)
        if len(values) < self.slow_window:
            return []
        bullish = sum(list(values)[-self.fast_window:]) / self.fast_window > sum(values) / len(values)
        previous = self.previous.get(bar.code, bullish)
        self.previous[bar.code] = bullish
        if bullish and not previous:
            return [self._intent(bar, "BUY", "dual moving average bullish cross")]
        if previous and not bullish:
            return [self._intent(bar, "SELL", "dual moving average bearish cross")]
        return []


class BreakoutTimingFactor(_DailyTimingFactor):
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 20) -> None:
        super().__init__("breakout")
        self.window = window
        self.history: dict[str, deque[float]] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.window + 1))
        prior = list(values)
        output: list[SignalIntent] = []
        if len(prior) >= self.window:
            if bar.close > max(prior[-self.window:]):
                output.append(self._intent(bar, "BUY", "daily high breakout"))
            elif bar.close < min(prior[-self.window:]):
                output.append(self._intent(bar, "SELL", "daily low breakdown"))
        values.append(bar.close)
        return output


class MeanReversionTimingFactor(_DailyTimingFactor):
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, window: int = 14, oversold: float = 30, overbought: float = 70) -> None:
        super().__init__("mean-reversion")
        self.window, self.oversold, self.overbought = window, oversold, overbought
        self.history: dict[str, deque[float]] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.window + 1))
        values.append(bar.close)
        if len(values) <= self.window:
            return []
        changes = [b - a for a, b in zip(values, list(values)[1:])]
        gains = sum(max(change, 0) for change in changes)
        losses = sum(max(-change, 0) for change in changes)
        rsi = 100.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses)
        if rsi <= self.oversold:
            return [self._intent(bar, "BUY", "RSI oversold reversal")]
        if rsi >= self.overbought:
            return [self._intent(bar, "SELL", "RSI overbought reversal")]
        return []


class GoldenCrossTimingFactor(_DailyTimingFactor):
    emitted_actions = frozenset({"BUY", "SELL"})

    def __init__(self, fast_window: int = 5, middle_window: int = 10, slow_window: int = 20) -> None:
        super().__init__("golden-cross")
        if not fast_window < middle_window < slow_window:
            raise ValueError("moving-average windows must be ascending")
        self.windows = (fast_window, middle_window, slow_window)
        self.history: dict[str, deque[float]] = {}
        self.previous: dict[str, bool] = {}

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        values = self.history.setdefault(bar.code, deque(maxlen=self.windows[-1]))
        values.append(bar.close)
        if len(values) < self.windows[-1]:
            return []
        numbers = list(values)
        fast, middle, slow = self.windows
        bullish = (
            sum(numbers[-fast:]) / fast
            > sum(numbers[-middle:]) / middle
            > sum(numbers[-slow:]) / slow
        )
        previous = self.previous.get(bar.code, bullish)
        self.previous[bar.code] = bullish
        if bullish and not previous:
            return [self._intent(bar, "BUY", "golden cross alignment")]
        if previous and not bullish:
            return [self._intent(bar, "SELL", "golden cross alignment ended")]
        return []


class PassiveTimingFactor(_DailyTimingFactor):
    def __init__(self) -> None:
        super().__init__("passive")

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


__all__ = [
    "BreakoutTimingFactor",
    "DualMaTimingFactor",
    "GoldenCrossTimingFactor",
    "MeanReversionTimingFactor",
    "PassiveTimingFactor",
]
