from __future__ import annotations

from collections.abc import Sequence

from trading.methods.selector.fixed import FixedStockPicking
from trading.methods.timer.passive import NoTradeTiming
from trading.methods.risk.full_position import FullPositionRisk
from trading.methods.risk.atr import AtrRiskControl
from trading.methods.selector.selection import MomentumSelector, PriceFilterSelector
from trading.factors.timer.tick.intent_executor import IntentExecutorFactor
from trading.factors.timer.kline.simple_timing import (
    BreakoutTimingFactor,
    DualMaTimingFactor,
    GoldenCrossTimingFactor,
    MeanReversionTimingFactor,
)
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


def _timing(name: str, factors: list) -> BaseTimeSelection:
    return BaseTimeSelection(name, factors, [IntentExecutorFactor()])


class DualMaStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("dual-ma", selector, _timing("dual-ma-timing", [DualMaTimingFactor()]), FullPositionRisk())


class BreakoutStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout", selector, _timing("breakout-timing", [BreakoutTimingFactor()]), FullPositionRisk())


class MeanReversionStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("mean-reversion", selector, _timing("mean-reversion-timing", [MeanReversionTimingFactor()]), FullPositionRisk())


class GoldenCrossStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("golden-cross", selector, _timing("golden-cross-timing", [GoldenCrossTimingFactor()]), FullPositionRisk())


class AtrStopStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("atr-stop", selector, _timing("atr-stop-timing", [DualMaTimingFactor()]), AtrRiskControl())


class BreakoutRiskStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("breakout-risk", selector, _timing("breakout-risk-timing", [BreakoutTimingFactor()]), AtrRiskControl())


class MomentumPickStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = MomentumSelector(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("momentum-pick", selector, NoTradeTiming(), FullPositionRisk())


class FilterPickStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = PriceFilterSelector(["000001.SZ", "600000.SH"] if stock_pool is None else stock_pool)
        super().__init__("filter-pick", selector, NoTradeTiming(), FullPositionRisk())


class EmptyPositionStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        selector = FixedStockPicking([] if stock_pool is None else stock_pool)
        super().__init__("empty-position", selector, NoTradeTiming(), FullPositionRisk())


__all__ = [
    "AtrStopStrategy",
    "BreakoutRiskStrategy",
    "BreakoutStrategy",
    "DualMaStrategy",
    "EmptyPositionStrategy",
    "FilterPickStrategy",
    "GoldenCrossStrategy",
    "MeanReversionStrategy",
    "MomentumPickStrategy",
]
