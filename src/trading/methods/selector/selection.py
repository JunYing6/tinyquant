from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading.factors.base import BinarySelectionFactor, FloatSelectionFactor
from importlib import import_module

PriceAboveMaSelectionFactor = import_module("trading.factors.selector.and.price_above_ma").PriceAboveMaSelectionFactor
MomentumSelectionFactor = import_module("trading.factors.selector.float.momentum").MomentumSelectionFactor
from trading.methods.base import BaseStockPicking


def _pool_frame(stock_pool: Sequence[str]) -> pd.DataFrame:
    if not stock_pool:
        return pd.DataFrame(columns=["asset", "weight"])
    weight = 1.0 / len(stock_pool)
    return pd.DataFrame({"asset": list(stock_pool), "weight": [weight] * len(stock_pool)})


class MomentumSelector(BaseStockPicking):
    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        factor = MomentumSelectionFactor()
        super().__init__("momentum-selector", [], [], [factor])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        return _pool_frame(self.stock_pool)


class PriceFilterSelector(BaseStockPicking):
    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        factor = PriceAboveMaSelectionFactor()
        super().__init__("price-filter-selector", [factor], [], [])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        return _pool_frame(self.stock_pool)


__all__ = ["MomentumSelectionFactor", "MomentumSelector", "PriceAboveMaSelectionFactor", "PriceFilterSelector"]
