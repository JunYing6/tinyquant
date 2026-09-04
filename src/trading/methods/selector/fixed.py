from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading.methods.base import BaseStockPicking


class FixedStockPicking(BaseStockPicking):
    def __init__(self, stock_pool: Sequence[str] | None = None) -> None:
        self.stock_pool = list(dict.fromkeys(stock_pool or []))
        super().__init__("fixed-stock-picking", [], [], [])

    def select_stocks(self, date: Any) -> pd.DataFrame:
        if not self.stock_pool:
            return pd.DataFrame(columns=["asset", "weight"])
        weight = 1.0 / len(self.stock_pool)
        return pd.DataFrame({"asset": self.stock_pool, "weight": [weight] * len(self.stock_pool)})


__all__ = ["FixedStockPicking"]
