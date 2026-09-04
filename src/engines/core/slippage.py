"""Pure price slippage models for in-memory execution."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Literal


class SlippageModel:
    VALID_MODELS = frozenset({"none", "proportional", "fixed"})

    def __init__(
        self,
        buy_slippage: float = 0.001,
        sell_slippage: float = 0.001,
        model: Literal["none", "proportional", "fixed"] = "proportional",
    ) -> None:
        if buy_slippage < 0 or sell_slippage < 0:
            raise ValueError("slippage must be non-negative")
        if model not in self.VALID_MODELS:
            raise ValueError(f"unsupported slippage model: {model!r}")
        self.buy_slippage = Decimal(str(buy_slippage))
        self.sell_slippage = Decimal(str(sell_slippage))
        self.model = model

    def apply(self, price: float, action: Literal["BUY", "SELL"]) -> float:
        if action == "BUY":
            return self.apply_buy(price)
        if action == "SELL":
            return self.apply_sell(price)
        raise ValueError("action must be BUY or SELL")

    def apply_buy(self, price: float) -> float:
        return self._apply(price, self.buy_slippage, 1)

    def apply_sell(self, price: float) -> float:
        return self._apply(price, self.sell_slippage, -1)

    def _apply(self, price: float, amount: Decimal, direction: int) -> float:
        if price < 0:
            raise ValueError("price must be non-negative")
        if self.model == "none" or amount == 0:
            return float(price)
        base = Decimal(str(price))
        result = base * (1 + direction * amount) if self.model == "proportional" else base + direction * amount
        return float(result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


__all__ = ["SlippageModel"]
