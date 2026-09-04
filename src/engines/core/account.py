"""In-memory account ledger used by backtest and stream runtimes."""

from __future__ import annotations

from typing import Any, Mapping


_DEFAULT_ORDER_COST = {
    "commission": 0.0000754,
    "gh_cost": 0.00001,
    "yh_cost": 0.0005,
}
_LOT_SIZE = 100
_MONEY_PRECISION = 2


class Account:
    """A synchronous, long-only account ledger with A-share lot sizing."""

    def __init__(self, money: float, oc: Mapping[str, float] | None = None) -> None:
        if money <= 0:
            raise ValueError("money must be positive")
        self.order_cost = dict(_DEFAULT_ORDER_COST if oc is None else oc)
        self._balance = round(float(money), _MONEY_PRECISION)
        self._total_equity = self._balance
        self._positions: dict[str, int] = {}
        self._cost_prices: dict[str, float] = {}
        self._trade_log: dict[str, list[tuple[Any, str, float, int]]] = {}

    @property
    def total_equity(self) -> float:
        return self._total_equity

    @property
    def total_assets(self) -> float:
        return self._total_equity

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def positions(self) -> dict[str, int]:
        return dict(self._positions)

    @property
    def cost_prices(self) -> dict[str, float]:
        return dict(self._cost_prices)

    @property
    def trade_log(self) -> dict[str, list[tuple[Any, str, float, int]]]:
        return {date: list(entries) for date, entries in self._trade_log.items()}

    def deposit(self, amount: float) -> None:
        if amount <= 0:
            raise ValueError("deposit amount must be positive")
        self._balance = round(self._balance + amount, _MONEY_PRECISION)
        self._total_equity = round(self._total_equity + amount, _MONEY_PRECISION)

    def withdraw(self, amount: float) -> float:
        if amount <= 0:
            raise ValueError("withdraw amount must be positive")
        actual = min(float(amount), self._balance)
        self._balance = round(self._balance - actual, _MONEY_PRECISION)
        self._total_equity = round(self._total_equity - actual, _MONEY_PRECISION)
        return round(actual, _MONEY_PRECISION)

    def get_position(self, code: str) -> int:
        return self._positions.get(code, 0)

    def order(self, orders: list[tuple]) -> list[tuple]:
        """Apply order tuples of ``(date, time, code, price, amount, type)``."""
        fills: list[tuple] = []
        for order in orders:
            if len(order) < 6:
                raise ValueError("order must contain date, time, code, price, amount, and type")
            date, time, code, price, amount, order_type = order[:6]
            if not isinstance(code, str) or not code:
                raise ValueError("order code must be a non-empty string")
            if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
                raise ValueError("order price must be positive")
            quantity = self._resolve_quantity(float(price), float(amount), str(order_type))
            if quantity > 0:
                filled = self._buy(str(date), time, code, float(price), quantity)
            elif quantity < 0:
                filled = -self._sell(str(date), time, code, float(price), -quantity)
            else:
                filled = 0
            if filled:
                fills.append((date, time, code, float(price), filled, "n"))
        return fills

    def _resolve_quantity(self, price: float, amount: float, order_type: str) -> int:
        if order_type == "n":
            return int(amount)
        if order_type == "m":
            return int(amount / price)
        if order_type == "p":
            return int(self._total_equity * amount / price)
        raise ValueError(f"unsupported order type: {order_type!r}")

    def _buy(self, date: str, time: Any, code: str, price: float, quantity: int) -> int:
        requested = self._round_down_lot(quantity)
        if requested <= 0:
            return 0
        unit_cost = price * (1 + self._cost_rate(code, is_sell=False))
        affordable = self._round_down_lot(int(self._balance / unit_cost))
        filled = min(requested, affordable)
        if filled <= 0:
            return 0
        value = filled * price
        fee = self._trade_cost(code, value, is_sell=False)
        previous = self._positions.get(code, 0)
        previous_cost = self._cost_prices.get(code, 0.0)
        self._positions[code] = previous + filled
        self._cost_prices[code] = (previous * previous_cost + filled * price) / (previous + filled)
        self._balance = round(self._balance - value - fee, _MONEY_PRECISION)
        self._record(date, time, code, price, filled)
        return filled

    def _sell(self, date: str, time: Any, code: str, price: float, quantity: int) -> int:
        held = self._positions.get(code, 0)
        filled = min(max(0, int(quantity)), held)
        if filled <= 0:
            return 0
        value = filled * price
        fee = self._trade_cost(code, value, is_sell=True)
        remaining = held - filled
        if remaining:
            self._positions[code] = remaining
        else:
            self._positions.pop(code, None)
            self._cost_prices.pop(code, None)
        self._balance = round(self._balance + value - fee, _MONEY_PRECISION)
        self._record(date, time, code, price, -filled)
        return filled

    def _record(self, date: str, time: Any, code: str, price: float, quantity: int) -> None:
        self._trade_log.setdefault(date, []).append((time, code, price, quantity))

    @staticmethod
    def _round_down_lot(quantity: int) -> int:
        return max(0, int(quantity) // _LOT_SIZE * _LOT_SIZE)

    def _cost_rate(self, code: str, is_sell: bool) -> float:
        rate = float(self.order_cost.get("commission", 0.0))
        if code.upper().endswith(".SH"):
            rate += float(self.order_cost.get("gh_cost", 0.0))
        if is_sell:
            rate += float(self.order_cost.get("yh_cost", 0.0))
        return rate

    def _trade_cost(self, code: str, value: float, is_sell: bool) -> float:
        return round(value * self._cost_rate(code, is_sell), _MONEY_PRECISION)

    def daily_summarize(self, price_dict: Mapping[str, float]) -> None:
        value = self._balance
        for code, quantity in self._positions.items():
            price = price_dict.get(code, self._cost_prices.get(code, 0.0))
            if isinstance(price, (int, float)) and not isinstance(price, bool) and price > 0:
                value += quantity * float(price)
        self._total_equity = round(value, _MONEY_PRECISION)


__all__ = ["Account"]
