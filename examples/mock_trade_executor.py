"""In-memory broker executor for safe live-engine examples."""

from __future__ import annotations

from typing import Any


class InMemoryTradeExecutor:
    def __init__(self, capital: float = 100_000.0) -> None:
        self.capital = capital
        self.positions: dict[str, int] = {}
        self.orders: list[tuple[str, str, int, float | None]] = []

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def get_account(self) -> dict[str, float]:
        return {"total_assets": self.capital, "available_cash": self.capital}

    def get_positions(self) -> list[dict[str, Any]]:
        return [
            {"symbol": code, "volume": volume, "cost_price": 0.0}
            for code, volume in self.positions.items()
            if volume > 0
        ]

    def buy(self, symbol: str, volume: int, **kwargs: Any) -> dict[str, bool]:
        self.positions[symbol] = self.positions.get(symbol, 0) + volume
        self.orders.append(("BUY", symbol, volume, kwargs.get("price")))
        return {"success": True}

    def sell(self, symbol: str, volume: int, **kwargs: Any) -> dict[str, bool]:
        self.positions[symbol] = max(0, self.positions.get(symbol, 0) - volume)
        self.orders.append(("SELL", symbol, volume, kwargs.get("price")))
        return {"success": True}
