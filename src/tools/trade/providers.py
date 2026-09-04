"""Provider protocol for order execution and broker snapshots."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class TradeExecutor(Protocol):
    def connect(self) -> None:
        """Open the broker connection."""

    def buy(self, symbol: str, volume: int, **kwargs: Any) -> Any:
        """Submit a buy order."""

    def sell(self, symbol: str, volume: int, **kwargs: Any) -> Any:
        """Submit a sell order."""

    def get_positions(self) -> list[Any]:
        """Return broker positions."""

    def get_account(self) -> Any:
        """Return a broker account snapshot."""

    def disconnect(self) -> None:
        """Close the broker connection."""
