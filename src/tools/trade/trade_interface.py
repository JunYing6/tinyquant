"""Pure trading value objects and the abstract broker interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class Order:
    """A broker-neutral order value returned by an execution provider."""

    def __init__(
        self,
        order_id: str,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        price: Optional[float],
        volume: int,
        status: OrderStatus = OrderStatus.PENDING,
        filled_volume: int = 0,
        filled_price: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.price = price
        self.volume = volume
        self.status = status
        self.filled_volume = filled_volume
        self.filled_price = filled_price
        self.extra = kwargs

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "price": self.price,
            "volume": self.volume,
            "status": self.status.value,
            "filled_volume": self.filled_volume,
            "filled_price": self.filled_price,
            **self.extra,
        }


class Position:
    """A broker-neutral position value returned by an execution provider."""

    def __init__(
        self,
        symbol: str,
        volume: int,
        available_volume: int,
        cost_price: float,
        market_price: float,
        market_value: float,
        profit: float,
        profit_ratio: float,
        **kwargs: Any,
    ) -> None:
        self.symbol = symbol
        self.volume = volume
        self.available_volume = available_volume
        self.cost_price = cost_price
        self.market_price = market_price
        self.market_value = market_value
        self.profit = profit
        self.profit_ratio = profit_ratio
        self.extra = kwargs

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "volume": self.volume,
            "available_volume": self.available_volume,
            "cost_price": self.cost_price,
            "market_price": self.market_price,
            "market_value": self.market_value,
            "profit": self.profit,
            "profit_ratio": self.profit_ratio,
            **self.extra,
        }


class AccountSnapshot:
    """Pure broker account snapshot; it is not an in-memory account ledger."""

    def __init__(
        self,
        total_assets: float,
        available_cash: float,
        frozen_cash: float,
        market_value: float,
        profit: float,
        profit_ratio: float,
        **kwargs: Any,
    ) -> None:
        self.total_assets = total_assets
        self.available_cash = available_cash
        self.frozen_cash = frozen_cash
        self.market_value = market_value
        self.profit = profit
        self.profit_ratio = profit_ratio
        self.extra = kwargs

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_assets": self.total_assets,
            "available_cash": self.available_cash,
            "frozen_cash": self.frozen_cash,
            "market_value": self.market_value,
            "profit": self.profit,
            "profit_ratio": self.profit_ratio,
            **self.extra,
        }


class TradeInterface(ABC):
    """Abstract broker interface retained for class-based adapters."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass

    @abstractmethod
    def get_account(self) -> AccountSnapshot:
        pass

    @abstractmethod
    def get_positions(self) -> list[Position]:
        pass

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        volume: int,
        order_type: OrderType = OrderType.LIMIT,
        price: Optional[float] = None,
    ) -> Order:
        pass

    def buy(
        self,
        symbol: str,
        volume: int,
        price: Optional[float] = None,
        order_type: OrderType = OrderType.LIMIT,
    ) -> Order:
        return self.place_order(symbol, OrderSide.BUY, volume, order_type, price)

    def sell(
        self,
        symbol: str,
        volume: int,
        price: Optional[float] = None,
        order_type: OrderType = OrderType.LIMIT,
    ) -> Order:
        return self.place_order(symbol, OrderSide.SELL, volume, order_type, price)

    def get_position(self, symbol: str) -> Optional[Position]:
        return next((position for position in self.get_positions() if position.symbol == symbol), None)

    def _ensure_connected(self) -> None:
        if not self.is_connected():
            raise RuntimeError("not connected to the trading system; call connect() first")
