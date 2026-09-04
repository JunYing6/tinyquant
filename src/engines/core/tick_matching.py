"""Provider-neutral, in-memory Tick order matching."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Callable, Mapping

from trading.factors.types import ExecutionMode, ExecutionRequest


class OrderStatus(StrEnum):
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REPLACED = "REPLACED"
    REJECTED = "REJECTED"


@dataclass
class MatchingOrder:
    order_id: str
    code: str
    action: str
    volume: int
    order_type: str
    limit_price: float | None
    trade_date: str
    submitted_time: Any = None
    status: OrderStatus = OrderStatus.SUBMITTED
    filled_volume: int = 0
    fill_price: float | None = None
    reason: str = ""
    mode: ExecutionMode | None = None


class TickMatchingEngine:
    def __init__(
        self,
        account_order: Callable[[list[tuple]], Any],
        event_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._account_order = account_order
        self._event_sink = event_sink
        self._orders: dict[str, MatchingOrder] = {}
        self._pending: list[str] = []

    def begin_day(self, trade_date: Any) -> None:
        current = self._date_key(trade_date)
        for order_id in list(self._pending):
            if self._orders[order_id].trade_date != current:
                self._transition(order_id, OrderStatus.EXPIRED, "EXPIRED_AT_DAY_INIT")

    def submit_requests(self, requests: list[ExecutionRequest], trade_date: Any) -> list[MatchingOrder]:
        submitted: list[MatchingOrder] = []
        for request in requests:
            if request.volume is None or request.volume <= 0:
                continue
            requested_type = (request.order_type or "").upper()
            order_type = requested_type if requested_type in {"LIMIT", "MARKET"} else "LIMIT"
            submitted.append(
                self.submit(
                    request.code,
                    request.action,
                    request.volume,
                    order_type=order_type,
                    limit_price=request.price,
                    trade_date=self._date_key(trade_date),
                    submitted_time=request.time,
                    reason=request.reason,
                    mode=request.mode,
                )
            )
        return submitted

    def submit(
        self,
        code: str,
        action: str,
        volume: int,
        *,
        order_type: str = "LIMIT",
        limit_price: float | None = None,
        trade_date: str,
        submitted_time: Any = None,
        reason: str = "",
        mode: ExecutionMode | None = None,
    ) -> MatchingOrder:
        if action not in {"BUY", "SELL"}:
            raise ValueError("action must be BUY or SELL")
        if order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("order_type must be LIMIT or MARKET")
        if volume <= 0:
            raise ValueError("volume must be positive")
        if (mode is ExecutionMode.LIMIT or (mode is None and order_type == "LIMIT")) and (limit_price is None or limit_price <= 0):
            raise ValueError("limit order requires a positive limit_price")
        order = MatchingOrder(
            uuid.uuid4().hex,
            code,
            action,
            int(volume),
            order_type,
            limit_price,
            self._date_key(trade_date),
            submitted_time,
            reason=reason,
            mode=mode,
        )
        self._orders[order.order_id] = order
        self._pending.append(order.order_id)
        self._emit(order, "SUBMITTED")
        return order

    def match(self, tick: Mapping[str, Any], trade_date: Any) -> list[MatchingOrder]:
        matched: list[MatchingOrder] = []
        current = self._date_key(trade_date)
        for order_id in list(self._pending):
            order = self._orders[order_id]
            if order.code != tick.get("code") or order.trade_date != current:
                continue
            price = self._fill_price(order, tick)
            if price is None:
                continue
            remaining_volume = order.volume - order.filled_volume
            signed_volume = remaining_volume if order.action == "BUY" else -remaining_volume
            try:
                result = self._account_order(
                    [(order.trade_date, tick.get("time"), order.code, price, signed_volume, "n")]
                )
            except Exception as error:
                order.reason = str(error)
                self._transition(order_id, OrderStatus.REJECTED, "ACCOUNT_REJECTED")
                continue
            filled_volume = self._reported_fill_volume(result, order, remaining_volume)
            if filled_volume <= 0:
                order.reason = "account reported no fill"
                self._transition(order_id, OrderStatus.REJECTED, "ACCOUNT_NO_FILL")
                continue
            order.filled_volume += filled_volume
            order.fill_price = price
            if order.filled_volume < order.volume:
                order.status = OrderStatus.PARTIALLY_FILLED
                self._emit(order, "PARTIALLY_FILLED")
            else:
                self._transition(order_id, OrderStatus.FILLED, "FILLED")
            matched.append(order)
        return matched

    def cancel(self, order_id: str) -> bool:
        if order_id not in self._pending:
            return False
        self._transition(order_id, OrderStatus.CANCELLED, "CANCELLED")
        return True

    def expire_day(self, trade_date: Any) -> None:
        day = self._date_key(trade_date)
        for order_id in list(self._pending):
            if self._orders[order_id].trade_date == day:
                self._transition(order_id, OrderStatus.EXPIRED, "EXPIRED")

    def pending_orders(self) -> list[MatchingOrder]:
        return [self._orders[order_id] for order_id in self._pending]

    def _transition(self, order_id: str, status: OrderStatus, reason: str) -> None:
        order = self._orders[order_id]
        order.status = status
        if order_id in self._pending:
            self._pending.remove(order_id)
        self._emit(order, reason)

    def _emit(self, order: MatchingOrder, reason: str) -> None:
        if self._event_sink is not None:
            self._event_sink(
                {
                    "order_id": order.order_id,
                    "status": order.status,
                    "code": order.code,
                    "action": order.action,
                    "requested_volume": order.volume,
                    "filled_volume": order.filled_volume,
                    "fill_price": order.fill_price,
                    "reason": reason,
                }
            )

    @staticmethod
    def _reported_fill_volume(result: Any, order: MatchingOrder, remaining: int) -> int:
        if result is None:
            return remaining
        if isinstance(result, (int, float)) and not isinstance(result, bool):
            return min(remaining, max(0, int(abs(result))))
        if not isinstance(result, list):
            return remaining
        filled = 0
        for record in result:
            if not isinstance(record, tuple) or len(record) < 5:
                continue
            if record[2] != order.code:
                continue
            try:
                volume = int(record[4])
            except (TypeError, ValueError):
                continue
            if (order.action == "BUY" and volume > 0) or (order.action == "SELL" and volume < 0):
                filled += abs(volume)
        return min(remaining, filled)

    @staticmethod
    def _date_key(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        text = str(value).replace("-", "")
        if len(text) < 8 or not text[:8].isdigit():
            raise ValueError("trade_date must use YYYYMMDD")
        return text[:8]

    @staticmethod
    def _fill_price(order: MatchingOrder, tick: Mapping[str, Any]) -> float | None:
        ask = tick.get("ask1_price") or tick.get("price")
        bid = tick.get("bid1_price") or tick.get("price")
        price = ask if order.action == "BUY" else bid
        if not isinstance(price, (int, float)) or price <= 0:
            return None
        if order.mode is ExecutionMode.LIMIT or (order.mode is None and order.order_type == "LIMIT"):
            if order.limit_price is None:
                return None
            if order.action == "BUY" and price > order.limit_price:
                return None
            if order.action == "SELL" and price < order.limit_price:
                return None
        return float(price)


__all__ = ["MatchingOrder", "OrderStatus", "TickMatchingEngine"]
