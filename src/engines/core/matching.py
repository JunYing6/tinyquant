"""统一撮合器：吸收旧 TickMatchingEngine 与 FastExecutionAdapter。

消费价格事件流（TradeTick / QuoteTick / KlineBar），订单携带撮合方式
（``timing``：limit/tick 交叉、next_bar_open/close、market），撮合器在
对应事件价成交，统一经 ``account_order`` 记账，并通过 ``event_sink`` 回发
成交事件（SUBMITTED/PARTIALLY_FILLED/FILLED/REJECTED/...）供
``BaseStrategy.handle_order_event`` / 网格订单簿消费。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Mapping

from trading_nodes_base.types import ExecutionMode, ExecutionRequest

_MATCH_MODES = frozenset({"quote_or_last", "last_trade_only", "quote_only"})


@dataclass
class MatchingOrder:
    order_id: str
    code: str
    action: str
    volume: int
    timing: str
    limit_price: float | None
    seq: dict[str, int] = field(default_factory=dict)
    trade_date: str = ""
    filled_volume: int = 0
    status: str = "SUBMITTED"
    reason: str = ""


class MatchingEngine:
    """按订单撮合方式在价格事件流上成交的统一匹配器。"""

    def __init__(
        self,
        account_order: Callable[[list[tuple]], Any],
        event_sink: Callable[[Mapping[str, Any]], None] | None = None,
        match_mode: str = "quote_or_last",
    ) -> None:
        if match_mode not in _MATCH_MODES:
            raise ValueError(f"match_mode must be one of {sorted(_MATCH_MODES)}")
        self._account_order = account_order
        self._event_sink = event_sink
        self._match_mode = match_mode
        self._orders: dict[str, MatchingOrder] = {}
        self._pending: list[str] = []
        self._seq: dict[str, int] = {}

    def _emit(self, order: MatchingOrder, status: str, reason: str = "") -> None:
        if self._event_sink is None:
            return
        self._event_sink(
            {
                "order_id": order.order_id,
                "status": status,
                "code": order.code,
                "action": order.action,
                "requested_volume": order.volume,
                "filled_volume": order.filled_volume,
                "reason": reason,
            }
        )

    def submit_requests(
        self, requests: list[ExecutionRequest], trade_date: Any
    ) -> list[MatchingOrder]:
        submitted: list[MatchingOrder] = []
        date_key = self._date_key(trade_date)
        for request in requests:
            if request.volume is None or request.volume <= 0:
                continue
            timing = request.timing or (
                "limit" if request.mode is ExecutionMode.LIMIT else "market"
            )
            order = MatchingOrder(
                uuid.uuid4().hex,
                request.code,
                request.action,
                int(request.volume),
                timing,
                request.price if timing == "limit" else None,
                dict(self._seq),
                date_key,
            )
            self._orders[order.order_id] = order
            self._pending.append(order.order_id)
            self._emit(order, "SUBMITTED")
            submitted.append(order)
        return submitted

    def match_event(self, event: Any, trade_date: Any) -> list[MatchingOrder]:
        code = getattr(event, "instrument_id", None) or getattr(event, "code", None)
        if not code:
            return []
        is_bar = hasattr(event, "open")
        is_quote = not is_bar and hasattr(event, "ask_levels")
        is_trade = not is_bar and not is_quote
        if is_quote and self._match_mode == "last_trade_only":
            return []
        if is_trade and self._match_mode == "quote_only":
            return []
        if is_bar:
            self._seq[code] = self._seq.get(code, 0) + 1
        matched: list[MatchingOrder] = []
        for order_id in list(self._pending):
            order = self._orders[order_id]
            if order.code != code:
                continue
            fill_price, ok = self._fill(order, event, self._seq.get(code, 0))
            if not ok or fill_price is None or fill_price <= 0:
                continue
            remaining = order.volume - order.filled_volume
            signed = remaining if order.action == "BUY" else -remaining
            try:
                result = self._account_order(
                    [("", "", code, float(fill_price), signed, "n")]
                )
            except Exception as error:
                order.reason = str(error)
                self._transition(order, "REJECTED", "ACCOUNT_REJECTED")
                continue
            filled = self._reported_fill(result, order, remaining)
            if filled <= 0:
                order.reason = "account reported no fill"
                self._transition(order, "REJECTED", "ACCOUNT_NO_FILL")
                continue
            order.filled_volume += filled
            if order.filled_volume < order.volume:
                order.status = "PARTIALLY_FILLED"
                self._emit(order, "PARTIALLY_FILLED")
            else:
                self._transition(order, "FILLED", "FILLED")
            matched.append(order)
        return matched

    def _fill(self, order: MatchingOrder, event: Any, seq: int) -> tuple[float | None, bool]:
        if order.timing in ("next_bar_open", "next_bar_close"):
            if not hasattr(event, "open"):
                return None, False
            price = getattr(event, "open" if order.timing == "next_bar_open" else "close", None)
            return price, seq == order.seq.get(order.code, -1) + 1
        ask = getattr(event, "ask1_price", None)
        if ask is None:
            levels = getattr(event, "ask_levels", None)
            ask = levels[0].price if levels else None
        bid = getattr(event, "bid1_price", None)
        if bid is None:
            levels = getattr(event, "bid_levels", None)
            bid = levels[0].price if levels else None
        ask = ask or getattr(event, "price", None)
        bid = bid or getattr(event, "price", None)
        price = ask if order.action == "BUY" else bid
        if price is None:
            return None, False
        if order.timing == "limit":
            if order.limit_price is None:
                return None, False
            ok = price <= order.limit_price if order.action == "BUY" else price >= order.limit_price
            return float(price), ok
        return float(price), True  # market

    def _transition(self, order: MatchingOrder, status: str, reason: str) -> None:
        order.status = status
        order.reason = reason
        if order.order_id in self._pending:
            self._pending.remove(order.order_id)
        self._emit(order, status, reason)

    def cancel(self, order_id: str) -> bool:
        if order_id not in self._pending:
            return False
        self._transition(self._orders[order_id], "CANCELLED", "CANCELLED")
        return True

    def expire_day(self, trade_date: Any) -> None:
        day = self._date_key(trade_date)
        for order_id in list(self._pending):
            if self._orders[order_id].trade_date == day:
                self._transition(self._orders[order_id], "EXPIRED", "EXPIRED")

    def pending_orders(self) -> list[MatchingOrder]:
        return [self._orders[order_id] for order_id in self._pending]

    @staticmethod
    def _reported_fill(result: Any, order: MatchingOrder, remaining: int) -> int:
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


__all__ = ["MatchingOrder", "MatchingEngine"]
