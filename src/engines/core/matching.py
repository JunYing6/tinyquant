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
from enum import StrEnum
from typing import Any, Callable, Mapping

from trading_nodes_base.types import ExecutionMode, ExecutionRequest

from engines.core.subscriptions import KIND_TICK, SubscriptionTracker, kline_kind

_MATCH_MODES = frozenset({"quote_or_last", "last_trade_only", "quote_only"})
_TIMINGS = frozenset({"market", "limit", "next_bar_open", "next_bar_close"})


class OrderStatus(StrEnum):
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


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
    status: OrderStatus = OrderStatus.SUBMITTED
    reason: str = ""
    stream_kind: str = ""


class MatchingEngine:
    """按订单撮合方式在价格事件流上成交的统一匹配器。"""

    def __init__(
        self,
        account_order: Callable[[list[tuple]], Any],
        event_sink: Callable[[Mapping[str, Any]], None] | None = None,
        match_mode: str = "quote_or_last",
        slippage: Callable[[float, str], float] | None = None,
        default_timing: str | None = None,
        execution_delay: int = 1,
        tracker: SubscriptionTracker | None = None,
    ) -> None:
        if match_mode not in _MATCH_MODES:
            raise ValueError(f"match_mode must be one of {sorted(_MATCH_MODES)}")
        if default_timing is not None and default_timing not in _TIMINGS:
            raise ValueError(f"default_timing must be one of {sorted(_TIMINGS)}")
        if not isinstance(execution_delay, int) or isinstance(execution_delay, bool) or execution_delay < 1:
            raise ValueError("execution_delay must be an integer >= 1")
        self._account_order = account_order
        self._event_sink = event_sink
        self._match_mode = match_mode
        self._slippage = slippage
        self._default_timing = default_timing
        self._execution_delay = execution_delay
        self._tracker = tracker
        self._orders: dict[str, MatchingOrder] = {}
        self._pending: list[str] = []
        self._bar_seq: dict[str, int] = {}
        self._tick_seq: dict[str, int] = {}
        # 每 (kind, code) 的在途单计数：归零时才向 registry 退订
        self._want_counts: dict[tuple[str, str], int] = {}

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

    def _resolve_timing(self, request: ExecutionRequest) -> str:
        """决定订单的成交时机。

        策略只表达订单类型（``mode``：市价/限价），不决定成交时机。
        - 显式 ``timing``（引擎注入）优先；
        - 限价 / 市价按 ``mode``；
        - 裸信号单（无 mode）：套用引擎配置的 ``default_timing``；未配置时退回
          独立撮合器的旧语义（带价即视为限价）。
        """
        if request.timing is not None:
            return request.timing
        if request.mode is ExecutionMode.LIMIT:
            return "limit"
        if request.mode is ExecutionMode.MARKET or request.price <= 0:
            return "market"
        return self._default_timing or "limit"

    def _counter_for(self, timing: str) -> dict[str, int]:
        """成交延迟按订单自身的价格事件类型计数：K 线单数 K 线，tick 单数 tick。"""
        return self._bar_seq if timing in ("next_bar_open", "next_bar_close") else self._tick_seq

    def submit_requests(
        self, requests: list[ExecutionRequest], trade_date: Any, frequency: str | None = None
    ) -> list[MatchingOrder]:
        submitted: list[MatchingOrder] = []
        date_key = self._date_key(trade_date)
        for request in requests:
            if request.volume is None or request.volume <= 0:
                continue
            timing = self._resolve_timing(request)
            kind = self._order_stream_kind(timing, frequency)
            order = MatchingOrder(
                uuid.uuid4().hex,
                request.code,
                request.action,
                int(request.volume),
                timing,
                request.price if timing == "limit" else None,
                dict(self._counter_for(timing)),
                date_key,
                stream_kind=kind,
            )
            self._orders[order.order_id] = order
            self._pending.append(order.order_id)
            self._want(order)
            self._emit(order, "SUBMITTED")
            submitted.append(order)
        return submitted

    @staticmethod
    def _order_stream_kind(timing: str, frequency: str | None) -> str:
        if timing in ("next_bar_open", "next_bar_close"):
            return kline_kind(frequency or "1d")
        return KIND_TICK

    def _want(self, order: MatchingOrder) -> None:
        key = (order.stream_kind, order.code)
        self._want_counts[key] = self._want_counts.get(key, 0) + 1
        if self._tracker is not None:
            self._tracker.want(order.stream_kind, [order.code])

    def _release(self, order: MatchingOrder) -> None:
        key = (order.stream_kind, order.code)
        count = self._want_counts.get(key, 0) - 1
        if count > 0:
            self._want_counts[key] = count
            return
        self._want_counts.pop(key, None)
        if self._tracker is not None:
            self._tracker.release(order.stream_kind, [order.code])

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
        # 推进该标的对应事件流的计数（用于成交延迟：第 N 个 k_line/tick）
        counter = self._bar_seq if is_bar else self._tick_seq
        counter[code] = counter.get(code, 0) + 1
        matched: list[MatchingOrder] = []
        for order_id in list(self._pending):
            order = self._orders[order_id]
            if order.code != code:
                continue
            order_counter = self._counter_for(order.timing)
            if order_counter.get(code, 0) - order.seq.get(code, 0) < self._execution_delay:
                continue  # 未到成交延迟
            fill_price, ok = self._fill(order, event)
            if not ok or fill_price is None or fill_price <= 0:
                continue
            if self._slippage is not None:
                fill_price = self._slippage(float(fill_price), order.action)
                if fill_price <= 0:
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
                order.status = OrderStatus.PARTIALLY_FILLED
                self._emit(order, "PARTIALLY_FILLED")
            else:
                self._transition(order, "FILLED", "FILLED")
            matched.append(order)
        return matched

    def _fill(self, order: MatchingOrder, event: Any) -> tuple[float | None, bool]:
        if order.timing in ("next_bar_open", "next_bar_close"):
            if not hasattr(event, "open"):
                return None, False
            price = getattr(event, "open" if order.timing == "next_bar_open" else "close", None)
            return price, True
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
        order.status = OrderStatus(status)
        order.reason = reason
        if order.order_id in self._pending:
            self._pending.remove(order.order_id)
            self._release(order)
        self._emit(order, status, reason)

    def cancel(self, order_id: str) -> bool:
        if order_id not in self._pending:
            return False
        self._transition(self._orders[order_id], "CANCELLED", "CANCELLED")
        return True

    def expire_day(self, trade_date: Any) -> None:
        day = self._date_key(trade_date)
        for order_id in list(self._pending):
            order = self._orders[order_id]
            if order.timing in ("next_bar_open", "next_bar_close"):
                # next_bar 订单需存活到第 N 个 K 线；仅当已过该窗口仍未成交才过期
                seen = self._bar_seq.get(order.code, 0) - order.seq.get(order.code, 0)
                expires = seen >= self._execution_delay
            else:
                # 日内（market/limit）订单当日未成交即过期
                expires = order.trade_date <= day
            if expires:
                self._transition(order, "EXPIRED", "EXPIRED")

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
