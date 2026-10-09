"""单策略运行时基类，含提供者无关的账户边界。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from tools.data import DataBatch
from trading_nodes_base.requests import canonical_request, decode_routing, with_routing
from trading_nodes_base.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading_nodes_base.methods import BaseRiskControl, BaseStockPicking, BaseTimeSelection, ContextSegment, RiskDecision, StateStorage, TradingContext


@dataclass
class _OrderSource:
    request: ExecutionRequest
    volume: int
    requested_volume: int


@dataclass
class _CanonicalOrder:
    request: ExecutionRequest
    order: tuple
    sources: List[_OrderSource]


@dataclass
class _InflightOrder:
    request: ExecutionRequest
    remaining_volume: int
    submitted_volume: int
    requested_volume: int
    filled_volume: int = 0


@dataclass
class _TimerOrder:
    """择时器台账记录：未乘风控缩放的原始订单及其生命周期。

    status: ``recorded``（已发出，未成交）→ ``filled``（成交）或 ``cancelled``（未成交撤单）。
    """

    request: ExecutionRequest
    code: str
    action: str
    target_volume: int
    status: str = "recorded"
    order_id: Any = None


def _request_to_dict(request: ExecutionRequest) -> Dict[str, Any]:
    return {
        "code": request.code,
        "action": request.action,
        "time": request.time,
        "price": request.price,
        "volume": request.volume,
        "sizing_intent": request.sizing_intent,
        "order_type": request.order_type,
        "reason": request.reason,
        "mode": request.mode.value if request.mode is not None else None,
    }


def _request_from_dict(data: Mapping[str, Any]) -> ExecutionRequest:
    mode = data.get("mode")
    return ExecutionRequest(
        code=data["code"],
        action=data["action"],
        time=data.get("time"),
        price=float(data.get("price", 0.0)),
        volume=data.get("volume"),
        sizing_intent=data.get("sizing_intent"),
        order_type=data.get("order_type"),
        reason=data.get("reason", ""),
        mode=ExecutionMode(mode) if mode else None,
    )


class BaseStrategy:
    tick_mode = False
    supports_fast_backtest = False
    # 风偏恢复时是否立即买入（默认 False：不马上买，统一等到尾盘收盘池买入）
    rebalance_buy_immediate = False

    def __init__(
        self,
        strategy_name: str,
        selector: Optional[BaseStockPicking] = None,
        timer: Optional[BaseTimeSelection] = None,
        risk_ctrl: Optional[BaseRiskControl] = None,
        state_storage: Optional[StateStorage] = None,
    ) -> None:
        if timer is None:
            raise ValueError("BaseStrategy requires a timer")
        if not isinstance(timer, BaseTimeSelection):
            raise TypeError("timer must be a BaseTimeSelection")
        # 允许纯 K 线策略（tick_factors 为空）：由引擎按下一根 K 线撮合，无需 tick 因子
        self.strategy_name = strategy_name
        self.selector = selector
        self.timer = timer
        self.risk_ctrl = risk_ctrl
        self.account: Any = None
        self._state_storage: Optional[StateStorage] = state_storage
        self.context = TradingContext(self._state_storage)
        self._current_date: Any = None
        self._daily_pipeline_triggered = False
        self._position_details: Dict[str, Dict[str, Any]] = {}
        self._pending_orders: List[ExecutionRequest] = []
        self._inflight_orders: Dict[Any, List[_InflightOrder]] = {}
        self._timer_orders: List[_TimerOrder] = []
        self._open_orders: List[ExecutionRequest] = []
        self._close_orders: List[ExecutionRequest] = []
        self._price_limits: Dict[str, Tuple[float, float]] = {}
        self.tick_matcher: Any = None
        self._original_buy_targets: Dict[str, int] = {}
        self._tail_replenished: bool = False
        self._last_risk_target_ratio: float = 1.0
        self.restore_state()

    @property
    def current_positions(self) -> Dict[str, int]:
        if self.account is None:
            return {}
        return dict(getattr(self.account, "positions", {}))

    def _inject_context(self) -> None:
        seen: Dict[str, object] = {}
        for component in (self.selector, self.timer, self.risk_ctrl):
            if component is None:
                continue
            if component.name in seen:
                raise ValueError(
                    f"duplicate component name {component.name!r} in strategy {self.strategy_name!r} "
                    "would collide in the shared TradingContext"
                )
            seen[component.name] = component
            component.set_context(self.context, strategy_name=self.strategy_name)

    @property
    def state(self) -> ContextSegment:
        return self.context.node(self.strategy_name)

    def set_account(self, account: Any) -> None:
        self.account = account
        self._sync_position_details()
        self._inject_context()

    def prepare_requirements(self, date: datetime) -> List[Any]:
        self._current_date = date
        self._daily_pipeline_triggered = False
        requirements: List[Any] = []
        for source, component in (("selector", self.selector), ("timer", self.timer), ("risk", self.risk_ctrl)):
            if component is None:
                continue
            for query in component.get_queries(date):
                requirements.append(
                    with_routing(canonical_request(query), source=source)
                )
        if self.timer is not None:
            for query in self.timer.get_history_requirements(date):
                requirements.append(
                    with_routing(canonical_request(query), source="timer_history")
                )
        return requirements

    def receive_data(self, batch: DataBatch, delivery_key: str | None = None) -> None:
        if isinstance(batch, dict) and delivery_key is not None and not isinstance(delivery_key, str):
            sign = batch
            component = {"selector": self.selector, "timer": self.timer, "timer_history": self.timer, "risk": self.risk_ctrl}.get(sign.get("source"))
            if component is not None:
                component.receive_data(sign, delivery_key)
            return
        sign = {**(decode_routing(delivery_key or batch.request_id) or {})}
        source = sign.get("source")
        if not isinstance(source, str):
            return
        component = {"selector": self.selector, "timer": self.timer, "timer_history": self.timer, "risk": self.risk_ctrl}.get(source)
        if component is None:
            return
        component.receive_data(sign, batch)
        if not self._daily_pipeline_triggered and self._is_pipeline_ready():
            self._run_daily_pipeline()

    def continue_pipeline(self, date: datetime) -> List[Any]:
        if self.selector is not None and not self.selector.check_data():
            return [
                with_routing(canonical_request(query), source="selector")
                for query in self.selector.get_queries(date)
            ]
        if not self._daily_pipeline_triggered and self._is_pipeline_ready():
            self._run_daily_pipeline()
        return []

    def _is_pipeline_ready(self) -> bool:
        return all(component is None or component.check_data() for component in (self.selector, self.timer, self.risk_ctrl))

    def _run_daily_pipeline(self) -> None:
        self._daily_pipeline_triggered = True
        if self.risk_ctrl is not None:
            self._apply_risk_decision(self.risk_ctrl.on_pre_market())
        target_pool, _ = self._run_stock_selection()
        if self.timer is not None:
            self.timer.selector_pool = list(target_pool)
            self.timer.position_pool = list(self.current_positions)
            self.timer.stock_pool = list(dict.fromkeys([*target_pool, *self.current_positions]))
            self.timer.on_daily(target_pool, self.current_positions, self._account_info(), filter_codes=self.selector is not None)
        self.context.set("selection_passed_to_timer", True)

    def _account_info(self) -> Dict[str, Any]:
        if self.account is None:
            return {}
        return {"total_equity": self.account.total_equity, "balance": self.account.balance}

    def _apply_risk_decision(self, decision: RiskDecision) -> None:
        if self.risk_ctrl is not None:
            self.risk_ctrl.risk_decision = decision
            self.risk_ctrl.target_position_ratio = decision.target_position_ratio
            self.risk_ctrl.is_trading_allowed = decision.trading_allowed
            self._enforce_risk_request_policy()
            if self._risk_requires_liquidation():
                self._queue_liquidation_requests()

    def _risk_blocks_buys(self) -> bool:
        if self.risk_ctrl is None:
            return False
        decision = self.risk_ctrl.risk_decision
        return (
            decision.force_liquidate
            or not decision.trading_allowed
            or decision.target_position_ratio <= 0
            or not self.risk_ctrl.is_trading_allowed
            or self.risk_ctrl.target_position_ratio <= 0
        )

    def _enforce_risk_request_policy(self) -> None:
        if self._risk_blocks_buys():
            self._pending_orders = [
                request for request in self._pending_orders
                if request.action != "BUY"
            ]

    def _risk_requires_liquidation(self) -> bool:
        if self.risk_ctrl is None:
            return False
        decision = self.risk_ctrl.risk_decision
        return decision.force_liquidate or (
            not decision.trading_allowed and decision.target_position_ratio <= 0
        )

    def _queue_liquidation_requests(self) -> None:
        self._sync_position_details()
        pending_codes = self._active_sell_codes()
        for code, details in self._position_details.items():
            volume = int(details.get("volume", 0))
            if volume <= 0 or code in pending_codes:
                continue
            self._pending_orders.append(ExecutionRequest(
                code=code,
                action="SELL",
                time=self._current_date,
                price=0,
                volume=volume,
                mode=ExecutionMode.MARKET,
                reason="risk_liquidation",
            ))

    def _run_stock_selection(self) -> tuple[List[str], Dict[str, float]]:
        if self.selector is None:
            return [], {}
        result = self.selector.select_stocks(self._current_date)
        if result is None or result.empty:
            return [], {}
        return result["asset"].tolist(), dict(zip(result["asset"], result["weight"]))

    def on_kline_bar(self, bar: KlineBar) -> List[SignalIntent]:
        if self.risk_ctrl is not None:
            self._apply_risk_decision(self.risk_ctrl.on_bar(bar))
        intents = self.timer.kline_bar_input(bar)
        if intents:
            self._materialize_kline_orders(intents, bar)
            self._dispatch_kline_orders(bar)
        return intents

    def _materialize_kline_orders(self, intents: List[SignalIntent], bar: KlineBar) -> None:
        """K线意图 → 订单，写入出站队列。

        因子只发裸信号；仓位由择时器 ``convert_intents`` 计算。成交时机不由策略
        决定（订单不带 ``timing``），由执行端（引擎配置的撮合器）统一套用。
        """
        price = float(bar.close)
        for intent in intents:
            created = self.timer.convert_intents([intent], price)
            self._record_timer_orders(created, price)
            self._pending_orders.extend(created)
        consumed = {id(intent) for intent in intents}
        self.timer.pending_intents = [
            intent for intent in self.timer.pending_intents if id(intent) not in consumed
        ]

    def _dispatch_kline_orders(self, bar: KlineBar) -> None:
        """K线订单交手给执行端：撮合器（回测）或就地执行（无撮合器，如实盘）。

        有撮合器时订单直接提交，由撮合器按自身配置的成交时机（如 ``next_bar_open``）
        在下一根 K 线自主成交；不再经引擎二次转发。风险缩放与数量换算由
        ``_canonicalize_requests`` 统一套用，只发生一次。
        """
        price_dict = {bar.code: float(bar.close)}
        if self.tick_matcher is not None:
            self._submit_pending_to_matcher(price_dict)
        else:
            self._execute_pending_orders(price_dict)

    def on_trade(self, trade: Any) -> List[tuple]:
        return self.on_tick({"code": trade.instrument_id, "time": trade.event_time.strftime("%H:%M:%S"), "price": trade.price, "volume": trade.size, "amount": trade.turnover})

    def on_quote(self, quote: Any) -> List[tuple]:
        return []

    def on_tick(self, tick: Dict[str, Any]) -> List[tuple]:
        active_request_keys = self._active_request_keys()
        active_sell_codes = self._active_sell_codes()
        if self.risk_ctrl is not None:
            self._apply_risk_decision(self.risk_ctrl.on_tick(tick))
        self._last_risk_target_ratio = (
            self.risk_ctrl.target_position_ratio if self.risk_ctrl is not None else 1.0
        )
        requests = self.timer.tick_input(tick)
        if not all(isinstance(request, ExecutionRequest) for request in requests):
            raise TypeError("timer tick_input must return ExecutionRequest values")
        self._record_timer_orders(requests, tick.get("price", 0))
        self._preserve_buy_targets(requests)
        if self.tick_matcher is not None:
            self._queue_tick_requests(requests, active_sell_codes, active_request_keys)
        else:
            self._queue_tick_requests(requests, active_keys=active_request_keys)
        self._maybe_queue_tail_replenishment(tick)
        price_dict = {tick["code"]: tick.get("price", 0)} if tick.get("code") else {}
        if self.tick_matcher is not None:
            self._submit_pending_to_matcher(price_dict)
            return []
        return self._execute_pending_orders(price_dict)

    def _preserve_buy_targets(self, requests: List[ExecutionRequest]) -> None:
        """Keep the timer-originated BUY volume before risk scaling.

        A 1000-share signal scaled to 800 by an 80% risk ratio keeps its
        original 1000 in ``_original_buy_targets``; tail replenishment uses it
        to compute how much to add back when risk recovers.
        """
        for request in requests:
            if request.action == "BUY" and request.volume is not None and request.volume > 0:
                self._original_buy_targets[request.code] = (
                    self._original_buy_targets.get(request.code, 0) + request.volume
                )

    def _record_timer_orders(self, requests: List[ExecutionRequest], price: Any) -> None:
        """择时器发出的订单登记入台账（未乘风控缩放的原始数量）。"""
        for request in requests:
            target = self._timer_target_volume(request, price)
            if target <= 0:
                continue
            self._timer_orders.append(
                _TimerOrder(request, request.code, request.action, int(target))
            )

    def _timer_target_volume(self, request: ExecutionRequest, price: Any) -> int:
        if request.volume is not None and request.volume > 0:
            return int(request.volume)
        if request.sizing_intent is not None and request.sizing_intent > 0 and price:
            if request.action == "BUY":
                return int(self._buy_position_volume(float(price), request.sizing_intent))
            held = int(self._position_details.get(request.code, {}).get("volume", 0))
            return int(self._sell_position_volume(held, min(request.sizing_intent, 1.0)))
        return 0

    def timer_positions(self) -> Dict[str, int]:
        """择时器台账的净原始持仓（``recorded`` + ``filled``；``cancelled`` 不计）。"""
        positions: Dict[str, int] = {}
        for order in self._timer_orders:
            if order.status == "cancelled":
                continue
            signed = order.target_volume if order.action == "BUY" else -order.target_volume
            positions[order.code] = positions.get(order.code, 0) + signed
        return positions

    def settled_timer_positions(self) -> Dict[str, int]:
        """台账中**已成交**的净原始持仓——调仓依据（未成交/当日新成交按 T+1 不计）。"""
        positions: Dict[str, int] = {}
        for order in self._timer_orders:
            if order.status != "filled":
                continue
            signed = order.target_volume if order.action == "BUY" else -order.target_volume
            positions[order.code] = positions.get(order.code, 0) + signed
        return positions

    def rebalance_gap(self, ratio: float) -> Dict[str, int]:
        """风控调仓差额：``应有(已成交择时器目标 × 比例) − 真实持仓``；正=需买入，负=需卖出。

        "应有"以**已成交**的台账户为准（未成交/当日新成交按 T+1 不能动）；
        "当前"一侧用**真实持仓**。
        """
        ratio = max(0.0, float(ratio))
        gap: Dict[str, int] = {}
        for code, target_volume in self.settled_timer_positions().items():
            desired = int(target_volume * ratio) // 100 * 100
            held = int(self.current_positions.get(code, 0))
            delta = desired - held
            if delta:
                gap[code] = delta
        return gap

    def set_price_limits(self, limits: Dict[str, Tuple[float, float]]) -> None:
        """注入当日各标的涨跌停价 ``{code: (up_limit, down_limit)}``（由引擎从日线指标取）。"""
        self._price_limits = {code: (float(up), float(down)) for code, (up, down) in limits.items()}

    def rebalance_to_risk(self, ratio: float) -> None:
        """按风控目标比例调仓：超额→开盘池卖出；不足→收盘池买入。

        订单价记跌停/涨停价（有则用，无则记 0，由引擎按结算价成交）。买入默认
        **不立即**执行（尾盘收盘池统一买）；置 ``rebalance_buy_immediate=True`` 可改为开盘即买。
        """
        for code, delta in self.rebalance_gap(ratio).items():
            up, down = self._price_limits.get(code, (0.0, 0.0))
            if delta < 0:
                volume = min(-delta, int(self.current_positions.get(code, 0)))
                if volume <= 0:
                    continue
                self._open_orders.append(ExecutionRequest(
                    code=code, action="SELL", time=self._current_date,
                    price=float(down), volume=int(volume), reason="risk_rebalance_sell",
                    mode=None if down > 0 else ExecutionMode.MARKET,
                ))
            else:
                pool = self._open_orders if self.rebalance_buy_immediate else self._close_orders
                pool.append(ExecutionRequest(
                    code=code, action="BUY", time=self._current_date,
                    price=float(up), volume=int(delta), reason="risk_rebalance_buy",
                    mode=None if up > 0 else ExecutionMode.MARKET,
                ))

    def drain_risk_rebalance(self, prices: Dict[str, float], open_pool: bool, slippage: Any = None) -> List[tuple]:
        """按当前风控比例重算调仓，取出对应订单池（有价者），返回待执行订单元组。

        开盘侧（``open_pool=True``）取开盘池（开盘/盘中即时卖）；收盘侧取收盘池
        （尾盘买）。无价订单保留在池中待下次结算。执行由调用方（引擎）负责分发。
        """
        if self.risk_ctrl is None:
            return []
        self.rebalance_to_risk(getattr(self.risk_ctrl, "target_position_ratio", 1.0))
        pool = self._open_orders if open_pool else self._close_orders
        if not pool:
            return []
        orders: List[tuple] = []
        kept: List[ExecutionRequest] = []
        for request in pool:
            price = prices.get(request.code)
            if not price or price <= 0 or request.volume is None or request.volume <= 0:
                kept.append(request)
                continue
            price = slippage.apply(float(price), request.action) if slippage is not None else float(price)
            signed = request.volume if request.action == "BUY" else -request.volume
            orders.append((self._current_date, request.time, request.code, price, signed, "n"))
        if open_pool:
            self._open_orders = kept
        else:
            self._close_orders = kept
        return orders

    def _maybe_queue_tail_replenishment(self, tick: Dict[str, Any]) -> None:
        """End-of-day 补仓: top up toward the ORIGINAL timer target scaled by
        the recovered risk ratio, filling only the gap left by risk scaling.

        Original 1000-share target, risk was 80% (bought 800); when risk
        recovers the tail fills the remaining ~200 to reach ``orig * ratio``.
        """
        if self._tail_replenished or str(tick.get("time", "")) <= "14:56:54":
            return
        if self.timer.pending_intents:
            return
        for code, original_volume in self._original_buy_targets.items():
            held = int(self._position_details.get(code, {}).get("volume", 0))
            target = self._buy_lot_round(original_volume * self._last_risk_target_ratio)
            volume = target - held
            if volume <= 0:
                continue
            self._pending_orders.append(ExecutionRequest(
                code=code,
                action="BUY",
                time=tick.get("time"),
                price=float(tick.get("price", 0.0)),
                volume=self._buy_lot_round(volume),
                reason="risk_tail_replenishment",
            ))
        self._tail_replenished = True

    def _size_requests_by_risk(self, requests: List[ExecutionRequest]) -> List[ExecutionRequest]:
        ratio = self.risk_ctrl.target_position_ratio if self.risk_ctrl is not None else 1.0
        force_liquidation = self._risk_requires_liquidation()
        sized: List[ExecutionRequest] = []
        for request in requests:
            if request.action == "BUY" and self._risk_blocks_buys():
                continue
            if request.action == "SELL" and force_liquidation:
                if request.volume is not None and request.volume > 0:
                    sized.append(replace(request, sizing_intent=None))
                elif request.sizing_intent is not None:
                    sized.append(replace(request, sizing_intent=1.0))
                else:
                    sized.append(request)
                continue
            if request.volume is not None:
                volume = self._buy_lot_round(request.volume * ratio) if request.action == "BUY" else int(request.volume * ratio)
                if volume > 0:
                    sized.append(replace(request, volume=volume, sizing_intent=None))
            elif request.sizing_intent is not None:
                sized.append(replace(request, sizing_intent=request.sizing_intent * ratio))
            else:
                sized.append(request)
        return sized

    def _active_requests(self) -> Iterator[ExecutionRequest]:
        """出站队列与在途订单中的全部请求（同一标的可能多处出现）。"""
        yield from self._pending_orders
        for orders in self._inflight_orders.values():
            for order in orders:
                yield order.request

    def _active_sell_codes(self) -> set[str]:
        return {request.code for request in self._active_requests() if request.action == "SELL"}

    def _active_request_keys(self) -> set[tuple[str, str]]:
        return {(request.code, request.action) for request in self._active_requests()}

    def _queue_tick_requests(
        self,
        requests: List[ExecutionRequest],
        active_sell_codes: Optional[set[str]] = None,
        active_keys: Optional[set[tuple[str, str]]] = None,
    ) -> None:
        active_sell_codes = active_sell_codes if active_sell_codes is not None else self._active_sell_codes()
        active_keys = active_keys if active_keys is not None else self._active_request_keys()
        for request in requests:
            key = (request.code, request.action)
            if key in active_keys:
                continue
            if request.action == "BUY" and self._risk_blocks_buys():
                continue
            if request.action == "SELL" and request.code in active_sell_codes:
                continue
            self._pending_orders.append(request)

    def _canonicalize_requests(
        self,
        requests: List[ExecutionRequest],
        price_dict: Optional[Dict[str, float]] = None,
    ) -> List[_CanonicalOrder]:
        canonical: List[_CanonicalOrder] = []
        canonical_indexes: Dict[tuple[str, str], int] = {}
        sell_volumes: Dict[str, int] = {}
        prices = price_dict or {}
        for original in requests:
            eligible = self._size_requests_by_risk([original])
            if not eligible:
                continue
            signal = eligible[0]
            price = signal.price or prices.get(signal.code, 0)
            if price <= 0:
                continue
            volume = signal.volume
            held_volume = int(
                self.current_positions.get(signal.code, 0)
                if self.account is not None
                else self._position_details.get(signal.code, {}).get("volume", 0)
            )
            if volume is None and signal.sizing_intent is not None:
                volume = (
                    self._buy_position_volume(price, signal.sizing_intent)
                    if signal.action == "BUY"
                    else self._sell_position_volume(held_volume, signal.sizing_intent)
                )
            if volume is None or volume <= 0:
                continue
            requested_volume = volume
            if signal.action == "SELL":
                volume = min(
                    volume,
                    max(0, held_volume - sell_volumes.get(signal.code, 0)),
                )
                if volume <= 0:
                    continue
            request = replace(signal, price=price, volume=volume, sizing_intent=None)
            source = _OrderSource(original, volume, requested_volume)
            key = (signal.code, signal.action)
            if key in canonical_indexes:
                index = canonical_indexes[key]
                existing = canonical[index]
                total_volume = (existing.request.volume or 0) + volume
                existing.request = replace(existing.request, volume=total_volume)
                existing.order = (
                    *existing.order[:4],
                    total_volume if signal.action == "BUY" else -total_volume,
                    existing.order[5],
                )
                existing.sources.append(source)
            else:
                amount = volume if signal.action == "BUY" else -volume
                canonical.append(_CanonicalOrder(
                    request,
                    (
                        self._current_date,
                        signal.time,
                        signal.code,
                        price,
                        amount,
                        signal.order_type or "n",
                    ),
                    [source],
                ))
                canonical_indexes[key] = len(canonical) - 1
            if signal.action == "SELL":
                sell_volumes[signal.code] = sell_volumes.get(signal.code, 0) + volume
        return canonical

    def _remove_pending_request(self, request: ExecutionRequest) -> None:
        self._pending_orders = [
            pending for pending in self._pending_orders if pending is not request
        ]

    def _acknowledge_pending_sources(
        self, sources: List[_OrderSource], filled_volume: int
    ) -> None:
        allocations = self._allocate_proportional_volume(
            filled_volume, [source.volume for source in sources]
        )
        for source, filled in zip(sources, allocations):
            if filled <= 0:
                continue
            if filled == source.volume:
                self._remove_pending_request(source.request)
                continue
            for index, pending in enumerate(self._pending_orders):
                if pending is source.request:
                    self._pending_orders[index] = replace(
                        pending,
                        volume=source.volume - filled,
                        sizing_intent=None,
                    )
                    break

    def _execute_pending_orders(self, price_dict: Dict[str, float]) -> List[tuple]:
        canonical = self._canonicalize_requests(self._pending_orders, price_dict)
        orders = [item.order for item in canonical]
        if not orders or self.account is None:
            return orders
        before = dict(getattr(self.account, "positions", {}))
        try:
            self.account.order(orders)
        except Exception:
            return orders
        after = dict(getattr(self.account, "positions", {}))
        self._sync_position_details()
        available_fills: Dict[tuple[str, str], int] = {}
        for item in canonical:
            key = (item.request.code, item.request.action)
            if key in available_fills:
                continue
            delta = after.get(key[0], 0) - before.get(key[0], 0)
            available_fills[key] = max(0, delta if key[1] == "BUY" else -delta)
        for item in canonical:
            key = (item.request.code, item.request.action)
            filled = min(abs(item.order[4]), available_fills.get(key, 0))
            available_fills[key] = available_fills.get(key, 0) - filled
            self._acknowledge_pending_sources(item.sources, filled)
        # 无撮合器直连成交：台账里对应记录回填为 filled
        executed = [source.request for item in canonical for source in item.sources]
        for timer_order in self._timer_orders:
            if timer_order.status == "recorded" and any(timer_order.request is r for r in executed):
                timer_order.status = "filled"
        return orders

    @staticmethod
    def _order_id_from_handle(handle: Any) -> Any:
        if isinstance(handle, Mapping):
            return handle.get("order_id")
        return getattr(handle, "order_id", None)

    def _submit_pending_to_matcher(self, price_dict: Dict[str, float]) -> None:
        inflight_sell_codes = {
            order.request.code
            for orders in self._inflight_orders.values()
            for order in orders
            if order.request.action == "SELL"
        }
        pending = [
            request for request in self._pending_orders
            if not (
                request.action == "SELL"
                and request.code in inflight_sell_codes
            )
        ]
        canonical = self._canonicalize_requests(pending, price_dict)
        if not canonical:
            return
        try:
            result = self.tick_matcher.submit_requests(
                [item.request for item in canonical], self._current_date
            )
        except Exception:
            return
        if isinstance(result, Mapping) or hasattr(result, "order_id"):
            handles = [result]
        else:
            try:
                handles = list(result) if result is not None else []
            except TypeError:
                handles = []
        for item, handle in zip(canonical, handles):
            order_id = self._order_id_from_handle(handle)
            if order_id is None or order_id in self._inflight_orders:
                continue
            inflight: List[_InflightOrder] = []
            for source in item.sources:
                if any(pending is source.request for pending in self._pending_orders):
                    self._remove_pending_request(source.request)
                    inflight.append(_InflightOrder(
                        source.request,
                        source.volume,
                        source.volume,
                        source.requested_volume,
                    ))
                    self._link_timer_order(source.request, order_id)
            if inflight:
                self._inflight_orders[order_id] = inflight

    def _link_timer_order(self, request: ExecutionRequest, order_id: Any) -> None:
        """把台账里对应这笔择时器订单的记录，关联到真实订单号（用于成交/撤单回填）。"""
        for order in self._timer_orders:
            if order.request is request and order.order_id is None:
                order.order_id = order_id

    def inflight_orders(self) -> Dict[Any, List[ExecutionRequest]]:
        """在途（已提交、未到终态）订单快照：``order_id`` → 原始请求列表。"""
        return {
            order_id: [order.request for order in orders]
            for order_id, orders in self._inflight_orders.items()
        }

    def cancel_inflight(self, order_id: Any) -> bool:
        """按执行端句柄撤销单个在途订单；未成交余量经 ``handle_order_event`` 回退出站队列。"""
        if self.tick_matcher is None or order_id not in self._inflight_orders:
            return False
        try:
            return bool(self.tick_matcher.cancel(order_id))
        except Exception:
            return False

    def cancel_inflight_orders(
        self, code: Optional[str] = None, action: Optional[str] = None
    ) -> List[Any]:
        """撤销在途未成交订单，返回被撤销的 ``order_id`` 列表（``code``/``action`` 为空即全部）。

        撤销决定在策略端；撮合器只按 ``order_id`` 执行，余量经 ``handle_order_event``
        回到出站队列，可随后的 tick/bar 重新下单。
        """
        if self.tick_matcher is None:
            return []
        cancelled: List[Any] = []
        for order_id, orders in list(self._inflight_orders.items()):
            if code is not None and not any(order.request.code == code for order in orders):
                continue
            if action is not None and not any(order.request.action == action for order in orders):
                continue
            if self.cancel_inflight(order_id):
                cancelled.append(order_id)
        return cancelled

    @staticmethod
    def _event_volume(event: Mapping[str, Any], *names: str) -> Optional[int]:
        for name in names:
            value = event.get(name)
            if value is None or isinstance(value, bool):
                continue
            try:
                volume = int(value)
            except (TypeError, ValueError):
                continue
            if volume >= 0:
                return volume
        return None

    @staticmethod
    def _allocate_proportional_volume(volume: int, capacities: List[int]) -> List[int]:
        total_capacity = sum(capacities)
        if volume <= 0 or total_capacity <= 0:
            return [0] * len(capacities)
        volume = min(volume, total_capacity)
        allocations = [volume * capacity // total_capacity for capacity in capacities]
        remainder = volume - sum(allocations)
        ranked = sorted(
            range(len(capacities)),
            key=lambda index: (-(volume * capacities[index] % total_capacity), index),
        )
        for index in ranked[:remainder]:
            allocations[index] += 1
        return allocations

    def _return_inflight_remainder(self, order: _InflightOrder) -> None:
        remainder = order.remaining_volume + max(
            0, order.requested_volume - order.submitted_volume
        )
        if remainder <= 0:
            return
        self._pending_orders.append(replace(
            order.request,
            volume=remainder,
            sizing_intent=None,
        ))

    def handle_order_event(self, event: Mapping[str, Any]) -> None:
        order_id = event.get("order_id")
        orders = self._inflight_orders.get(order_id)
        if not orders:
            return
        status = str(event.get("status", "")).upper()
        terminal = {"FILLED", "REJECTED", "CANCELLED", "CANCELED", "EXPIRED"}
        if status not in terminal | {"PARTIALLY_FILLED"}:
            return
        submitted_volume = sum(order.submitted_volume for order in orders)
        filled_volume = self._event_volume(
            event, "filled_volume", "filled", "filled_qty", "filled_quantity"
        )
        if filled_volume is None and status == "FILLED":
            filled_volume = self._event_volume(
                event, "requested_volume", "requested", "requested_qty", "volume"
            )
        if filled_volume is None and status == "FILLED":
            filled_volume = submitted_volume
        reported_fill = min(submitted_volume, filled_volume or 0)
        acknowledged = sum(order.filled_volume for order in orders)
        remaining_fill = max(0, reported_fill - acknowledged)
        allocations = self._allocate_proportional_volume(
            remaining_fill, [order.remaining_volume for order in orders]
        )
        for order, filled in zip(orders, allocations):
            order.remaining_volume -= filled
            order.filled_volume += filled
        if status not in terminal:
            return
        self._inflight_orders.pop(order_id, None)
        for order in orders:
            self._return_inflight_remainder(order)
        self._enforce_risk_request_policy()
        # 台账回填：成交 → filled；未成交撤单/拒绝/过期 → cancelled
        for timer_order in self._timer_orders:
            if timer_order.order_id == order_id and timer_order.status == "recorded":
                timer_order.status = "filled" if status == "FILLED" else "cancelled"

    @staticmethod
    def _buy_lot_round(volume: float) -> int:
        return int(volume / 100 + 0.5) * 100

    def _buy_position_volume(self, price: float, position: float) -> int:
        if self.account is None or price <= 0 or position <= 0:
            return 0
        return min(self._buy_lot_round(self.account.total_equity * position / price), int(self.account.balance / price / 100) * 100)

    @staticmethod
    def _sell_position_volume(current_volume: int, position: float) -> int:
        return min(current_volume, int(current_volume * position / 100 + 0.999999999) * 100) if current_volume > 0 and position > 0 else 0

    def _sync_position_details(self) -> None:
        if self.account is None:
            return
        self._position_details = {code: {"volume": volume, "cost": getattr(self.account, "cost_prices", {}).get(code, 0.0)} for code, volume in getattr(self.account, "positions", {}).items()}

    def _collect_state(self) -> Dict[str, Any]:
        return {
            "current_date": str(self._current_date) if self._current_date is not None else None,
            "original_buy_targets": dict(self._original_buy_targets),
            "tail_replenished": self._tail_replenished,
            "last_risk_target_ratio": self._last_risk_target_ratio,
            "pending_orders": [_request_to_dict(request) for request in self._pending_orders],
        }

    def save_state(self) -> None:
        """Persist this strategy's node state through the shared TradingContext."""
        node = self.context.node(self.strategy_name)
        for key, value in self._collect_state().items():
            node.set(key, value)
        self.context.save()

    def restore_state(self) -> None:
        """Load this strategy's node state from the shared TradingContext."""
        node = self.context.node(self.strategy_name)
        state = node.to_dict()
        if not state:
            return
        self._original_buy_targets = dict(state.get("original_buy_targets", {}) or {})
        self._tail_replenished = bool(state.get("tail_replenished", False))
        self._last_risk_target_ratio = float(state.get("last_risk_target_ratio", 1.0) or 1.0)
        self._pending_orders = [
            _request_from_dict(data) for data in (state.get("pending_orders", []) or [])
        ]

    def daily_execute(self, price_dict: Dict[str, float]) -> List[tuple]:
        self._enforce_risk_request_policy()
        if self._risk_requires_liquidation():
            self._queue_liquidation_requests()
        return self._execute_pending_orders(price_dict)

    def on_day_end(self) -> None:
        self.timer.assert_no_unconsumed_intents()
        self.save_state()


__all__ = ["BaseStrategy", "StateStorage"]
