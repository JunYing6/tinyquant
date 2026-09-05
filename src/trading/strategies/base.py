"""Single-strategy runtime base with provider-neutral account boundaries."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Dict, List, Optional

from tools.data import DataBatch, DataRequest
from trading.requests import canonical_request, decode_routing, table_records, with_routing
from trading.factors.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading.methods.base import BaseRiskControl, BaseStockPicking, BaseTimeSelection, RiskDecision, StrategyContext


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


class BaseStrategy:
    tick_mode = False
    execution_timing = "close"
    supports_fast_backtest = False
    fast_buy_timing = "open"
    fast_sell_timing = "open"

    def __init__(
        self,
        strategy_name: str,
        selector: Optional[BaseStockPicking] = None,
        timer: Optional[BaseTimeSelection] = None,
        risk_ctrl: Optional[BaseRiskControl] = None,
        trade_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        if timer is None:
            raise ValueError("BaseStrategy requires a timer with Tick execution")
        if not isinstance(timer, BaseTimeSelection):
            raise TypeError("timer must be a BaseTimeSelection")
        if not timer.tick_factors:
            raise ValueError("BaseStrategy timer requires at least one Tick factor")
        self.strategy_name = strategy_name
        self.selector = selector
        self.timer = timer
        self.risk_ctrl = risk_ctrl
        self.account: Any = None
        self.context = StrategyContext()
        self._current_date: Any = None
        self._daily_pipeline_triggered = False
        self._live_trading = False
        self.trade_config = trade_config or {"buy_type": "p", "buy_amount": 0.1, "sell_type": "n"}
        self._position_details: Dict[str, Dict[str, Any]] = {}
        self._pending_orders: List[ExecutionRequest] = []
        self._inflight_orders: Dict[Any, List[_InflightOrder]] = {}
        self._target_weights: Dict[str, float] = {}
        self.order_events: List[dict[str, Any]] = []
        self.tick_matcher: Any = None

    @staticmethod
    def table_records(batch: DataBatch) -> tuple:
        return table_records(batch)

    @property
    def current_positions(self) -> Dict[str, int]:
        if self.account is None:
            return {}
        return dict(getattr(self.account, "positions", {}))

    def _inject_context(self) -> None:
        for component in (self.selector, self.timer, self.risk_ctrl):
            if component is not None:
                component.set_context(self.context)

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
                coerce_data_request(query).with_params(source="selector")
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
        target_pool, weights = self._run_stock_selection()
        self._target_weights = weights
        if self.timer is not None:
            self.timer.selector_pool = list(target_pool)
            self.timer.position_pool = list(self.current_positions)
            self.timer.stock_pool = list(dict.fromkeys([*target_pool, *self.current_positions]))
            self.timer.on_daily(target_pool, self.current_positions, self._account_info())
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
        return self.timer.kline_bar_input(bar)

    def on_tick(self, tick: Dict[str, Any]) -> List[tuple]:
        active_request_keys = self._active_request_keys()
        active_sell_codes = self._active_sell_codes()
        if self.risk_ctrl is not None:
            self._apply_risk_decision(self.risk_ctrl.on_tick(tick))
        requests = self.timer.tick_input(tick)
        if not all(isinstance(request, ExecutionRequest) for request in requests):
            raise TypeError("timer tick_input must return ExecutionRequest values")
        if self.tick_matcher is not None:
            self._queue_tick_requests(requests, active_sell_codes, active_request_keys)
        else:
            self._queue_tick_requests(requests, active_keys=active_request_keys)
        price_dict = {tick["code"]: tick.get("price", 0)} if tick.get("code") else {}
        if self.tick_matcher is not None:
            self._submit_pending_to_matcher(price_dict)
            return []
        return self._execute_pending_orders(price_dict)

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

    def _active_sell_codes(self) -> set[str]:
        return {
            request.code
            for request in self._pending_orders
            if request.action == "SELL"
        } | {
            order.request.code
            for orders in self._inflight_orders.values()
            for order in orders
            if order.request.action == "SELL"
        }

    def _active_request_keys(self) -> set[tuple[str, str]]:
        return {
            (request.code, request.action) for request in self._pending_orders
        } | {
            (inflight.request.code, inflight.request.action)
            for orders in self._inflight_orders.values()
            for inflight in orders
        }

    def _has_active_request(self, request: ExecutionRequest) -> bool:
        if any(
            pending.code == request.code and pending.action == request.action
            for pending in self._pending_orders
        ):
            return True
        return any(
            inflight.request.code == request.code
            and inflight.request.action == request.action
            for orders in self._inflight_orders.values()
            for inflight in orders
        )

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
        return orders

    def _build_orders_from_signals(
        self,
        signals: List[ExecutionRequest],
        price_dict: Optional[Dict[str, float]] = None,
        apply_slippage: bool = True,
    ) -> List[tuple]:
        return [item.order for item in self._canonicalize_requests(signals, price_dict)]

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
            if inflight:
                self._inflight_orders[order_id] = inflight

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

    def daily_execute(self, price_dict: Dict[str, float]) -> List[tuple]:
        self._enforce_risk_request_policy()
        if self._risk_requires_liquidation():
            self._queue_liquidation_requests()
        return self._execute_pending_orders(price_dict)

    def on_day_end(self) -> None:
        self.timer.assert_no_unconsumed_intents()


__all__ = ["BaseStrategy"]
