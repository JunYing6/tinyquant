"""规范的、由流驱动的实时交易引擎。"""

from __future__ import annotations

import logging
import queue
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping

from engines.core.account import Account
from engines.core.pipeline import UnifiedDataPipeline
from engines.core.trading_adapter import TradingContractAdapter
from tools.data import (
    CalendarRequest,
    DataGapError,
    DataGapEvent,
    DataGateway,
    DataSourceStateEvent,
    LiveClock,
    QuoteTick,
    Session,
    StreamEvent,
    StreamRequest,
    Subscription,
    TradeTick,
)
from tools.trade.providers import TradeExecutor
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream
from trading_nodes_base.methods import TradingContext

logger = logging.getLogger(__name__)
_RESUME_STATES = frozenset({"healthy", "ok", "resumed", "active"})
_CONTROL_EVENTS = (DataGapEvent, DataSourceStateEvent)


class RealTimeTradeEngine:
    """根据规范的实时行情事件驱动策略或策略流。"""

    def __init__(
        self,
        entity: BaseStrategy | BaseStream,
        data_gateway: DataGateway,
        trade_executor: TradeExecutor,
        initial_capital: float = 1_000_000,
        market: str = "CN",
        control_callback: Callable[[StreamEvent], None] | None = None,
        simulate: bool = False,
        state_storage: Any = None,
        sim_step_sleep: float = 0.5,
        sim_step_advance: timedelta = timedelta(seconds=1),
    ) -> None:
        if data_gateway is None:
            raise ValueError("data_gateway is required")
        if trade_executor is None:
            raise ValueError("trade_executor is required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.entity = entity
        self.data_gateway = data_gateway
        self.trade_executor = trade_executor
        self.initial_capital = float(initial_capital)
        self.market = market
        self.control_callback = control_callback
        self.is_stream = isinstance(entity, BaseStream)
        self.simulate = bool(simulate)
        self._state_storage = state_storage
        self.sim_step_sleep = float(sim_step_sleep)
        self.sim_step_advance = sim_step_advance
        self._clock_thread: threading.Thread | None = None
        self._clock_stop = threading.Event()
        # 共享上下文由引擎统一持有并分发到策略/流及各节点；save/restore 走同一 storage。
        self.context = TradingContext(self._state_storage)
        self.account = Account(self.initial_capital)
        self._distribute_context(entity)
        entity.set_account(self.account)
        self._adapter = TradingContractAdapter(entity)
        self._pipeline = UnifiedDataPipeline(entity, data_gateway, self._adapter)
        if self.is_stream:
            entity.set_live_order_executor(self._enqueue_orders)
        self._running = False
        self._started = False
        self._paused = False
        self._subscribed = False
        self._session: Session | None = None
        self._clock: LiveClock | None = None
        self._gap_error: DataGapError | None = None
        self._subscriptions: dict[str, Subscription] = {}
        self._current_date: date | None = None
        self._last_prices: dict[str, float] = {}
        self._signal_queue: queue.Queue[tuple[Any, ...] | None] = queue.Queue()
        self._signal_thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._active_pool: set[str] | None = None

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._started = True
        try:
            if not self.simulate:
                self.trade_executor.connect()
                self._sync_account()
            else:
                self._restore_sim_state()
            self._session = self._resolve_session()
            self._clock = LiveClock(self._session)
            self._current_date = self._session.trading_date
            self._pipeline.clock = self._clock
            self._start_clock_thread()
            self._run_daily_pipeline(self._session.trading_date)
            self._subscribe_streams()
            self._adapter.set_risk_bar_sink(self._on_risk_bar)
            self._subscribed = True
            self._signal_thread = threading.Thread(target=self._signal_consumer, daemon=True)
            self._signal_thread.start()
        except Exception:
            logger.exception("real-time engine start failed")
            self.stop()
            raise

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._running = False
            self._subscribed = False
            self._paused = False
        self._stop_clock_thread()
        for dataset, subscription in tuple(self._subscriptions.items()):
            try:
                subscription.cancel()
            except Exception:
                logger.exception("subscription cancel failed", extra={"dataset": dataset})
        self._signal_queue.put(None)
        thread = self._signal_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
        if not self.simulate:
            try:
                self.trade_executor.disconnect()
            except Exception:
                logger.exception("trade executor disconnect failed")
        try:
            close = getattr(self.data_gateway, "close", None)
            if close is not None:
                close()
        except Exception:
            logger.exception("data gateway close failed")
        self._save_sim_state()  # 停机落盘：模拟持仓账户 + 策略状态跨日保全
        with self._lock:
            self._started = False

    def drain_poll(self, request: StreamRequest) -> None:
        for event in self.data_gateway.poll(request):
            self._route_event(event)

    def _subscribe_streams(self) -> None:
        codes = tuple(sorted(self._stock_pool()))
        if not codes:
            raise ValueError("no instruments available in the stock pool to subscribe")
        correlation_id = uuid.uuid4().hex
        for dataset in ("market.trade", "market.quote"):
            request = StreamRequest(dataset=dataset, instruments=codes, correlation_id=correlation_id)
            self._subscriptions[dataset] = self.data_gateway.subscribe(
                request, self._on_stream_event, control_sink=self._on_control_event
            )
        self._active_pool = set(codes)
        for strategy in self._strategies():
            strategy.timer.set_pool_change_callback(self._on_pool_change)

    def _on_pool_change(self, pool: list[str]) -> None:
        """因子日内 drop 所需代码后：收窄活动池，引擎跳过已不需要的标的。"""
        with self._lock:
            self._active_pool = set(pool)

    def _on_stream_event(self, event: TradeTick | QuoteTick) -> None:
        with self._lock:
            if not self._running or not self._subscribed or self._paused:
                return
            if self._clock is None or event.event_time is None:
                return
            if self._active_pool is not None and event.instrument_id not in self._active_pool:
                return  # 因子已 drop：该标的后续事件不再处理
            try:
                self._maybe_roll_day(event.trading_date)
                if not self._in_phase(event):
                    return
                self._clock.update(event.event_time)
                before = dict(self.account.positions)
                self._update_price(event)
                self._adapter.feed_market_event(event)
                if not self.is_stream:
                    self._queue_position_delta(before, event.event_time)
            except Exception:
                logger.exception(
                    "market event handling failed",
                    extra={"instrument": event.instrument_id, "event_type": type(event).__name__},
                )

    def _on_control_event(self, event: StreamEvent) -> None:
        action = self._gap_action()
        try:
            if action == "raise":
                message = getattr(event, "reason", None) or str(event)
                self._gap_error = DataGapError(message, dataset=getattr(event, "dataset", None))
                logger.error("data gap requested engine stop", extra={"dataset": getattr(event, "dataset", None)})
                self.stop()
            elif action == "pause":
                state = getattr(event, "state", "").lower()
                with self._lock:
                    self._paused = state not in _RESUME_STATES
                logger.info("data source state changed", extra={"state": state, "paused": self._paused})
            else:
                logger.warning("realtime control event", extra={"event": type(event).__name__})
                if self.control_callback is not None:
                    self.control_callback(event)
        except Exception:
            logger.exception("control event handling failed", extra={"event": type(event).__name__})

    def _route_event(self, event: StreamEvent) -> None:
        if isinstance(event, _CONTROL_EVENTS):
            self._on_control_event(event)
        else:
            self._on_stream_event(event)

    def _maybe_roll_day(self, trading_date: date | None) -> None:
        if trading_date is None or self._current_date is None or trading_date == self._current_date:
            return
        prices = dict(self._last_prices)
        if self.is_stream:
            self.entity.daily_settle(prices)
        else:
            self.account.daily_summarize(prices)
            self.entity.on_day_end()
        self._save_sim_state()  # 收盘落盘（模拟关机）
        batch = self.data_gateway.sessions(
            CalendarRequest(market=self.market, start=trading_date, end=trading_date)
        )
        if not batch.records:
            raise RuntimeError(f"no trading session available for {trading_date:%Y%m%d}")
        self._session = batch.records[-1]
        self._clock = LiveClock(self._session)
        self._current_date = trading_date
        self._pipeline.clock = self._clock
        self._run_daily_pipeline(trading_date)

    def _in_phase(self, event: TradeTick | QuoteTick) -> bool:
        if self._session is None:
            return False
        for phase in self._session.phases:
            if phase.start <= event.event_time <= phase.end:
                return phase.accepts_trades if isinstance(event, TradeTick) else phase.accepts_quotes
        return False

    def _update_price(self, event: TradeTick | QuoteTick) -> None:
        code = event.instrument_id
        if not code:
            return
        price: float | None
        if isinstance(event, TradeTick):
            price = event.price
        else:
            price = event.last_price
            if price is None:
                bid = event.bid_levels[0].price if event.bid_levels else None
                ask = event.ask_levels[0].price if event.ask_levels else None
                price = (bid + ask) / 2 if bid is not None and ask is not None else bid or ask
        if isinstance(price, (int, float)) and not isinstance(price, bool) and price > 0:
            self._last_prices[code] = float(price)

    def _on_risk_bar(self, bar: Any) -> None:
        """风控日内 kline bar 完成：按当前价结算开盘池（风控下降即时卖）。"""
        code = getattr(bar, "code", None)
        if code:
            self._last_prices[code] = float(bar.close)
        self._settle_risk_rebalance(dict(self._last_prices))

    def _settle_risk_rebalance(self, prices: dict[str, float]) -> None:
        for strategy in self._strategies():
            if strategy.risk_ctrl is None:
                continue
            orders = strategy.drain_risk_rebalance(prices, open_pool=True)
            if not orders:
                continue
            strategy.account.order(orders)
            if self.is_stream:
                scaled = self.entity.scale_close_orders(strategy.strategy_name, orders)
                if scaled:
                    self._enqueue_orders(scaled)

    def _queue_position_delta(self, before: Mapping[str, int], event_time: datetime) -> None:
        date_key = self._current_date.strftime("%Y%m%d") if self._current_date else None
        time_key = event_time.strftime("%H:%M:%S")
        for code in sorted(set(before) | set(self.account.positions)):
            delta = self.account.positions.get(code, 0) - before.get(code, 0)
            price = self._last_prices.get(code)
            if delta and isinstance(price, (int, float)) and price > 0:
                self._signal_queue.put((date_key, time_key, code, float(price), delta, "n"))

    def _enqueue_orders(self, orders: list[tuple]) -> None:
        for order in orders:
            self._signal_queue.put(tuple(order))

    def _strategies(self) -> list[Any]:
        return list(self.entity.strategies.values()) if self.is_stream else [self.entity]

    def _start_clock_thread(self) -> None:
        """模拟模式：起一个时钟线程按 ``sim_step_sleep`` 睡眠、每步把模拟时钟
        单调推进 ``sim_step_advance``，实现"真实走秒"（可加速/减速）且不阻塞事件消费。"""
        if not self.simulate or self.sim_step_sleep <= 0 or self._clock is None:
            return
        self._clock_stop.clear()

        def _tick() -> None:
            while not self._clock_stop.is_set():
                self._clock_stop.wait(self.sim_step_sleep)
                if self._clock_stop.is_set():
                    break
                clock = self._clock
                if clock is not None:
                    clock.advance_step(self.sim_step_advance)

        self._clock_thread = threading.Thread(target=_tick, daemon=True, name="sim-clock")
        self._clock_thread.start()

    def _stop_clock_thread(self) -> None:
        thread = self._clock_thread
        if thread is None:
            return
        self._clock_stop.set()
        thread.join(timeout=5.0)
        self._clock_thread = None

    def _distribute_context(self, entity: Any) -> None:
        """把引擎持有的共享上下文分发到策略/流；在 ``set_account`` 前调用，
        使组件的 ``set_context`` 在注入时即用共享上下文（命名空间按 strategy_name 隔离）。"""
        if self.is_stream:
            entity.context = self.context
            for strategy in entity.strategies.values():
                strategy.context = self.context
        else:
            entity.context = self.context

    def _signal_consumer(self) -> None:
        while self._running or not self._signal_queue.empty():
            try:
                order = self._signal_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if order is None:
                break
            try:
                self._execute_order(order)
            except Exception:
                logger.exception("order execution failed")
                try:
                    self._sync_account()
                except Exception:
                    logger.exception("account sync after order failure failed")

    def _execute_order(self, order: Mapping[str, Any] | tuple[Any, ...]) -> None:
        if isinstance(order, Mapping):
            code, action, price, volume, order_type = order.get("code"), order.get("action"), order.get("price"), order.get("volume"), order.get("order_type")
        else:
            _, _, code, price, signed_volume, order_type = order[:6]
            action, volume = ("BUY" if signed_volume > 0 else "SELL"), abs(int(signed_volume))
        if not isinstance(code, str) or not isinstance(volume, int) or volume <= 0:
            return
        if self.simulate:
            # 模拟模式：撮合走引擎自管账户，模拟持仓账户由引擎负责跨日保存
            signed = volume if action == "BUY" else -volume
            self.account.order([(self._current_date, "00:00:00", code, float(price), signed, order_type or "n")])
            return
        kwargs = {"price": price, "order_type": order_type} if price else {"order_type": order_type}
        result = self.trade_executor.buy(code, volume, **kwargs) if action == "BUY" else self.trade_executor.sell(code, volume, **kwargs) if action == "SELL" else None
        if isinstance(result, Mapping) and result.get("success") is False:
            raise RuntimeError("trade executor rejected order")
        if result is not None:
            self._sync_account()

    def _sync_account(self) -> None:
        if self.simulate:
            return  # 模拟模式：持仓账户由引擎自己管理（见 _execute_order），不从券商同步
        with self._lock:
            snapshot = self.trade_executor.get_account()
            positions = self.trade_executor.get_positions() or []
            balance = self._snapshot_value(snapshot, "available_cash", "balance")
            total_equity = self._snapshot_value(snapshot, "total_assets", "total_equity")
            if balance is not None:
                self.account._balance = balance
            if total_equity is not None:
                self.account._total_equity = total_equity
            self.account._positions = {}
            self.account._cost_prices = {}
            for position in positions:
                code = position.get("symbol") or position.get("code") if isinstance(position, Mapping) else getattr(position, "symbol", None)
                volume = position.get("volume", 0) if isinstance(position, Mapping) else getattr(position, "volume", 0)
                cost = position.get("cost_price", 0.0) if isinstance(position, Mapping) else getattr(position, "cost_price", 0.0)
                if isinstance(code, str) and isinstance(volume, int) and volume > 0:
                    self.account._positions[code] = volume
                    if isinstance(cost, (int, float)) and cost > 0:
                        self.account._cost_prices[code] = float(cost)

    def _save_sim_state(self) -> None:
        """模拟模式收盘：整份共享上下文落盘（引擎模拟账户 + 各策略/流状态）。

        用于模拟"收盘下线、内存清空"；次日盘前由 :meth:`_restore_sim_state` 导入。
        """
        if not self.simulate or self._state_storage is None:
            return
        node = self.context.node("engine:sim_account")
        node.set("state", {
            "money": self.account._balance,
            "total_equity": self.account._total_equity,
            "positions": dict(self.account._positions),
            "cost_prices": dict(self.account._cost_prices),
        })
        if self.is_stream:
            self.entity.save_state()  # 影子账户 + 真实账户 + 子策略
            for strategy in self._strategies():
                strategy.save_state()
        else:
            for strategy in self._strategies():
                strategy.save_state()
        self.context.save()

    def _restore_sim_state(self) -> None:
        """模拟模式盘前：从共享上下文导入引擎账户 + 各策略/流状态。"""
        if not self.simulate or self._state_storage is None:
            return
        state = self.context.node("engine:sim_account").get("state") or {}
        if state:
            self.account._balance = float(state.get("money", self.account._balance))
            self.account._total_equity = float(state.get("total_equity", self.account._total_equity))
            self.account._positions = dict(state.get("positions", {}))
            self.account._cost_prices = dict(state.get("cost_prices", {}))
        if self.is_stream:
            self.entity.restore_state()
            for strategy in self._strategies():
                strategy.restore_state()
        else:
            for strategy in self._strategies():
                strategy.restore_state()

    def _run_daily_pipeline(self, trading_date: date) -> None:
        value = datetime.combine(trading_date, time.min, tzinfo=timezone.utc)
        self._pipeline.run_daily(value)
        if self.is_stream:
            self.entity.on_daily({})

    def _resolve_session(self) -> Session:
        today = datetime.now(timezone.utc).date()
        batch = self.data_gateway.sessions(CalendarRequest(market=self.market, start=today, end=today))
        if not batch.records:
            raise RuntimeError(f"no trading session available for {today:%Y%m%d}")
        return batch.records[-1]

    def _stock_pool(self) -> set[str]:
        strategies = self.entity.strategies.values() if self.is_stream else (self.entity,)
        codes: set[str] = set()
        for strategy in strategies:
            codes.update(strategy.current_positions)
            codes.update(strategy.timer.stock_pool)
        return codes

    def _gap_action(self) -> str:
        policy = getattr(self.data_gateway, "data_policy", None) or getattr(self.data_gateway, "_policy", None)
        return getattr(policy, "gap_action", "continue")

    @staticmethod
    def _snapshot_value(snapshot: Any, *names: str) -> float | None:
        for name in names:
            value = snapshot.get(name) if isinstance(snapshot, Mapping) else getattr(snapshot, name, None)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
        return None


__all__ = ["RealTimeTradeEngine"]
