"""Provider-driven real-time trading engine."""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import replace
from datetime import datetime, time, timezone
from typing import Any, Mapping

from engines.core.account import Account
from engines.core.pipeline import UnifiedDataPipeline
from engines.core.trading_adapter import TradingContractAdapter
from tools.data import DataGateway
from trading.market_events import market_event_from_dict
from tools.real_trade.providers import QuoteProvider
from tools.trade.providers import TradeExecutor
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream


logger = logging.getLogger(__name__)


class RealTimeTradeEngine:
    """Drive a strategy with injected quote and broker providers."""

    def __init__(
        self,
        entity: BaseStrategy | BaseStream,
        quote_provider: QuoteProvider | None,
        trade_executor: TradeExecutor | None,
        initial_capital: float = 1_000_000,
        trade_start_time: str = "09:30:00",
        trade_end_time: str = "14:55:00",
        data_gateway: DataGateway | None = None,
    ) -> None:
        if quote_provider is None:
            raise ValueError("quote_provider is required")
        if trade_executor is None:
            raise ValueError("trade_executor is required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.entity = entity
        self.quote_provider: QuoteProvider = quote_provider
        self.trade_executor: TradeExecutor = trade_executor
        self.initial_capital = float(initial_capital)
        self.data_gateway = data_gateway
        self.trade_start_time = self._parse_time(trade_start_time)
        self.trade_end_time = self._parse_time(trade_end_time)
        self.is_stream = isinstance(entity, BaseStream)
        self.account = Account(self.initial_capital)
        self.entity.set_account(self.account)
        self._adapter = TradingContractAdapter(entity)
        self._pipeline = (
            UnifiedDataPipeline(self.entity, data_gateway, self._adapter)
            if data_gateway is not None
            else None
        )
        self._running = False
        self._started = False
        self._current_date: str | None = None
        self._last_prices: dict[str, float] = {}
        self._signal_queue: queue.Queue[tuple[Any, ...] | None] = queue.Queue()
        self._signal_thread: threading.Thread | None = None
        self._lock = threading.RLock()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._started = True
        try:
            self.trade_executor.connect()
            self._sync_account()
            self._run_daily_pipeline()
            codes = sorted(self._stock_pool())
            self.quote_provider.subscribe(codes, self._on_tick)
            self._signal_thread = threading.Thread(target=self._signal_consumer, daemon=True)
            self._signal_thread.start()
            self.quote_provider.start()
        except Exception:
            logger.exception("real-time engine start failed")
            self.stop()
            raise

    def stop(self) -> None:
        if not self._started:
            return
        self._running = False
        self._signal_queue.put(None)
        try:
            self.quote_provider.stop()
        except Exception:
            logger.exception("quote provider stop failed")
        if self._signal_thread is not None and self._signal_thread is not threading.current_thread():
            self._signal_thread.join(timeout=5.0)
        try:
            self.trade_executor.disconnect()
        except Exception:
            logger.exception("trade executor disconnect failed")
        self._started = False

    def subscribe(self, *codes: str) -> None:
        self.quote_provider.subscribe(list(codes), self._on_tick)

    def _on_tick(self, tick: Mapping[str, Any]) -> None:
        if not self._running:
            return
        try:
            tick_time = self._tick_time(tick.get("time"))
            if tick_time is not None and not self.trade_start_time <= tick_time <= self.trade_end_time:
                return
            tick_dict = dict(tick)
            trade_date = self._date_key(tick_dict.get("trade_date") or tick_dict.get("date"))
            if trade_date is not None and trade_date != self._current_date:
                if isinstance(self.entity, BaseStream) and self._last_prices:
                    self.entity.daily_settle(self._last_prices)
                self._current_date = trade_date
            code = tick_dict.get("code")
            price = tick_dict.get("price")
            if isinstance(code, str) and isinstance(price, (int, float)) and price > 0:
                self._last_prices[code] = float(price)
            before_positions = self.account.positions
            self._feed_market_event_from_quote(tick_dict, self._current_date)
            self._queue_position_delta(before_positions, tick_dict)
        except Exception:
            logger.exception("quote callback failed for code=%s time=%s", tick.get("code"), tick.get("time"))

    def _feed_market_event_from_quote(self, tick: Mapping[str, Any], trade_date: str | None) -> None:
        event = market_event_from_dict(tick)
        trading_date = self._date_key(trade_date)
        if trading_date is not None:
            event_time = datetime.combine(
                datetime.strptime(trading_date, "%Y%m%d").date(),
                event.event_time.time(),
                tzinfo=timezone.utc,
            )
            event = replace(
                event,
                effective_time=event_time,
                event_time=event_time,
                trading_date=event_time.date(),
            )
        self._adapter.feed_market_event(event)

    def _queue_position_delta(
        self, before_positions: Mapping[str, int], tick: Mapping[str, Any]
    ) -> None:
        price = tick.get("price")
        if not isinstance(price, (int, float)) or price <= 0:
            return
        for code in sorted(set(before_positions) | set(self.account.positions)):
            delta = self.account.positions.get(code, 0) - before_positions.get(code, 0)
            if delta:
                self._signal_queue.put(
                    (
                        self._current_date,
                        tick.get("time"),
                        code,
                        float(price),
                        delta,
                        "n",
                    )
                )

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

    def _execute_order(self, order: Mapping[str, Any] | tuple) -> None:
        if isinstance(order, Mapping):
            code = order.get("code")
            action = order.get("action")
            price = order.get("price")
            volume = order.get("volume")
            order_type = order.get("order_type")
        else:
            _, _, code, price, signed_volume, order_type = order[:6]
            action = "BUY" if signed_volume > 0 else "SELL"
            volume = abs(int(signed_volume))
        if not isinstance(code, str) or not isinstance(volume, int) or volume <= 0:
            return
        kwargs = {"price": price, "order_type": order_type} if price else {"order_type": order_type}
        if action == "BUY":
            result = self.trade_executor.buy(code, volume, **kwargs)
        elif action == "SELL":
            result = self.trade_executor.sell(code, volume, **kwargs)
        else:
            return
        if isinstance(result, Mapping) and result.get("success") is False:
            raise RuntimeError("trade executor rejected order")
        self._sync_account()

    def _sync_account(self) -> None:
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
            if isinstance(position, Mapping):
                code = position.get("symbol") or position.get("code")
                volume = position.get("volume", 0)
                cost = position.get("cost_price", 0.0)
            else:
                code = getattr(position, "symbol", None)
                volume = getattr(position, "volume", 0)
                cost = getattr(position, "cost_price", 0.0)
            if isinstance(code, str) and isinstance(volume, int) and volume > 0:
                self.account._positions[code] = volume
                if isinstance(cost, (int, float)) and cost > 0:
                    self.account._cost_prices[code] = float(cost)

    def _run_daily_pipeline(self) -> None:
        today = datetime.now()
        if self._pipeline is not None:
            self._pipeline.run_daily(today)
        else:
            requirements = self.entity.prepare_requirements(today)
            if requirements:
                raise RuntimeError(
                    "real-time strategies with daily data requirements need a dedicated market data provider"
                )
            self.entity.continue_pipeline(today)
        if isinstance(self.entity, BaseStream):
            self.entity.on_daily({})

    def _stock_pool(self) -> set[str]:
        codes: set[str] = set()
        strategies = (
            self.entity.strategies.values()
            if isinstance(self.entity, BaseStream)
            else (self.entity,)
        )
        for strategy in strategies:
            codes.update(strategy.current_positions)
            codes.update(strategy.timer.stock_pool)
        return codes

    @staticmethod
    def _snapshot_value(snapshot: Any, *names: str) -> float | None:
        for name in names:
            value = snapshot.get(name) if isinstance(snapshot, Mapping) else getattr(snapshot, name, None)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
        return None

    @staticmethod
    def _parse_time(value: str) -> time:
        parts = value.split(":")
        if len(parts) not in {2, 3}:
            raise ValueError("time must use HH:MM or HH:MM:SS")
        return time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) == 3 else 0)

    @staticmethod
    def _tick_time(value: Any) -> time | None:
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            try:
                return RealTimeTradeEngine._parse_time(value[-8:])
            except ValueError:
                return None
        return None

    @staticmethod
    def _date_key(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            text = value.replace("-", "")
            return text[:8] if len(text) >= 8 and text[:8].isdigit() else None
        return None


__all__ = ["RealTimeTradeEngine"]
