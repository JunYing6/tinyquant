"""Gateway-backed historical backtest engine without persistent state."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime

from tools.data.errors import DataContractError
from typing import Any, Literal, Mapping, cast

from engines.core.account import Account
from engines.core.fast_execution import FastExecutionAdapter
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.tick_matching import TickMatchingEngine
from engines.core.trading_adapter import TradingContractAdapter
from tools.data import Bar, CalendarRequest, DataGateway, DataRequest, ReplayClock, replay_events
from trading.factors.types import ExecutionRequest, KlineBar
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream

_VALID_MODES = {"auto", "fast", "tick", "kline", "daily"}


class FastBacktestEngine:
    def __init__(self, entity: BaseStrategy | BaseStream, start_date: str, end_date: str,
                 initial_capital: float = 1_000_000, mode: str = "auto",
                 slippage: Mapping[str, Any] | SlippageModel | None = None,
                 order_cost: dict[str, float] | None = None, progress_bar: bool = True,
                 progress_callback: Any = None, data_gateway: DataGateway | None = None) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {sorted(_VALID_MODES)}")
        if data_gateway is None:
            raise ValueError("data_gateway is required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.entity = entity
        self.start_date = self._normalize_date(start_date)
        self.end_date = self._normalize_date(end_date)
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        self.initial_capital = float(initial_capital)
        self.data_gateway = data_gateway
        self.progress_bar = progress_bar
        self.progress_callback = progress_callback
        self.is_stream = isinstance(entity, BaseStream)
        self.mode = self._resolve_mode(mode)
        self.slippage = self._init_slippage(slippage)
        self.account = Account(self.initial_capital, order_cost)
        entity.set_account(self.account)
        self._adapter = TradingContractAdapter(entity)
        self._pipeline = UnifiedDataPipeline(entity, data_gateway, self._adapter)
        self._fast_adapters = {strategy.strategy_name: FastExecutionAdapter(getattr(strategy, "fast_buy_timing", "open"), getattr(strategy, "fast_sell_timing", "open")) for strategy in self._strategies}
        if self.mode == "tick":
            for strategy in self._strategies:
                strategy.tick_matcher = TickMatchingEngine(strategy.account.order, strategy.handle_order_event)
        self.trade_dates: list[str] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.daily_positions: list[dict[str, Any]] = []

    @property
    def _strategies(self) -> list[BaseStrategy]:
        return list(self.entity.strategies.values()) if isinstance(self.entity, BaseStream) else [cast(BaseStrategy, self.entity)]

    def run(self) -> None:
        sessions = self.data_gateway.sessions(CalendarRequest(market="CN", start=datetime.strptime(self.start_date, "%Y%m%d").date(), end=datetime.strptime(self.end_date, "%Y%m%d").date()))
        if not sessions.complete or sessions.next_cursor is not None:
            raise DataContractError("calendar batch must be complete and fully consumed")
        records = tuple(sessions.records)
        expected_start = datetime.strptime(self.start_date, "%Y%m%d").date()
        expected_end = datetime.strptime(self.end_date, "%Y%m%d").date()
        dates = [session.trading_date for session in records]
        if dates != sorted(set(dates)) or any(not expected_start <= value <= expected_end for value in dates):
            raise DataContractError("calendar sessions are not unique, ordered, and in range")
        self.trade_dates = [session.trading_date.strftime("%Y%m%d") for session in records]
        for index, session in enumerate(records, 1):
            if self.progress_callback is not None:
                self.progress_callback(f"Trading day {self.trade_dates[index - 1]}", index, len(self.trade_dates))
            self._run_day(session)
        for adapter in self._fast_adapters.values():
            adapter.expire_day()

    def get_stats(self) -> dict[str, float | int]:
        return compute_stats(self.equity_curve, self.initial_capital, self.account.trade_log) if self.equity_curve else {}

    def _run_day(self, session: Any) -> None:
        clock = ReplayClock(session)
        self._pipeline.clock = clock
        date_key = session.trading_date.strftime("%Y%m%d")
        self._pipeline.run_daily(date_key)
        if isinstance(self.entity, BaseStream):
            self.entity.on_daily({})
        try:
            batch = self.data_gateway.read(DataRequest(dataset="market.bar", start=session.open, end=session.close, fields=("open", "high", "low", "close", "volume", "turnover"), frequency="1d", price_basis="raw"))
        except Exception as error:
            raise DataProviderError(f"market.bar read failed on {date_key}: {error}") from error
        if not batch.complete or batch.next_cursor is not None:
            raise DataContractError(f"{batch.dataset} batch must be complete and fully consumed (request_id={batch.request_id})", dataset=batch.dataset, request_id=batch.request_id)
        bars = [record for record in batch.records if isinstance(record, Bar)]
        price_dict = {bar.instrument_id: float(bar.close) for bar in bars if bar.instrument_id is not None}
        if self.mode == "fast":
            self._run_fast_day(price_dict, bars)
        else:
            self._run_tick_day(session, price_dict)
        self._settle_day(price_dict)
        self._record_day(date_key)

    def _run_fast_day(self, price_dict: dict[str, float], bars: list[Bar]) -> None:
        for source in bars:
            for strategy in self._strategies:
                strategy._pending_orders.extend(self._apply_slippage(self._fast_adapters[strategy.strategy_name].on_source_bar(source)))
            self._adapter.feed_daily_bar(source)
            for strategy in self._strategies:
                intents = list(strategy.timer.pending_intents)
                if intents:
                    self._fast_adapters[strategy.strategy_name].queue(intents)
                    strategy.timer.pending_intents.clear()
        self._price_priceless_fast_requests(price_dict)
        self.entity.execute_daily(price_dict) if isinstance(self.entity, BaseStream) else cast(BaseStrategy, self.entity).daily_execute(price_dict)

    def _run_tick_day(self, session: Any, price_dict: dict[str, float]) -> None:
        codes = sorted(set(price_dict) | self._active_codes())
        events = []
        for dataset in ("market.trade", "market.quote"):
            batch = self.data_gateway.read(DataRequest(dataset=dataset, instruments=tuple(codes), start=session.open, end=session.close))
            if not batch.complete or batch.next_cursor is not None:
                raise DataContractError(f"{dataset} batch must be complete and fully consumed")
            events.extend(batch.records)
        events.sort(key=lambda event: (event.event_time or datetime.min.replace(tzinfo=session.open.tzinfo), getattr(event, "sequence", 0) or 0))
        clock = ReplayClock(session)
        replay_events(events, clock, self._adapter.feed_market_event)
        self._adapter.flush_day(session.trading_date)

    def _settle_day(self, price_dict: dict[str, float]) -> None:
        self.entity.daily_settle(price_dict) if isinstance(self.entity, BaseStream) else (self.account.daily_summarize(price_dict), cast(BaseStrategy, self.entity).on_day_end())

    def _record_day(self, date_key: str) -> None:
        self.equity_curve.append({"date": date_key, "trade_date": date_key, "equity": self.account.total_equity, "balance": self.account.balance})
        self.daily_positions.append({"date": date_key, "trade_date": date_key, "positions": self.account.positions})

    def _active_codes(self) -> set[str]:
        codes: set[str] = set()
        for strategy in self._strategies:
            codes.update(strategy.current_positions)
            codes.update(strategy.timer.stock_pool)
        return codes

    def _resolve_mode(self, requested: str) -> str:
        if requested in {"kline", "daily"}:
            requested = "fast"
        eligible = all(strategy.supports_fast_backtest and strategy.timer.fast_eligible and not getattr(strategy.risk_ctrl, "risk_tick_factors", []) for strategy in self._strategies)
        if requested == "auto":
            return "fast" if eligible else "tick"
        if requested == "fast" and not eligible:
            raise ValueError("mode='fast' requires fast-eligible strategy components")
        return requested

    @staticmethod
    def _normalize_date(value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("dates must be YYYYMMDD strings")
        try:
            return datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d")
        except ValueError as error:
            raise ValueError("dates must use YYYYMMDD") from error

    @staticmethod
    def _init_slippage(value: Mapping[str, Any] | SlippageModel | None) -> SlippageModel:
        if isinstance(value, SlippageModel): return value
        if isinstance(value, Mapping): return SlippageModel(buy_slippage=value.get("buy", 0.001), sell_slippage=value.get("sell", 0.001), model=cast(Literal["none", "proportional", "fixed"], value.get("model", "proportional")))
        return SlippageModel(model="none")

    def _apply_slippage(self, requests: list[ExecutionRequest]) -> list[ExecutionRequest]:
        return [replace(request, price=self.slippage.apply(request.price, cast(Literal["BUY", "SELL"], request.action))) if request.price > 0 else request for request in requests]

    def _price_priceless_fast_requests(self, prices: Mapping[str, float]) -> None:
        for strategy in self._strategies:
            strategy._pending_orders = [replace(request, price=self.slippage.apply(prices[request.code], cast(Literal["BUY", "SELL"], request.action))) if request.price <= 0 and prices.get(request.code, 0) > 0 else request for request in strategy._pending_orders]


__all__ = ["FastBacktestEngine"]
