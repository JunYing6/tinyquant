"""基于网关的历史回测引擎，不保留持久状态。"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from tools.data.errors import DataContractError
from typing import Any, Literal, Mapping, cast

from engines.core.account import Account
from engines.core.matching import MatchingEngine
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.trading_adapter import TradingContractAdapter
from tools.data import Bar, CalendarRequest, DataGateway, DataRequest, ReplayClock, replay_events
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream
from trading_nodes_base.types import ExecutionMode

_VALID_MODES = {"auto", "fast", "tick", "kline", "daily"}
_VALID_TIMINGS = {"market", "limit", "next_bar_open", "next_bar_close"}


class _MirrorMatcher:
    """子策略撮合器转发器（组合流逐笔复刻）。

    子策略提交的订单：原请求照常进影子撮合器（决定子策略自身成交/持仓），
    同时把 ×子策略权重的副本逐笔交给真实账户撮合器，由它按相同时机独立成交。
    """

    def __init__(self, inner: MatchingEngine, stream: BaseStream, strategy_name: str) -> None:
        self._inner = inner
        self._stream = stream
        self._strategy_name = strategy_name

    def submit_requests(self, requests: list[Any], trade_date: Any) -> Any:
        result = self._inner.submit_requests(requests, trade_date)
        self._stream.replicate_orders(self._strategy_name, requests, trade_date)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class FastBacktestEngine:
    def __init__(self, entity: BaseStrategy | BaseStream, start_date: str, end_date: str,
                 initial_capital: float = 1_000_000, mode: str = "auto",
                 slippage: Mapping[str, Any] | SlippageModel | None = None,
                 order_cost: dict[str, float] | None = None, progress_bar: bool = True,
                 progress_callback: Any = None, data_gateway: DataGateway | None = None,
                 execution_timing: str = "next_bar_open", execution_delay: int = 1) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {sorted(_VALID_MODES)}")
        if execution_timing not in _VALID_TIMINGS:
            raise ValueError(f"execution_timing must be one of {sorted(_VALID_TIMINGS)}")
        if not isinstance(execution_delay, int) or isinstance(execution_delay, bool) or execution_delay < 1:
            raise ValueError("execution_delay must be an integer >= 1")
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
        self.execution_timing = execution_timing
        self.execution_delay = execution_delay
        self.progress_bar = progress_bar
        self.progress_callback = progress_callback
        self.is_stream = isinstance(entity, BaseStream)
        self.mode = self._resolve_mode(mode)
        self.slippage = self._init_slippage(slippage)
        self.account = Account(self.initial_capital, order_cost)
        entity.set_account(self.account)
        self._adapter = TradingContractAdapter(entity)
        self._pipeline = UnifiedDataPipeline(entity, data_gateway, self._adapter)
        for strategy in self._strategies:
            inner = MatchingEngine(
                strategy.account.order, strategy.handle_order_event,
                slippage=self.slippage.apply,
                default_timing=self.execution_timing,
                execution_delay=self.execution_delay,
            )
            strategy.tick_matcher = _MirrorMatcher(inner, entity, strategy.strategy_name) if self.is_stream else inner
        if self.is_stream:
            entity.set_real_matcher(MatchingEngine(
                self.account.order, None,
                slippage=self.slippage.apply,
                default_timing=self.execution_timing,
                execution_delay=self.execution_delay,
            ))
        self.trade_dates: list[str] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.daily_positions: list[dict[str, Any]] = []
        self.member_curve: list[dict[str, Any]] = []

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
            # 读数范围 = 因子声明池(含池外基准/相关股) ∪ 持仓/股票池，缺一则回退全量
            codes = sorted(set(self._day_feed_codes()) | self._active_codes())
            batch = self.data_gateway.read(DataRequest(dataset="market.bar", instruments=tuple(codes) if codes else None, start=session.open, end=session.close, fields=("open", "high", "low", "close", "volume", "turnover"), frequency="1d", price_basis="raw"))
        except Exception as error:
            raise DataProviderError(f"market.bar read failed on {date_key}: {error}") from error
        if not batch.complete or batch.next_cursor is not None:
            raise DataContractError(f"{batch.dataset} batch must be complete and fully consumed (request_id={batch.request_id})", dataset=batch.dataset, request_id=batch.request_id)
        bars = [record for record in batch.records if isinstance(record, Bar)]
        price_dict = {bar.instrument_id: float(bar.close) for bar in bars if bar.instrument_id is not None}
        active = self._day_feed_codes()
        # 统一事件流：先喂每日 bar（撮合 + K线信号），再视策略需要补 tick
        for bar in bars:
            if active and bar.instrument_id and bar.instrument_id not in active:
                continue  # 因子已 drop：跳过该标的数据（流式提速）
            self._adapter.feed_daily_bar(bar)
        ticked = False
        if any(getattr(factor, "execution_role", "") != "intent_executor" for strategy in self._strategies for factor in strategy.timer.tick_factors):
            self._run_tick_replay(session, price_dict, active)
            ticked = True
        # 直接订单（策略 _pending_orders，price=0 的 MARKET 单）按当日收盘价+滑点成交
        self._execute_pending_daily(price_dict)
        if ticked:
            self._adapter.flush_day(session.trading_date)
        else:
            for strategy in self._strategies:
                if strategy.tick_matcher is not None:
                    strategy.tick_matcher.expire_day(session.trading_date)
            if self.is_stream and self.entity.real_matcher is not None:
                self.entity.real_matcher.expire_day(session.trading_date)
        self._settle_day(price_dict)
        self._record_day(date_key, price_dict)

    def _day_feed_codes(self) -> set[str]:
        """各策略 data_required_pool 的并集；空集表示不作限定（喂全部代码）。"""
        pools: list[str] = []
        for strategy in self._strategies:
            pool = getattr(strategy.timer, "data_required_pool", None)
            if pool:
                pools.extend(pool)
        return set(pools)

    @staticmethod
    def _time_slices(start: Any, end: Any, minutes: int = 30) -> list[tuple[Any, Any]]:
        slices: list[tuple[Any, Any]] = []
        cur = start
        delta = timedelta(minutes=minutes)
        while cur < end:
            nxt = min(cur + delta, end)
            slices.append((cur, nxt))
            cur = nxt
        return slices

    def _execute_pending_daily(self, price_dict: dict[str, float]) -> None:
        # 仅剩 price=0 的 MARKET 单（策略自定义日终单 / 风控清仓），按当日收盘成交；
        # K 线（next_bar）订单已直接交撮合器，绝不在此按收盘价兜底。
        for strategy in self._strategies:
            if not strategy._pending_orders:
                continue
            pending = list(strategy._pending_orders)
            strategy._pending_orders = []
            orders: list[tuple] = []
            for request in pending:
                if request.mode is not ExecutionMode.MARKET:
                    strategy._pending_orders.append(request)
                    continue
                price = request.price if request.price and request.price > 0 else price_dict.get(request.code)
                if not price or price <= 0 or request.volume is None or request.volume <= 0:
                    strategy._pending_orders.append(request)
                    continue
                price = self.slippage.apply(float(price), request.action)
                signed = request.volume if request.action == "BUY" else -request.volume
                orders.append((strategy._current_date, request.time, request.code, price, signed, "n"))
            if orders:
                strategy.account.order(orders)

    def _run_tick_replay(self, session: Any, price_dict: dict[str, float], active: set[str] | None = None, slice_minutes: int = 30) -> None:
        base = set(price_dict) | self._active_codes()
        for start, end in self._time_slices(session.open, session.close, slice_minutes):
            # 每片前重算活动池：因子 drop 掉的代码，本片起不再拉取
            pool = set(self._day_feed_codes())
            codes = sorted((pool & base) if pool else base)
            if active:
                codes = [c for c in codes if c in active]
            if not codes:
                continue
            events = []
            for dataset in ("market.trade", "market.quote"):
                batch = self.data_gateway.read(DataRequest(dataset=dataset, instruments=tuple(codes), start=start, end=end))
                if not batch.complete or batch.next_cursor is not None:
                    raise DataContractError(f"{dataset} batch must be complete and fully consumed")
                events.extend(batch.records)
            events.sort(key=lambda event: (event.event_time or datetime.min.replace(tzinfo=session.open.tzinfo), getattr(event, "sequence", 0) or 0))
            clock = ReplayClock(session)
            replay_events(events, clock, self._adapter.feed_market_event)

    def _settle_day(self, price_dict: dict[str, float]) -> None:
        self.entity.daily_settle(price_dict) if isinstance(self.entity, BaseStream) else (self.account.daily_summarize(price_dict), cast(BaseStrategy, self.entity).on_day_end())

    def _record_day(self, date_key: str, price_dict: dict[str, float]) -> None:
        positions = self.account.positions
        self.equity_curve.append({"date": date_key, "trade_date": date_key, "equity": self.account.total_equity, "balance": self.account.balance, "position_value": self.account.total_equity - self.account.balance, "position_count": len(positions)})
        self.daily_positions.append({"date": date_key, "trade_date": date_key, "positions": positions, "close_prices": dict(price_dict)})
        if self.is_stream:
            self.member_curve.append({
                "date": date_key,
                "members": {
                    name: {"equity": shadow.total_equity, "position_value": shadow.total_equity - shadow.balance}
                    for name, shadow in self.entity.shadow_accounts.items()
                },
                "weights": dict(self.entity.mind.current_weights),
            })

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


__all__ = ["FastBacktestEngine"]
