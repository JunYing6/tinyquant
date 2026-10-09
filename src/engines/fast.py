"""基于网关的历史回测引擎，不保留持久状态。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from heapq import heappop, heappush

from tools.data.errors import DataContractError
from typing import Any, Literal, Mapping, cast

from engines.core.account import Account
from engines.core.matching import MatchingEngine
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.trading_adapter import TradingContractAdapter
from engines.core.subscriptions import KIND_TICK, StreamRegistry, SubscriptionTracker, kline_kind
from tools.data import Bar, CalendarRequest, DataGateway, DataRequest, PRIORITY_SCHEDULED, SimulationClock, TradeTick
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream
from trading_nodes_base.types import ExecutionMode, KlineBar, normalize_frequency

_VALID_MODES = {"auto", "fast", "tick", "kline", "daily"}
_VALID_TIMINGS = {"market", "limit", "next_bar_open", "next_bar_close"}

# 内置 A 股日程（可按 ``schedule`` 覆盖）：盘前数据整理、开盘调仓、尾盘调仓。
_DEFAULT_SCHEDULE = {"pre_open": "08:30:00", "open_rebalance": "09:23:30", "close_rebalance": "14:57:00"}


class _MirrorMatcher:
    """子策略撮合器转发器（组合流逐笔复刻）。

    子策略提交的订单：原请求照常进影子撮合器（决定子策略自身成交/持仓），
    同时把 ×子策略权重的副本逐笔交给真实账户撮合器，由它按相同时机独立成交。
    """

    def __init__(self, inner: MatchingEngine, stream: BaseStream, strategy_name: str) -> None:
        self._inner = inner
        self._stream = stream
        self._strategy_name = strategy_name

    def submit_requests(self, requests: list[Any], trade_date: Any, frequency: str | None = None) -> Any:
        result = self._inner.submit_requests(requests, trade_date, frequency)
        self._stream.replicate_orders(self._strategy_name, requests, trade_date, frequency)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _MergeStream:
    """把多路按 ``(event_time, sequence)`` 有序的 tick 迭代器归并成单一事件流。

    支持按订阅变化增代：新增 code 时以 ``start``（当前时钟时刻）另开一路，
    与既有路归并；已处理过的事件不会重复（调用方以事件键去重）。
    """

    def __init__(self) -> None:
        self._iters: list[Any] = []
        self._heads: list[tuple[Any, ...]] = []
        self._counter = 0

    def add(self, iterator: Any) -> None:
        index = len(self._iters)
        self._iters.append(iterator)
        self._advance(index)

    def _advance(self, index: int) -> None:
        event = next(self._iters[index], None)
        if event is None:
            return
        key = (event.event_time, getattr(event, "sequence", 0) or 0, self._counter)
        self._counter += 1
        heappush(self._heads, (key, index, event))

    def __bool__(self) -> bool:
        return bool(self._heads)

    def pop(self) -> Any:
        _key, index, event = heappop(self._heads)
        self._advance(index)
        return event


class FastBacktestEngine:
    def __init__(self, entity: BaseStrategy | BaseStream, start_date: str, end_date: str,
                 initial_capital: float = 1_000_000, mode: str = "auto",
                 slippage: Mapping[str, Any] | SlippageModel | None = None,
                 order_cost: dict[str, float] | None = None, progress_bar: bool = True,
                 progress_callback: Any = None, data_gateway: DataGateway | None = None,
                 execution_timing: str = "next_bar_open", execution_delay: int = 1,
                 schedule: Mapping[str, str] | None = None) -> None:
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
        self._schedule_times = {**_DEFAULT_SCHEDULE, **dict(schedule or {})}
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
        self._registry = StreamRegistry()
        self._adapter = TradingContractAdapter(entity)
        self._adapter.set_risk_bar_sink(self._on_risk_bar)
        self._pipeline = UnifiedDataPipeline(entity, data_gateway, self._adapter)
        for strategy in self._strategies:
            inner = MatchingEngine(
                strategy.account.order, strategy.handle_order_event,
                slippage=self.slippage.apply,
                default_timing=self.execution_timing,
                execution_delay=self.execution_delay,
                tracker=SubscriptionTracker(self._registry, f"matcher:{strategy.strategy_name}"),
            )
            strategy.tick_matcher = _MirrorMatcher(inner, entity, strategy.strategy_name) if self.is_stream else inner
        if self.is_stream:
            entity.set_real_matcher(MatchingEngine(
                self.account.order, None,
                slippage=self.slippage.apply,
                default_timing=self.execution_timing,
                execution_delay=self.execution_delay,
                tracker=SubscriptionTracker(self._registry, "matcher:real"),
            ))
        self.trade_dates: list[str] = []
        self._limits_by_day: dict[str, dict[str, tuple[float, float]]] = {}
        self._limits_loaded = False
        self._intraday_prices: dict[str, float] = {}
        self._pool_dirty = False
        self._stream_base: set[str] = set()
        for strategy in self._strategies:
            strategy.timer.set_pool_change_callback(self._mark_pool_dirty)
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
        date_key = session.trading_date.strftime("%Y%m%d")
        clock = SimulationClock(session, start=self._day_time(session, self._schedule_times["pre_open"]))
        self._pipeline.clock = clock
        self._intraday_prices = {}
        state: dict[str, Any] = {"bars": [], "open": {}, "close": {}, "active": set(), "base": set(), "tick": {}, "ticked": False, "aggregate_freqs": set()}

        def pre_market() -> None:
            """盘前数据整理：跑日线管道、读日线并喂入、注入涨跌停、排入日内 kline。"""
            self._pipeline.run_daily(date_key)
            if isinstance(self.entity, BaseStream):
                self.entity.on_daily({})
            try:
                codes = sorted(set(self._day_feed_codes()) | self._active_codes())
                batch = self.data_gateway.read(DataRequest(dataset="market.bar", instruments=tuple(codes) if codes else None, start=session.open, end=session.close, fields=("open", "high", "low", "close", "volume", "turnover"), frequency="1d", price_basis="raw"))
            except Exception as error:
                raise DataProviderError(f"market.bar read failed on {date_key}: {error}") from error
            if not batch.complete or batch.next_cursor is not None:
                raise DataContractError(f"{batch.dataset} batch must be complete and fully consumed (request_id={batch.request_id})", dataset=batch.dataset, request_id=batch.request_id)
            bars = [record for record in batch.records if isinstance(record, Bar)]
            state["bars"] = bars
            state["open"] = {bar.instrument_id: float(bar.open) for bar in bars if bar.instrument_id is not None}
            state["close"] = {bar.instrument_id: float(bar.close) for bar in bars if bar.instrument_id is not None}
            state["active"] = self._day_feed_codes()
            state["base"] = set(codes)
            self._inject_price_limits(session, sorted(set(state["open"]) | self._active_codes()))
            for bar in bars:
                if state["active"] and bar.instrument_id and bar.instrument_id not in state["active"]:
                    continue  # 因子已 drop：跳过该标的数据（流式提速）
                self._adapter.feed_daily_bar(bar)
            # 日内 kline（择时 ∪ 风控）走同一条路径：历史优先，缺者交 tick 聚合；
            # 有历史的频率置为外部来源（tick 重放不再聚合），随后按各自 end_time 排入时钟。
            # 投喂按订阅过滤：某 code 的该频率无人订阅就不喂（用不到不空转）。
            historic = self._load_intraday_klines(session, codes)
            for frequency in historic:
                self._adapter.mark_external_frequency(frequency)
            self._sync_stream_subscriptions(state["base"])
            for frequency in sorted(historic):
                for bar in historic[frequency]:
                    when = bar.end_time if isinstance(bar.end_time, datetime) else clock.now
                    clock.push_kline(when, bar, lambda b=bar, f=frequency: self._emit_kline(f, b))
            state["aggregate_freqs"] = self._intraday_frequencies() - set(historic)
            state["ticked"] = self._needs_intraday() or bool(state["aggregate_freqs"])

        # 时间轴：盘前整理 → 开盘调仓（开盘价）→（日内 tick/kline）→ 尾盘调仓（末价/收盘价）
        clock.schedule(self._day_time(session, self._schedule_times["pre_open"]), pre_market, PRIORITY_SCHEDULED)
        clock.schedule(self._day_time(session, self._schedule_times["open_rebalance"]),
                       lambda: self._settle_rebalance(state["open"], open_pool=True), PRIORITY_SCHEDULED)
        clock.schedule(self._day_time(session, self._schedule_times["close_rebalance"]),
                       lambda: self._settle_rebalance(state["tick"] or state["close"], open_pool=False), PRIORITY_SCHEDULED)
        clock.run_until(session.open)  # 触发盘前 + 开盘调仓
        if state["ticked"]:
            self._run_tick_replay(session, state["open"], clock, state["tick"], state["aggregate_freqs"])
        clock.flush()  # 收尾：最后一根 kline 与尾盘调仓
        self._execute_pending_daily(state["close"], state["tick"] or None)
        if state["ticked"]:
            self._adapter.flush_day(session.trading_date)
        else:
            for strategy in self._strategies:
                if strategy.tick_matcher is not None:
                    strategy.tick_matcher.expire_day(session.trading_date)
            if self.is_stream and self.entity.real_matcher is not None:
                self.entity.real_matcher.expire_day(session.trading_date)
        self._settle_day(state["close"])
        self._record_day(date_key, state["close"])

    @staticmethod
    def _day_time(session: Any, spec: str | datetime) -> datetime:
        if isinstance(spec, datetime):
            return spec
        parts = spec.split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
        return datetime.combine(session.trading_date, time(hour, minute, second), tzinfo=session.open.tzinfo)


    def _day_feed_codes(self) -> set[str]:
        """各策略 data_required_pool 的并集；空集表示不作限定（喂全部代码）。"""
        pools: list[str] = []
        for strategy in self._strategies:
            pool = getattr(strategy.timer, "data_required_pool", None)
            if pool:
                pools.extend(pool)
        return set(pools)

    def _needs_intraday(self) -> bool:
        """是否需要日内重放（喂 tick 给撮合器/风控）。

        择时器含 tick 因子，**或风控器含 tick 因子**（日线择时 + 盘中风控）时为真。
        """
        for strategy in self._strategies:
            for factor in strategy.timer.tick_factors:
                if getattr(factor, "execution_role", "") != "intent_executor":
                    return True
            if strategy.risk_ctrl is not None and getattr(strategy.risk_ctrl, "risk_tick_factors", None):
                return True
        return False

    def _intraday_frequencies(self) -> set[str]:
        """所有日内 kline 频率（择时 ∪ 风控，<1d，如 30m）。"""
        freqs: set[str] = set()
        for strategy in self._strategies:
            for frequency in strategy.timer.kline_frequencies:
                if not frequency.endswith("d"):
                    freqs.add(frequency)
            risk = strategy.risk_ctrl
            if risk is None:
                continue
            for factor in getattr(risk, "risk_kline_factors", []):
                frequency = normalize_frequency(getattr(type(factor), "frequency", "1d"))
                if not frequency.endswith("d"):
                    freqs.add(frequency)
        return freqs

    def _sync_stream_subscriptions(self, base: set[str]) -> None:
        """由择时器/风控器声明的数据需求（``data_required_pool``）重建流订阅。

        按因子类型拆成 tick / kline:<freq> 两类；因子未声明目标（``_targets_configured``
        为假）时视为需要全部 ``base``。撮合器订单的订阅由撮合器自行维护，不受影响。
        """
        tick_codes: set[str] = set()
        kline: dict[str, set[str]] = {}
        for strategy in self._strategies:
            for factor in strategy.timer.kline_factors:
                frequency = normalize_frequency(type(factor).frequency)
                if not frequency.endswith("d"):
                    kline.setdefault(frequency, set()).update(self._factor_subscription_codes(factor, base))
            for factor in strategy.timer.tick_factors:
                tick_codes.update(self._factor_subscription_codes(factor, base))
            risk = strategy.risk_ctrl
            if risk is None:
                continue
            for factor in getattr(risk, "risk_kline_factors", []):
                frequency = normalize_frequency(getattr(type(factor), "frequency", "1d"))
                if not frequency.endswith("d"):
                    kline.setdefault(frequency, set()).update(self._factor_subscription_codes(factor, base))
            for factor in getattr(risk, "risk_tick_factors", []):
                tick_codes.update(self._factor_subscription_codes(factor, base))
        self._registry.drop_source("factor")
        self._registry.subscribe("factor", KIND_TICK, tick_codes)
        for frequency, codes in kline.items():
            self._registry.subscribe("factor", kline_kind(frequency), codes)

    @staticmethod
    def _factor_subscription_codes(factor: Any, base: set[str]) -> set[str]:
        if getattr(factor, "_targets_configured", False):
            return set(getattr(factor, "_target_codes", set()))
        return set(base)

    def _emit_kline(self, frequency: str, bar: KlineBar) -> None:
        """按订阅投喂单根历史日内 kline：该 code 的该频率无人订阅则跳过。"""
        if self._registry.is_subscribed(kline_kind(frequency), bar.code):
            self._adapter.feed_prebuilt_bars(frequency, [bar])

    def _mark_pool_dirty(self, *_: Any) -> None:
        """因子日内增减声明代码后置脏标记；下一个流步惰性重建订阅集。"""
        self._pool_dirty = True

    def _fetch_set(self, aggregate_freqs: set[str], base: set[str]) -> frozenset[str]:
        """当前需要拉取的 code：tick 订阅 ∪ 需聚合的 kline 频率订阅，∩ base。"""
        if self._pool_dirty:
            self._sync_stream_subscriptions(base)
            self._pool_dirty = False
        codes = set(self._registry.codes(KIND_TICK))
        for frequency in aggregate_freqs:
            codes |= self._registry.codes(kline_kind(frequency))
        return frozenset(codes & base)

    def _tick_iterator(self, dataset: str, codes: list[str], start: datetime, session: Any):
        """按分块历史流拉取某 dataset 的 tick（惰性）；不支持 iterate 则回退 read。"""
        request = DataRequest(dataset=dataset, instruments=tuple(codes), start=start, end=session.close)
        try:
            iterator = self.data_gateway.iterate(request)
        except (AttributeError, NotImplementedError):
            iterator = None
        if iterator is None:
            for record in self.data_gateway.read(request).records:
                yield record
            return
        for chunk in iterator:
            for record in chunk.records:
                yield record

    def _load_intraday_klines(self, session: Any, codes: list[str]) -> dict[str, list[KlineBar]]:
        """读取各日内频率的历史 kline bar（优先数据源）；读不到的频率缺省，交给 tick 聚合。"""
        instruments = tuple(codes) if codes else None
        historic: dict[str, list[KlineBar]] = {}
        for frequency in sorted(self._intraday_frequencies()):
            try:
                batch = self.data_gateway.read(DataRequest(
                    dataset="market.bar", instruments=instruments,
                    start=session.open, end=session.close, frequency=frequency, price_basis="raw",
                ))
            except Exception:
                continue
            records = sorted((r for r in batch.records if isinstance(r, Bar)), key=lambda b: b.interval_end)
            bars = [
                KlineBar(
                    code=bar.instrument_id or "",
                    frequency=frequency,
                    end_time=bar.interval_end,
                    open=float(bar.open), high=float(bar.high), low=float(bar.low), close=float(bar.close),
                    volume=float(bar.volume), amount=float(bar.turnover),
                )
                for bar in records
            ]
            if bars:
                historic[frequency] = bars
        return historic

    def _on_risk_bar(self, bar: KlineBar) -> None:
        """风控日内 kline bar 完成：按最新价结算开盘池（风控下降即时卖）。"""
        if bar.code:
            self._intraday_prices[bar.code] = float(bar.close)
        self._settle_rebalance(self._intraday_prices, open_pool=True)

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

    def _inject_price_limits(self, session: Any, codes: list[str]) -> None:
        """盘前从 ``market.daily_metric`` 取当日涨跌停价，注入各策略。

        整个回测只读一次（按运行区间 + 首批活跃代码），按日缓存，避免逐日全表扫描。
        """
        if not codes:
            return
        if not self._limits_loaded:
            self._limits_loaded = True
            self._load_price_limits(session, codes)
        limits = self._limits_by_day.get(session.trading_date.strftime("%Y%m%d"), {})
        if limits:
            for strategy in self._strategies:
                strategy.set_price_limits(limits)

    def _load_price_limits(self, session: Any, codes: list[str]) -> None:
        tz = session.open.tzinfo
        start = datetime.strptime(self.start_date, "%Y%m%d").replace(tzinfo=tz)
        end = datetime.strptime(self.end_date, "%Y%m%d").replace(tzinfo=tz)
        try:
            batch = self.data_gateway.read(DataRequest(
                dataset="market.daily_metric", instruments=tuple(codes),
                start=start, end=end,
            ))
        except Exception:
            return
        for record in batch.records:
            if not isinstance(record, Mapping):
                continue
            code, up, down, day = record.get("instrument_id"), record.get("up_limit"), record.get("down_limit"), record.get("trading_date")
            if not (code and up and down and day is not None):
                continue
            key = day.strftime("%Y%m%d") if hasattr(day, "strftime") else str(day).replace("-", "")[:8]
            self._limits_by_day.setdefault(key, {})[str(code)] = (float(up), float(down))

    def _settle_rebalance(self, prices: dict[str, float], open_pool: bool) -> None:
        """风控调仓：按目标比例产生订单，并按 open/close 价单独结算对应订单池。

        开盘侧（open_pool=True）按开盘价结算开盘池；收盘侧按收盘价（或末 tick）结算收盘池。
        `rebalance_to_risk` 同时会把卖出排入开盘池（次日结算）、买入排入收盘池（当日结算）。
        """
        for strategy in self._strategies:
            if strategy.risk_ctrl is None:
                continue
            orders = strategy.drain_risk_rebalance(prices, open_pool, self.slippage)
            if orders:
                strategy.account.order(orders)
                if self.is_stream:
                    self.entity.replicate_close_orders(strategy.strategy_name, orders)

    def _execute_pending_daily(self, price_dict: dict[str, float], tick_prices: dict[str, float] | None = None) -> None:
        # 仅剩 price=0 的 MARKET 单（策略自定义日终单 / 风控清仓）单独撮合：
        # 日线模式按收盘 K 线 close 成交；tick 模式按当日最后一根 tick 成交。
        # K 线（next_bar）订单已直接交撮合器，绝不在此按收盘价兜底。
        closes = tick_prices if tick_prices else price_dict
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
                price = request.price if request.price and request.price > 0 else closes.get(request.code)
                if not price or price <= 0 or request.volume is None or request.volume <= 0:
                    strategy._pending_orders.append(request)
                    continue
                price = self.slippage.apply(float(price), request.action)
                signed = request.volume if request.action == "BUY" else -request.volume
                orders.append((strategy._current_date, request.time, request.code, price, signed, "n"))
            if orders:
                strategy.account.order(orders)
                if self.is_stream:
                    self.entity.replicate_close_orders(strategy.strategy_name, orders)

    def _run_tick_replay(self, session: Any, price_dict: dict[str, float], clock: SimulationClock | None = None, last_prices: dict[str, float] | None = None, aggregate_freqs: set[str] | None = None) -> dict[str, float]:
        """流式重放 tick：按当前订阅拉取，随时钟逐事件派发；订阅变化即从当前时刻续读。

        每处理一个事件后惰性对账声明代码集：不变则沿用当前流（不重读），变化则按
        新集从当前时钟时刻另开流并归并；同刻先 tick 后 kline 由时钟保证。
        """
        base = set(price_dict) | self._active_codes()
        aggregate = set(aggregate_freqs or ())
        if last_prices is None:
            last_prices = {}
        if clock is None:
            clock = SimulationClock(session)
        self._pool_dirty = True
        self._stream_base = base
        delivered: set[tuple] = set()

        def open_stream(codes: frozenset[str]) -> _MergeStream:
            stream = _MergeStream()
            for dataset in ("market.trade", "market.quote"):
                stream.add(self._tick_iterator(dataset, sorted(codes), clock.now, session))
            return stream

        current = self._fetch_set(aggregate, base)
        if not current:
            return last_prices
        stream = open_stream(current)
        while stream:
            event = stream.pop()
            if event.event_time is not None and event.event_time < clock.now:
                continue  # 重开流可能回吐早于当前时刻的事件（适配器未按时间下推）——已处理过
            key = (getattr(event, "event_type", None), getattr(event, "instrument_id", None), event.event_time, getattr(event, "sequence", None))
            if key in delivered:
                continue
            delivered.add(key)
            clock.advance(event.event_time)
            if isinstance(event, TradeTick) and event.instrument_id and event.price:
                last_prices[event.instrument_id] = float(event.price)
            self._adapter.feed_market_event(event)
            changed = self._fetch_set(aggregate, base)
            if changed != current:
                current = changed
                if not current:
                    break
                stream = open_stream(current)
        return last_prices

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
