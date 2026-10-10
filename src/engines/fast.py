"""基于网关的历史回测引擎，不保留持久状态。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from heapq import heappop, heappush

from tools.data.errors import DataContractError
from typing import Any, Iterable, Literal, Mapping, cast

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


class _Gen:
    """一路 tick 流：覆盖一个 code 子集、按 ``(event_time, sequence)`` 有序。"""

    __slots__ = ("codes", "iterator", "head", "closed")

    def __init__(self, codes: frozenset[str], iterator: Any) -> None:
        self.codes = codes
        self.iterator = iterator
        self.head: Any = None
        self.closed = False


class _TickFanout:
    """多路 tick 流的归并器：支持**随时增路/闭路**，同刻先 tick 后 kline 由时钟保证。

    每个 code 只属于一路，故不会重复投递。增路用于新订阅（如越界码单开流），
    闭路用于某路子集全部退订。
    """

    def __init__(self) -> None:
        self._gens: list[_Gen] = []
        self._heap: list[tuple[Any, ...]] = []
        self._counter = 0

    @staticmethod
    def _key(event: Any) -> tuple[Any, Any]:
        return (event.event_time, getattr(event, "sequence", 0) or 0)

    def _pull(self, gen: _Gen) -> None:
        if gen.closed or gen.head is not None:
            return
        gen.head = next(gen.iterator, None)

    def add(self, codes: Iterable[str], iterator: Any) -> _Gen:
        gen = _Gen(frozenset(codes), iterator)
        self._gens.append(gen)
        self._pull(gen)
        self._rebuild()
        return gen

    def close(self, gens: Iterable[_Gen]) -> None:
        for gen in gens:
            gen.closed = True
            gen.head = None
            closer = getattr(gen.iterator, "close", None)
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass
        self._rebuild()

    def _rebuild(self) -> None:
        self._counter = 0
        self._heap = []
        for index, gen in enumerate(self._gens):
            if gen.head is not None and not gen.closed:
                heappush(self._heap, (self._key(gen.head), self._counter, index))
                self._counter += 1

    def __bool__(self) -> bool:
        return any(gen.head is not None and not gen.closed for gen in self._gens)

    def peek(self) -> Any:
        """返回时间轴上最早的一笔事件（不弹出）；无事件返回 ``None``。"""
        if not self._heap:
            return None
        return self._gens[self._heap[0][2]].head

    def pop(self) -> Any:
        if not self._heap:
            raise StopIteration
        _key, _counter, index = heappop(self._heap)
        gen = self._gens[index]
        event = gen.head
        gen.head = None
        self._pull(gen)
        if gen.head is not None and not gen.closed:
            heappush(self._heap, (self._key(gen.head), self._counter, index))
            self._counter += 1
        return event


class FastBacktestEngine:
    def __init__(self, entity: BaseStrategy | BaseStream, start_date: str, end_date: str,
                 initial_capital: float = 1_000_000, mode: str = "auto",
                 slippage: Mapping[str, Any] | SlippageModel | None = None,
                 order_cost: dict[str, float] | None = None, progress_bar: bool = True,
                 progress_callback: Any = None, data_gateway: DataGateway | None = None,
                 execution_timing: str = "next_bar_open", execution_delay: int = 1,
                 schedule: Mapping[str, str] | None = None, stream_shard_size: int | None = None) -> None:
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
        if stream_shard_size is not None and (not isinstance(stream_shard_size, int) or isinstance(stream_shard_size, bool) or stream_shard_size < 1):
            raise ValueError("stream_shard_size must be a positive integer or None")
        self.stream_shard_size = stream_shard_size
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
        # 时钟自主推进：tick 只是挂进来的"事件源"，纯 kline 也会按自身时刻逐根走
        self._run_intraday_clock(clock, state)
        clock.flush()  # 保险收尾
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

    @staticmethod
    def _resolve_shard_size(pool_size: int) -> int:
        """按池大小自动定分片大小：

        - 池 <= 180：20/片
        - 池 180~900：固定 9 片（每片 ceil(池/9)，落在 [20,100]）
        - 池 > 900：100/片
        """
        if pool_size > 900:
            return 100
        if pool_size < 180:
            return 20
        return -(-pool_size // 9)

    def _mark_pool_dirty(self, *_: Any) -> None:
        """因子日内增减声明代码后置脏标记；下一个流步惰性重建订阅集。"""
        self._pool_dirty = True

    def _current_subscriptions(self, aggregate_freqs: set[str], base: set[str]) -> set[str]:
        """当前需要 tick 的 code：tick 订阅 ∪ 需聚合的 kline 频率订阅。

        因子声明变化时惰性重建订阅集。**不**与 base 求交——越界码（池外）也要单开流。
        """
        if self._pool_dirty:
            self._sync_stream_subscriptions(base)
            self._pool_dirty = False
        codes = set(self._registry.codes(KIND_TICK))
        for frequency in aggregate_freqs:
            codes |= self._registry.codes(kline_kind(frequency))
        return codes

    def _tick_iterator(self, dataset: str, codes: list[str], start: datetime, session: Any):
        """按分块历史流拉取某 dataset 的 tick（惰性）；不支持 iterate 则回退 read。

        ``try/finally`` 保证生成器被关闭（分片全退订）时主动关掉内层流，释放数据源连接。
        """
        request = DataRequest(dataset=dataset, instruments=tuple(codes), start=start, end=session.close)
        try:
            iterator = self.data_gateway.iterate(request)
        except (AttributeError, NotImplementedError):
            iterator = None
        try:
            if iterator is None:
                for record in self.data_gateway.read(request).records:
                    yield record
                return
            for chunk in iterator:
                for record in chunk.records:
                    yield record
        finally:
            closer = getattr(iterator, "close", None)
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass

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

    def _run_intraday_clock(self, clock: SimulationClock, state: dict[str, Any]) -> None:
        """时钟**自主**推进：tick 只是挂进来的一路事件源，kline/定时任务由时钟自己到点触发。

        主循环取"最早的一笔 tick"与"最早到期的定时任务"，把时钟推进到两者**更早**的那个时刻：
        到点先派发该时刻的全部 tick（同刻先 tick 后 kline），再触发该时刻的定时任务。纯 kline
        （无 tick）时，时钟就逐任务推进——不再依赖 tick 把时钟往前顶。
        """
        base = set(state["open"]) | self._active_codes()
        aggregate = set(state["aggregate_freqs"])
        last_prices = state["tick"]
        self._pool_dirty = True
        self._stream_base = base
        datasets = ("market.trade", "market.quote")
        ordered = sorted(base)
        shard_size = self.stream_shard_size if self.stream_shard_size is not None else self._resolve_shard_size(len(base))
        shards: dict[int, tuple[str, ...]] = {}
        code_shard: dict[str, int] = {}
        for start in range(0, len(ordered), shard_size):
            sid = len(shards)
            chunk = tuple(ordered[start:start + shard_size])
            shards[sid] = chunk
            for code in chunk:
                code_shard[code] = sid

        fanout = _TickFanout()
        shard_gens: dict[int, list[_Gen]] = {}
        percode_gens: dict[str, list[_Gen]] = {}
        delivered: set[tuple] = set()

        def open_shard(sid: int) -> None:
            if sid in shard_gens:
                return
            codes = shards[sid]
            shard_gens[sid] = [fanout.add(codes, self._tick_iterator(dataset, list(codes), clock.now, clock.session)) for dataset in datasets]

        def close_shard(sid: int) -> None:
            gens = shard_gens.pop(sid, None)
            if gens:
                fanout.close(gens)

        def open_percode(code: str) -> None:
            if code in percode_gens:
                return
            percode_gens[code] = [fanout.add((code,), self._tick_iterator(dataset, [code], clock.now, clock.session)) for dataset in datasets]

        def close_percode(code: str) -> None:
            gens = percode_gens.pop(code, None)
            if gens:
                fanout.close(gens)

        def reconcile(subs: set[str]) -> None:
            for sid, codes in shards.items():
                if sid in shard_gens:
                    if not (set(codes) & subs):
                        close_shard(sid)
                elif set(codes) & subs:
                    open_shard(sid)
            out_of_pool = {code for code in subs if code not in code_shard}
            for code in list(percode_gens):
                if code not in out_of_pool:
                    close_percode(code)
            for code in out_of_pool:
                if code not in percode_gens:
                    open_percode(code)

        def current_subs() -> set[str]:
            return self._current_subscriptions(aggregate, base)

        def deliver_tick(event: Any) -> None:
            nonlocal subs
            code = getattr(event, "instrument_id", None)
            if code not in subs:
                return
            key = (getattr(event, "event_type", None), code, event.event_time, getattr(event, "sequence", None))
            if key in delivered:
                return
            delivered.add(key)
            if isinstance(event, TradeTick) and code and event.price:
                last_prices[code] = float(event.price)
            self._adapter.feed_market_event(event)
            new_subs = current_subs()
            if new_subs != subs:
                subs = new_subs
                reconcile(subs)

        subs = current_subs()
        reconcile(subs)
        while True:
            tick = fanout.peek()
            task = clock.peek_task()
            if tick is None and task is None:
                break
            if tick is not None and tick.event_time < clock.now:
                fanout.pop()  # 新开流可能回吐早于当前时刻的事件——已处理过
                continue
            if task is not None and (tick is None or tick.event_time > task[0]):
                at = task[0]
                if at < clock.now:
                    clock.fire_next()
                    continue
                clock.advance(at)  # 触发严格早于 at 的任务
                while fanout.peek() is not None and fanout.peek().event_time == at:
                    deliver_tick(fanout.pop())
                while clock.peek_task() is not None and clock.peek_task()[0] == at:
                    clock.fire_next()
                new_subs = current_subs()  # 任务派发可能产生新的 tick 订阅
                if new_subs != subs:
                    subs = new_subs
                    reconcile(subs)
            else:
                at = tick.event_time
                clock.advance(at)  # 触发严格早于 at 的任务
                while fanout.peek() is not None and fanout.peek().event_time == at:
                    deliver_tick(fanout.pop())

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
