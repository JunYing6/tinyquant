from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import pytest

from engines.core.pipeline import DataProviderError
from engines.fast import FastBacktestEngine, _TickFanout
from tools.data import Bar, DataContractError, DataRequest, InMemoryGateway, Session, TradeTick, TradingPhase
from trading_nodes_base.factors import KlineTimingFactor, TickTimingFactor
from trading_nodes_base.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading_nodes_base.methods import BaseStockPicking, BaseTimeSelection
from trading_nodes_base.minds import BaseMind
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream


def _instant(day: str, clock: str = "15:00:00") -> datetime:
    return datetime.strptime(f"{day} {clock}", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _bar(day: str, price: float, code: str = "000001.SZ") -> Bar:
    instant = _instant(day)
    return Bar(schema_version="1", event_id=None, instrument_id=code, asset_type="equity", effective_time=instant, event_time=instant, available_at=instant, trading_date=instant.date(), source="test", quality="valid", metadata={}, frequency="1d", interval_start=instant, interval_end=instant, open=price, high=price, low=price, close=price, volume=0, turnover=0, is_complete=True, price_basis="raw")


def _session(day: str) -> Session:
    phase = TradingPhase(name="regular", start=_instant(day, "09:00:00"), end=_instant(day, "15:00:00"), accepts_trades=True, accepts_quotes=True)
    return Session(market="CN", trading_date=_instant(day).date(), timezone="UTC", phases=(phase,))


def _gateway(bars: list[Bar], events: list[Any] | None = None, days: list[str] | None = None) -> InMemoryGateway:
    days = days or ["20240102", "20240103"]
    return InMemoryGateway(bars=bars, events=events or [], sessions=[_session(day) for day in days])


class PassiveKlineFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("passive-kline")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class PassiveIntentExecutor(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("passive-executor")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []


class BuyingFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self, name: str = "fast") -> None:
        timer = BaseTimeSelection(f"{name}-timer", [PassiveKlineFactor()], [PassiveIntentExecutor()])
        super().__init__(name, timer=timer)
        self._queued_once = False

    def _run_daily_pipeline(self) -> None:
        super()._run_daily_pipeline()
        if not self._queued_once:
            self._pending_orders.append(
                ExecutionRequest("000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET)
            )
            self._queued_once = True


class DirectBuyTickFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("direct-buy")
        self.fired = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[SignalIntent]:
        if self.fired:
            return []
        self.fired = True
        return [SignalIntent(tick["code"], "BUY", tick["time"], "direct-buy", {"volume": 100})]


class TickStrategy(BaseStrategy):
    def __init__(self, name: str = "tick") -> None:
        super().__init__(name, timer=BaseTimeSelection(f"{name}-timer", [], [DirectBuyTickFactor()]))


class SequenceRecordingFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("sequence-recording")
        self.prices: list[float] = []

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[SignalIntent]:
        self.prices.append(tick["price"])
        return []


class SequenceTickStrategy(BaseStrategy):
    def __init__(self) -> None:
        self.factor = SequenceRecordingFactor()
        super().__init__("sequence-tick", timer=BaseTimeSelection("sequence-tick-timer", [], [self.factor]))


class NextBarBuyFactor(KlineTimingFactor):
    emitted_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("next-bar-buy")
        self.emitted = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        if self.emitted:
            return []
        self.emitted = True
        return [SignalIntent(bar.code, "BUY", bar.end_time, "next bar entry", {"volume": 100})]


class NextBarFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        super().__init__("next-bar", timer=BaseTimeSelection("next-bar-timer", [NextBarBuyFactor()], [PassiveIntentExecutor()]))


class SizingBarFactor(KlineTimingFactor):
    emitted_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("sizing-bar-buy")
        self.emitted = False

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        if self.emitted:
            return []
        self.emitted = True
        return [SignalIntent(bar.code, "BUY", bar.end_time, "sizing entry", {"sizing_intent": 1.0})]


class SizingFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        super().__init__("sizing", timer=BaseTimeSelection("sizing-timer", [SizingBarFactor()], [PassiveIntentExecutor()]))


class EqualMind(BaseMind):
    def calculate_weights(self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]) -> dict[str, float]:
        return {name: 1.0 for name in strategies_performance}


BARS = [_bar("20240102", 10.0), _bar("20240103", 11.0)]


def test_backtest_requires_data_gateway() -> None:
    with pytest.raises(ValueError, match="data_gateway"):
        FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240103")


def test_fast_backtest_runs_with_injected_memory_gateway() -> None:
    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240103", initial_capital=100_000, mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    assert [row["trade_date"] for row in engine.equity_curve] == ["20240102", "20240103"]
    assert engine.daily_positions[-1]["positions"] == {"000001.SZ": 100}
    assert engine.get_stats()["final_equity"] == engine.equity_curve[-1]["equity"]


def test_fast_backtest_rejects_incomplete_market_bar_batch() -> None:
    class IncompleteBarGateway(InMemoryGateway):
        def read(self, request: DataRequest) -> Any:
            batch = super().read(request)
            if request.dataset == "market.bar":
                return type(batch)(request_id="bar-request-1", dataset=batch.dataset, schema_version=batch.schema_version, correlation_id=batch.correlation_id, records=batch.records, complete=False, next_cursor="cursor-1", provenance=batch.provenance, quality=batch.quality)
            return batch

    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240102", mode="fast", data_gateway=IncompleteBarGateway(bars=[BARS[0]], sessions=[_session("20240102")]), progress_bar=False)

    with pytest.raises(DataContractError, match="market.bar.*bar-request-1"):
        engine.run()


def test_fast_backtest_applies_slippage_to_pending_daily_orders() -> None:
    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240102", initial_capital=100_000, mode="fast", slippage={"buy": 0.01, "sell": 0.0, "model": "proportional"}, data_gateway=_gateway([BARS[0]], days=["20240102"]), progress_bar=False)

    engine.run()

    assert engine.account.cost_prices["000001.SZ"] == 10.1


def test_fast_backtest_executes_kline_intent_on_next_daily_bar() -> None:
    engine = FastBacktestEngine(NextBarFastStrategy(), "20240102", "20240103", mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    assert engine.account.positions == {"000001.SZ": 100}
    assert engine.account.cost_prices["000001.SZ"] == 11.0


def test_fast_backtest_executes_sizing_intent_kline_intent() -> None:
    engine = FastBacktestEngine(SizingFastStrategy(), "20240102", "20240103", initial_capital=100_000, mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    # sizing_intent=1.0 -> 权益100000 / 信号bar收盘10 = 10000 股，下根bar close(11) 成交
    # 但账户余额 100000 在 11.0 价只能买 9000（余额限制）
    assert engine.account.positions["000001.SZ"] == 9000


def test_tick_backtest_replays_trade_events_through_matching_runtime() -> None:
    ticks = [
        TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=_instant("20240102", "09:31:00"), event_time=_instant("20240102", "09:31:00"), available_at=None, trading_date=_instant("20240102").date(), source="test", quality="valid", metadata={}, event_type="trade", price=10.0, size=100.0, turnover=1_000.0, side="UNKNOWN", sequence=None),
        TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=_instant("20240102", "09:32:00"), event_time=_instant("20240102", "09:32:00"), available_at=None, trading_date=_instant("20240102").date(), source="test", quality="valid", metadata={}, event_type="trade", price=10.0, size=100.0, turnover=1_000.0, side="UNKNOWN", sequence=None),
    ]
    engine = FastBacktestEngine(TickStrategy(), "20240102", "20240102", mode="tick", data_gateway=_gateway([BARS[0]], events=ticks, days=["20240102"]), progress_bar=False)

    engine.run()

    # 信号在第一个 tick 产出，按“下一个 tick 成交”（execution_delay 默认 1）
    assert engine.daily_positions[-1]["positions"] == {"000001.SZ": 100}


def test_tick_backtest_orders_same_time_events_by_sequence() -> None:
    ticks = [
        TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=_instant("20240102", "09:31:00"), event_time=_instant("20240102", "09:31:00"), available_at=None, trading_date=_instant("20240102").date(), source="test", quality="valid", metadata={}, event_type="trade", price=20.0, size=100.0, turnover=2_000.0, side="UNKNOWN", sequence=2),
        TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=_instant("20240102", "09:31:00"), event_time=_instant("20240102", "09:31:00"), available_at=None, trading_date=_instant("20240102").date(), source="test", quality="valid", metadata={}, event_type="trade", price=10.0, size=100.0, turnover=1_000.0, side="UNKNOWN", sequence=1),
    ]
    strategy = SequenceTickStrategy()
    engine = FastBacktestEngine(strategy, "20240102", "20240102", mode="tick", data_gateway=_gateway([BARS[0]], events=ticks, days=["20240102"]), progress_bar=False)

    engine.run()

    assert strategy.factor.prices == [10.0, 20.0]


def test_stream_backtest_uses_shared_real_account_and_mind_weights() -> None:
    stream = BaseStream("stream", [BuyingFastStrategy("stream-child")], EqualMind())
    engine = FastBacktestEngine(stream, "20240102", "20240103", mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    assert len(engine.equity_curve) == 2
    assert stream.real_account is engine.account
    assert stream.mind.current_weights == {"stream-child": 1.0}


def test_stream_backtest_replicates_day_end_market_orders_to_real_account() -> None:
    # BuyingFastStrategy 在盘前挂一笔 price=0 的 MARKET 单（日终单独撮合，按收盘 close 成交）
    stream = BaseStream("day-end", [BuyingFastStrategy("child")], EqualMind())
    engine = FastBacktestEngine(stream, "20240102", "20240103", initial_capital=100_000, mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    assert engine.account.positions == {"000001.SZ": 100}  # 日终单复刻到真实账户
    assert stream.shadow_accounts["child"].positions == {"000001.SZ": 100}


def test_backtest_wraps_gateway_errors_with_dataset_and_date() -> None:
    class FailingGateway(InMemoryGateway):
        def read(self, request: DataRequest) -> Any:
            raise RuntimeError("offline")

    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240102", data_gateway=FailingGateway(bars=[BARS[0]], sessions=[_session("20240102")]), progress_bar=False)

    with pytest.raises(DataProviderError, match="market.bar.*20240102.*offline"):
        engine.run()


def test_stream_engine_collects_member_curve() -> None:
    stream = BaseStream("stream", [BuyingFastStrategy("s1"), BuyingFastStrategy("s2")], EqualMind())
    engine = FastBacktestEngine(stream, "20240102", "20240103", mode="fast", data_gateway=_gateway(BARS), progress_bar=False)

    engine.run()

    assert len(engine.member_curve) == 2
    assert set(engine.member_curve[0]["members"].keys()) == {"s1", "s2"}
    assert set(engine.member_curve[0]["weights"].keys()) == {"s1", "s2"}
    assert engine.member_curve[-1]["members"]["s1"]["equity"] > 0


class _BothPicking(BaseStockPicking):
    def __init__(self, codes: list[str]) -> None:
        import pandas as pd

        self._pd = pd
        self.codes = list(codes)
        super().__init__("both", [], [], [])

    def select_stocks(self, date: object) -> Any:
        return self._pd.DataFrame({"asset": self.codes, "weight": [1.0 / len(self.codes)] * len(self.codes)})


class _TargetedTickFactor(TickTimingFactor):
    def __init__(self, codes: list[str]) -> None:
        super().__init__("targeted-tick")
        self._required = list(codes)
        self.seen: list[str] = []

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        return []

    def required_codes(self, pool=None, positions=None, account_info=None, **extra: Any) -> list[str]:
        return list(self._required)

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[SignalIntent]:
        self.seen.append(tick.get("code"))
        return []


class _NarrowTickStrategy(BaseStrategy):
    def __init__(self) -> None:
        self.factor = _TargetedTickFactor(["000001.SZ"])
        super().__init__(
            "narrow",
            selector=_BothPicking(["000001.SZ", "600000.SH"]),
            timer=BaseTimeSelection("narrow-timer", [], [self.factor]),
        )


class _RecordingGateway(InMemoryGateway):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.reads: list[DataRequest] = []

    def read(self, request: DataRequest) -> Any:
        self.reads.append(request)
        return super().read(request)


def _trade(day: str, code: str, clock: str = "10:00:00", price: float = 10.0) -> TradeTick:
    return TradeTick(schema_version="1", event_id=None, instrument_id=code, asset_type="equity", effective_time=_instant(day, clock), event_time=_instant(day, clock), available_at=None, trading_date=_instant(day).date(), source="test", quality="valid", metadata={}, event_type="trade", price=price, size=100.0, turnover=price * 100, side="UNKNOWN", sequence=None)


def test_tick_replay_reads_only_subscribed_codes() -> None:
    # 因子只声明需要 000001.SZ；股票池含 600000.SH，但无订阅 -> 不拉取该 code 的 tick
    ticks = [_trade("20240102", "000001.SZ"), _trade("20240102", "600000.SH")]
    gateway = _RecordingGateway(
        bars=[_bar("20240102", 10.0, "000001.SZ"), _bar("20240102", 10.0, "600000.SH")],
        events=ticks,
        sessions=[_session("20240102")],
    )
    strategy = _NarrowTickStrategy()
    engine = FastBacktestEngine(strategy, "20240102", "20240102", mode="tick", data_gateway=gateway, progress_bar=False, stream_shard_size=1)

    engine.run()

    tick_reads = [request for request in gateway.reads if request.dataset == "market.trade"]
    assert tick_reads and all(request.instruments == ("000001.SZ",) for request in tick_reads)
    assert set(strategy.factor.seen) == {"000001.SZ"}


class _RecordingBothFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("record-both")
        self.seen: list[tuple[str, str]] = []

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        return []

    def required_codes(self, pool=None, positions=None, account_info=None, **extra: Any) -> list[str]:
        return ["000001.SZ", "600000.SH"]

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[SignalIntent]:
        self.seen.append((tick.get("code"), tick.get("time")))
        return []


class _BothTickStrategy(BaseStrategy):
    def __init__(self) -> None:
        self.factor = _RecordingBothFactor()
        super().__init__(
            "both-tick",
            selector=_BothPicking(["000001.SZ", "600000.SH"]),
            timer=BaseTimeSelection("both-tick-timer", [], [self.factor]),
        )


def test_tick_replay_delivers_subscribed_codes_in_global_time_order() -> None:
    # 两个 code 的 tick 交错；引擎须按全局时间单调投递（源数据按标的分组，不能只靠组内顺序）
    ticks = [
        _trade("20240102", "000001.SZ", "09:31:00"),
        _trade("20240102", "600000.SH", "09:32:00"),
        _trade("20240102", "000001.SZ", "09:33:00"),
    ]
    gateway = _RecordingGateway(
        bars=[_bar("20240102", 10.0, "000001.SZ"), _bar("20240102", 10.0, "600000.SH")],
        events=ticks,
        sessions=[_session("20240102")],
    )
    strategy = _BothTickStrategy()
    engine = FastBacktestEngine(strategy, "20240102", "20240102", mode="tick", data_gateway=gateway, progress_bar=False)

    engine.run()

    assert [code for code, _ in strategy.factor.seen] == ["000001.SZ", "600000.SH", "000001.SZ"]
    assert [clock for _, clock in strategy.factor.seen] == ["09:31:00", "09:32:00", "09:33:00"]


def _fanout_event(t: int, code: str = "A") -> Any:
    return type("E", (), {"event_time": t, "sequence": 1, "instrument_id": code})()


def test_tick_fanout_merges_generations_by_time() -> None:
    fanout = _TickFanout()
    fanout.add(("A",), iter([_fanout_event(1), _fanout_event(3)]))
    fanout.add(("B",), iter([_fanout_event(2), _fanout_event(4)]))
    drained = []
    while fanout:
        drained.append(fanout.pop().event_time)
    assert drained == [1, 2, 3, 4]


def test_tick_fanout_close_drops_a_generation() -> None:
    fanout = _TickFanout()
    gen_a = fanout.add(("A",), iter([_fanout_event(1), _fanout_event(3)]))
    gen_b = fanout.add(("B",), iter([_fanout_event(2), _fanout_event(4)]))
    assert gen_a.head is not None
    fanout.close([gen_b])
    drained = []
    while fanout:
        drained.append(fanout.pop().event_time)
    assert drained == [1, 3]  # B 的路已结束


def test_resolve_shard_size_rules() -> None:
    resolve = FastBacktestEngine._resolve_shard_size
    assert resolve(10) == 20          # <=180 -> 20/片
    assert resolve(180) == 20         # ==180 -> 20/片
    assert resolve(181) == 21         # 180~900 -> 9片
    assert resolve(900) == 100        # 180~900 -> 9片
    assert resolve(360) == 40         # 360/9
    assert resolve(901) == 100        # >900 -> 100/片


def test_resolve_shard_sizes_by_asset() -> None:
    sizes = FastBacktestEngine._resolve_shard_sizes
    assert sizes("index", 200) == [200]                  # 指数不分片
    assert sizes("tradable", 60) == [36, 36]             # 默认3片每片[36,100]，60 -> 2片36
    assert sizes("tradable", 180) == [60, 60, 60]        # 180 -> 3片60
    assert sizes("tradable", 300) == [100, 100, 100]     # 300 -> 3片100
    tradable_100 = sizes("tradable", 100)
    assert all(36 <= s <= 100 for s in tradable_100)     # 每片落在 [36,100]
    assert sizes("equity", 360) == [40] * 9              # 股票沿用9片规则


def test_freq_seconds_and_kline_shards() -> None:
    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240102", mode="fast", data_gateway=_gateway(BARS), progress_bar=False)
    assert engine._freq_seconds("30m") == 1800
    assert engine._freq_seconds("1m") == 60
    assert engine._freq_seconds("15s") == 15
    assert engine._freq_seconds("1h") == 3600

    # >=1min：单流不分片
    shards, _ = engine._kline_shards({f"C{i}" for i in range(1000)}, "30m")
    assert len(shards) == 1 and len(shards[0][0]) == 1000
    shards_1m, _ = engine._kline_shards({f"C{i}" for i in range(1000)}, "1m")
    assert len(shards_1m) == 1

    # <1min：分5片，每片 [120,360]
    hi, _ = engine._kline_shards({f"C{i}" for i in range(900)}, "15s")
    sizes = [len(codes) for codes, _meta in hi.values()]
    assert len(hi) == 5 and all(120 <= s <= 360 for s in sizes)
    hi2, _ = engine._kline_shards({f"C{i}" for i in range(2000)}, "15s")
    assert all(120 <= len(codes) <= 360 for codes, _meta in hi2.values())
