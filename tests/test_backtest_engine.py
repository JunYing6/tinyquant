from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import pytest

from engines.core.pipeline import DataProviderError
from engines.fast import FastBacktestEngine
from tools.data import Bar, DataContractError, DataRequest, InMemoryGateway, Session, TradeTick, TradingPhase
from trading_nodes_base.factors.base import KlineTimingFactor, TickTimingFactor
from trading_nodes_base.factors.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading_nodes_base.methods.base import BaseTimeSelection
from trading_nodes_base.minds.base import BaseMind
from trading_nodes_base.strategies.base import BaseStrategy
from trading_nodes_base.streams.base import BaseStream


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
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class PassiveIntentExecutor(TickTimingFactor):
    execution_role = "intent_executor"
    accepted_intent_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("passive-executor")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
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
        self.sign["fit"] = True
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[ExecutionRequest]:
        if self.fired:
            return []
        self.fired = True
        return [ExecutionRequest(tick["code"], "BUY", tick["time"], price=tick["price"], volume=100, mode=ExecutionMode.MARKET)]


class TickStrategy(BaseStrategy):
    def __init__(self, name: str = "tick") -> None:
        super().__init__(name, timer=BaseTimeSelection(f"{name}-timer", [], [DirectBuyTickFactor()]))


class SequenceRecordingFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("sequence-recording")
        self.prices: list[float] = []

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[ExecutionRequest]:
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
        self.sign["fit"] = True
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


def test_tick_backtest_replays_trade_events_through_matching_runtime() -> None:
    tick = TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=_instant("20240102", "09:31:00"), event_time=_instant("20240102", "09:31:00"), available_at=None, trading_date=_instant("20240102").date(), source="test", quality="valid", metadata={}, event_type="trade", price=10.0, size=100.0, turnover=1_000.0, side="UNKNOWN", sequence=None)
    engine = FastBacktestEngine(TickStrategy(), "20240102", "20240102", mode="tick", data_gateway=_gateway([BARS[0]], events=[tick], days=["20240102"]), progress_bar=False)

    engine.run()

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


def test_backtest_wraps_gateway_errors_with_dataset_and_date() -> None:
    class FailingGateway(InMemoryGateway):
        def read(self, request: DataRequest) -> Any:
            raise RuntimeError("offline")

    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240102", data_gateway=FailingGateway(bars=[BARS[0]], sessions=[_session("20240102")]), progress_bar=False)

    with pytest.raises(DataProviderError, match="market.bar.*20240102.*offline"):
        engine.run()
