from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

import pytest

from engines.core.account import Account
from engines.core.fast_execution import FastExecutionAdapter
from engines.core.kline_aggregator import KlineAggregator
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.tick_matching import MatchingOrder, OrderStatus, TickMatchingEngine
from engines.core.trading_adapter import TradingContractAdapter
from engines.core.trading_clock import TradingDayContext
from tools.data_getter.market.schema import DataRequest
from trading.factors.base import KlineTimingFactor, TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


ZERO_COST = {"commission": 0.0, "gh_cost": 0.0, "yh_cost": 0.0}


def test_account_buy_and_daily_settlement_updates_equity() -> None:
    account = Account(money=100_000, oc=ZERO_COST)

    account.order([("20240102", "09:30:00", "000001.SZ", 10.0, 1_000, "n")])
    account.daily_summarize({"000001.SZ": 11.0})

    assert account.positions == {"000001.SZ": 1_000}
    assert account.balance == 90_000
    assert account.total_equity == 101_000


def test_slippage_model_supports_none_and_proportional_prices() -> None:
    model = SlippageModel(buy_slippage=0.01, sell_slippage=0.01)

    assert model.apply(10.0, "BUY") == 10.1
    assert model.apply(10.0, "SELL") == 9.9
    assert SlippageModel(model="none").apply(10.0, "BUY") == 10.0


def test_tick_matcher_does_not_fill_outside_order_conditions() -> None:
    fills: list[tuple] = []
    events: list[dict] = []
    matcher = TickMatchingEngine(fills.extend, events.append)
    matcher.begin_day("20240102")
    matcher.submit_requests(
        [
            ExecutionRequest(
                "000001.SZ",
                "BUY",
                "09:30:00",
                price=10.0,
                volume=100,
                mode=ExecutionMode.LIMIT,
            )
        ],
        "20240102",
    )

    matched = matcher.match(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.1},
        "20240102",
    )

    assert matched == []
    assert fills == []
    assert events[-1]["status"] == OrderStatus.SUBMITTED


def test_tick_matcher_uses_limit_semantics_for_priced_request_without_mode() -> None:
    fills: list[tuple] = []
    matcher = TickMatchingEngine(fills.extend)
    matcher.submit_requests(
        [ExecutionRequest("000001.SZ", "BUY", "09:30:00", price=10.0, volume=100)],
        "20240102",
    )

    matched = matcher.match(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.1},
        "20240102",
    )

    assert matched == []
    assert fills == []


def test_tick_matcher_ignores_account_sizing_order_type_for_matching_mode() -> None:
    fills: list[tuple] = []
    matcher = TickMatchingEngine(fills.extend)
    matcher.submit_requests(
        [
            ExecutionRequest(
                "000001.SZ", "BUY", "09:30:00", price=10.0, volume=100, order_type="n"
            )
        ],
        "20240102",
    )

    matched = matcher.match(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.1},
        "20240102",
    )

    assert matched == []
    assert fills == []


def test_kline_aggregator_emits_completed_bars_in_order() -> None:
    emitted: list[KlineBar] = []
    aggregator = KlineAggregator("1m", emitted.append)

    aggregator.feed_tick(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.0, "volume": 100, "amount": 1_000}
    )
    aggregator.feed_tick(
        {"code": "000001.SZ", "time": "09:31:01", "price": 11.0, "volume": 200, "amount": 2_200}
    )
    aggregator.flush_all()

    assert [(bar.open, bar.close, bar.volume) for bar in emitted] == [
        (10.0, 10.0, 100.0),
        (11.0, 11.0, 200.0),
    ]


def test_fast_execution_adapter_defers_intent_to_next_source_bar() -> None:
    adapter = FastExecutionAdapter()
    adapter.queue([SignalIntent("000001.SZ", "BUY", "20240102", "entry")])

    requests = adapter.on_source_bar(
        KlineBar("000001.SZ", "1d", "20240103", 10.0, 11.0, 9.0, 10.5, 100, 1_050)
    )

    assert requests == [
        ExecutionRequest("000001.SZ", "BUY", "20240103", price=10.0, reason="entry")
    ]


def test_trading_day_context_requires_prior_as_of_date() -> None:
    assert TradingDayContext("20240108", "20240105").as_of_date == "20240105"

    with pytest.raises(ValueError, match="before"):
        TradingDayContext("20240108", "20240108")


class RecordingProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[DataRequest, str]] = []

    def fetch(self, request: DataRequest, date: str) -> list[dict]:
        self.calls.append((request, date))
        return [{"close": 10.0}]


class PipelineEntity:
    def __init__(self) -> None:
        self.received: list[tuple[dict, object]] = []

    def prepare_requirements(self, date: datetime) -> list[DataRequest]:
        return [DataRequest("market", "daily", {"date": date.strftime("%Y%m%d")}, "daily")]

    def receive_data(self, sign: dict, data: object) -> None:
        self.received.append((sign, data))

    def continue_pipeline(self, date: datetime) -> list[DataRequest]:
        return []


def test_pipeline_routes_provider_data_and_request_metadata() -> None:
    entity = PipelineEntity()
    provider = RecordingProvider()

    UnifiedDataPipeline(entity, provider).run_daily("20240102")

    assert provider.calls[0][1] == "20240102"
    assert entity.received == [
        ({"idx": "daily", "date": "20240102"}, [{"close": 10.0}])
    ]


def test_pipeline_wraps_provider_failure_with_request_context() -> None:
    class FailingProvider:
        def fetch(self, request: DataRequest, date: str) -> object:
            raise RuntimeError("unavailable")

    with pytest.raises(DataProviderError, match="market/daily.*20240102.*unavailable"):
        UnifiedDataPipeline(PipelineEntity(), FailingProvider()).run_daily("20240102")


def test_pipeline_continues_queryless_entity_once() -> None:
    class QuerylessEntity:
        def __init__(self) -> None:
            self.continued = 0

        def prepare_requirements(self, date: datetime) -> list[DataRequest]:
            return []

        def receive_data(self, sign: dict, data: object) -> None:
            raise AssertionError("queryless entity must not receive data")

        def continue_pipeline(self, date: datetime) -> list[DataRequest]:
            self.continued += 1
            return []

    entity = QuerylessEntity()

    UnifiedDataPipeline(entity, RecordingProvider()).run_daily("20240102")

    assert entity.continued == 1


def test_tick_matcher_reports_partial_account_fill_and_retries_remainder() -> None:
    calls: list[list[tuple]] = []
    events: list[dict] = []

    def partially_filling_account(orders: list[tuple]) -> list[tuple]:
        calls.append(orders)
        return [(orders[0][0], orders[0][1], orders[0][2], orders[0][3], 50, "n")]

    matcher = TickMatchingEngine(partially_filling_account, events.append)
    matcher.submit_requests(
        [ExecutionRequest("000001.SZ", "BUY", "09:30:00", 10.0, volume=100)],
        "20240102",
    )

    first = matcher.match(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.0},
        "20240102",
    )

    assert first[0].status is OrderStatus.PARTIALLY_FILLED
    assert first[0].filled_volume == 50

    second = matcher.match(
        {"code": "000001.SZ", "time": "09:30:02", "price": 10.0},
        "20240102",
    )

    assert calls[1][0][4] == 50
    assert second[0].status is OrderStatus.FILLED
    assert events[-1]["filled_volume"] == 100


class RecordingKlineFactor(KlineTimingFactor):
    frequency = "1m"

    def __init__(self) -> None:
        super().__init__("recording-kline")
        self.bars: list[KlineBar] = []

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        self.bars.append(bar)
        return []


class NoopTickFactor(TickTimingFactor):
    def __init__(self) -> None:
        super().__init__("noop-tick")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


def test_trading_adapter_routes_completed_kline_bars_to_strategy() -> None:
    factor = RecordingKlineFactor()
    timer = BaseTimeSelection("adapter-timer", [factor], [NoopTickFactor()])
    strategy = BaseStrategy("adapter-strategy", timer=timer)
    adapter = TradingContractAdapter(strategy)

    adapter.feed_tick(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.0, "volume": 100, "amount": 1_000}
    )
    adapter.feed_tick(
        {"code": "000001.SZ", "time": "09:31:01", "price": 11.0, "volume": 100, "amount": 1_100}
    )

    assert [(bar.open, bar.close) for bar in factor.bars] == [(10.0, 10.0)]


def test_trading_adapter_forwards_partial_match_fill_volume() -> None:
    class EventStrategy(BaseStrategy):
        def __init__(self) -> None:
            super().__init__("event-strategy", timer=BaseTimeSelection("event-timer", [], [NoopTickFactor()]))
            self.events: list[Mapping[str, Any]] = []

        def handle_order_event(self, event: Mapping[str, Any]) -> None:
            self.events.append(event)

    class PartialMatcher:
        def match(self, tick: dict, trade_date: object) -> list[MatchingOrder]:
            return [
                MatchingOrder(
                    order_id="partial-1",
                    code="000001.SZ",
                    action="BUY",
                    volume=100,
                    order_type="MARKET",
                    limit_price=None,
                    trade_date="20240102",
                    status=OrderStatus.PARTIALLY_FILLED,
                    filled_volume=50,
                )
            ]

    strategy = EventStrategy()
    strategy.tick_matcher = PartialMatcher()

    TradingContractAdapter(strategy).feed_tick(
        {"code": "000001.SZ", "time": "09:30:01", "price": 10.0},
        "20240102",
    )

    assert strategy.events[0]["filled_volume"] == 50


def test_compute_stats_summarizes_in_memory_equity_curve() -> None:
    stats = compute_stats(
        [
            {"date": "20240102", "equity": 100.0},
            {"date": "20240103", "equity": 110.0},
            {"date": "20240104", "equity": 99.0},
        ],
        initial_capital=100.0,
    )

    assert stats["total_return"] == pytest.approx(-0.01)
    assert stats["max_drawdown"] == pytest.approx(-0.1)
    assert stats["final_equity"] == 99.0
