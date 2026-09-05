from __future__ import annotations

from datetime import date, datetime, timezone
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
from tools.data import Bar, DataBatch, DataProvenance, DataRequest, QualityReport, TradeTick
from trading_nodes_base.factors import KlineTimingFactor, TickTimingFactor
from trading_nodes_base.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy


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

    bar_end = datetime(2024, 1, 3, 15, 0, tzinfo=timezone.utc)
    source = Bar(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=bar_end, event_time=bar_end, available_at=bar_end, trading_date=bar_end.date(), source="test", quality="valid", metadata={}, frequency="1d", interval_start=bar_end, interval_end=bar_end, open=10.0, high=11.0, low=9.0, close=10.5, volume=100, turnover=1050, is_complete=True, price_basis="raw")

    requests = adapter.on_source_bar(source)

    assert requests == [
        ExecutionRequest("000001.SZ", "BUY", bar_end, price=10.0, reason="entry")
    ]


def test_trading_day_context_requires_prior_as_of_date() -> None:
    assert TradingDayContext("20240108", "20240105").as_of_date == "20240105"

    with pytest.raises(ValueError, match="before"):
        TradingDayContext("20240108", "20240108")


def _batch(records: list[Any]) -> DataBatch:
    return DataBatch(request_id="test", dataset="market.bar", schema_version="1", correlation_id=None, records=tuple(records), complete=True, next_cursor=None, provenance=DataProvenance(adapter_name="test", source_revision="1", request_fingerprint="test", read_at=datetime.now(timezone.utc)), quality=QualityReport(status="ok", checked_count=len(records)))


class RecordingGateway:
    def __init__(self) -> None:
        self.calls: list[DataRequest] = []

    def read(self, request: DataRequest) -> DataBatch:
        self.calls.append(request)
        return _batch([{"close": 10.0}])

    def sessions(self, request: object) -> object:
        return None


class PipelineEntity:
    def __init__(self) -> None:
        self.received: list[tuple[object, object]] = []

    def prepare_requirements(self, date: datetime) -> list[DataRequest]:
        return [DataRequest(dataset="market.bar", anchor=date.replace(tzinfo=timezone.utc), delivery_key="daily")]

    def receive_data(self, batch: DataBatch, delivery_key: str | None) -> None:
        self.received.append((batch, delivery_key))

    def continue_pipeline(self, date: datetime) -> list[DataRequest]:
        return []


def test_pipeline_routes_gateway_data_and_delivery_key() -> None:
    entity = PipelineEntity()
    gateway = RecordingGateway()

    UnifiedDataPipeline(entity, gateway).run_daily("20240102")

    assert gateway.calls[0].dataset == "market.bar"
    assert gateway.calls[0].as_of is not None
    assert entity.received[0][1] == "daily"
    assert entity.received[0][0].records == ({"close": 10.0},)


def test_pipeline_wraps_gateway_failure_with_request_context() -> None:
    class FailingGateway:
        def read(self, request: DataRequest) -> object:
            raise RuntimeError("unavailable")

    with pytest.raises(DataProviderError, match="market.bar.*20240102.*unavailable"):
        UnifiedDataPipeline(PipelineEntity(), FailingGateway()).run_daily("20240102")


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

    UnifiedDataPipeline(entity, RecordingGateway()).run_daily("20240102")

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


def _trade_event(clock: str, price: float, size: float = 100.0, turnover: float = 1000.0) -> TradeTick:
    event_time = datetime.strptime(f"20240102 {clock}", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return TradeTick(schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity", effective_time=event_time, event_time=event_time, available_at=None, trading_date=event_time.date(), source="test", quality="valid", metadata={}, event_type="trade", price=price, size=size, turnover=turnover, side="UNKNOWN", sequence=None)


def test_trading_adapter_routes_completed_kline_bars_to_strategy() -> None:
    factor = RecordingKlineFactor()
    timer = BaseTimeSelection("adapter-timer", [factor], [NoopTickFactor()])
    strategy = BaseStrategy("adapter-strategy", timer=timer)
    adapter = TradingContractAdapter(strategy)

    adapter.feed_market_event(_trade_event("09:30:01", 10.0, 100.0, 1000.0))
    adapter.feed_market_event(_trade_event("09:31:01", 11.0, 100.0, 1100.0))

    assert [(bar.open, bar.close) for bar in factor.bars] == [(10.0, 10.0)]


def test_typed_trade_tick_preserves_trading_date() -> None:
    factor = NoopTickFactor()
    strategy = BaseStrategy(
        "typed-adapter-strategy",
        timer=BaseTimeSelection("typed-adapter-timer", [], [factor]),
    )
    adapter = TradingContractAdapter(strategy)
    event_time = datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc)
    event = TradeTick(
        schema_version="1",
        event_id=None,
        instrument_id="000001.SZ",
        asset_type=None,
        effective_time=event_time,
        event_time=event_time,
        available_at=None,
        trading_date=event_time.date(),
        source="test",
        quality="valid",
        metadata={},
        event_type="trade",
        price=10.0,
        size=100.0,
        turnover=1000.0,
        side="UNKNOWN",
        sequence=None,
    )

    adapter.feed_market_event(event)

    assert adapter._current_date == date(2024, 1, 3)


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

    TradingContractAdapter(strategy).feed_market_event(_trade_event("09:30:01", 10.0))

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
