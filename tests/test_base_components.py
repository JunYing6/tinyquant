from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import pandas as pd  # type: ignore[import-untyped]
import pytest

from tools.data import DataRequest as CanonicalDataRequest
from tools.data import DataRequest
from trading_nodes_base.factors.types import (
    ExecutionMode,
    ExecutionRequest,
    KlineBar,
    RiskSignal,
    SignalIntent,
)
from trading_nodes_base.factors.base import (
    BaseFactor,
    BinarySelectionFactor,
    FloatSelectionFactor,
    KlineTimingFactor,
    RiskKlineFactor,
    TickTimingFactor,
)
from trading_nodes_base.methods.base import (
    BaseRiskControl,
    BaseStockPicking,
    BaseTimeSelection,
    RiskDecision,
    StrategyContext,
)
from trading_nodes_base.minds.base import BaseMind
from trading_nodes_base.strategies.base import BaseStrategy
from trading_nodes_base.streams.base import BaseStream


class FakeAccount:
    def __init__(self, money: float) -> None:
        self.balance = money
        self.positions: dict[str, int] = {}
        self.cost_prices: dict[str, float] = {}
        self.short_positions: dict[str, int] = {}
        self.short_cost_prices: dict[str, float] = {}

    @property
    def total_equity(self) -> float:
        return self.balance

    def order(self, orders: list[tuple]) -> None:
        for _, _, code, _, volume, _ in orders:
            if volume > 0:
                self.positions[code] = self.positions.get(code, 0) + volume
            else:
                self.positions[code] = max(0, self.positions.get(code, 0) + volume)


class PartialFillAccount(FakeAccount):
    def order(self, orders: list[tuple]) -> None:
        for _, _, code, _, volume, _ in orders:
            filled = int(abs(volume) / 2)
            if volume > 0:
                self.positions[code] = self.positions.get(code, 0) + filled
            else:
                self.positions[code] = max(0, self.positions.get(code, 0) - filled)


class FailingAccount(FakeAccount):
    def order(self, orders: list[tuple]) -> None:
        raise RuntimeError("broker unavailable")


class FakeMatcher:
    def __init__(self) -> None:
        self.submissions: list[list[ExecutionRequest]] = []

    def submit_requests(
        self, requests: list[ExecutionRequest], date: object
    ) -> list[dict[str, str]]:
        self.submissions.append(list(requests))
        return [{"order_id": f"order-{index}"} for index, _ in enumerate(requests, 1)]


class QueryFactor(BaseFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": date, "fields": ["close"]}}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1.0})


class NoClearFactor(BaseFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": date, "fields": ["close"]}}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": float(len(data_cache))})


class DuplicateRequestFactor(BaseFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return [
            {"scope": "market/daily", "params": {"date": date, "fields": ["close"]}},
            {"scope": "market/daily", "params": {"date": date, "fields": ["volume"]}},
        ]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        self.payloads = list(data_cache.values())
        return pd.Series({"000001.SZ": len(self.payloads)})


class MappingRequestFactor(BaseFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[dict]:
        self._data_clear()
        self.sign["fit"] = True
        return [{
            "domain": "market",
            "kind": "daily",
            "params": {"date": date, "fields": ["close"]},
        }]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1.0})


class RequestFormsFactor(BaseFactor):
    def __init__(self, factor_name: str, request_form: str) -> None:
        super().__init__(factor_name)
        self.request_form = request_form

    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        params = {"date": date, "fields": ["close"]}
        if self.request_form == "data_request":
            return [{"scope": "market/daily", "params": params, "idx": "request"}]
        if self.request_form == "scope":
            return [{"scope": "market/daily", "params": params, "idx": "scope"}]
        if self.request_form == "domain_kind":
            return [{"domain": "market", "kind": "daily", "params": params, "idx": "domain"}]
        return [{
            "type": "market/daily",
            "trade_date": date,
            "period": ["close"],
            "codes": codes or ["000001.SZ"],
            "idx": "legacy",
        }]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1.0})


class BinaryFactor(BinarySelectionFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": date, "fields": ["close"]}}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1, "000002.SZ": 0})


class QuerylessBinaryFactor(BinarySelectionFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1, "000002.SZ": 0})


class UnreadyQuerylessBinaryFactor(BinarySelectionFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list:
        self._data_clear()
        return []

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 1, "000002.SZ": 0})


class FloatFactor(FloatSelectionFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"scope": "market/daily", "params": {"date": date, "fields": ["close"]}}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        return pd.Series({"000001.SZ": 2.0, "000002.SZ": 1.0})


class Picker(BaseStockPicking):
    def select_stocks(self, date: str) -> pd.DataFrame:
        return pd.DataFrame({"asset": ["000001.SZ"], "weight": [1.0]})


class KlineFactor(KlineTimingFactor):
    emitted_actions = frozenset({"BUY"})

    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return [SignalIntent(bar.code, "BUY", bar.end_time, "entry")]


class HistoryClearingKlineFactor(KlineTimingFactor):
    emitted_actions = frozenset({"BUY"})

    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        self.set_targets({"000001.SZ"})
        return [{"scope": "market/daily", "params": {"date": date, "fields": ["close"]}}]


class TickFactor(TickTimingFactor):
    accepted_intent_actions = frozenset({"BUY"})

    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_tick(
        self, tick: dict, intents: Sequence[SignalIntent] = ()
    ) -> list[ExecutionRequest]:
        return [
            ExecutionRequest(
                code=tick["code"],
                action="BUY",
                time=tick["time"],
                price=tick["price"],
            )
        ] if intents else []


class DirectTickFactor(TickTimingFactor):
    accepted_intent_actions = frozenset()
    action = "BUY"
    volume = 100

    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_tick(
        self, tick: dict, intents: Sequence[SignalIntent] = ()
    ) -> list[ExecutionRequest]:
        return [ExecutionRequest(
            code=tick["code"],
            action=self.action,
            time=tick["time"],
            price=tick["price"],
            volume=self.volume,
        )]


class Timer(BaseTimeSelection):
    def __init__(self) -> None:
        super().__init__("timer", [KlineFactor("kline")], [TickFactor("tick")])


class RiskBarFactor(RiskKlineFactor):
    def get_query_lst(self, date: str, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[RiskSignal]:
        return [RiskSignal(True, "event", reason="halt")]


class Risk(BaseRiskControl):
    def __init__(self) -> None:
        super().__init__("risk", risk_kline_factors=[RiskBarFactor("risk")])


class Strategy(BaseStrategy):
    pass


class Mind(BaseMind):
    def calculate_weights(
        self,
        market_data: dict,
        strategies_performance: dict,
    ) -> dict[str, float]:
        return {name: 0.5 for name in strategies_performance}


class Stream(BaseStream):
    pass


def test_factor_lifecycle_normalizes_output_and_component_routes_queries() -> None:
    factor = QueryFactor("query")
    component = Risk()
    component.all_factors = [factor]
    context = StrategyContext()
    component.set_context(context)

    queries = component.get_queries("20240102")

    assert queries[0].get("idy") == 0
    assert factor.context is context
    component.receive_data({"idy": 0, "idx": "001"}, object())
    assert component.check_data()
    assert factor.calculate().index.name == "ts_code"


def test_factor_requires_fit_and_data_and_consumes_one_shot_request_state() -> None:
    factor = QueryFactor("readiness")
    factor.sign["fit"] = True
    with pytest.raises(ValueError, match="Data not ready"):
        factor.calculate()

    factor._data_clear()
    factor.sign["data"] = True
    with pytest.raises(ValueError, match="Data not ready"):
        factor.calculate()

    query = factor.get_query_lst("20240102")[0]
    factor.receive_data({"idx": "001"}, object())
    assert factor.calculate().iloc[0] == 1.0
    with pytest.raises(ValueError, match="Data not ready"):
        factor.calculate()
    factor.receive_data({"idx": "001"}, "stale-data")
    assert factor._received_data == {}


def test_factor_requires_each_same_idx_request_and_keeps_both_payloads() -> None:
    factor = DuplicateRequestFactor("duplicate")
    component = Risk()
    component.all_factors = [factor]

    queries = component.get_queries("20240102")
    first_id = queries[0].get("request_id")
    second_id = queries[1].get("request_id")
    assert first_id != second_id

    component.receive_data({"idy": 0, "idx": "001", "request_id": first_id}, "close-data")
    assert not component.check_data()

    component.receive_data({"idy": 0, "idx": "001", "request_id": second_id}, "volume-data")
    assert component.check_data()
    assert factor.calculate().iloc[0] == 2
    assert factor.payloads == ["close-data", "volume-data"]


def test_stale_request_from_previous_cycle_is_rejected() -> None:
    factor = BinaryFactor("cycle")
    component = Picker("cycle", [factor], [], [])

    first_cycle = component.get_queries("20240102")
    stale_id = first_cycle[0].get("request_id")
    component.reset_state("20240103")
    second_cycle = component.get_queries("20240103")

    assert stale_id != second_cycle[0].get("request_id")
    component.receive_data(
        {"idy": 0, "idx": "001", "request_id": stale_id}, "stale-data"
    )

    assert not component.check_data()
    assert factor._received_data == {}


def test_request_generations_reject_stale_generation_tokens() -> None:
    factor = NoClearFactor("generation")
    component = BaseRiskControl("generation-risk", daily_factors=[factor])

    first = component.get_queries("20240102")[0]
    second = component.get_queries("20240103")[0]
    component.receive_data(
        {
            "idy": 0,
            "request_id": second.get("request_id"),
            "generation": first.get("generation"),
        },
        "stale-data",
    )

    assert first.get("generation") != second.get("generation")
    assert factor._received_data == {}
    assert not component.check_data()


def test_direct_data_clear_advances_generation_for_unlabelled_responses() -> None:
    factor = QueryFactor("direct-generation")

    factor.get_query_lst("20240102")
    first_generation = factor._active_generation
    factor.get_query_lst("20240103")
    second_generation = factor._active_generation

    assert first_generation != second_generation
    factor.receive_data(
        {"idx": "001", "generation": first_generation}, "stale-data"
    )
    assert not factor._is_ready()

    factor.receive_data(
        {"idx": "001", "generation": second_generation}, "fresh-data"
    )
    assert factor._is_ready()
    assert factor.calculate().iloc[0] == 1.0


def test_component_starts_generation_for_factor_without_data_clear() -> None:
    factor = NoClearFactor("no-clear")
    component = BaseRiskControl("no-clear-risk", daily_factors=[factor])

    query = component.get_queries("20240102")[0]
    assert query.get("generation") is not None
    component.receive_data(
        {
            "idy": query.get("idy"),
            "request_id": query.get("request_id"),
            "generation": query.get("generation"),
        },
        "close-data",
    )

    assert component.check_data()
    assert factor.calculate().iloc[0] == 1.0


def test_queryless_component_generation_rejects_unlabelled_stale_data() -> None:
    factor = QuerylessBinaryFactor("queryless-generation")
    component = BaseRiskControl("queryless-risk", daily_factors=[factor])

    assert component.get_queries("20240102") == []
    assert component.check_data()

    component.receive_data({"idy": 0, "idx": "001"}, "stale-data")

    assert factor._received_data == {}


def test_same_idx_legacy_responses_follow_registration_order() -> None:
    factor = DuplicateRequestFactor("legacy")
    component = Risk()
    component.all_factors = [factor]
    component.get_queries("20240102")

    component.receive_data({"idy": 0, "idx": "001"}, "first")
    assert not component.check_data()
    component.receive_data({"idy": 0, "idx": "001"}, "second")

    assert component.check_data()
    assert factor.calculate().iloc[0] == 2
    assert factor.payloads == ["first", "second"]


def test_selector_funnel_completes_after_and_factor_data_arrives() -> None:
    picker = Picker("picker", [BinaryFactor("binary")], [], [])

    queries = picker.get_queries("20240102")
    assert len(queries) == 1
    picker.receive_data({"idy": queries[0].get("idy"), "idx": "001"}, object())

    assert picker.get_queries("20240102") == []
    assert picker.is_done()
    assert picker.select_stocks("20240102").iloc[0]["asset"] == "000001.SZ"


def test_timer_routes_kline_intents_to_typed_tick_requests() -> None:
    timer = Timer()
    timer.on_daily(["000001.SZ"], {})
    bar = KlineBar("000001.SZ", "1d", "20240102", 10, 11, 9, 10.5, 100, 1050)

    intents = timer.kline_bar_input(bar)
    requests = timer.tick_input(
        {"code": "000001.SZ", "time": "09:31:00", "price": 10.6}
    )

    assert isinstance(intents[0], SignalIntent)
    assert isinstance(requests[0], ExecutionRequest)
    assert timer.pending_intents == []


def test_timer_consumes_only_intents_matched_by_emitted_requests() -> None:
    timer = BaseTimeSelection("intent-timer", [], [TickFactor("tick")])
    timer.pending_intents = [
        SignalIntent("000001.SZ", "BUY", "09:30:00", "first"),
        SignalIntent("000001.SZ", "BUY", "09:30:01", "second"),
    ]

    requests = timer.tick_input(
        {"code": "000001.SZ", "time": "09:31:00", "price": 10.0}
    )

    assert len(requests) == 1
    assert [intent.reason for intent in timer.pending_intents] == ["second"]


def test_unconfigured_tick_factor_accepts_target_code_by_default() -> None:
    factor = DirectTickFactor("unconfigured")
    timer = BaseTimeSelection("unconfigured-timer", [], [factor])

    requests = timer.tick_input(
        {"code": "000001.SZ", "time": "09:31:00", "price": 10.6}
    )

    assert len(requests) == 1
    assert requests[0].code == "000001.SZ"


def test_risk_routes_bar_signal_into_a_blocking_decision() -> None:
    risk = Risk()
    risk.on_daily()

    decision = risk.on_bar(
        KlineBar("000001.SZ", "1d", "20240102", 10, 11, 9, 10.5, 100, 1050)
    )

    assert isinstance(decision, RiskDecision)
    assert not decision.trading_allowed


def test_strategy_injects_context_and_account_into_composed_components() -> None:
    picker = Picker("picker", [], [], [])
    timer = Timer()
    risk = Risk()
    strategy = Strategy("strategy", picker, timer, risk)
    account = FakeAccount(1000.0)

    strategy.set_account(account)

    assert strategy.account is account
    assert picker.context is strategy.context
    assert timer.context is strategy.context
    assert risk.context is strategy.context


def test_forced_liquidation_refreshes_position_details_from_current_account() -> None:
    risk = BaseRiskControl("current-account-risk")
    risk.risk_decision = RiskDecision(False, 0.0, True)
    strategy = Strategy("current-account-liquidation", timer=Timer(), risk_ctrl=risk)
    account = FakeAccount(1000.0)
    strategy.set_account(account)
    account.positions["000001.SZ"] = 75

    strategy._apply_risk_decision(risk.risk_decision)

    assert [(request.code, request.volume) for request in strategy._pending_orders] == [
        ("000001.SZ", 75),
    ]


def test_timer_history_routes_without_clearing_active_factor_state() -> None:
    kline = HistoryClearingKlineFactor("history")
    kline.set_targets({"000001.SZ"})
    timer = BaseTimeSelection("history-timer", [kline], [TickFactor("tick")])
    strategy = Strategy("history-strategy", timer=timer)

    requirements = strategy.prepare_requirements(datetime(2024, 1, 2))
    active = next(item for item in requirements if item.get("source") == "timer")
    history = next(item for item in requirements if item.get("source") == "timer_history")

    assert active.get("idy") == history.get("idy")
    assert active.get("request_channel") != history.get("request_channel")
    timer.receive_data(
        {
            "source": "timer",
            "idy": active.get("idy"),
            "idx": "001",
                "request_id": active.get("request_id"),
                "generation": active.get("generation"),
            },
            "active-data",
    )
    assert "active-data" in timer.all_factors[0]._received_data.values()
    timer.receive_data(
        {
            "source": "timer_history",
            "idy": history.get("idy"),
            "idx": "001",
            "request_id": history.get("request_id"),
        },
        "history-data",
    )

    assert "history-data" not in timer.all_factors[0]._received_data.values()
    assert "history-data" in timer.all_factors[0]._history_received_data.values()
    assert active.get("request_id") in timer.all_factors[0]._expected_request_ids
    assert history.get("request_id") not in timer.all_factors[0]._request_status
    assert timer.all_factors[0]._history_request_status[history.get("request_id")] is True
    assert timer.all_factors[0]._request_status.get(active.get("request_id"), False) is True
    assert timer.all_factors[0]._targets_configured
    assert timer.all_factors[0]._target_codes == {"000001.SZ"}
    assert timer.check_data()

    timer.get_queries(datetime(2024, 1, 3))

    assert "history-data" in timer.all_factors[0]._history_received_data.values()


def test_mapping_requests_work_standalone_and_inside_a_stream() -> None:
    mapping_risk = BaseRiskControl("mapping-risk", daily_factors=[MappingRequestFactor("streamed")])
    strategy = Strategy("mapping-strategy", timer=Timer(), risk_ctrl=mapping_risk)
    standalone_queries = strategy.prepare_requirements(datetime(2024, 1, 2))
    stream = Stream("mapping-stream", [strategy], Mind())
    stream_queries = stream.prepare_requirements(datetime(2024, 1, 2))

    standalone = next(item for item in standalone_queries if item.get("source") == "risk")
    assert isinstance(standalone, CanonicalDataRequest)
    assert standalone.dataset == "market.bar"
    assert isinstance(stream_queries[0], CanonicalDataRequest)
    assert stream_queries[0].dataset == "market.bar"
    assert stream_queries[0].get("source_strategy") == "mapping-strategy"


@pytest.mark.parametrize("request_form", ["data_request", "scope", "domain_kind", "legacy"])
def test_strategy_and_stream_normalize_equivalent_request_forms(request_form: str) -> None:
    factor = RequestFormsFactor(f"form-{request_form}", request_form)
    risk = BaseRiskControl(f"risk-{request_form}", daily_factors=[factor])
    strategy = Strategy(f"strategy-{request_form}", timer=Timer(), risk_ctrl=risk)
    standalone = next(
        item for item in strategy.prepare_requirements(datetime(2024, 1, 2))
        if item.get("source") == "risk"
    )

    stream = Stream(f"stream-{request_form}", [strategy], Mind())
    streamed = next(
        item for item in stream.prepare_requirements(datetime(2024, 1, 2))
        if item.get("source_strategy") == strategy.strategy_name
        and item.get("source") == "risk"
    )

    assert isinstance(standalone, CanonicalDataRequest)
    assert isinstance(streamed, CanonicalDataRequest)
    assert standalone.dataset == streamed.dataset == "market.bar"
    assert standalone.anchor == streamed.anchor
    assert standalone.anchor.replace(tzinfo=None) == datetime(2024, 1, 2)
    assert standalone.get("idy") == streamed.get("idy")
    assert standalone.delivery_key != streamed.delivery_key
    assert streamed.get("source") == "risk"
    assert streamed.get("source_strategy") == strategy.strategy_name


def test_strategy_daily_execute_uses_price_dict_for_pending_requests() -> None:
    strategy = Strategy("daily-price", timer=Timer())
    account = FakeAccount(1000.0)
    strategy.set_account(account)
    strategy._pending_orders = [ExecutionRequest(
        code="000001.SZ",
        action="BUY",
        time="15:00:00",
        price=0,
        mode=ExecutionMode.MARKET,
        volume=100,
    )]

    orders = strategy.daily_execute({"000001.SZ": 10.0})

    assert orders == [(None, "15:00:00", "000001.SZ", 10.0, 100, "n")]
    assert account.positions == {"000001.SZ": 100}


@pytest.mark.parametrize(
    "decision",
    [RiskDecision(True, 1.0, True), RiskDecision(False, 0.0, False)],
)
def test_strategy_liquidates_positions_and_blocks_buys_for_forced_risk(decision: RiskDecision) -> None:
    factor = DirectTickFactor("direct")
    factor.set_targets({"000001.SZ"})
    timer = BaseTimeSelection("direct-timer", [], [factor])
    risk = BaseRiskControl("risk")
    risk.risk_decision = decision
    risk.is_trading_allowed = decision.trading_allowed
    risk.target_position_ratio = decision.target_position_ratio
    strategy = Strategy("risk-strategy", timer=timer, risk_ctrl=risk)
    account = FakeAccount(1000.0)
    account.positions["000001.SZ"] = 100
    strategy.set_account(account)

    orders = strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert [order[4] for order in orders] == [-100]
    assert account.positions.get("000001.SZ", 0) == 0


def test_strategy_drops_pending_buys_at_risk_request_boundary() -> None:
    factor = DirectTickFactor("boundary-buy")
    factor.set_targets({"000001.SZ"})
    timer = BaseTimeSelection("boundary-timer", [], [factor])
    risk = BaseRiskControl("boundary-risk")
    blocked = RiskDecision(False, 0.0, True)
    risk.risk_decision = blocked
    risk.is_trading_allowed = blocked.trading_allowed
    risk.target_position_ratio = blocked.target_position_ratio
    strategy = Strategy("boundary-strategy", timer=timer, risk_ctrl=risk)
    account = FakeAccount(1000.0)
    account.positions.update({"000001.SZ": 100, "000002.SZ": 50})
    strategy.set_account(account)

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert [(request.code, request.action) for request in strategy._pending_orders] == [
        ("000002.SZ", "SELL"),
    ]

    strategy._apply_risk_decision(RiskDecision(True, 1.0, False))
    orders = strategy.daily_execute({"000001.SZ": 10.0, "000002.SZ": 10.0})

    assert all(order[4] < 0 for order in orders)
    assert not any(request.action == "BUY" for request in strategy._pending_orders)


def test_strategy_retains_unpriceable_liquidation_and_deduplicates_sell_sources() -> None:
    sell_factor = DirectTickFactor("sell")
    sell_factor.action = "SELL"
    sell_factor.volume = 100
    sell_factor.set_targets({"000001.SZ"})
    timer = BaseTimeSelection("sell-timer", [], [sell_factor])
    risk = BaseRiskControl("risk")
    risk.risk_decision = RiskDecision(True, 1.0, True)
    strategy = Strategy("multi-code-liquidation", timer=timer, risk_ctrl=risk)
    account = FakeAccount(1000.0)
    account.positions.update({"000001.SZ": 100, "000002.SZ": 50})
    strategy.set_account(account)

    orders = strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert [order[2:5] for order in orders] == [("000001.SZ", 10.0, -100)]
    assert account.positions == {"000001.SZ": 0, "000002.SZ": 50}
    assert [request.code for request in strategy._pending_orders] == ["000002.SZ"]


def test_strategy_matcher_submission_is_not_repeated_before_order_event() -> None:
    factor = DirectTickFactor("matcher")
    factor.set_targets({"000001.SZ"})
    strategy = Strategy("matcher-strategy", timer=BaseTimeSelection("matcher-timer", [], [factor]))
    matcher = FakeMatcher()
    strategy.tick_matcher = matcher

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})
    strategy.on_tick({"code": "000001.SZ", "time": "09:32:00", "price": 10.0})

    assert len(matcher.submissions) == 1
    assert matcher.submissions[0][0].volume == 100
    assert set(strategy._inflight_orders) == {"order-1"}
    assert strategy._pending_orders == []


def test_strategy_filled_matcher_event_removes_inflight_source() -> None:
    factor = DirectTickFactor("filled")
    factor.set_targets({"000001.SZ"})
    strategy = Strategy("filled-strategy", timer=BaseTimeSelection("filled-timer", [], [factor]))
    strategy.tick_matcher = FakeMatcher()
    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    strategy.handle_order_event({
        "order_id": "order-1", "status": "FILLED", "filled_volume": 100,
        "requested_volume": 100,
    })

    assert strategy._inflight_orders == {}
    assert strategy._pending_orders == []


@pytest.mark.parametrize("terminal_status", ["CANCELLED", "EXPIRED"])
def test_strategy_terminal_matcher_events_return_partial_remainder_idempotently(
    terminal_status: str,
) -> None:
    factor = DirectTickFactor(f"{terminal_status.lower()}-partial")
    factor.set_targets({"000001.SZ"})
    strategy = Strategy(
        f"{terminal_status.lower()}-partial-strategy",
        timer=BaseTimeSelection(f"{terminal_status.lower()}-partial-timer", [], [factor]),
    )
    strategy.tick_matcher = FakeMatcher()
    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    strategy.handle_order_event({
        "order_id": "order-1", "status": "PARTIALLY_FILLED", "filled_volume": 30,
    })
    strategy.handle_order_event({"order_id": "order-1", "status": terminal_status})
    strategy.handle_order_event({"order_id": "order-1", "status": terminal_status})

    assert strategy._inflight_orders == {}
    assert [request.volume for request in strategy._pending_orders] == [70]


def test_strategy_direct_partial_fill_acknowledges_same_code_sources_proportionally() -> None:
    first_buy = DirectTickFactor("direct-first-buy")
    first_buy.volume = 100
    first_buy.set_targets({"000001.SZ"})
    second_buy = DirectTickFactor("direct-second-buy")
    second_buy.volume = 100
    second_buy.set_targets({"000001.SZ"})
    strategy = Strategy(
        "direct-merged-buys",
        timer=BaseTimeSelection("direct-merged-buy-timer", [], [first_buy, second_buy]),
    )
    account = PartialFillAccount(10000.0)
    strategy.set_account(account)

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert account.positions == {"000001.SZ": 100}
    assert [request.volume for request in strategy._pending_orders] == [50, 50]


def test_strategy_bare_filled_matcher_event_acknowledges_entire_inflight_request() -> None:
    factor = DirectTickFactor("bare-filled")
    factor.set_targets({"000001.SZ"})
    strategy = Strategy("bare-filled-strategy", timer=BaseTimeSelection("bare-filled-timer", [], [factor]))
    strategy.tick_matcher = FakeMatcher()
    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    strategy.handle_order_event({"order_id": "order-1", "status": "FILLED"})

    assert strategy._inflight_orders == {}
    assert strategy._pending_orders == []


def test_strategy_rejected_matcher_event_returns_source_to_pending() -> None:
    factor = DirectTickFactor("rejected")
    factor.set_targets({"000001.SZ"})
    strategy = Strategy("rejected-strategy", timer=BaseTimeSelection("rejected-timer", [], [factor]))
    strategy.tick_matcher = FakeMatcher()
    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    strategy.handle_order_event({"order_id": "order-1", "status": "REJECTED"})

    assert strategy._inflight_orders == {}
    assert [request.volume for request in strategy._pending_orders] == [100]


def test_strategy_partial_direct_fill_reduces_pending_request() -> None:
    strategy = Strategy("partial-direct", timer=Timer())
    account = PartialFillAccount(1000.0)
    strategy.set_account(account)
    strategy._pending_orders = [ExecutionRequest(
        "000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET,
    )]

    strategy.daily_execute({"000001.SZ": 10.0})

    assert account.positions == {"000001.SZ": 50}
    assert [request.volume for request in strategy._pending_orders] == [50]


def test_strategy_no_account_or_account_failure_keeps_pending_requests() -> None:
    request = ExecutionRequest(
        "000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET,
    )
    no_account = Strategy("no-account", timer=Timer())
    no_account._pending_orders = [request]
    failing_account = Strategy("failing-account", timer=Timer())
    failing_account.set_account(FailingAccount(1000.0))
    failing_account._pending_orders = [request]

    no_account.daily_execute({"000001.SZ": 10.0})
    failing_account.daily_execute({"000001.SZ": 10.0})

    assert no_account._pending_orders == [request]
    assert failing_account._pending_orders == [request]


def test_strategy_matcher_forced_liquidation_uses_capped_sell_canonicalization() -> None:
    sell_factor = DirectTickFactor("matcher-sell")
    sell_factor.action = "SELL"
    sell_factor.volume = 200
    sell_factor.set_targets({"000001.SZ"})
    risk = BaseRiskControl("matcher-risk")
    risk.risk_decision = RiskDecision(False, 0.0, True)
    risk.is_trading_allowed = False
    risk.target_position_ratio = 0.0
    strategy = Strategy(
        "matcher-liquidation",
        timer=BaseTimeSelection("matcher-sell-timer", [], [sell_factor]),
        risk_ctrl=risk,
    )
    account = FakeAccount(1000.0)
    account.positions["000001.SZ"] = 100
    strategy.set_account(account)
    matcher = FakeMatcher()
    strategy.tick_matcher = matcher

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert len(matcher.submissions) == 1
    assert [(request.code, request.action, request.volume) for request in matcher.submissions[0]] == [
        ("000001.SZ", "SELL", 100),
    ]
    assert [request.volume for request in strategy._pending_orders] == [200]

    strategy.on_tick({"code": "000001.SZ", "time": "09:32:00", "price": 10.0})

    assert len(matcher.submissions) == 1


def test_strategy_matcher_merges_same_tick_sell_sources_and_retains_remainders() -> None:
    first_sell = DirectTickFactor("first-sell")
    first_sell.action = "SELL"
    first_sell.volume = 80
    first_sell.set_targets({"000001.SZ"})
    second_sell = DirectTickFactor("second-sell")
    second_sell.action = "SELL"
    second_sell.volume = 80
    second_sell.set_targets({"000001.SZ"})
    strategy = Strategy(
        "merged-sells",
        timer=BaseTimeSelection("merged-sells-timer", [], [first_sell, second_sell]),
    )
    account = FakeAccount(1000.0)
    account.positions["000001.SZ"] = 100
    strategy.set_account(account)
    matcher = FakeMatcher()
    strategy.tick_matcher = matcher

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})
    strategy.on_tick({"code": "000001.SZ", "time": "09:32:00", "price": 10.0})

    assert [(request.code, request.action, request.volume) for request in matcher.submissions[0]] == [
        ("000001.SZ", "SELL", 100),
    ]
    assert len(matcher.submissions) == 1
    assert [order.submitted_volume for order in strategy._inflight_orders["order-1"]] == [80, 20]

    strategy.handle_order_event({
        "order_id": "order-1", "status": "FILLED", "filled_volume": 90,
    })

    assert strategy._inflight_orders == {}
    assert [request.volume for request in strategy._pending_orders] == [8, 62]


def test_strategy_canonicalizes_same_code_buy_sources_before_direct_partial_fill() -> None:
    first_buy = DirectTickFactor("first-buy")
    first_buy.volume = 100
    first_buy.set_targets({"000001.SZ"})
    second_buy = DirectTickFactor("second-buy")
    second_buy.volume = 100
    second_buy.set_targets({"000001.SZ"})
    strategy = Strategy(
        "merged-buys",
        timer=BaseTimeSelection("merged-buys-timer", [], [first_buy, second_buy]),
    )
    account = PartialFillAccount(10000.0)
    strategy.set_account(account)

    orders = strategy.on_tick(
        {"code": "000001.SZ", "time": "09:31:00", "price": 10.0}
    )

    assert orders == [(None, "09:31:00", "000001.SZ", 10.0, 200, "n")]
    assert account.positions == {"000001.SZ": 100}
    assert [request.volume for request in strategy._pending_orders] == [50, 50]


def test_strategy_matcher_canonicalizes_same_code_sources_and_acknowledges_partial_volume() -> None:
    first_buy = DirectTickFactor("matcher-first-buy")
    first_buy.volume = 100
    first_buy.set_targets({"000001.SZ"})
    second_buy = DirectTickFactor("matcher-second-buy")
    second_buy.volume = 100
    second_buy.set_targets({"000001.SZ"})
    strategy = Strategy(
        "matcher-merged-buys",
        timer=BaseTimeSelection("matcher-merged-buy-timer", [], [first_buy, second_buy]),
    )
    matcher = FakeMatcher()
    strategy.tick_matcher = matcher

    strategy.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert [request.volume for request in matcher.submissions[0]] == [200]
    assert [order.submitted_volume for order in strategy._inflight_orders["order-1"]] == [100, 100]

    strategy.handle_order_event({
        "order_id": "order-1", "status": "PARTIALLY_FILLED", "filled_volume": 100,
    })

    assert [order.filled_volume for order in strategy._inflight_orders["order-1"]] == [50, 50]
    assert [order.remaining_volume for order in strategy._inflight_orders["order-1"]] == [50, 50]


def test_stream_skips_daily_execution_and_mirroring_for_skipped_children() -> None:
    strategy = Strategy("skipped", timer=Timer())
    stream = Stream("skip-daily", [strategy], Mind())
    stream.set_account(FakeAccount(1000.0))
    strategy._pending_orders = [ExecutionRequest(
        "000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET,
    )]
    stream.set_skip_children({"skipped"})
    captured: list[list[tuple]] = []
    stream.set_live_order_executor(captured.append)

    stream.execute_daily({"000001.SZ": 10.0})

    assert captured == []
    assert strategy._pending_orders[0].volume == 100
    assert strategy.account.positions == {}


def test_stream_skips_daily_pipeline_for_skipped_children() -> None:
    strategy = Strategy("skipped-daily-pipeline", timer=Timer())
    stream = Stream("skip-daily-pipeline", [strategy], Mind())
    stream.set_account(FakeAccount(1000.0))
    calls: list[bool] = []
    strategy._run_daily_pipeline = lambda: calls.append(True)  # type: ignore[method-assign]
    stream.set_skip_children({"skipped-daily-pipeline"})

    stream.on_daily()

    assert calls == []


def test_stream_skips_preexisting_tick_delta_for_skipped_children() -> None:
    strategy = Strategy("skipped-tick", timer=Timer())
    stream = Stream("skip-tick", [strategy], Mind())
    stream.set_account(FakeAccount(1000.0))
    stream.mind.current_weights = {"skipped-tick": 1.0}
    stream.shadow_accounts["skipped-tick"].positions["000001.SZ"] = 100
    captured: list[list[tuple]] = []
    stream.set_live_order_executor(captured.append)
    stream.set_skip_children({"skipped-tick"})

    stream.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert captured == []


def test_stream_skips_settlement_callbacks_for_skipped_children() -> None:
    strategy = Strategy("skipped-settle", timer=Timer())
    stream = Stream("skip-settle", [strategy], Mind())
    stream.set_account(FakeAccount(1000.0))
    calls: list[bool] = []
    strategy.on_day_end = lambda: calls.append(True)  # type: ignore[method-assign]
    stream.set_skip_children({"skipped-settle"})

    stream.daily_settle({"000001.SZ": 10.0})

    assert calls == []
    assert "skipped-settle" not in stream._substrategy_return_history


def test_strategy_removes_original_daily_request_after_risk_scaling() -> None:
    strategy = Strategy("scaled-daily", timer=Timer())
    risk = BaseRiskControl("risk")
    risk.risk_decision = RiskDecision(True, 0.5, False)
    risk.target_position_ratio = 0.5
    risk.is_trading_allowed = True
    strategy.risk_ctrl = risk
    account = FakeAccount(1000.0)
    strategy.set_account(account)
    request = ExecutionRequest(
        "000001.SZ", "BUY", "15:00:00", price=0, volume=200, mode=ExecutionMode.MARKET,
    )
    strategy._pending_orders = [request]

    first_orders = strategy.daily_execute({"000001.SZ": 10.0})
    second_orders = strategy.daily_execute({"000001.SZ": 10.0})

    assert first_orders == [(None, "15:00:00", "000001.SZ", 10.0, 100, "n")]
    assert second_orders == []
    assert strategy._pending_orders == []


@pytest.mark.parametrize(
    ("sell_volume", "expected_volume"),
    [(50, 50), (100, 0)],
)
def test_stream_nets_opposing_tick_child_deltas_by_code(
    sell_volume: int, expected_volume: int
) -> None:
    buy_factor = DirectTickFactor("buy")
    buy_factor.volume = 100
    buy_factor.set_targets({"000001.SZ"})
    sell_factor = DirectTickFactor("sell")
    sell_factor.action = "SELL"
    sell_factor.volume = sell_volume
    sell_factor.set_targets({"000001.SZ"})
    first = Strategy("buy", timer=BaseTimeSelection("buy-timer", [], [buy_factor]))
    second = Strategy("sell", timer=BaseTimeSelection("sell-timer", [], [sell_factor]))
    stream = Stream("net-tick", [first, second], Mind())
    stream.set_account(FakeAccount(1000.0))
    stream.mind.current_weights = {"buy": 0.5, "sell": 0.5}
    stream.shadow_accounts["sell"].positions["000001.SZ"] = 100
    captured: list[list[tuple]] = []
    stream.set_live_order_executor(captured.append)

    stream.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert len(captured) == (1 if expected_volume else 0)
    if expected_volume:
        assert captured[0][0][2] == "000001.SZ"
        assert captured[0][0][4] == expected_volume


def test_stream_nets_opposing_daily_child_deltas_by_code() -> None:
    first = Strategy("buy", timer=Timer())
    second = Strategy("sell", timer=Timer())
    stream = Stream("net-daily", [first, second], Mind())
    stream.set_account(FakeAccount(1000.0))
    stream.mind.current_weights = {"buy": 0.5, "sell": 0.5}
    stream.shadow_accounts["sell"].positions["000001.SZ"] = 100
    first._pending_orders = [ExecutionRequest(
        "000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET,
    )]
    second._pending_orders = [ExecutionRequest(
        "000001.SZ", "SELL", "15:00:00", price=0, volume=50, mode=ExecutionMode.MARKET,
    )]
    captured: list[list[tuple]] = []
    stream.set_live_order_executor(captured.append)

    stream.execute_daily({"000001.SZ": 10.0})

    assert captured == [[(None, "15:00:00", "000001.SZ", 10.0, 50, "n")]]


def test_stream_nets_weighted_tick_deltas_before_rounding() -> None:
    first_factor = DirectTickFactor("fractional-first")
    first_factor.action = "SELL"
    first_factor.volume = 101
    second_factor = DirectTickFactor("fractional-second")
    second_factor.action = "SELL"
    second_factor.volume = 101
    first = Strategy("fractional-first", timer=BaseTimeSelection("fractional-first-timer", [], [first_factor]))
    second = Strategy("fractional-second", timer=BaseTimeSelection("fractional-second-timer", [], [second_factor]))
    stream = Stream("fractional-net", [first, second], Mind())
    stream.set_account(FakeAccount(1000.0))
    stream.mind.current_weights = {"fractional-first": 0.25, "fractional-second": 0.25}
    stream.shadow_accounts["fractional-first"].positions["000001.SZ"] = 101
    stream.shadow_accounts["fractional-second"].positions["000001.SZ"] = 101
    captured: list[list[tuple]] = []
    stream.set_live_order_executor(captured.append)

    stream.on_tick({"code": "000001.SZ", "time": "09:31:00", "price": 10.0})

    assert captured == [[(None, "09:31:00", "000001.SZ", 10.0, -101, "n")]]


def test_queryless_or_factor_is_calculated_before_float_and_done() -> None:
    picker = Picker("queryless-or", [], [QuerylessBinaryFactor("or")], [])

    assert picker.get_queries("20240102") == []

    assert picker.is_done()
    assert picker._valid_codes == {"000001.SZ"}


def test_queryless_stage_requires_factor_readiness_before_advancing() -> None:
    unready_picker = Picker(
        "queryless-unready", [UnreadyQuerylessBinaryFactor("and")], [], []
    )

    assert unready_picker.get_queries("20240102") == []
    assert not unready_picker.check_data()
    assert not unready_picker.is_done()

    ready_picker = Picker(
        "queryless-ready", [], [QuerylessBinaryFactor("or")], []
    )

    assert ready_picker.get_queries("20240102") == []
    assert ready_picker._valid_codes == {"000001.SZ"}
    assert ready_picker.check_data()
    assert ready_picker.is_done()


def test_stream_rejects_duplicate_strategy_names() -> None:
    first = Strategy("duplicate", timer=Timer())
    second = Strategy("duplicate", timer=Timer())

    with pytest.raises(ValueError, match="duplicate strategy_name"):
        Stream("stream", [first, second], Mind())


def test_mind_validates_weights_and_stream_creates_shadow_accounts() -> None:
    mind = Mind()
    first = Strategy("first", timer=Timer())
    second = Strategy("second", timer=Timer())
    stream = Stream("stream", [first, second], mind)

    stream.set_account(FakeAccount(1000.0))
    weights = mind._validate_weights({"first": 2.0, "second": -1.0})

    assert stream.mind is mind
    assert mind._stream is stream
    assert set(stream.shadow_accounts) == {"first", "second"}
    assert all(isinstance(account, FakeAccount) for account in stream.shadow_accounts.values())
    assert all(account.total_equity == 500.0 for account in stream.shadow_accounts.values())
    assert weights == {"first": 1.0, "second": 0.0}


def test_removed_factor_bases_are_not_importable() -> None:
    import trading_nodes_base.factors.base as factor_base

    with pytest.raises(AttributeError):
        getattr(factor_base, "Base" + "Composite" + "Factor")

    with pytest.raises(AttributeError):
        getattr(factor_base, "Cached" + "Factor")
