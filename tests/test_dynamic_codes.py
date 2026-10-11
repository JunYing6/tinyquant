"""Task 6: 因子日内动态缩减所需代码（drop_required_codes）+ 择时器重聚合 + 引擎回调；需求表构建。"""

from __future__ import annotations

from trading_nodes_base.methods import BaseRiskControl, BaseTimeSelection
from trading_nodes_base.factors import (
    IntradayKlineFactor,
    RiskIntradayKlineFactor,
    RiskNonIntradayKlineFactor,
    RiskTickFactor,
    TickStreamFactor,
)


class _Noop(TickStreamFactor):
    accepted_intent_actions = frozenset()

    def __init__(self):
        super().__init__("noop")

    def get_query_lst(self, date, codes=None):
        self._data_clear()
        self.sign["fit"] = True
        return []


class _Decl(TickStreamFactor):
    accepted_intent_actions = frozenset()

    def __init__(self, needed):
        super().__init__("decl")
        self.set_needed(needed)

    def get_query_lst(self, date, codes=None):
        self._data_clear()
        self.sign["fit"] = True
        return []


def test_factor_drop_required_codes_reaggregates_pool():
    timer = BaseTimeSelection("timer", [], [_Decl(["A", "B"]), _Noop()])
    changes: list[list[str]] = []
    timer.set_pool_change_callback(lambda pool: changes.append(list(pool)))
    timer.on_daily(pool=["A", "B", "C"], positions={}, account_info=None)
    assert timer.data_required_pool == ["A", "B"]

    decl = [f for f in timer.all_factors if f.factor_name == "decl"][0]
    decl.drop_required_codes("B")
    assert decl.required_codes() == ["A"]
    assert timer.data_required_pool == ["A"]
    assert changes == [["A"]]


def test_factor_is_target_code_reflects_immediate_drop():
    f = _Decl(["A", "B", "C"])
    f.set_needed(["A", "B", "C"])
    assert f.is_target_code("B")
    f.drop_required_codes("B")
    assert f.is_target_code("A") and not f.is_target_code("B")


def test_timer_stream_demands_partitions_by_kind_and_frequency():
    timer = BaseTimeSelection("t", [], [_Decl(["A", "B"]), _Noop()])
    timer.on_daily(pool=["A", "B", "C"], positions={}, account_info=None)

    demands = timer.stream_demands(["A", "B", "C"])
    # _Decl 声明了 A/B（scoped）；_Noop 未声明 -> 视为需要全部 base
    assert demands["tick"] == {"A", "B", "C"}
    assert demands["kline"] == {}
    assert demands["factors"]["decl"] == ("tick", None, ["A", "B"])


class _Scoped30mTiming(IntradayKlineFactor):
    frequency = "30m"

    def __init__(self):
        super().__init__("scoped-30m")
        self.set_needed(["A"])


def test_timer_stream_demands_kline_frequency_and_declared_not_counted():
    timer = BaseTimeSelection("t", [_Scoped30mTiming()], [_Decl(["A"])])
    timer.on_daily(pool=["A", "B"], positions={}, account_info=None)

    demands = timer.stream_demands(["A", "B"])
    assert demands["kline"] == {"30m": {"A"}}
    assert demands["factors"]["scoped-30m"] == ("kline", "30m", ["A"])


class _ScopedRiskTick(RiskTickFactor):
    def __init__(self):
        super().__init__("scoped-risk-tick")
        self.set_needed(["X"])


class _ScopedRisk30m(RiskIntradayKlineFactor):
    frequency = "30m"

    def __init__(self):
        super().__init__("scoped-risk-30m")


class _DailyRiskKline(RiskNonIntradayKlineFactor):
    def __init__(self):
        super().__init__("daily-risk-kline")

    def get_query_lst(self, date, codes=None):
        return []


def test_risk_stream_demands_scopes_and_skips_declared():
    risk = BaseRiskControl(
        "risk",
        risk_tick_factors=[_ScopedRiskTick()],
        risk_kline_factors=[_ScopedRisk30m(), _DailyRiskKline()],
    )
    risk.on_pre_market()  # _scope_stream_factors 会给流因子设目标代码

    demands = risk.stream_demands(["X", "Y"])
    assert demands["tick"] == {"X"}
    # 30m 流式因子未声明目标 -> 需要全 base；非日内声明式因子不计入
    assert demands["kline"] == {"30m": {"X", "Y"}}