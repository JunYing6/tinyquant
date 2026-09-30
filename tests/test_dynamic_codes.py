"""Task 6: 因子日内动态缩减所需代码（drop_required_codes）+ 择时器重聚合 + 引擎回调。"""

from __future__ import annotations

from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.factors import TickTimingFactor


class _Noop(TickTimingFactor):
    accepted_intent_actions = frozenset()
    execution_role = "intent_executor"

    def __init__(self):
        super().__init__("noop")

    def get_query_lst(self, date, codes=None):
        self._data_clear()
        self.sign["fit"] = True
        return []


class _Decl(TickTimingFactor):
    accepted_intent_actions = frozenset()
    execution_role = "intent_executor"

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