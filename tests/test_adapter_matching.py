"""TradingContractAdapter 统一撮合接线测试：bar/tick 事件都喂撮合器并经成交回调。"""

from __future__ import annotations

from trading_nodes_base.factors import KlineTimingFactor
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.types import ExecutionRequest, KlineBar, SignalIntent
from engines.core.matching import MatchingEngine
from engines.core.trading_adapter import TradingContractAdapter


class _Strat:
    def __init__(self, account_order):
        self.strategy_name = "s"
        self.order_events = []
        self.tick_matcher = MatchingEngine(account_order, self.handle_order_event)

    def handle_order_event(self, event):
        self.order_events.append(event)


def _bar(i, o, c):
    return type("Bar", (), {"instrument_id": "A", "frequency": "1d", "open": o, "close": c, "interval_end": f"d{i}"})()


def _mk_adapter(strategy, current_date="20260930"):
    adapter = TradingContractAdapter.__new__(TradingContractAdapter)
    adapter.strategies = (strategy,)
    adapter.entity = strategy
    adapter._skip_names = set()
    adapter._current_date = current_date
    return adapter


def test_match_strategy_fills_next_bar_order_on_bar():
    fills = []
    strategy = _Strat(lambda orders: fills.extend(orders) or orders)
    adapter = _mk_adapter(strategy)
    # 信号在 bar1 后提交 next_bar_open（seq 快照=1）
    adapter._match_strategy(strategy, _bar(1, 10.0, 10.0))
    strategy.tick_matcher.submit_requests(
        [ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="next_bar_open")], "20260930"
    )
    adapter._match_strategy(strategy, _bar(2, 12.0, 12.0))  # seq=2==1+1 -> open=12
    assert fills and fills[0][3] == 12.0 and fills[0][4] == 100
    assert strategy.order_events and strategy.order_events[-1]["status"] == "FILLED"


def test_match_strategy_fills_market_order_on_tick():
    fills = []
    strategy = _Strat(lambda orders: fills.extend(orders) or orders)
    adapter = _mk_adapter(strategy)
    strategy.tick_matcher.submit_requests(
        [ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930"
    )
    tick = type("T", (), {"instrument_id": "A", "price": 15.0, "ask_levels": None, "bid_levels": None})()
    adapter._match_strategy(strategy, tick)
    assert fills and fills[0][3] == 15.0 and fills[0][4] == 100


class _Emit30m(KlineTimingFactor):
    frequency = "30m"
    emitted_actions = frozenset({"BUY"})

    def __init__(self):
        super().__init__("emit-30m")
        self.emitted = False

    def get_query_lst(self, date, codes=None):
        return []

    def on_bar(self, bar):
        if self.emitted:
            return []
        self.emitted = True
        return [SignalIntent(bar.code, "BUY", bar.end_time, "entry", {"volume": 100})]


class _Recorder:
    def __init__(self):
        self.fills = []
        self.positions = {}

    def order(self, orders):
        self.fills.extend(orders)
        return orders


def _kline30m(code, end, price):
    return KlineBar(code=code, frequency="30m", end_time=end, open=price, high=price, low=price,
                    close=price, volume=0.0, amount=0.0)


def test_prebuilt_intraday_bar_matches_next_bar_order():
    account = _Recorder()
    strategy = BaseStrategy("s30m", timer=BaseTimeSelection("t30m", [_Emit30m()], []))
    strategy.set_account(account)
    strategy._current_date = "20240102"
    strategy.tick_matcher = MatchingEngine(account.order, strategy.handle_order_event, default_timing="next_bar_open")

    adapter = TradingContractAdapter(strategy)
    adapter.mark_external_frequency("30m")

    adapter.feed_prebuilt_bars("30m", [_kline30m("A", "10:00:00", 10.0)])  # 信号 bar：下单
    assert account.fills == []
    adapter.feed_prebuilt_bars("30m", [_kline30m("A", "10:30:00", 11.0)])  # 下一根：按 open 成交
    assert account.fills and account.fills[0][3] == 11.0 and account.fills[0][4] == 100