"""统一 MatchingEngine 撮合语义测试（next_bar / limit / market / match_mode / 回调）。"""

from __future__ import annotations

from trading_nodes_base.types import ExecutionMode, ExecutionRequest
from engines.core.matching import MatchingEngine
from engines.core.subscriptions import KIND_TICK, StreamRegistry, SubscriptionTracker, kline_kind


class Rec:
    def __init__(self):
        self.fills: list[tuple] = []

    def order(self, orders):
        self.fills.extend(orders)
        return orders


def bar(code, i, o, c):
    return type("Bar", (), {"instrument_id": code, "open": o, "close": c, "interval_end": f"d{i}"})()


def tick(code, price, ask=None, bid=None):
    return type("T", (), {"instrument_id": code, "price": price, "ask1_price": ask, "bid1_price": bid, "ask_levels": None, "bid_levels": None})()


def test_next_bar_open_fills_at_next_open():
    acct = Rec()
    m = MatchingEngine(acct.order, None)
    m.match_event(bar("A", 1, 10.0, 10.0), "20260930")  # 信号bar
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="next_bar_open")], "20260930")
    m.match_event(bar("A", 2, 12.0, 12.0), "20260930")  # 下根 -> open=12 成交
    assert len(acct.fills) == 1 and acct.fills[0][3] == 12.0 and acct.fills[0][4] == 100


def test_next_bar_close_fills_at_next_close():
    acct = Rec()
    m = MatchingEngine(acct.order, None)
    m.match_event(bar("A", 1, 10.0, 11.0), "20260930")
    m.submit_requests([ExecutionRequest(code="A", action="SELL", time="t", volume=100, timing="next_bar_close")], "20260930")
    m.match_event(bar("A", 2, 13.0, 14.0), "20260930")
    assert acct.fills and acct.fills[0][3] == 14.0 and acct.fills[0][4] == -100


def test_market_fills_on_next_event():
    acct = Rec()
    m = MatchingEngine(acct.order, None)
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    m.match_event(tick("A", 15.0, ask=15.0, bid=14.9), "20260930")
    assert acct.fills and acct.fills[0][3] == 15.0  # BUY 按 ask


def test_limit_fills_on_bid_ask_cross():
    acct = Rec()
    m = MatchingEngine(acct.order, None)
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="limit", price=11.0, mode=ExecutionMode.LIMIT)], "20260930")
    m.match_event(tick("A", 12.0, ask=11.0, bid=10.5), "20260930")  # BUY 按 ask1=11 <= 限价11
    assert acct.fills and acct.fills[0][3] == 11.0


def test_match_mode_last_trade_only_ignores_quote():
    acct = Rec()
    m = MatchingEngine(acct.order, None, match_mode="last_trade_only")
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    quote = type("Q", (), {"instrument_id": "A", "price": 12.0, "ask_levels": None, "bid_levels": None})()
    m.match_event(quote, "20260930")  # 报价事件被跳过
    assert len(acct.fills) == 0


def test_event_sink_receives_submitted_and_filled():
    events = []
    acct = Rec()
    m = MatchingEngine(acct.order, lambda e: events.append(e))
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    assert events and events[0]["status"] == "SUBMITTED"
    m.match_event(tick("A", 10.0, ask=10.0, bid=9.9), "20260930")
    assert events[-1]["status"] == "FILLED" and events[-1]["filled_volume"] == 100


def test_cancel_removes_pending():
    acct = Rec()
    m = MatchingEngine(acct.order, None)
    (order,) = m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    assert m.cancel(order.order_id)
    assert not m.pending_orders()


def test_execution_delay_fills_on_nth_bar_after_signal():
    acct = Rec()
    m = MatchingEngine(acct.order, None, execution_delay=2)
    m.match_event(bar("A", 1, 10.0, 10.0), "20260930")  # 信号 bar（seq=1）
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="next_bar_open")], "20260930")
    assert m.match_event(bar("A", 2, 12.0, 12.0), "20260930") == []  # 第 1 个事件，未到延迟
    assert acct.fills == []
    m.match_event(bar("A", 3, 14.0, 14.0), "20260930")  # 第 2 个事件 -> open=14 成交
    assert acct.fills and acct.fills[0][3] == 14.0 and acct.fills[0][4] == 100


def test_execution_delay_fills_on_nth_tick_after_submit():
    acct = Rec()
    m = MatchingEngine(acct.order, None, execution_delay=2)
    m.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    assert m.match_event(tick("A", 15.0, ask=15.0, bid=14.9), "20260930") == []  # 第 1 个 tick
    m.match_event(tick("A", 16.0, ask=16.0, bid=15.9), "20260930")  # 第 2 个 tick -> 按 ask=16
    assert acct.fills and acct.fills[0][3] == 16.0


def test_matcher_subscribes_tick_on_submit_and_releases_on_fill():
    registry = StreamRegistry()
    matcher = MatchingEngine(Rec().order, None, tracker=SubscriptionTracker(registry, "matcher:x"))

    matcher.submit_requests([ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")], "20260930")
    assert registry.codes(KIND_TICK) == {"A"}

    matcher.match_event(tick("A", 10.0, ask=10.0), "20260930")
    assert registry.codes(KIND_TICK) == set()


def test_matcher_next_bar_subscribes_source_kline_frequency():
    registry = StreamRegistry()
    matcher = MatchingEngine(Rec().order, None, tracker=SubscriptionTracker(registry, "matcher:x"))

    matcher.submit_requests(
        [ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="next_bar_open")],
        "20260930",
        "30m",
    )
    assert registry.is_subscribed(kline_kind("30m"), "A")
    assert registry.codes(KIND_TICK) == set()


def test_matcher_releases_only_after_last_order_for_code_leaves():
    registry = StreamRegistry()
    matcher = MatchingEngine(Rec().order, None, tracker=SubscriptionTracker(registry, "matcher:x"))

    matcher.submit_requests(
        [
            ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market"),
            ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="limit", price=1.0, mode=ExecutionMode.LIMIT),
        ],
        "20260930",
    )
    assert registry.is_subscribed(KIND_TICK, "A")

    matcher.match_event(tick("A", 10.0, ask=10.0, bid=9.9), "20260930")  # 市价成交；限价 price<=1 不成交
    assert registry.is_subscribed(KIND_TICK, "A")

    remaining = matcher.pending_orders()
    assert len(remaining) == 1
    matcher.cancel(remaining[0].order_id)
    assert not registry.is_subscribed(KIND_TICK, "A")


def test_stream_registry_refcounts_sources_and_drops_source():
    registry = StreamRegistry()
    registry.subscribe("a", KIND_TICK, ["X", "Y"])
    registry.subscribe("b", KIND_TICK, ["Y"])

    registry.unsubscribe("a", KIND_TICK, ["Y"])
    assert registry.codes(KIND_TICK) == {"X", "Y"}  # b 仍订阅 Y
    registry.unsubscribe("a", KIND_TICK, ["X"])
    assert registry.codes(KIND_TICK) == {"Y"}

    registry.drop_source("b")
    assert registry.codes(KIND_TICK) == set()
    assert registry.kinds() == set()
