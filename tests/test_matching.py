"""统一 MatchingEngine 撮合语义测试（next_bar / limit / market / match_mode / 回调）。"""

from __future__ import annotations

from trading_nodes_base.types import ExecutionMode, ExecutionRequest
from engines.core.matching import MatchingEngine


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
