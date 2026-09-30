"""ExecutionRequest timing / price-validation contract tests."""

from __future__ import annotations

import pytest

from trading_nodes_base.types import ExecutionRequest


def test_execution_request_timing_field() -> None:
    request = ExecutionRequest(
        code="A", action="BUY", time="t", price=10.0, volume=100, timing="next_bar_open"
    )
    assert request.timing == "next_bar_open"


def test_timing_rejects_unknown_value() -> None:
    with pytest.raises(ValueError):
        ExecutionRequest(code="A", action="BUY", time="t", price=10.0, volume=100, timing="bogus")


def test_market_timing_allows_zero_price() -> None:
    request = ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="market")
    assert request.price == 0.0


def test_next_bar_open_allows_zero_price() -> None:
    request = ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="next_bar_open")
    assert request.price == 0.0


def test_limit_timing_still_requires_positive_price() -> None:
    with pytest.raises(ValueError):
        ExecutionRequest(code="A", action="BUY", time="t", volume=100, timing="limit")


def test_default_timing_requires_positive_price() -> None:
    with pytest.raises(ValueError):
        ExecutionRequest(code="A", action="BUY", time="t", volume=100)
