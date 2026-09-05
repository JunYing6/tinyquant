"""End-to-end smoke test for the example memory data adapters.

Assembles a real :class:`DataGateway` from the adapters in
``examples/adapters/memory_adapters.py`` (default_catalog + DataBinding),
then drives a :class:`FastBacktestEngine` backtest and asserts trades settle
and an equity curve is produced.  Also verifies the adapters satisfy the port
protocols and that a plain ``tools.data`` build matches the adapter build.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))

import pytest

from adapters.memory_adapters import (
    CalendarAdapter,
    MemoryHistoricalAdapter,
    make_gateway,
)
from engines.fast import FastBacktestEngine
from tools.data import (
    Bar,
    CalendarRequest,
    DataBinding,
    DataGateway,
    DataPolicy,
    DataRequest,
    HistoricalDataPort,
    InMemoryGateway,
    Session,
    TradingCalendarPort,
    TradingPhase,
)
from trading.factors.base import KlineTimingFactor, TickTimingFactor
from trading.factors.types import ExecutionMode, ExecutionRequest, KlineBar, SignalIntent
from trading.methods.base import BaseTimeSelection
from trading.strategies.base import BaseStrategy


def _instant(day: str, clock: str = "15:00:00") -> datetime:
    return datetime.strptime(f"{day} {clock}", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _bar(day: str, price: float) -> Bar:
    instant = _instant(day)
    return Bar(
        schema_version="1", event_id=None, instrument_id="000001.SZ", asset_type="equity",
        effective_time=instant, event_time=instant, available_at=instant,
        trading_date=instant.date(), source="demo", quality="valid", metadata={},
        frequency="1d", interval_start=instant, interval_end=instant,
        open=price, high=price, low=price, close=price,
        volume=1000.0, turnover=price * 1000.0, is_complete=True, price_basis="raw",
    )


def _session(day: str) -> Session:
    phase = TradingPhase(
        name="regular", start=_instant(day, "09:00:00"), end=_instant(day, "15:00:00"),
        accepts_trades=True, accepts_quotes=True,
    )
    return Session(market="CN", trading_date=_instant(day).date(), timezone="UTC", phases=(phase,))


BARS = [_bar("20240102", 10.0), _bar("20240103", 11.0)]
SESSIONS = [_session("20240102"), _session("20240103")]


class BuyingFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("buying-kline")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
        return []


class PassiveExecutor(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("passive-executor")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


class BuyingFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        timer = BaseTimeSelection("smoke-timer", [BuyingFactor()], [PassiveExecutor()])
        super().__init__("smoke-fast", timer=timer)
        self._queued_once = False

    def _run_daily_pipeline(self) -> None:
        super()._run_daily_pipeline()
        if not self._queued_once:
            self._pending_orders.append(
                ExecutionRequest("000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET)
            )
            self._queued_once = True


# ---------------------------------------------------------------------------
# Port contracts
# ---------------------------------------------------------------------------


def test_adapters_satisfy_port_protocols() -> None:
    historic, calendar = MemoryHistoricalAdapter(BARS), CalendarAdapter(SESSIONS)
    assert isinstance(historic, HistoricalDataPort)
    assert isinstance(calendar, TradingCalendarPort)


def test_adapters_declare_descriptors() -> None:
    historic, calendar = MemoryHistoricalAdapter(BARS), CalendarAdapter(SESSIONS)
    assert historic.descriptor.name == "memory-historical"
    assert "market.bar" in historic.descriptor.datasets
    assert "calendar.session" in historic.descriptor.datasets
    assert calendar.descriptor.name == "memory-calendar"
    assert "calendar.session" in calendar.descriptor.datasets


def test_historical_adapter_read_and_iter_round_trip() -> None:
    adapter = MemoryHistoricalAdapter(BARS)
    batch = adapter.read(DataRequest(dataset="market.bar", start=_instant("20240102"), end=_instant("20240104")))
    assert [r.instrument_id for r in batch.records] == ["000001.SZ", "000001.SZ"]
    chunk = list(adapter.iter(DataRequest(dataset="market.bar", start=_instant("20240102"), end=_instant("20240104")), chunk_size=1))
    assert len(chunk) == 2
    assert sum(len(b.records) for b in chunk) == 2


# ---------------------------------------------------------------------------
# Real DataGateway assembly
# ---------------------------------------------------------------------------


def test_make_gateway_returns_real_data_gateway() -> None:
    gateway = make_gateway(BARS, SESSIONS)
    assert isinstance(gateway, DataGateway)
    assert gateway.is_open is False


def test_gateway_routes_read_and_sessions() -> None:
    gateway = make_gateway(BARS, SESSIONS)
    batch = gateway.read(DataRequest(dataset="market.bar", start=_instant("20240102"), end=_instant("20240104")))
    assert batch.dataset == "market.bar"
    assert len(batch.records) == 2
    cal = gateway.sessions(CalendarRequest(market="CN", start=_instant("20240102").date(), end=_instant("20240104").date()))
    assert len(cal.records) == 2


# ---------------------------------------------------------------------------
# End-to-end backtest over the adapters
# ---------------------------------------------------------------------------


def test_fast_backtest_trades_and_produces_equity_via_adapters() -> None:
    engine = FastBacktestEngine(
        BuyingFastStrategy(), "20240102", "20240103",
        initial_capital=100_000, mode="fast",
        data_gateway=make_gateway(BARS, SESSIONS), progress_bar=False,
    )
    engine.run()
    assert [row["trade_date"] for row in engine.equity_curve] == ["20240102", "20240103"]
    assert engine.daily_positions[-1]["positions"] == {"000001.SZ": 100}
    stats = engine.get_stats()
    assert stats["final_equity"] > 0
    assert stats["total_return"] > 0


def test_adapter_gateway_matches_toolsdata_memory_gateway() -> None:
    in_memory = InMemoryGateway(bars=BARS, sessions=SESSIONS)
    via_adapters = make_gateway(BARS, SESSIONS)
    ref = FastBacktestEngine(
        BuyingFastStrategy(), "20240102", "20240103", initial_capital=100_000,
        mode="fast", data_gateway=in_memory, progress_bar=False,
    )
    subject = FastBacktestEngine(
        BuyingFastStrategy(), "20240102", "20240103", initial_capital=100_000,
        mode="fast", data_gateway=via_adapters, progress_bar=False,
    )
    ref.run()
    subject.run()
    assert subject.get_stats() == ref.get_stats()
    assert subject.daily_positions == ref.daily_positions


def test_manual_binding_assembly_is_equivalent() -> None:
    historic, calendar = MemoryHistoricalAdapter(BARS), CalendarAdapter(SESSIONS)
    gateway = DataGateway(
        catalog=__import__("tools.data", fromlist=["default_catalog"]).default_catalog(),
        bindings=[
            (DataBinding(dataset="market.bar", adapter="memory-historical", priority=1), historic),
            (DataBinding(dataset="calendar.session", adapter="memory-calendar", priority=1), calendar),
        ],
        policy=DataPolicy(timezone="UTC"),
    )
    engine = FastBacktestEngine(
        BuyingFastStrategy(), "20240102", "20240103", initial_capital=100_000,
        mode="fast", data_gateway=gateway, progress_bar=False,
    )
    engine.run()
    assert engine.get_stats()["final_equity"] > 0