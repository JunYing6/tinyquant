from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from typing import Any

from rich.console import Console

from tinyquant_cli.commands.backtest import run_backtest
from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.runtime import SessionState
from tools.data import InMemoryGateway, Session, TradingPhase
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.factors import KlineTimingFactor, TickTimingFactor


def test_doctor_reports_core_runtime_status() -> None:
    console = Console(record=True, force_terminal=False)

    assert run_doctor(console, None) == 0

    output = console.export_text()
    assert "Python" in output
    assert "core imports" in output


def test_backtest_factory_failure_returns_nonzero_and_renders_error() -> None:
    console = Console(record=True, force_terminal=False)
    state = SessionState()

    code = run_backtest(
        console,
        state,
        "missing_module:build",
        "20240102",
        "20240103",
        1_000_000,
        "fast",
    )

    assert code == 2
    assert "missing_module:build" in console.export_text()


def _cli_instant(day: str) -> datetime:
    return datetime.strptime(f"{day} 15:00:00", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _cli_session(day: str) -> Session:
    phase = TradingPhase(name="regular", start=_cli_instant(day), end=_cli_instant(day), accepts_trades=True, accepts_quotes=True)
    return Session(market="CN", trading_date=_cli_instant(day).date(), timezone="UTC", phases=(phase,))


class _EmptyKline(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("cli-kline")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        return []


class _EmptyTick(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("cli-tick")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        return []


class _CliFastStrategy(BaseStrategy):
    supports_fast_backtest = True


def _build_cli_factory():
    strategy = _CliFastStrategy(
        "cli-fixture",
        timer=BaseTimeSelection("cli-timer", [_EmptyKline()], [_EmptyTick()]),
    )
    sessions = [_cli_session("20240102"), _cli_session("20240103")]
    return strategy, InMemoryGateway(bars=[], sessions=sessions)


def test_run_backtest_success_with_progress(monkeypatch) -> None:
    module = types.ModuleType("cli_fixture")
    module.build = _build_cli_factory
    monkeypatch.setitem(sys.modules, "cli_fixture", module)

    console = Console(record=True, force_terminal=False, width=100)
    state = SessionState()

    code = run_backtest(console, state, "cli_fixture:build", "20240102", "20240103", 100000, "fast", write_excel=False)

    assert code == 0
    assert "Backtest" in console.export_text()
