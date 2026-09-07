from __future__ import annotations

from rich.console import Console

from tinyquant_cli.commands.backtest import run_backtest
from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.runtime import SessionState


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
