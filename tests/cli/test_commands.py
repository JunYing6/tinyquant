from __future__ import annotations

from rich.console import Console

from tinyquant_cli.commands.backtest import run_backtest
from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.commands.examples import run_examples
from tinyquant_cli.runtime import SessionState


def test_examples_list_and_backtest_render_without_credentials() -> None:
    console = Console(record=True, force_terminal=False)
    state = SessionState()

    assert run_examples(console, state, "list", None) == 0
    assert run_examples(console, state, "run", "backtest") == 0

    output = console.export_text()
    assert "in-memory backtest" in output.lower()
    assert state.last_final_equity is not None


def test_doctor_reports_core_runtime_status() -> None:
    console = Console(record=True, force_terminal=False)

    assert run_doctor(console, None) == 0

    output = console.export_text()
    assert "Python" in output
    assert "core imports" in output


def test_backtest_runs_factory_and_updates_session_state() -> None:
    console = Console(record=True, force_terminal=False)
    state = SessionState()

    code = run_backtest(
        console,
        state,
        "tinyquant_cli.demos:build_demo_backtest",
        "20240102",
        "20240103",
        1_000_000,
        "fast",
    )

    assert code == 0
    assert state.last_total_return is not None
    assert "Backtest" in console.export_text()


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
