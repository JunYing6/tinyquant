"""Bundled example commands."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from tinyquant_cli.commands.backtest import run_backtest
from tinyquant_cli.runtime import SessionState


def run_examples(
    console: Console,
    state: SessionState,
    action: str,
    name: str | None,
) -> int:
    if action == "list":
        table = Table(title="tinyquant Examples", header_style="bold cyan")
        table.add_column("Name", style="cyan")
        table.add_column("Description")
        table.add_row("backtest", "Credential-free in-memory backtest")
        console.print(table)
        return 0
    if action == "run" and name == "backtest":
        return run_backtest(
            console,
            state,
            "tinyquant_cli.demos:build_demo_backtest",
            "20240102",
            "20240103",
            1_000_000.0,
            "fast",
        )
    console.print(f"Unknown example: {name or action}", style="red")
    return 2
