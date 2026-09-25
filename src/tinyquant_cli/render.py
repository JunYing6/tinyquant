"""tinyquant 量化工作台的 Rich 渲染。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme

from tinyquant_cli.registry import Command


WORKBENCH_THEME = Theme(
    {
        "label": "cyan",
        "positive": "green",
        "negative": "red",
        "warning": "yellow",
        "muted": "bright_black",
    }
)


def render_welcome(console: Console, status: Mapping[str, Any]) -> None:
    table = Table.grid(padding=(0, 2))
    table.add_row("Python", str(status.get("python", "unknown")))
    table.add_row("Core", str(status.get("core", "unknown")))
    table.add_row("CLI", str(status.get("cli", "ready")))
    console.print(
        Panel(
            Group("[bold cyan]tinyquant 1.1[/bold cyan]", table, "输入 [cyan]help[/cyan] 查看命令"),
            border_style="cyan",
            padding=(1, 2),
        )
    )


def render_help(console: Console, commands: Iterable[Command]) -> None:
    table = Table(title="tinyquant 命令", header_style="bold cyan")
    table.add_column("命令", style="cyan")
    table.add_column("别名", style="bright_black")
    table.add_column("分类", style="yellow")
    table.add_column("描述")
    for command in sorted(commands, key=lambda item: (item.category, item.name)):
        table.add_row(command.name, ", ".join(command.aliases) or "-", command.category, command.description)
    console.print(table)


def render_error(console: Console, message: str) -> None:
    console.print(Panel(message, title="错误", border_style="red", title_align="left"))


def render_doctor(console: Console, rows: Mapping[str, Any]) -> None:
    table = Table(title="运行时诊断", header_style="bold cyan")
    table.add_column("检查项", style="cyan")
    table.add_column("状态")
    for name, value in rows.items():
        rendered = str(value)
        style = "positive" if rendered.lower() in {"ok", "ready", "pass", "installed"} else "warning"
        table.add_row(name, f"[{style}]{rendered}[/{style}]")
    console.print(table)


def render_backtest(
    console: Console,
    stats: Mapping[str, Any],
    equity_curve: Iterable[Mapping[str, Any]],
    positions: Mapping[str, Any],
) -> None:
    metrics = Table(title="回测摘要", header_style="bold cyan")
    metrics.add_column("指标", style="cyan")
    metrics.add_column("数值", justify="right")
    for key in ("total_return", "max_drawdown", "sharpe", "final_equity", "trading_days", "trade_count"):
        if key not in stats:
            continue
        value = stats[key]
        style = "positive" if key == "total_return" and float(value) >= 0 else "negative" if key in {"total_return", "max_drawdown"} else "white"
        metrics.add_row(key, f"[{style}]{value}[/{style}]")

    equity = Table(title="近期权益", header_style="bold cyan")
    equity.add_column("日期", style="cyan")
    equity.add_column("权益", justify="right")
    for row in list(equity_curve)[-5:]:
        equity.add_row(str(row.get("date", row.get("trade_date", "-"))), str(row.get("equity", "-")))

    holdings = Table(title="持仓", header_style="bold cyan")
    holdings.add_column("代码", style="cyan")
    holdings.add_column("数量", justify="right")
    for code, volume in sorted(positions.items()):
        holdings.add_row(str(code), str(volume))
    console.print(Group(metrics, equity, holdings))


__all__ = ["WORKBENCH_THEME", "render_backtest", "render_doctor", "render_error", "render_help", "render_welcome"]
