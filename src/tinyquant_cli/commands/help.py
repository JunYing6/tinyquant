"""tinyquant 工作台的帮助命令。"""

from __future__ import annotations

from rich.console import Console

from tinyquant_cli.registry import Registry
from tinyquant_cli.render import render_error, render_help


def run_help(console: Console, topic: str | None = None) -> int:
    from tinyquant_cli.commands import build_registry
    from tinyquant_cli.runtime import SessionState

    registry = build_registry(console, SessionState())
    if topic:
        command = registry.resolve(topic)
        if command is None:
            suggestion = registry.suggest(topic)
            message = f"未知的帮助主题: {topic}"
            if suggestion:
                message += f"; 是否想输入: {suggestion}"
            render_error(console, message)
            return 2
        render_help(console, [command])
        return 0
    render_help(console, registry.commands())
    return 0
