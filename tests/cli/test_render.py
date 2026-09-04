from __future__ import annotations

from rich.console import Console

from tinyquant_cli.render import render_error, render_help, render_welcome
from tinyquant_cli.registry import Command


def test_render_workbench_surfaces_semantic_content() -> None:
    console = Console(record=True, force_terminal=False)
    commands = [Command("doctor", "Diagnostics", "Check runtime", ["diag"], None, lambda args: 0)]

    render_welcome(console, {"python": "3.12", "core": "ready"})
    render_help(console, commands)
    render_error(console, "factory failed")

    output = console.export_text()
    assert "tinyquant 1.1" in output
    assert "Diagnostics" in output
    assert "factory failed" in output
