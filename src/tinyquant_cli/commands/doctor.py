"""Credential-free tinyquant environment diagnostics."""

from __future__ import annotations

import sys

from rich.console import Console

from tinyquant_cli.loading import FactoryContractError, load_backtest_factory
from tinyquant_cli.render import render_doctor, render_error


def run_doctor(console: Console, factory_path: str | None) -> int:
    rows: dict[str, str] = {}
    healthy = True
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    rows["Python"] = version if sys.version_info >= (3, 11) else "requires >= 3.11"
    healthy = healthy and sys.version_info >= (3, 11)
    for name in ("numpy", "pandas"):
        try:
            module = __import__(name)
            rows[name] = f"installed {getattr(module, '__version__', 'unknown')}"
        except ImportError:
            rows[name] = "missing"
            healthy = False
    try:
        __import__("engines")
        __import__("trading")
        __import__("tools")
        rows["core imports"] = "ready"
    except Exception as error:
        rows["core imports"] = f"failed: {error}"
        healthy = False
    if factory_path:
        try:
            load_backtest_factory(factory_path)
            rows["factory"] = "ready"
        except FactoryContractError as error:
            rows["factory"] = f"failed: {error}"
            healthy = False
    render_doctor(console, rows)
    return 0 if healthy else 1
