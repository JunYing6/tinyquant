"""tinyquant CLI command definitions and handlers."""

from __future__ import annotations

import argparse
from typing import Any

from rich.console import Console

from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.commands.help import run_help
from tinyquant_cli.registry import Command, Registry
from tinyquant_cli.runtime import SessionState


def _configure_help(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("topic", nargs="?", help="optional command name")


def _help_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_help(console, args.topic)

    return handler


def _configure_doctor(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--factory", help="optional module:function to validate")


def _doctor_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_doctor(console, args.factory)

    return handler


def _backtest_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        from tinyquant_cli.wizard import run_backtest_wizard

        return run_backtest_wizard(console, state)

    return handler


def build_registry(console: Console, state: SessionState) -> Registry:
    registry = Registry()
    registry.register(Command("help", "Run", "Show command help", ["?"], _configure_help, _help_handler(console, state)))
    registry.register(Command("backtest", "Run", "交互式回测向导", ["bt"], None, _backtest_handler(console, state)))
    registry.register(Command("doctor", "Diagnostics", "Check the local runtime", ["diag"], _configure_doctor, _doctor_handler(console, state)))
    return registry


__all__ = ["build_registry"]
