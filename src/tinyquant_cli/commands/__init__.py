"""tinyquant CLI command definitions and handlers."""

from __future__ import annotations

import argparse
from typing import Any

from rich.console import Console

from tinyquant_cli.commands.backtest import run_backtest
from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.commands.examples import run_examples
from tinyquant_cli.commands.help import run_help
from tinyquant_cli.registry import Command, Registry
from tinyquant_cli.runtime import SessionState


def _configure_help(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("topic", nargs="?", help="optional command name")


def _help_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_help(console, args.topic)

    return handler


def _configure_examples(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="action")
    subparsers.add_parser("list", help="list bundled examples")
    run_parser = subparsers.add_parser("run", help="run a bundled example")
    run_parser.add_argument("name", choices=["backtest"])


def _examples_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        if args.action is None:
            return run_help(console, "examples")
        return run_examples(console, state, args.action, getattr(args, "name", None))

    return handler


def _configure_doctor(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--factory", help="optional module:function to validate")


def _doctor_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_doctor(console, args.factory)

    return handler


def _configure_backtest(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="action")
    run_parser = subparsers.add_parser("run", help="run a provider-driven backtest")
    run_parser.add_argument("factory", help="zero-argument module:function factory")
    run_parser.add_argument("--start", required=True, help="start date YYYYMMDD")
    run_parser.add_argument("--end", required=True, help="end date YYYYMMDD")
    run_parser.add_argument("--capital", type=float, default=1_000_000.0)
    run_parser.add_argument("--mode", choices=["auto", "fast", "tick"], default="auto")


def _backtest_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        if args.action is None:
            return run_help(console, "backtest")
        if args.action != "run":
            return 2
        return run_backtest(
            console,
            state,
            args.factory,
            args.start,
            args.end,
            args.capital,
            args.mode,
        )

    return handler


def build_registry(console: Console, state: SessionState) -> Registry:
    registry = Registry()
    registry.register(Command("help", "Run", "Show command help", ["?"], _configure_help, _help_handler(console, state)))
    registry.register(Command("backtest", "Run", "Run a provider-driven backtest", ["bt"], _configure_backtest, _backtest_handler(console, state)))
    registry.register(Command("examples", "Examples", "List or run bundled examples", ["ex"], _configure_examples, _examples_handler(console, state)))
    registry.register(Command("doctor", "Diagnostics", "Check the local runtime", ["diag"], _configure_doctor, _doctor_handler(console, state)))
    return registry


__all__ = ["build_registry"]
