"""tinyquant CLI 命令定义与处理器。"""

from __future__ import annotations

import argparse
from typing import Any

from rich.console import Console

from tinyquant_cli.commands.doctor import run_doctor
from tinyquant_cli.commands.help import run_help
from tinyquant_cli.registry import Command, Registry
from tinyquant_cli.runtime import SessionState


def _configure_help(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("topic", nargs="?", help="可选的命令名")


def _help_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_help(console, args.topic)

    return handler


def _configure_doctor(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--factory", help="可选的要校验的 module:function")


def _doctor_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        return run_doctor(console, args.factory)

    return handler


def _backtest_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        from tinyquant_cli.wizard import run_backtest_wizard

        return run_backtest_wizard(console, state)

    return handler


def _live_handler(console: Console, state: SessionState):
    def handler(args: argparse.Namespace) -> int:
        from tinyquant_cli.live import run_live_wizard

        return run_live_wizard(console, state)

    return handler


def build_registry(console: Console, state: SessionState) -> Registry:
    registry = Registry()
    registry.register(Command("help", "Run", "显示命令帮助", ["?"], _configure_help, _help_handler(console, state)))
    registry.register(Command("backtest", "Run", "交互式回测向导", ["bt"], None, _backtest_handler(console, state)))
    registry.register(Command("live", "Run", "交互式实盘交易", [], None, _live_handler(console, state)))
    registry.register(Command("doctor", "Diagnostics", "检查本地运行时", ["diag"], _configure_doctor, _doctor_handler(console, state)))
    return registry


__all__ = ["build_registry"]
