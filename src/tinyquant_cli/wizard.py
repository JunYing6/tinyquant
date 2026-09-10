from __future__ import annotations

import importlib
import os
from datetime import datetime
from typing import Any, Callable

from rich.console import Console
from prompt_toolkit import PromptSession

from tinyquant_cli.render import render_error
from tinyquant_cli.runtime import SessionState

DEFAULT_MODULE = "trading_nodes.backtests"
MODULE_ENV = "TINYQUANT_BACKTEST_MODULE"
_VALID_MODES = {"auto", "fast", "tick"}


class WizardError(ValueError):
    pass


def default_module_name() -> str:
    return os.environ.get(MODULE_ENV, DEFAULT_MODULE)


def load_backtests(module_name: str | None = None) -> list[dict]:
    name = module_name or default_module_name()
    try:
        module = importlib.import_module(name)
    except Exception as error:
        raise WizardError(f"cannot load backtest registry {name!r}: {error}") from error
    backtests = getattr(module, "BACKTESTS", None)
    if not isinstance(backtests, list) or not backtests:
        raise WizardError(f"{name!r} does not define a non-empty BACKTESTS list")
    for index, entry in enumerate(backtests):
        if not isinstance(entry, dict) or not all(key in entry for key in ("name", "kind", "factory")):
            raise WizardError(f"BACKTESTS[{index}] must have name/kind/factory")
        if entry["kind"] not in ("strategy", "stream"):
            raise WizardError(f"BACKTESTS[{index}] kind must be 'strategy' or 'stream'")
        if not isinstance(entry["factory"], str) or ":" not in entry["factory"]:
            raise WizardError(f"BACKTESTS[{index}] factory must be 'module:function'")
    return backtests


def filter_by_kind(backtests: list[dict], kind: str) -> list[dict]:
    return [entry for entry in backtests if entry["kind"] == kind]


def resolve_choice(items: list[dict], choice: str) -> dict:
    text = choice.strip()
    if text.isdigit():
        index = int(text) - 1
        if 0 <= index < len(items):
            return items[index]
        raise WizardError("choice is out of range")
    lowered = text.lower()
    for entry in items:
        if entry["name"].lower() == lowered:
            return entry
    raise WizardError(f"no backtest named {choice!r}")


def parse_date(value: str) -> str:
    text = value.strip()
    try:
        return datetime.strptime(text, "%Y%m%d").strftime("%Y%m%d")
    except ValueError as error:
        raise WizardError(f"date must be YYYYMMDD, got {value!r}") from error


def validate_window(start: str, end: str) -> None:
    if start > end:
        raise WizardError(f"start date {start} must not be after end date {end}")


def parse_capital(value: str) -> float:
    try:
        capital = float(value.strip())
    except ValueError as error:
        raise WizardError(f"initial capital must be a number, got {value!r}") from error
    if capital <= 0:
        raise WizardError("initial capital must be positive")
    return capital


def parse_mode(value: str) -> str:
    mode = value.strip().lower()
    if mode not in _VALID_MODES:
        raise WizardError(f"mode must be one of {sorted(_VALID_MODES)}")
    return mode


def _ask(session: PromptSession, console: Console, label: str, default: str, parse: Callable[[str], Any]) -> Any:
    while True:
        raw = session.prompt(f"{label} [默认 {default}]: ", default=default)
        if not raw.strip():
            return default
        try:
            return parse(raw)
        except WizardError as error:
            console.print(str(error))


def _ask_kind(session: PromptSession, console: Console) -> str:
    while True:
        raw = session.prompt("回测类型 (1=策略, 2=Stream) [1]: ", default="1")
        value = raw.strip()
        if value in ("", "1"):
            return "strategy"
        if value in ("2", "stream"):
            return "stream"
        console.print("请输入 1(策略) 或 2(Stream)")


def _ask_entry(session: PromptSession, console: Console, items: list[dict]) -> dict:
    listing = "\n".join(f"  {index}. {entry['name']}" for index, entry in enumerate(items, 1))
    first = items[0]["name"]
    while True:
        raw = session.prompt(f"请选择 (编号或名称):\n{listing}\n> ", default=first)
        try:
            return resolve_choice(items, raw)
        except WizardError as error:
            console.print(str(error))


def _ask_excel(session: PromptSession, console: Console) -> bool:
    while True:
        raw = session.prompt("是否导出 Excel? (y/N): ", default="n")
        value = raw.strip().lower()
        if value in ("", "n"):
            return False
        if value == "y":
            return True
        console.print("请输入 y 或 N")


def run_backtest_wizard(console: Console, state: SessionState, module_name: str | None = None) -> int:
    try:
        backtests = load_backtests(module_name)
    except WizardError as error:
        render_error(console, str(error))
        return 2
    try:
        import prompt_toolkit  # noqa: F401
    except ImportError:
        render_error(console, "wizard requires prompt_toolkit; pip install tinyquant[cli]")
        return 2
    session = PromptSession()
    try:
        kind = _ask_kind(session, console)
        items = filter_by_kind(backtests, kind)
        if not items:
            render_error(console, f"no backtests registered for kind {kind!r}")
            return 2
        chosen = _ask_entry(session, console, items)
        start = _ask(session, console, "开始日期", "20240101", parse_date)
        end = _ask(session, console, "结束日期", "20241231", parse_date)
        validate_window(start, end)
        capital = _ask(session, console, "初始资金", "1000000", parse_capital)
        mode = _ask(session, console, "运行模式", "auto", parse_mode)
        excel = _ask_excel(session, console)
    except WizardError as error:
        render_error(console, str(error))
        return 2
    except (EOFError, KeyboardInterrupt):
        console.print("回测已取消", style="cyan")
        return 2
    from tinyquant_cli.commands.backtest import run_backtest

    return run_backtest(console, state, chosen["factory"], start, end, capital, mode, write_excel=excel)
