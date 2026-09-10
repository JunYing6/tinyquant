"""Interactive live trading wizard."""

from __future__ import annotations

from rich.console import Console
from prompt_toolkit import PromptSession

from tinyquant_cli.render import render_error
from tinyquant_cli.runtime import SessionState
from tinyquant_cli.wizard import (
    WizardError,
    _ask,
    _ask_entry,
    _ask_kind,
    filter_by_kind,
    load_backtests,
    parse_capital,
    resolve_choice,
)

DEFAULT_LIVE_MODULE = "trading_nodes.live"
LIVE_MODULE_ENV = "TINYQUANT_LIVE_MODULE"

QUOTE_SOURCE_LABELS = {"mock": "本地模拟行情", "jvquant": "jvQuant 实时行情"}
EXECUTOR_LABELS = {"paper": "Paper 模拟撮合", "gm": "掘金量化实盘"}


def resolve_route(choices: dict, choice: str) -> str:
    text = choice.strip()
    if text.isdigit():
        index = int(text) - 1
        if 0 <= index < len(choices):
            return list(choices)[index]
        raise WizardError("choice is out of range")
    lowered = text.lower()
    for key in choices:
        if key.lower() == lowered or choices[key].lower() == lowered:
            return key
    raise WizardError(f"no such option: {choice!r}")


def _pick_route(session: PromptSession, console: Console, title: str, labels: dict) -> str:
    listing = "\n".join(f"  {index}. {labels[key]} ({key})" for index, key in enumerate(labels, 1))
    first = list(labels)[0]
    while True:
        raw = session.prompt(f"{title}:\n{listing}\n> ", default=first)
        try:
            return resolve_route(labels, raw)
        except WizardError as error:
            console.print(str(error))


def run_live_wizard(console: Console, state: SessionState, module_name: str | None = None) -> int:
    import os

    live_module = module_name or os.environ.get(LIVE_MODULE_ENV, DEFAULT_LIVE_MODULE)
    try:
        backtests = load_backtests()
    except WizardError as error:
        render_error(console, str(error))
        return 2
    session = PromptSession()
    try:
        kind = _ask_kind(session, console)
        items = filter_by_kind(backtests, kind)
        if not items:
            render_error(console, f"no backtests registered for kind {kind!r}")
            return 2
        chosen = _ask_entry(session, console, items)
        quote_route = _pick_route(session, console, "请选择行情源", QUOTE_SOURCE_LABELS)
        executor_route = _pick_route(session, console, "请选择执行器", EXECUTOR_LABELS)
        capital = _ask(session, console, "初始资金", "1000000", parse_capital)
    except WizardError as error:
        render_error(console, str(error))
        return 2
    except (EOFError, KeyboardInterrupt):
        console.print("实盘已取消", style="cyan")
        return 2

    try:
        module = __import__(live_module, fromlist=["build_live", "build_quote_client"])
        from tinyquant_cli.loading import load_backtest_factory

        entity, _ = load_backtest_factory(chosen["factory"])
        quote_client = module.build_quote_client(quote_route)
        gateway = module.build_realtime_gateway(quote_client)
        engine = module.build_live(entity, gateway, route=executor_route, initial_capital=capital)
    except Exception as error:
        render_error(console, f"{type(error).__name__}: {error}")
        return 1

    try:
        engine.start()
    except Exception as error:
        render_error(console, f"{type(error).__name__}: {error}")
        return 1
    return 0