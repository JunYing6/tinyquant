from __future__ import annotations

import importlib
import os
from datetime import datetime

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
