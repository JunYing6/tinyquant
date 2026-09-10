from __future__ import annotations

import sys
import types

import pytest

from tinyquant_cli.wizard import (
    WizardError,
    default_module_name,
    filter_by_kind,
    load_backtests,
    parse_capital,
    parse_date,
    parse_mode,
    resolve_choice,
    validate_window,
)


def _module_with(backtests):
    module = types.ModuleType("fake_registry")
    module.BACKTESTS = backtests
    return module


def test_load_backtests_returns_validated_list(monkeypatch) -> None:
    module = _module_with([{"name": "a", "kind": "strategy", "factory": "mod:a"}])
    monkeypatch.setitem(sys.modules, "fake_registry", module)

    result = load_backtests("fake_registry")

    assert result == [{"name": "a", "kind": "strategy", "factory": "mod:a"}]


def test_load_backtests_missing_module(monkeypatch) -> None:
    monkeypatch.delitem(sys.modules, "no_such_registry", raising=False)

    with pytest.raises(WizardError, match="cannot load"):
        load_backtests("no_such_registry")


def test_load_backtests_rejects_bad_shape(monkeypatch) -> None:
    for bad in (None, [], [{"name": "x"}]):
        module = types.ModuleType("bad_registry")
        module.BACKTESTS = bad
        monkeypatch.setitem(sys.modules, "bad_registry", module)
        with pytest.raises(WizardError):
            load_backtests("bad_registry")


def test_filter_by_kind_separates_strategy_and_stream() -> None:
    items = [
        {"name": "s1", "kind": "strategy", "factory": "m:s1"},
        {"name": "st1", "kind": "stream", "factory": "m:st1"},
    ]
    assert [i["name"] for i in filter_by_kind(items, "strategy")] == ["s1"]
    assert [i["name"] for i in filter_by_kind(items, "stream")] == ["st1"]


def test_resolve_choice_by_number_and_name() -> None:
    items = [{"name": "双均线", "kind": "strategy", "factory": "m:a"}]
    assert resolve_choice(items, "1")["factory"] == "m:a"
    assert resolve_choice(items, "双均线")["factory"] == "m:a"


def test_resolve_choice_rejects_invalid() -> None:
    items = [{"name": "a", "kind": "strategy", "factory": "m:a"}]
    with pytest.raises(WizardError):
        resolve_choice(items, "9")
    with pytest.raises(WizardError):
        resolve_choice(items, "不存在")


def test_parse_date_and_window() -> None:
    assert parse_date("20240101") == "20240101"
    with pytest.raises(WizardError):
        parse_date("202401")
    validate_window("20240101", "20241231")
    with pytest.raises(WizardError):
        validate_window("20250101", "20240101")


def test_parse_capital_and_mode() -> None:
    assert parse_capital("1000000") == 1_000_000.0
    with pytest.raises(WizardError):
        parse_capital("-1")
    with pytest.raises(WizardError):
        parse_capital("abc")
    assert parse_mode("FAST") == "fast"
    with pytest.raises(WizardError):
        parse_mode("bogus")


def test_default_module_name(monkeypatch) -> None:
    assert default_module_name() == "trading_nodes.backtests"
    monkeypatch.setenv("TINYQUANT_BACKTEST_MODULE", "other.registry")
    assert default_module_name() == "other.registry"