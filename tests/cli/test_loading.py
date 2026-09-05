from __future__ import annotations

import sys
import types

import pytest

from tinyquant_cli.demos import build_demo_backtest
from tinyquant_cli.loading import FactoryContractError, load_backtest_factory
from trading.strategies.base import BaseStrategy


def test_loader_returns_factory_tuple_from_module_path(monkeypatch) -> None:
    module = types.ModuleType("cli_fixture")
    setattr(module, "build", build_demo_backtest)
    monkeypatch.setitem(sys.modules, "cli_fixture", module)

    entity, gateway = load_backtest_factory("cli_fixture:build")

    assert isinstance(entity, BaseStrategy)
    assert callable(gateway.read)
    assert callable(gateway.sessions)


@pytest.mark.parametrize("path", ["missing_separator", "cli_fixture:missing"])
def test_loader_rejects_invalid_factory_path(path, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "cli_fixture", types.ModuleType("cli_fixture"))

    with pytest.raises(FactoryContractError):
        load_backtest_factory(path)


def test_loader_rejects_non_tuple_return(monkeypatch) -> None:
    module = types.ModuleType("bad_fixture")
    setattr(module, "build", lambda: BaseStrategy)
    monkeypatch.setitem(sys.modules, "bad_fixture", module)

    with pytest.raises(FactoryContractError, match="two-item tuple"):
        load_backtest_factory("bad_fixture:build")


def test_loader_rejects_factory_with_optional_arguments(monkeypatch) -> None:
    module = types.ModuleType("parameterized_fixture")
    setattr(module, "build", lambda mode="fast": build_demo_backtest())
    monkeypatch.setitem(sys.modules, "parameterized_fixture", module)

    with pytest.raises(FactoryContractError, match="must not declare arguments"):
        load_backtest_factory("parameterized_fixture:build")
