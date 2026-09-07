from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from tinyquant_cli.loading import FactoryContractError, load_backtest_factory
from tools.data import InMemoryGateway
from trading_nodes_base.factors import KlineTimingFactor, TickTimingFactor
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy


class _EmptyKline(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("loader-kline")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        return []


class _EmptyTick(TickTimingFactor):
    execution_role = "intent_executor"

    def __init__(self) -> None:
        super().__init__("loader-tick")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list:
        self._data_clear()
        self.sign["fit"] = True
        return []


def _build_loader_fixture() -> tuple[BaseStrategy, InMemoryGateway]:
    strategy = BaseStrategy(
        "loader-fixture",
        timer=BaseTimeSelection("loader-timer", [_EmptyKline()], [_EmptyTick()]),
    )
    return strategy, InMemoryGateway(bars=[], sessions=[])


def test_loader_returns_factory_tuple_from_module_path(monkeypatch) -> None:
    module = types.ModuleType("cli_fixture")
    setattr(module, "build", _build_loader_fixture)
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
    setattr(module, "build", lambda mode="fast": _build_loader_fixture())
    monkeypatch.setitem(sys.modules, "parameterized_fixture", module)

    with pytest.raises(FactoryContractError, match="must not declare arguments"):
        load_backtest_factory("parameterized_fixture:build")
