"""Load and validate user-provided tinyquant factories."""

from __future__ import annotations

import importlib
import inspect
from typing import Any

from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream


class FactoryContractError(ValueError):
    """Raised when a backtest factory cannot satisfy the CLI contract."""


def load_backtest_factory(
    path: str,
) -> tuple[BaseStrategy | BaseStream, Any]:
    module_name, separator, function_name = path.partition(":")
    if not separator or not module_name or not function_name or ":" in function_name:
        raise FactoryContractError("factory must use module:function")
    try:
        module = importlib.import_module(module_name)
        factory = getattr(module, function_name)
    except (ImportError, AttributeError) as error:
        raise FactoryContractError(f"cannot load factory {path}: {error}") from error
    if not callable(factory):
        raise FactoryContractError(f"factory is not callable: {path}")
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError) as error:
        raise FactoryContractError(f"cannot inspect factory {path}: {error}") from error
    if signature.parameters:
        raise FactoryContractError("backtest factory must not declare arguments")
    try:
        value = factory()
    except Exception as error:
        raise FactoryContractError(f"factory {path} failed: {error}") from error
    if not isinstance(value, tuple) or len(value) != 2:
        raise FactoryContractError("factory must return a two-item tuple")
    entity, data_gateway = value
    if not isinstance(entity, (BaseStrategy, BaseStream)):
        raise FactoryContractError("factory item 1 must be BaseStrategy or BaseStream")
    if not callable(getattr(data_gateway, "read", None)):
        raise FactoryContractError("factory item 2 must implement read(request)")
    if not callable(getattr(data_gateway, "sessions", None)):
        raise FactoryContractError("factory item 2 must implement sessions(request)")
    return entity, data_gateway
