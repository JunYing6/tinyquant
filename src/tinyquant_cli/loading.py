"""加载并校验用户提供的 tinyquant 工厂。"""

from __future__ import annotations

import importlib
import inspect
from typing import Any

from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream


class FactoryContractError(ValueError):
    """当回测工厂无法满足 CLI 契约时抛出。"""


def load_backtest_factory(
    path: str,
) -> tuple[BaseStrategy | BaseStream, Any]:
    module_name, separator, function_name = path.partition(":")
    if not separator or not module_name or not function_name or ":" in function_name:
        raise FactoryContractError("factory 必须采用 module:function 形式")
    try:
        module = importlib.import_module(module_name)
        factory = getattr(module, function_name)
    except (ImportError, AttributeError) as error:
        raise FactoryContractError(f"无法加载工厂 {path}: {error}") from error
    if not callable(factory):
        raise FactoryContractError(f"factory 不可调用: {path}")
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError) as error:
        raise FactoryContractError(f"无法检查工厂 {path}: {error}") from error
    if signature.parameters:
        raise FactoryContractError("回测 factory 不得声明参数")
    try:
        value = factory()
    except Exception as error:
        raise FactoryContractError(f"factory {path} 执行失败: {error}") from error
    if not isinstance(value, tuple) or len(value) != 2:
        raise FactoryContractError("factory 必须返回包含两项的元组")
    entity, data_gateway = value
    if not isinstance(entity, (BaseStrategy, BaseStream)):
        raise FactoryContractError("factory 第 1 项必须是 BaseStrategy 或 BaseStream")
    if not callable(getattr(data_gateway, "read", None)):
        raise FactoryContractError("factory 第 2 项必须实现 read(request)")
    if not callable(getattr(data_gateway, "sessions", None)):
        raise FactoryContractError("factory 第 2 项必须实现 sessions(request)")
    return entity, data_gateway
