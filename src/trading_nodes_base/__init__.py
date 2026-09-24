"""tinyquant 交易节点的基类与共享契约。

七个公开基类在此统一重导出，方便用户代码直接从包根导入。
重导出是惰性的（PEP 562）：仅导入 ``trading_nodes_base`` 不会拉取各分类
模块，从而让 ``tools.data`` 可以无环地导入 ``trading_nodes_base.types``。
"""

_EXPORT_MODULES = {
    "BaseFactor": "trading_nodes_base.factors",
    "BaseGridStrategy": "trading_nodes_base.strategies",
    "BaseMind": "trading_nodes_base.minds",
    "BaseRiskControl": "trading_nodes_base.methods",
    "BaseStockPicking": "trading_nodes_base.methods",
    "BaseStrategy": "trading_nodes_base.strategies",
    "BaseStream": "trading_nodes_base.streams",
    "BaseTimeSelection": "trading_nodes_base.methods",
}

__all__ = list(_EXPORT_MODULES)


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    return getattr(import_module(module_name), name)
