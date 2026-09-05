"""Base classes and shared contracts for tinyquant trading nodes.

The seven public bases are re-exported here so user code can import them
directly from the package root.  The re-export is lazy (PEP 562): importing
``trading_nodes_base`` alone does not pull the category modules, which keeps
``tools.data`` free to import ``trading_nodes_base.types`` without a cycle.
"""

_EXPORT_MODULES = {
    "BaseFactor": "trading_nodes_base.factors",
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
