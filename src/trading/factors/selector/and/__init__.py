from importlib import import_module

PriceAboveMaSelectionFactor = import_module(
    "trading.factors.selector.and.price_above_ma"
).PriceAboveMaSelectionFactor

__all__ = ["PriceAboveMaSelectionFactor"]
