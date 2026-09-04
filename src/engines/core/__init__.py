"""Pure in-memory execution utilities used by tinyquant engines."""

from engines.core.account import Account
from engines.core.fast_execution import FastExecutionAdapter
from engines.core.kline_aggregator import KlineAggregator
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.tick_matching import MatchingOrder, OrderStatus, TickMatchingEngine
from engines.core.trading_adapter import TradingContractAdapter
from engines.core.trading_clock import TradingDayContext

__all__ = [
    "Account",
    "DataProviderError",
    "FastExecutionAdapter",
    "KlineAggregator",
    "MatchingOrder",
    "OrderStatus",
    "SlippageModel",
    "TickMatchingEngine",
    "TradingContractAdapter",
    "TradingDayContext",
    "UnifiedDataPipeline",
    "compute_stats",
]
