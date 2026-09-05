from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    root = Path(__file__).parents[2]
    sys.path.insert(0, str(root))
    sys.path.insert(0, str(root / "src"))

from engines.fast import FastBacktestEngine
from examples.multi_strategy.data import InMemoryStrategyData
from trading.streams.multi_strategy import MultiStrategyStream
from trading.strategies.simple_strategies import (
    AtrStopStrategy,
    BreakoutRiskStrategy,
    BreakoutStrategy,
    DualMaStrategy,
    EmptyPositionStrategy,
    FilterPickStrategy,
    GoldenCrossStrategy,
    MeanReversionStrategy,
    MomentumPickStrategy,
)


def main() -> int:
    gateway = InMemoryStrategyData()
    dates = [session.trading_date.strftime("%Y%m%d") for session in gateway._sessions]
    strategies = [
        DualMaStrategy,
        BreakoutStrategy,
        MeanReversionStrategy,
        GoldenCrossStrategy,
        AtrStopStrategy,
        BreakoutRiskStrategy,
        MomentumPickStrategy,
        FilterPickStrategy,
        EmptyPositionStrategy,
    ]
    for strategy_type in strategies:
        engine = FastBacktestEngine(
            strategy_type(),
            "20240102",
            dates[-1],
            data_gateway=gateway,
            mode="fast",
            progress_bar=False,
        )
        engine.run()
        stats = engine.get_stats()
        print(f"{strategy_type.__name__}: final_equity={stats['final_equity']:.2f} trades={stats.get('trade_count', 0)}")

    stream = MultiStrategyStream()
    stream_engine = FastBacktestEngine(
        stream,
        "20240102",
        dates[-1],
        data_gateway=gateway,
        mode="fast",
        progress_bar=False,
    )
    stream_engine.run()
    print(f"stream weights: {stream.mind.current_weights}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())