"""In-memory backtest performance statistics."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd  # type: ignore[import-untyped]


def compute_stats(
    equity_curve: Sequence[Mapping[str, Any]],
    initial_capital: float,
    trade_log: Mapping[str, Sequence[Any]] | None = None,
) -> dict[str, float | int]:
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive")
    if not equity_curve:
        raise ValueError("equity_curve must not be empty")
    frame = pd.DataFrame(equity_curve)
    if "date" not in frame or "equity" not in frame:
        raise ValueError("equity_curve entries must contain date and equity")
    equity = pd.to_numeric(frame["equity"], errors="raise")
    if (equity <= 0).any():
        raise ValueError("equity values must be positive")
    returns = equity.pct_change().fillna(0.0)
    total_return = float(equity.iloc[-1] / initial_capital - 1)
    drawdowns = equity / equity.cummax() - 1
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    excess = returns
    sharpe = float(excess.mean() / excess.std(ddof=0) * np.sqrt(252)) if excess.std(ddof=0) else 0.0
    downside = excess[excess < 0]
    sortino = float(excess.mean() / np.sqrt((downside**2).mean()) * np.sqrt(252)) if not downside.empty else 0.0
    annualized_return = float((1 + total_return) ** (252 / len(equity)) - 1)
    max_drawdown = float(drawdowns.min())
    trade_count = sum(len(entries) for entries in (trade_log or {}).values())
    return {
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "annualized_return": annualized_return,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": annualized_return / abs(max_drawdown) if max_drawdown else 0.0,
        "win_rate": float((returns > 0).mean()),
        "final_equity": float(equity.iloc[-1]),
        "trading_days": len(equity),
        "trade_count": trade_count,
    }


__all__ = ["compute_stats"]
