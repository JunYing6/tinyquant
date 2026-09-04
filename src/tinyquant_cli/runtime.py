"""Session-only state for the interactive CLI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SessionState:
    last_total_return: float | None = None
    last_max_drawdown: float | None = None
    last_final_equity: float | None = None

    def update_backtest(self, stats: dict[str, float | int]) -> None:
        self.last_total_return = float(stats.get("total_return", 0.0))
        self.last_max_drawdown = float(stats.get("max_drawdown", 0.0))
        self.last_final_equity = float(stats.get("final_equity", 0.0))
