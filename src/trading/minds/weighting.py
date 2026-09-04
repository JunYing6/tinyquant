from __future__ import annotations

from typing import Any

from trading.minds.base import BaseMind


class PerformanceWeightMind(BaseMind):
    def __init__(self, lookback: int = 5) -> None:
        super().__init__()
        if lookback < 1:
            raise ValueError("lookback must be positive")
        self.lookback = lookback

    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        raw: dict[str, float] = {}
        for name in strategies_performance:
            equities = [
                float(value["total_equity"])
                for value in self._performance_history.get(name, [])
                if isinstance(value.get("total_equity"), (int, float))
            ]
            raw[name] = self._score(equities)
        total = sum(raw.values())
        if total > 0:
            weights = {name: score / total for name, score in raw.items()}
        else:
            weights = {name: 1.0 / len(raw) if raw else 0.0 for name in strategies_performance}
        return self._validate_weights(weights)

    def _score(self, equities: list[float]) -> float:
        if len(equities) < 2:
            return 0.0
        window = equities[-self.lookback - 1:]
        first, last = window[0], window[-1]
        if first <= 0:
            return 0.0
        return max(last / first - 1.0, 0.0)


class KellyPositionMind(BaseMind):
    def __init__(
        self,
        empty_strategy_name: str = "empty-position",
        fraction: float = 0.5,
        max_strategy_weight: float = 1.0,
    ) -> None:
        super().__init__()
        if not 0 < fraction <= 1 or not 0 < max_strategy_weight <= 1:
            raise ValueError("fraction and max_strategy_weight must be in (0, 1]")
        self.empty_strategy_name = empty_strategy_name
        self.fraction = fraction
        self.max_strategy_weight = max_strategy_weight

    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        weights: dict[str, float] = {}
        for name, stats in strategies_performance.items():
            if name == self.empty_strategy_name:
                continue
            probability = self._number(stats.get("win_rate"))
            average_win = self._number(stats.get("avg_win"))
            average_loss = self._number(stats.get("avg_loss"))
            if average_loss <= 0 or average_win <= 0:
                weights[name] = 0.0
                continue
            payoff = average_win / average_loss
            kelly = probability - (1.0 - probability) / payoff
            weights[name] = min(self.max_strategy_weight, max(0.0, kelly * self.fraction))
        allocated = min(1.0, sum(weights.values()))
        remainder = max(0.0, 1.0 - allocated)
        if self.empty_strategy_name in strategies_performance:
            weights[self.empty_strategy_name] = remainder
        elif weights:
            scale = 1.0 / sum(weights.values()) if sum(weights.values()) else 0.0
            weights = {name: value * scale for name, value in weights.items()}
        return self._validate_weights(weights)

    @staticmethod
    def _number(value: Any) -> float:
        try:
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return 0.0


class MarketRegimeMind(BaseMind):
    def __init__(self, regime_strategies: dict[str, list[str]]) -> None:
        super().__init__()
        required = {"down", "sideways", "up"}
        if set(regime_strategies) != required:
            raise ValueError("regime_strategies must define down, sideways, and up")
        self.regime_strategies = {key: list(value) for key, value in regime_strategies.items()}
        self.current_regime = "sideways"

    def calculate_weights(
        self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        regime = str(market_data.get("regime", "sideways")).lower()
        if regime not in self.regime_strategies:
            regime = "sideways"
        self.current_regime = regime
        active = [name for name in self.regime_strategies[regime] if name in strategies_performance]
        if not active:
            return {name: 0.0 for name in strategies_performance}
        share = 1.0 / len(active)
        return self._validate_weights({name: share if name in active else 0.0 for name in strategies_performance})


__all__ = ["KellyPositionMind", "MarketRegimeMind", "PerformanceWeightMind"]
