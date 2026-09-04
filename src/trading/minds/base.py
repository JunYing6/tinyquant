"""Strategy-combination mind base."""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from trading.streams.base import BaseStream


class BaseMind(ABC):
    def __init__(self) -> None:
        self.current_weights: Dict[str, float] = {}
        self._stream: Optional[BaseStream] = None
        self._performance_history: Dict[str, List[Dict[str, float]]] = {}

    @abstractmethod
    def calculate_weights(
        self,
        market_data: Dict[str, Any],
        strategies_performance: Dict[str, Dict[str, float]],
    ) -> Dict[str, float]:
        raise NotImplementedError

    def update_performance(self, strategy_name: str, performance_dict: Dict[str, float]) -> None:
        self._performance_history.setdefault(strategy_name, []).append(dict(performance_dict))

    def set_stream(self, stream: BaseStream) -> None:
        self._stream = stream

    def _validate_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        validated: Dict[str, float] = {}
        for name, weight in weights.items():
            value = float(weight)
            if not math.isfinite(value):
                raise ValueError(f"weight for {name!r} must be finite")
            validated[name] = max(0.0, min(1.0, value))
        total = sum(validated.values())
        if total > 1.0:
            validated = {name: weight / total for name, weight in validated.items()}
        return validated


__all__ = ["BaseMind"]
