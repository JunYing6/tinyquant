from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from trading.factors.base import TickTimingFactor
from trading.factors.types import ExecutionRequest, SignalIntent


class IntentExecutorFactor(TickTimingFactor):
    execution_role = "intent_executor"
    accepted_intent_actions = frozenset({"BUY", "SELL"})

    def __init__(self) -> None:
        super().__init__("intent-executor")

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[Any]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_tick(self, tick: dict[str, Any], intents: Sequence[SignalIntent] = ()) -> list[ExecutionRequest]:
        requests = []
        for intent in intents:
            metadata = dict(intent.metadata)
            requests.append(ExecutionRequest(
                intent.code, intent.action, tick.get("time"),
                price=float(tick.get("price", 0.0)),
                volume=metadata.get("volume"),
                sizing_intent=metadata.get("sizing_intent"),
                order_type=metadata.get("order_type"),
                reason=intent.reason,
            ))
        return requests


__all__ = ["IntentExecutorFactor"]
