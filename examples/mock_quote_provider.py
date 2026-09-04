"""Finite in-memory quote provider for safe live-engine examples."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class InMemoryQuoteProvider:
    def __init__(self, ticks: list[dict[str, Any]]) -> None:
        self.ticks = list(ticks)
        self._callback: Callable[[dict[str, Any]], None] | None = None

    def subscribe(
        self, codes: list[str], on_tick: Callable[[dict[str, Any]], None]
    ) -> None:
        self._callback = on_tick

    def start(self) -> None:
        if self._callback is None:
            raise RuntimeError("subscribe before start")
        for tick in self.ticks:
            self._callback(tick)

    def stop(self) -> None:
        return None
