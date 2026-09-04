"""Runtime quote-provider protocol for live trading adapters."""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class QuoteProvider(Protocol):
    def subscribe(
        self,
        codes: list[str],
        on_tick: Callable[[dict[str, Any]], None],
    ) -> None:
        """Subscribe a callback to quote updates for instrument codes."""

    def start(self) -> None:
        """Start quote delivery."""

    def stop(self) -> None:
        """Stop quote delivery."""
