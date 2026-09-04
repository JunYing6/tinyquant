"""Next-source-bar execution for fast backtests."""

from __future__ import annotations

from dataclasses import dataclass, field

from trading.factors.types import ExecutionRequest, KlineBar, SignalIntent


@dataclass
class _QueuedIntent:
    intent: SignalIntent
    source_seq: dict[str, int] = field(default_factory=dict)


class FastExecutionAdapter:
    def __init__(self, fast_buy_timing: str = "open", fast_sell_timing: str = "open") -> None:
        self._queued: list[_QueuedIntent] = []
        self._seq: dict[str, int] = {}
        self._timings = {"BUY": fast_buy_timing, "SELL": fast_sell_timing}
        if any(timing not in {"open", "close"} for timing in self._timings.values()):
            raise ValueError("fast execution timing must be open or close")

    def queue(self, intents: list[SignalIntent]) -> None:
        for intent in intents:
            self._queued.append(_QueuedIntent(intent, dict(self._seq)))

    def on_source_bar(self, bar: KlineBar) -> list[ExecutionRequest]:
        self._seq[bar.code] = self._seq.get(bar.code, 0) + 1
        ready: list[ExecutionRequest] = []
        remaining: list[_QueuedIntent] = []
        for record in self._queued:
            intent = record.intent
            if intent.code != bar.code or self._seq[bar.code] <= record.source_seq.get(bar.code, 0):
                remaining.append(record)
                continue
            timing = intent.execution_timing or self._timings[intent.action]
            if timing not in {"open", "close"}:
                raise ValueError("intent execution_timing must be open or close")
            ready.append(
                ExecutionRequest(
                    code=intent.code,
                    action=intent.action,
                    time=bar.end_time,
                    price=bar.open if timing == "open" else bar.close,
                    volume=intent.metadata.get("volume"),
                    sizing_intent=intent.metadata.get("sizing_intent"),
                    order_type=intent.metadata.get("order_type"),
                    reason=intent.reason,
                )
            )
        self._queued = remaining
        return ready

    def expire_day(self) -> list[SignalIntent]:
        expired = [record.intent for record in self._queued]
        self._queued = []
        return expired

    def expire_unavailable(self, available_codes: set[str]) -> list[SignalIntent]:
        expired: list[SignalIntent] = []
        remaining: list[_QueuedIntent] = []
        for record in self._queued:
            if record.intent.action == "BUY" and record.intent.code not in available_codes:
                expired.append(record.intent)
            else:
                remaining.append(record)
        self._queued = remaining
        return expired

    def assert_drained(self) -> None:
        if self._queued:
            raise RuntimeError("unconsumed fast intent(s)")


__all__ = ["FastExecutionAdapter"]
