"""Pure value objects shared by trading factors and execution code."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any


FACTOR_KINDS = frozenset(
    {
        "selection_binary",
        "selection_float",
        "timing_kline",
        "timing_tick",
        "risk_event",
        "risk_float",
        "risk_kline",
        "risk_tick",
    }
)
ACTIONS = frozenset({"BUY", "SELL"})
RISK_KINDS = frozenset({"event", "float"})


class ExecutionMode(StrEnum):
    """Execution behavior selected by the matching or broker layer."""

    LIMIT = "limit"
    BUY1 = "buy1"
    SELL1 = "sell1"
    UP_LIMIT = "up_limit"
    DOWN_LIMIT = "down_limit"
    MARKET = "market"


_FREQUENCY_PATTERN = re.compile(r"[1-9][0-9]*[mhd]\Z")


def normalize_frequency(value: str) -> str:
    """Validate and return a canonical K-line frequency."""
    if not isinstance(value, str) or _FREQUENCY_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "frequency must be a positive integer followed by m, h, or d"
        )
    return value


def _validate_finite_number(name: str, value: Any) -> None:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        finite = math.isfinite(value)
    except (TypeError, ValueError):
        finite = False
    if not finite:
        raise ValueError(f"{name} must be a finite number")


def _validate_action(action: str) -> None:
    if not isinstance(action, str) or action not in ACTIONS:
        raise ValueError(f"action must be one of {sorted(ACTIONS)}")


@dataclass(frozen=True)
class KlineBar:
    code: str
    frequency: str
    end_time: Any
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "frequency", normalize_frequency(self.frequency))
        for name in ("open", "high", "low", "close", "volume", "amount"):
            _validate_finite_number(name, getattr(self, name))
        if not self.low <= self.open <= self.high or not self.low <= self.close <= self.high:
            raise ValueError("OHLC values must satisfy low <= open/close <= high")
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.amount < 0:
            raise ValueError("amount must be non-negative")


@dataclass(frozen=True)
class SignalIntent:
    code: str
    action: str
    trigger_time: Any
    reason: str
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)
    execution_timing: str | None = None

    def __post_init__(self) -> None:
        _validate_action(self.action)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        if self.execution_timing not in (None, "open", "close"):
            raise ValueError("execution_timing must be None, 'open', or 'close'")


@dataclass(frozen=True)
class ExecutionRequest:
    code: str
    action: str
    time: Any
    price: float = 0.0
    volume: int | None = None
    sizing_intent: float | None = None
    order_type: str | None = None
    reason: str = ""
    mode: ExecutionMode | None = None

    def __post_init__(self) -> None:
        _validate_action(self.action)
        if self.mode is not None:
            if self.mode == ExecutionMode.LIMIT:
                _validate_finite_number("price", self.price)
                if self.price <= 0:
                    raise ValueError("limit mode requires a positive price")
            elif self.price not in (None, 0):
                _validate_finite_number("price", self.price)
                if self.price < 0:
                    raise ValueError("non-limit mode price must be non-negative")
        else:
            _validate_finite_number("price", self.price)
            if self.price <= 0:
                raise ValueError("price must be positive")

        if self.volume is not None and self.sizing_intent is not None:
            raise ValueError("volume and sizing_intent are mutually exclusive")
        if self.volume is not None:
            if isinstance(self.volume, bool) or not isinstance(self.volume, int):
                raise ValueError("volume must be a non-negative integer")
            if self.volume < 0:
                raise ValueError("volume must be non-negative")
        if self.sizing_intent is not None:
            _validate_finite_number("sizing_intent", self.sizing_intent)
            if self.sizing_intent <= 0:
                raise ValueError("sizing_intent must be positive")
            if self.action == "SELL" and self.sizing_intent > 1:
                raise ValueError("SELL sizing_intent must be in (0, 1]")


@dataclass(frozen=True)
class RiskSignal:
    """A factual risk signal; it does not place orders or mutate an account."""

    triggered: bool
    risk_kind: str
    value: float | None = None
    code: str | None = None
    time: Any = None
    reason: str = ""
    state: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))
