"""Port abstractions and value objects for the data-extension interface.

Task 3 of the unified data-extension interface.  The Protocols in this module
are the dependency-inversion boundary adapters implement against: historical,
realtime, calendar and recovery data flows.  The frozen value objects describe
adapter capabilities, request routing and runtime policy without coupling to a
concrete vendor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Callable,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    runtime_checkable,
)

from .contracts import (
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataRequest,
    DataSourceStateEvent,
    DataGapEvent,
    EventPosition,
    MarketEvent,
    StreamRequest,
)

OrderingGuarantee = Literal["none", "per_instrument", "global"]
PriceBasis = Literal["raw", "adjusted_forward", "adjusted_backward"]
Mode = str
SchemaVersion = str

SubscriptionState = Literal["created", "active", "cancelled", "failed", "paused"]

StreamEvent = MarketEvent | DataGapEvent | DataSourceStateEvent

_ORDERING = frozenset({"none", "per_instrument", "global"})
_GAP_ACTIONS = frozenset({"raise", "pause", "continue"})
_PRICE_BASES = frozenset({"raw", "adjusted_forward", "adjusted_backward"})


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        from types import MappingProxyType

        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(value)
    return value


# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------


@runtime_checkable
class HistoricalDataPort(Protocol):
    """Random-access historical reads over a dataset."""

    def read(self, request: DataRequest) -> DataBatch:
        raise NotImplementedError

    def iter(self, request: DataRequest, chunk_size: int = 10_000) -> Iterator[DataBatch]:
        raise NotImplementedError


@runtime_checkable
class RealtimeDataPort(Protocol):
    """Push/pull realtime event access."""

    def subscribe(self, request: StreamRequest, sink: Callable[[StreamEvent], None]) -> Subscription:
        raise NotImplementedError

    def poll(self, request: StreamRequest) -> Iterator[StreamEvent]:
        raise NotImplementedError


@runtime_checkable
class TradingCalendarPort(Protocol):
    """Trading-calendar session queries."""

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        raise NotImplementedError


@runtime_checkable
class RecoveryPort(Protocol):
    """Replay from a stream position to recover missed events."""

    def recover(self, request: StreamRequest, from_position: EventPosition) -> Iterator[MarketEvent]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetCapability:
    """What one adapter can serve for one dataset."""

    dataset: str
    modes: tuple[str, ...]
    asset_types: frozenset[str]
    frequencies: tuple[str, ...]
    fields: tuple[str, ...]
    point_in_time: bool
    max_range: str | None = None
    ordering_guarantee: OrderingGuarantee = "none"
    deduplication_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dataset, str) or not self.dataset.strip():
            raise ValueError("dataset must be a non-empty string")
        object.__setattr__(self, "modes", tuple(self.modes))
        object.__setattr__(self, "asset_types", frozenset(self.asset_types))
        object.__setattr__(self, "frequencies", tuple(self.frequencies))
        object.__setattr__(self, "fields", tuple(self.fields))
        if self.ordering_guarantee not in _ORDERING:
            raise ValueError(f"invalid ordering_guarantee: {self.ordering_guarantee!r}")


@dataclass(frozen=True)
class AdapterDescriptor:
    """Static description of an adapter's capabilities."""

    name: str
    datasets: Mapping[str, DatasetCapability]
    historical_modes: tuple[str, ...]
    realtime_modes: tuple[str, ...]
    supports_point_in_time: bool
    supported_price_basis: frozenset[str]
    supported_asset_types: frozenset[str]
    schema_versions: tuple[str, ...]
    ordering_guarantee: OrderingGuarantee = "none"
    deduplication_key: str | None = None
    source_revision: str = ""
    supports_recovery: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("adapter name must be a non-empty string")
        object.__setattr__(self, "datasets", _freeze(self.datasets))
        object.__setattr__(self, "historical_modes", tuple(self.historical_modes))
        object.__setattr__(self, "realtime_modes", tuple(self.realtime_modes))
        object.__setattr__(self, "supported_price_basis", frozenset(self.supported_price_basis))
        object.__setattr__(self, "supported_asset_types", frozenset(self.supported_asset_types))
        object.__setattr__(self, "schema_versions", tuple(self.schema_versions))
        if self.ordering_guarantee not in _ORDERING:
            raise ValueError(f"invalid ordering_guarantee: {self.ordering_guarantee!r}")
        if not self.supported_price_basis.issubset(_PRICE_BASES):
            raise ValueError("supported_price_basis contains unknown price basis")


@dataclass(frozen=True)
class DataBinding:
    """Maps a dataset to an adapter with routing priority."""

    dataset: str
    adapter: str
    priority: int
    modes: tuple[str, ...] = ()
    allow_fallback: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.dataset, str) or not self.dataset.strip():
            raise ValueError("dataset must be a non-empty string")
        if not isinstance(self.adapter, str) or not self.adapter.strip():
            raise ValueError("adapter must be a non-empty string")
        if isinstance(self.priority, bool) or not isinstance(self.priority, int) or self.priority < 1:
            raise ValueError("priority must be a positive integer")
        object.__setattr__(self, "modes", tuple(self.modes))


@dataclass(frozen=True)
class RouteOptions:
    """Per-request routing/fallback instructions."""

    adapter_name: str | None = None
    allow_fallback: bool = True
    fallback_on_empty: bool = False


@dataclass(frozen=True)
class DataPolicy:
    """Global runtime policy controlling validation and failure behaviour."""

    strict: bool = True
    fallback: bool = False
    fallback_on_empty: bool = False
    timezone: str = "Asia/Shanghai"
    max_retries: int = 2
    retry_backoff: float = 0.5
    gap_action: str = "raise"

    def __post_init__(self) -> None:
        if self.timezone:
            from zoneinfo import ZoneInfo

            try:
                ZoneInfo(self.timezone)
            except Exception as exc:
                raise ValueError(f"invalid IANA timezone: {self.timezone!r}") from exc
        if self.gap_action not in _GAP_ACTIONS:
            raise ValueError("gap_action must be one of raise/pause/continue")
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")
        if isinstance(self.retry_backoff, bool) or not isinstance(self.retry_backoff, (int, float)) or self.retry_backoff < 0:
            raise ValueError("retry_backoff must be a non-negative number")


@dataclass(frozen=True)
class Subscription:
    """Mutable handle to an active stream subscription."""

    state: SubscriptionState = "created"
    last_position: EventPosition | None = None
    error: str | None = None

    def cancel(self) -> None:
        """Idempotently cancel the subscription."""
        object.__setattr__(self, "state", "cancelled")

    def is_active(self) -> bool:
        return self.state in ("created", "active")


__all__ = [
    "AdapterDescriptor",
    "DataBinding",
    "DataPolicy",
    "DatasetCapability",
    "HistoricalDataPort",
    "OrderingGuarantee",
    "RealtimeDataPort",
    "RecoveryPort",
    "RouteOptions",
    "Subscription",
    "SubscriptionState",
    "TradingCalendarPort",
]
