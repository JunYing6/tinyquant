"""Immutable market data contracts and market-event value objects.

Task 2 of the unified data-extension interface.  These frozen dataclasses are
the provider-neutral contract layer: request objects validated on construction
(:class:`DataRequest`, :class:`StreamRequest`, :class:`CalendarRequest`) and
record envelopes emitted by adapters (:class:`Bar`, :class:`TradeTick`,
:class:`QuoteTick`, :class:`RegisteredEvent`, control events) carried in a
:class:`DataBatch`.

All time-valued fields are timezone-aware when they carry a ``datetime``;
mappings and sequences are deep-frozen on construction so instances stay
immutable end to end.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Generic, Literal, TypeVar

from trading.factors.types import normalize_frequency

PriceBasis = Literal["raw", "adjusted_forward", "adjusted_backward"]
PriceSide = Literal["BUY", "SELL", "UNKNOWN"]
Scalar = str | int | float | bool | date | datetime
FilterValue = Scalar | tuple[Scalar, ...]
JSONValue = str | int | float | bool | None | list | Mapping[str, "JSONValue"]

_PRICE_BASES: frozenset[str] = frozenset({"raw", "adjusted_forward", "adjusted_backward"})


def _freeze(value: Any, seen: set[int] | None = None) -> Any:
    """Recursively freeze mappings/sequences, raising ValueError on cycles."""
    if seen is None:
        seen = set()
    value_id = id(value)
    if value_id in seen:
        raise ValueError("circular reference detected in data structure")
    if isinstance(value, Mapping):
        seen.add(value_id)
        try:
            return MappingProxyType({key: _freeze(item, seen) for key, item in value.items()})
        finally:
            seen.discard(value_id)
    if isinstance(value, (list, tuple)):
        seen.add(value_id)
        try:
            return tuple(_freeze(item, seen) for item in value)
        finally:
            seen.discard(value_id)
    if isinstance(value, (set, frozenset)):
        seen.add(value_id)
        try:
            return frozenset(_freeze(item, seen) for item in value)
        finally:
            seen.discard(value_id)
    return value


def _require_tz(value: Any, name: str) -> None:
    if isinstance(value, datetime) and value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")


def _validate_finite(name: str, value: Any) -> None:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        finite = math.isfinite(value)
    except (TypeError, ValueError):
        finite = False
    if not finite:
        raise ValueError(f"{name} must be a finite number")


def _validate_non_negative(name: str, value: Any) -> None:
    _validate_finite(name, value)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _validate_book(levels: tuple[PriceLevel, ...], descending: bool) -> None:
    for index, level in enumerate(levels):
        if level.level != index + 1:
            raise ValueError("levels must start at 1 and increase consecutively")
        if index == 0:
            continue
        previous = levels[index - 1].price
        if descending and not (level.price < previous):
            raise ValueError("bid prices must strictly decrease")
        if not descending and not (level.price > previous):
            raise ValueError("ask prices must strictly increase")


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataRequest:
    dataset: str
    schema_version: str | None = None
    instruments: tuple[str, ...] | None = None
    anchor: date | datetime | None = None
    start: date | datetime | None = None
    end: date | datetime | None = None
    fields: tuple[str, ...] | None = None
    frequency: str | None = None
    event_types: tuple[str, ...] = ()
    as_of: datetime | None = None
    price_basis: PriceBasis = "raw"
    session: str | None = None
    filters: Mapping[str, FilterValue] = field(default_factory=dict)
    session_window: tuple[int, int] | None = None
    correlation_id: str | None = None
    delivery_key: str | None = None
    cursor: str | None = None
    limit: int | None = None
    asset_type: str | None = None

    @property
    def scope(self) -> str:
        return {"market.bar": "market/daily", "index.bar": "index/daily"}.get(self.dataset, self.dataset)

    def __getitem__(self, key: str) -> Any:
        value = self.get(key, None)
        if value is None:
            raise KeyError(key)
        return value

    def get(self, key: str, default: Any = None) -> Any:
        if self.delivery_key and "=" in self.delivery_key:
            import re
            if key in {"generation", "request_id"}:
                match = re.search(rf"(?:^|:){key}=(.*?)(?=:[A-Za-z_]\w*=|$)", self.delivery_key)
                if match:
                    return match.group(1)
            routing = dict(item.split("=", 1) for item in re.split(r":(?=[A-Za-z_]\w*=)", self.delivery_key) if "=" in item)
            if key in routing:
                value = routing[key]
                return int(value) if key == "idy" and value.isdigit() else value
        return getattr(self, key, default)

    def __post_init__(self) -> None:
        if not isinstance(self.dataset, str) or not self.dataset.strip():
            raise ValueError("dataset must be a non-empty string")
        if self.instruments is not None:
            object.__setattr__(self, "instruments", tuple(self.instruments))
        if self.fields is not None:
            object.__setattr__(self, "fields", tuple(self.fields))
        object.__setattr__(self, "event_types", tuple(self.event_types))
        if self.session_window is not None:
            object.__setattr__(self, "session_window", tuple(self.session_window))
        object.__setattr__(self, "filters", _freeze(self.filters))

        for name in ("anchor", "start", "end", "as_of"):
            _require_tz(getattr(self, name), name)

        if self.price_basis not in _PRICE_BASES:
            raise ValueError("price_basis must be one of raw/adjusted_forward/adjusted_backward")
        if self.frequency is not None:
            normalize_frequency(self.frequency)

        has_start = self.start is not None
        has_end = self.end is not None
        if has_start != has_end:
            raise ValueError("start and end must be provided together")
        if self.session_window is not None and self.anchor is None:
            raise ValueError("session_window requires an anchor")
        if (has_start or has_end) and (self.anchor is not None or self.session_window is not None):
            raise ValueError("start/end are mutually exclusive with anchor/session_window")

        if self.limit is not None:
            if isinstance(self.limit, bool) or not isinstance(self.limit, int) or self.limit <= 0:
                raise ValueError("limit must be a positive integer or None")


@dataclass(frozen=True)
class StreamRequest:
    dataset: Literal["market.trade", "market.quote"]
    instruments: tuple[str, ...]
    schema_version: str | None = None
    event_types: tuple[str, ...] = ()
    session: str | None = None
    fields: tuple[str, ...] | None = None
    start_position: EventPosition | None = None
    correlation_id: str | None = None
    delivery_key: str | None = None

    def __post_init__(self) -> None:
        if self.dataset not in ("market.trade", "market.quote"):
            raise ValueError("dataset must be market.trade or market.quote")
        object.__setattr__(self, "instruments", tuple(self.instruments))
        if not self.instruments:
            raise ValueError("instruments must be a non-empty tuple")
        object.__setattr__(self, "event_types", tuple(self.event_types))
        if self.fields is not None:
            object.__setattr__(self, "fields", tuple(self.fields))


@dataclass(frozen=True)
class EventPosition:
    event_time: datetime
    sequence: int | str | None = None

    def __post_init__(self) -> None:
        _require_tz(self.event_time, "event_time")


@dataclass(frozen=True)
class CalendarRequest:
    market: str
    start: date
    end: date
    timezone: str | None = None
    include_closed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.market, str) or not self.market.strip():
            raise ValueError("market must be a non-empty string")
        if self.start > self.end:
            raise ValueError("start must not exceed end")
        if not isinstance(self.include_closed, bool):
            raise TypeError("include_closed must be a bool")


# ---------------------------------------------------------------------------
# Provenance / quality
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class DataProvenance:
    adapter_name: str
    source_revision: str
    request_fingerprint: str
    read_at: datetime
    fallback_used: bool = False
    upstream_request: str | None = None

    def __post_init__(self) -> None:
        _require_tz(self.read_at, "read_at")


@dataclass(frozen=True, kw_only=True)
class QualityWarning:
    code: str
    message: str
    count: int = 1
    severity: Literal["info", "warning"] = "warning"
    sample_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code:
            raise ValueError("code must be a non-empty string")
        if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 1:
            raise ValueError("count must be a positive integer")
        if self.severity not in ("info", "warning"):
            raise ValueError("severity must be 'info' or 'warning'")
        object.__setattr__(self, "sample_keys", tuple(self.sample_keys))


@dataclass(frozen=True, kw_only=True)
class QualityReport:
    status: Literal["ok", "warning", "error"]
    warnings: tuple[QualityWarning, ...] = ()
    rejected_count: int = 0
    checked_count: int = 0

    def __post_init__(self) -> None:
        if self.status not in ("ok", "warning", "error"):
            raise ValueError("status must be ok/warning/error")
        object.__setattr__(self, "warnings", tuple(self.warnings))
        for name in ("rejected_count", "checked_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class TradingPhase:
    name: str
    start: datetime
    end: datetime
    accepts_trades: bool
    accepts_quotes: bool

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name must be a non-empty string")
        _require_tz(self.start, "start")
        _require_tz(self.end, "end")
        if self.start > self.end:
            raise ValueError("phase start must not exceed end")


@dataclass(frozen=True, kw_only=True)
class Session:
    market: str
    trading_date: date
    timezone: str
    phases: tuple[TradingPhase, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.market, str) or not self.market:
            raise ValueError("market must be a non-empty string")
        if not isinstance(self.timezone, str) or not self.timezone:
            raise ValueError("timezone must be a non-empty string")
        object.__setattr__(self, "phases", tuple(self.phases))
        if not self.phases:
            raise ValueError("phases must be non-empty")

    @property
    def open(self) -> datetime:
        return min(phase.start for phase in self.phases)

    @property
    def close(self) -> datetime:
        return max(phase.end for phase in self.phases)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class RecordEnvelope:
    schema_version: str
    event_id: str | None
    instrument_id: str | None
    asset_type: str | None
    effective_time: date | datetime | None
    event_time: datetime | None
    available_at: datetime | None
    trading_date: date | None
    source: str
    quality: Literal["valid", "warning"]
    metadata: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        _require_tz(self.effective_time, "effective_time")
        _require_tz(self.event_time, "event_time")
        _require_tz(self.available_at, "available_at")
        if self.quality not in ("valid", "warning"):
            raise ValueError("quality must be 'valid' or 'warning'")
        object.__setattr__(self, "metadata", _freeze(self.metadata))


@dataclass(frozen=True, kw_only=True, slots=True)
class PriceLevel:
    price: float
    size: float
    level: int

    def __post_init__(self) -> None:
        _validate_finite("price", self.price)
        if self.price <= 0:
            raise ValueError("price must be positive")
        _validate_non_negative("size", self.size)
        if isinstance(self.level, bool) or not isinstance(self.level, int) or self.level < 1:
            raise ValueError("level must be a positive integer")


@dataclass(frozen=True, kw_only=True)
class Bar(RecordEnvelope):
    frequency: str
    interval_start: datetime
    interval_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    is_complete: bool
    price_basis: PriceBasis

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "frequency", normalize_frequency(self.frequency))
        if self.price_basis not in _PRICE_BASES:
            raise ValueError("price_basis must be one of raw/adjusted_forward/adjusted_backward")
        _require_tz(self.interval_start, "interval_start")
        _require_tz(self.interval_end, "interval_end")
        if self.event_time is None:
            raise ValueError("event_time is required")
        if self.event_time != self.interval_end:
            raise ValueError("event_time must equal interval_end")
        if self.interval_start > self.interval_end:
            raise ValueError("interval_start must not exceed interval_end")
        for name in ("open", "high", "low", "close", "volume", "turnover"):
            _validate_finite(name, getattr(self, name))
        if not self.low <= self.open <= self.high or not self.low <= self.close <= self.high:
            raise ValueError("OHLC values must satisfy low <= open/close <= high")
        _validate_non_negative("volume", self.volume)
        _validate_non_negative("turnover", self.turnover)


@dataclass(frozen=True, kw_only=True)
class TradeTick(RecordEnvelope):
    event_type: Literal["trade"]
    price: float
    size: float
    turnover: float
    side: PriceSide
    sequence: int | str | None
    cumulative_volume: float | None = None
    cumulative_turnover: float | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.event_type != "trade":
            raise ValueError("event_type must be 'trade'")
        if self.side not in ("BUY", "SELL", "UNKNOWN"):
            raise ValueError("side must be one of BUY/SELL/UNKNOWN")
        _validate_finite("price", self.price)
        if self.price <= 0:
            raise ValueError("price must be positive")
        _validate_non_negative("size", self.size)
        _validate_non_negative("turnover", self.turnover)
        for name, value in (
            ("cumulative_volume", self.cumulative_volume),
            ("cumulative_turnover", self.cumulative_turnover),
        ):
            if value is not None:
                _validate_non_negative(name, value)


@dataclass(frozen=True, kw_only=True)
class QuoteTick(RecordEnvelope):
    event_type: Literal["quote"]
    bid_levels: tuple[PriceLevel, ...]
    ask_levels: tuple[PriceLevel, ...]
    last_price: float | None
    last_size: float | None
    sequence: int | str | None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.event_type != "quote":
            raise ValueError("event_type must be 'quote'")
        object.__setattr__(self, "bid_levels", tuple(self.bid_levels))
        object.__setattr__(self, "ask_levels", tuple(self.ask_levels))
        _validate_book(self.bid_levels, descending=True)
        _validate_book(self.ask_levels, descending=False)
        if self.last_price is not None:
            _validate_finite("last_price", self.last_price)
            if self.last_price <= 0:
                raise ValueError("last_price must be positive")
        if self.last_size is not None:
            _validate_non_negative("last_size", self.last_size)


@dataclass(frozen=True, kw_only=True)
class RegisteredEvent(RecordEnvelope):
    event_type: str
    values: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.event_type, str) or not self.event_type:
            raise ValueError("event_type must be a non-empty string")
        object.__setattr__(self, "values", _freeze(self.values))


MarketEvent = TradeTick | QuoteTick | RegisteredEvent


@dataclass(frozen=True)
class DataGapEvent:
    dataset: str
    instrument_id: str | None
    detected_at: datetime
    from_position: EventPosition | None
    to_position: EventPosition | None
    recoverable: bool
    reason: str

    def __post_init__(self) -> None:
        _require_tz(self.detected_at, "detected_at")


@dataclass(frozen=True)
class DataSourceStateEvent:
    dataset: str
    source: str
    state: str
    occurred_at: datetime
    error: str | None

    def __post_init__(self) -> None:
        _require_tz(self.occurred_at, "occurred_at")


StreamEvent = MarketEvent | DataGapEvent | DataSourceStateEvent

T = TypeVar("T")


@dataclass(frozen=True)
class DataBatch(Generic[T]):
    request_id: str
    dataset: str
    schema_version: str
    correlation_id: str | None
    records: tuple[T, ...]
    complete: bool
    next_cursor: str | None
    provenance: DataProvenance
    quality: QualityReport

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))
        if not self.complete:
            has_gap = any(warning.code == "DATA_GAP" for warning in self.quality.warnings)
            if self.next_cursor is None and not has_gap:
                raise ValueError("incomplete batch must provide next_cursor or a DATA_GAP warning")
        elif not self.records and self.next_cursor is not None:
            raise ValueError("a complete empty batch must have next_cursor None")

    def with_request_context(
        self,
        request_id: str,
        correlation_id: str | None,
        provenance: DataProvenance,
    ) -> DataBatch[T]:
        return DataBatch(
            request_id=request_id,
            dataset=self.dataset,
            schema_version=self.schema_version,
            correlation_id=correlation_id,
            records=self.records,
            complete=self.complete,
            next_cursor=self.next_cursor,
            provenance=provenance,
            quality=self.quality,
        )


TableBatch = DataBatch[Mapping[str, Any]]
CalendarBatch = DataBatch[Session]

__all__ = [
    "Bar",
    "CalendarBatch",
    "CalendarRequest",
    "DataBatch",
    "DataGapEvent",
    "DataProvenance",
    "DataRequest",
    "DataSourceStateEvent",
    "EventPosition",
    "FilterValue",
    "JSONValue",
    "MarketEvent",
    "PriceBasis",
    "PriceLevel",
    "PriceSide",
    "QualityReport",
    "QualityWarning",
    "QuoteTick",
    "RecordEnvelope",
    "RegisteredEvent",
    "Scalar",
    "Session",
    "StreamEvent",
    "StreamRequest",
    "TableBatch",
    "TradeTick",
    "TradingPhase",
]
