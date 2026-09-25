"""数据扩展接口的端口抽象与值对象。

统一数据扩展接口的任务 3。本模块中的 Protocols 是适配器所实现的依赖倒置边界：
历史、实时、日历与恢复四类数据流。冻结的值对象在不耦合具体厂商的前提下，
描述适配器能力、请求路由与运行时策略。
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
# 端口
# ---------------------------------------------------------------------------


@runtime_checkable
class HistoricalDataPort(Protocol):
    """对某数据集的随机访问历史读取。"""

    def read(self, request: DataRequest) -> DataBatch:
        raise NotImplementedError

    def iter(self, request: DataRequest, chunk_size: int = 10_000) -> Iterator[DataBatch]:
        raise NotImplementedError


@runtime_checkable
class RealtimeDataPort(Protocol):
    """实时事件的推送/拉取访问。"""

    def subscribe(self, request: StreamRequest, sink: Callable[[StreamEvent], None]) -> Subscription:
        raise NotImplementedError

    def poll(self, request: StreamRequest) -> Iterator[StreamEvent]:
        raise NotImplementedError


@runtime_checkable
class TradingCalendarPort(Protocol):
    """交易日历会话查询。"""

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        raise NotImplementedError


@runtime_checkable
class RecoveryPort(Protocol):
    """从某个流位置回放以恢复遗漏的事件。"""

    def recover(self, request: StreamRequest, from_position: EventPosition) -> Iterator[MarketEvent]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# 值对象
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetCapability:
    """某个适配器能为某个数据集提供什么能力。"""

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
    """适配器能力的静态描述。"""

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
    """以路由优先级将数据集映射到适配器。"""

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
    """按请求的路由/回退指令。"""

    adapter_name: str | None = None
    allow_fallback: bool = True
    fallback_on_empty: bool = False


@dataclass(frozen=True)
class DataPolicy:
    """控制校验与失败行为的全局运行时策略。"""

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
    """指向活跃流订阅的可变句柄。"""

    state: SubscriptionState = "created"
    last_position: EventPosition | None = None
    error: str | None = None

    def cancel(self) -> None:
        """幂等地取消订阅。"""
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
