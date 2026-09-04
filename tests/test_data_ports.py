from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from tools.data import (
    AdapterDescriptor,
    DataBinding,
    DataCache,
    DataContractError,
    DataError,
    DataGapError,
    DataPolicy,
    DataSourceError,
    DataUnavailableError,
    DatasetCapability,
    EventPosition,
    HistoricalDataPort,
    PointInTimeError,
    RealtimeDataPort,
    RecoveryPort,
    Subscription,
    SubscriptionState,
    TradingCalendarPort,
    UnsupportedDatasetError,
    DataRequest,
    cache_key,
)


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


def test_data_error_attributes_and_cause() -> None:
    cause = ValueError("inner")
    error = DataError(
        "boom",
        dataset="d",
        source="s",
        request_id="rid",
        retryable=True,
        partial=True,
        cause=cause,
    )
    assert error.message == "boom"
    assert error.dataset == "d"
    assert error.source == "s"
    assert error.request_id == "rid"
    assert error.retryable is True
    assert error.partial is True
    assert error.cause is cause
    assert isinstance(error, RuntimeError)


def test_data_error_auto_generates_request_id() -> None:
    first = DataError("a").request_id
    second = DataError("b").request_id
    assert first and second
    assert len(first) == 32
    assert first != second


def test_data_error_as_dict_excludes_cause() -> None:
    error = DataError("boom", dataset="d", cause=ValueError("inner"))
    payload = error.as_dict()
    assert "cause" not in payload
    assert payload["error_type"] == "DataError"
    assert payload["dataset"] == "d"
    assert payload["source"] is None
    assert payload["request_id"] == error.request_id
    assert payload["retryable"] is False
    assert payload["partial"] is False


def test_error_subclass_retryable_defaults() -> None:
    assert UnsupportedDatasetError("x").retryable is False
    assert DataContractError("x").retryable is False
    assert DataUnavailableError("x").retryable is True
    assert DataSourceError("x").retryable is True
    assert DataGapError("x").retryable is True
    assert PointInTimeError("x").retryable is False


def test_error_duplicate_retryable_kwarg_is_popped() -> None:
    assert DataUnavailableError("x", retryable=False).retryable is True
    assert DataGapError("x", retryable=False).retryable is True
    assert UnsupportedDatasetError("x", retryable=True).retryable is False
    assert DataContractError("x", retryable=True).retryable is False


def test_datasource_error_retryable_overridable() -> None:
    assert DataSourceError("x").retryable is True
    assert DataSourceError("x", retryable=False).retryable is False


def test_error_as_dict_is_json_serializable() -> None:
    import json

    payload = DataError("boom", dataset="d", request_id="r").as_dict()
    json.dumps(payload)


# ---------------------------------------------------------------------------
# ports (runtime_checkable protocols)
# ---------------------------------------------------------------------------


class _Historical:
    def read(self, request):
        raise NotImplementedError

    def iter(self, request, chunk_size=10_000):
        raise NotImplementedError


class _Realtime:
    def subscribe(self, request, sink):
        raise NotImplementedError

    def poll(self, request):
        raise NotImplementedError


class _Calendar:
    def sessions(self, request):
        raise NotImplementedError


class _Recovery:
    def recover(self, request, from_position):
        raise NotImplementedError


class _Empty:
    pass


def test_ports_are_runtime_checkable() -> None:
    assert isinstance(_Historical(), HistoricalDataPort)
    assert isinstance(_Realtime(), RealtimeDataPort)
    assert isinstance(_Calendar(), TradingCalendarPort)
    assert isinstance(_Recovery(), RecoveryPort)
    assert not isinstance(_Empty(), HistoricalDataPort)
    assert not isinstance(_Empty(), RealtimeDataPort)
    assert not isinstance(_Empty(), TradingCalendarPort)
    assert not isinstance(_Empty(), RecoveryPort)
    assert not isinstance(_Historical(), RealtimeDataPort)


def test_protocol_methods_raise_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        _Historical().read(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# DataPolicy
# ---------------------------------------------------------------------------


def test_datapolicy_defaults() -> None:
    policy = DataPolicy()
    assert policy.strict is True
    assert policy.fallback is False
    assert policy.fallback_on_empty is False
    assert policy.timezone == "Asia/Shanghai"
    assert policy.max_retries == 2
    assert policy.retry_backoff == 0.5
    assert policy.gap_action == "raise"


def test_datapolicy_validates_iana_timezone() -> None:
    assert DataPolicy(timezone="UTC").timezone == "UTC"
    DataPolicy(timezone="America/New_York")
    DataPolicy(timezone="Asia/Shanghai")
    with pytest.raises(ValueError):
        DataPolicy(timezone="Not/AZone")


def test_datapolicy_gap_action_three_states() -> None:
    for action in ("raise", "pause", "continue"):
        assert DataPolicy(gap_action=action).gap_action == action
    with pytest.raises(ValueError):
        DataPolicy(gap_action="ignore")


def test_datapolicy_validation() -> None:
    with pytest.raises(ValueError):
        DataPolicy(max_retries=-1)
    with pytest.raises(ValueError):
        DataPolicy(max_retries=True)
    with pytest.raises(ValueError):
        DataPolicy(retry_backoff=-0.1)


# ---------------------------------------------------------------------------
# Subscription
# ---------------------------------------------------------------------------


def test_subscription_initial_state_and_active() -> None:
    subscription = Subscription()
    assert subscription.state == "created"
    assert subscription.is_active()
    assert subscription.last_position is None
    assert subscription.error is None


def test_subscription_cancel_is_idempotent() -> None:
    subscription = Subscription()
    subscription.cancel()
    subscription.cancel()
    assert subscription.state == "cancelled"
    assert not subscription.is_active()


def test_subscription_state_variants() -> None:
    assert Subscription(state="active").is_active()
    assert not Subscription(state="paused").is_active()
    assert not Subscription(state="failed").is_active()
    assert not Subscription(state="cancelled").is_active()
    for state in ("created", "active", "cancelled", "failed", "paused"):
        assert Subscription(state=state).state == state  # type: ignore[arg-type]


def test_subscription_holds_position() -> None:
    subscription = Subscription(state="active", last_position=EventPosition(event_time=utc(2024, 1, 1, 9, 30)))
    assert subscription.last_position is not None


# ---------------------------------------------------------------------------
# DataBinding / value objects
# ---------------------------------------------------------------------------


def test_databinding_priority_must_be_positive_integer() -> None:
    assert DataBinding(dataset="d", adapter="a", priority=1).priority == 1
    DataBinding(dataset="d", adapter="a", priority=999)
    with pytest.raises(ValueError):
        DataBinding(dataset="d", adapter="a", priority=0)
    with pytest.raises(ValueError):
        DataBinding(dataset="d", adapter="a", priority=-1)
    with pytest.raises(ValueError):
        DataBinding(dataset="d", adapter="a", priority=True)


def test_adapter_descriptor_is_immutable() -> None:
    capability = DatasetCapability(
        dataset="market.bar",
        modes=("historical",),
        asset_types=frozenset({"equity"}),
        frequencies=("1d",),
        fields=("open",),
        point_in_time=False,
    )
    descriptor = AdapterDescriptor(
        name="adapter-a",
        datasets={"market.bar": capability},
        historical_modes=("read",),
        realtime_modes=(),
        supports_point_in_time=False,
        supported_price_basis=frozenset({"raw"}),
        supported_asset_types=frozenset({"equity"}),
        schema_versions=("1.0",),
        source_revision="rev1",
    )
    with pytest.raises(FrozenInstanceError):
        descriptor.name = "other"  # type: ignore[misc]
    with pytest.raises(TypeError):
        descriptor.datasets["x"] = capability  # type: ignore[index]
    assert descriptor.supports_recovery is False


def test_dataset_capability_validates_ordering() -> None:
    with pytest.raises(ValueError):
        DatasetCapability(
            dataset="d",
            modes=("m",),
            asset_types=frozenset({"equity"}),
            frequencies=(),
            fields=(),
            point_in_time=False,
            ordering_guarantee="bogus",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


def test_cache_key_is_stable_and_sha256() -> None:
    request = DataRequest(
        dataset="market.bar",
        instruments=("600000", "600001"),
        price_basis="raw",
        as_of=utc(2024, 1, 1, 12),
    )
    first = cache_key(request, "rev1", "adapter-a")
    second = cache_key(request, "rev1", "adapter-a")
    assert first == second
    assert len(first) == 64


def test_cache_key_includes_adapter_and_revision() -> None:
    request = DataRequest(dataset="market.bar")
    assert cache_key(request, "rev1", "a") != cache_key(request, "rev2", "a")
    assert cache_key(request, "rev1", "a") != cache_key(request, "rev1", "b")


def test_cache_key_ignores_delivery_key_and_correlation_id() -> None:
    first = DataRequest(dataset="market.bar", delivery_key="k1", correlation_id="c1")
    second = DataRequest(dataset="market.bar", delivery_key="k2", correlation_id="c2")
    assert cache_key(first, "rev", "a") == cache_key(second, "rev", "a")


def test_cache_key_is_order_insensitive_for_sets() -> None:
    first = DataRequest(dataset="market.bar", instruments=("600000", "600001"))
    second = DataRequest(dataset="market.bar", instruments=("600001", "600000"))
    assert cache_key(first, "rev", "a") == cache_key(second, "rev", "a")


def test_cache_key_differs_when_semantics_change() -> None:
    base = DataRequest(dataset="market.bar", as_of=utc(2024, 1, 1, 12))
    other = DataRequest(dataset="market.bar", as_of=utc(2024, 1, 1, 13))
    assert cache_key(base, "rev", "a") != cache_key(other, "rev", "a")


def test_datacache_protocol_runtime_checkable() -> None:
    class MemoryCache:
        def get(self, key):
            return None

        def put(self, key, value):
            pass

        def invalidate(self, key):
            pass

    assert isinstance(MemoryCache(), DataCache)
