from dataclasses import FrozenInstanceError, fields
from datetime import date
from typing import Any, Callable, get_type_hints

import pytest

from tools.data_getter.market.errors import DataContractError
from tools.data_getter.market.schema import (
    DataRequest,
    DataSchemaRegistry,
    ParamSpec,
    RequestResolver,
    ResolvedRequest,
)
from tools.data_getter.providers import MarketDataProvider, TradingCalendarProvider
from tools.real_trade.providers import QuoteProvider
from tools.trade.providers import TradeExecutor
from trading.factors.types import ExecutionRequest, SignalIntent


def test_data_request_is_immutable_and_with_params_copies_values() -> None:
    request = DataRequest("trade_data", "daily", {"date": "20240102"}, "daily")

    with pytest.raises(FrozenInstanceError):
        request.kind = "tick"  # type: ignore[misc]

    with pytest.raises(TypeError):
        request.params["date"] = "20240103"  # type: ignore[index]

    copied = request.with_params(codes=["000001.SZ"])

    assert request.scope == "trade_data/daily"
    assert request["date"] == "20240102"
    assert copied.params == {"date": "20240102", "codes": ("000001.SZ",)}
    assert request.params == {"date": "20240102"}
    assert dict(request.items()) == {
        "idx": "daily",
        "scope": "trade_data/daily",
        "date": "20240102",
    }


def test_request_resolver_returns_a_pure_resolved_request_with_exact_fields() -> None:
    registry = DataSchemaRegistry()
    registry.register("trade_data/daily", [ParamSpec("date", required=True)])
    resolver = RequestResolver(registry)

    resolved = resolver.resolve(
        DataRequest("trade_data", "daily", {"date": "2024-01-02"})
    )
    resolved_from_dict = resolver.resolve(
        {"scope": "trade_data/daily", "params": {"date": "20240103"}}
    )

    assert [item.name for item in fields(ResolvedRequest)] == ["scope", "params"]
    assert resolved.scope == "trade_data/daily"
    assert resolved.params == {"date": "20240102"}
    assert resolved_from_dict.params == {"date": "20240103"}
    assert not hasattr(resolved, "fetcher")
    with pytest.raises(TypeError):
        resolved.params["date"] = "20240104"  # type: ignore[index]

    nested = ResolvedRequest(
        "example/data",
        {"codes": ["000001.SZ"], "options": {"levels": {"close"}}},
    )
    assert nested.params == {
        "codes": ("000001.SZ",),
        "options": {"levels": frozenset({"close"})},
    }
    with pytest.raises(TypeError):
        nested.params["options"]["levels"] = frozenset()
    with pytest.raises(AttributeError):
        nested.params["codes"].append("000002.SZ")


def test_data_request_with_params_deep_copies_nested_mutable_values() -> None:
    request = DataRequest(
        "trade_data",
        "daily",
        {"codes": ["000001.SZ"], "options": {"fields": ["close"]}},
    )
    derived = request.with_params(codes=["000002.SZ"])

    with pytest.raises(TypeError):
        derived.params["options"]["fields"] = ("close", "volume")
    with pytest.raises(AttributeError):
        derived.params["options"]["fields"].append("volume")

    assert request.params["codes"] == ("000001.SZ",)
    assert request.params["options"] == {"fields": ("close",)}
    assert derived.params["codes"] == ("000002.SZ",)
    assert request.params["options"] is not derived.params["options"]
    assert request.params["options"]["fields"] is not derived.params["options"]["fields"]


def test_provider_protocols_are_runtime_checkable() -> None:
    class MarketProvider:
        def fetch(self, request: DataRequest, date: date) -> Any:
            return None

    class CalendarProvider:
        def get_trade_dates(self, start: date, end: date) -> list[date]:
            return []

    class Quotes:
        def subscribe(self, codes: list[str], on_tick: Any) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    class Executor:
        def connect(self) -> None:
            pass

        def buy(self, symbol: str, volume: int, **kwargs: Any) -> Any:
            return None

        def sell(self, symbol: str, volume: int, **kwargs: Any) -> Any:
            return None

        def get_positions(self) -> list[Any]:
            return []

        def get_account(self) -> Any:
            return None

        def disconnect(self) -> None:
            pass

    assert isinstance(MarketProvider(), MarketDataProvider)
    assert isinstance(CalendarProvider(), TradingCalendarProvider)
    assert isinstance(Quotes(), QuoteProvider)
    assert isinstance(Executor(), TradeExecutor)

    assert get_type_hints(MarketDataProvider.fetch) == {
        "request": DataRequest,
        "date": str,
        "return": Any,
    }
    assert get_type_hints(TradingCalendarProvider.get_trade_dates) == {
        "start": str,
        "end": str,
        "return": list[str],
    }
    assert get_type_hints(QuoteProvider.subscribe) == {
        "codes": list[str],
        "on_tick": Callable[[dict[str, Any]], None],
        "return": type(None),
    }
    assert get_type_hints(TradeExecutor.buy)["return"] is Any
    assert get_type_hints(TradeExecutor.sell)["return"] is Any
    assert get_type_hints(TradeExecutor.get_positions)["return"] == list[Any]
    assert get_type_hints(TradeExecutor.get_account)["return"] is Any


@pytest.mark.parametrize("value", ["20240230", "2024-02-30", "202401", "bad", date(2024, 1, 2)])
def test_malformed_dates_raise_data_contract_error(value: object) -> None:
    registry = DataSchemaRegistry()
    registry.register("trade_data/daily", [ParamSpec("date", required=True)])

    with pytest.raises(DataContractError):
        registry.validate(DataRequest("trade_data", "daily", {"date": value}))


def test_schema_applies_defaults_none_rules_and_declared_types() -> None:
    registry = DataSchemaRegistry()
    registry.register(
        "example/data",
        [
            ParamSpec("required", required=True, type=str),
            ParamSpec("limit", type=int, default=10),
            ParamSpec("optional", type=str, allow_none=True),
            ParamSpec("strict_optional", type=str),
        ],
    )

    request = DataRequest("example", "data", {"required": "yes"})
    assert registry.validate(request) == {
        "required": "yes",
        "limit": 10,
    }
    assert registry.validate(
        DataRequest(
            "example",
            "data",
            {"required": "yes", "optional": None, "strict_optional": "ok"},
        )
    )["optional"] is None

    with pytest.raises(DataContractError):
        registry.validate(DataRequest("example", "data", {"required": 1}))
    with pytest.raises(DataContractError):
        registry.validate(
            DataRequest(
                "example",
                "data",
                {"required": "yes", "strict_optional": None},
            )
        )


def test_required_nullable_parameter_accepts_explicit_none_but_not_missing() -> None:
    registry = DataSchemaRegistry()
    registry.register(
        "example/data",
        [ParamSpec("value", required=True, type=str, allow_none=True)],
    )

    assert registry.validate(
        DataRequest("example", "data", {"value": None})
    ) == {"value": None}
    with pytest.raises(DataContractError):
        registry.validate(DataRequest("example", "data"))


def test_schema_validates_defaults_and_preserves_explicit_none_default() -> None:
    registry = DataSchemaRegistry()
    registry.register(
        "example/data",
        [
            ParamSpec("nullable", type=str, default=None, allow_none=True),
            ParamSpec("limit", type=int, default="ten"),
        ],
    )

    with pytest.raises(DataContractError, match="limit"):
        registry.validate(DataRequest("example", "data"))

    registry.register(
        "example/data",
        [ParamSpec("nullable", type=str, default=None, allow_none=True)],
    )
    assert registry.validate(DataRequest("example", "data")) == {"nullable": None}


def test_schema_requires_exact_declared_types_for_values_and_defaults() -> None:
    class IntSubclass(int):
        pass

    registry = DataSchemaRegistry()
    registry.register("example/data", [ParamSpec("value", type=int)])

    with pytest.raises(DataContractError):
        registry.validate(DataRequest("example", "data", {"value": True}))
    with pytest.raises(DataContractError):
        registry.validate(DataRequest("example", "data", {"value": IntSubclass(1)}))

    registry.register(
        "example/data",
        [ParamSpec("value", type=(int, str), default=IntSubclass(1))],
    )
    with pytest.raises(DataContractError):
        registry.validate(DataRequest("example", "data"))


def test_schema_supports_nested_classinfo_tuples_with_exact_types() -> None:
    class IntSubclass(int):
        pass

    registry = DataSchemaRegistry()
    registry.register(
        "example/data",
        [ParamSpec("value", type=(int, (str, bytes)))],
    )

    for value in (1, "text", b"text"):
        assert registry.validate(
            DataRequest("example", "data", {"value": value})
        ) == {"value": value}
    for value in (True, IntSubclass(1)):
        with pytest.raises(DataContractError):
            registry.validate(DataRequest("example", "data", {"value": value}))

    assert "(int, (str, bytes))" in registry.describe("example/data")


def test_schema_accepts_normalized_lists_but_rejects_direct_tuples() -> None:
    class ListSubclass(list[str]):
        pass

    class TupleSubclass(tuple[str, ...]):
        pass

    registry = DataSchemaRegistry()
    registry.register("example/data", [ParamSpec("codes", type=list)])

    assert registry.validate(
        DataRequest("example", "data", {"codes": ["000001.SZ"]})
    )["codes"] == ("000001.SZ",)
    with pytest.raises(DataContractError):
        registry.validate(
            DataRequest("example", "data", {"codes": ("000001.SZ",)})
        )
    with pytest.raises(DataContractError):
        registry.validate(
            DataRequest("example", "data", {"codes": ListSubclass(["000001.SZ"])})
        )
    with pytest.raises(DataContractError):
        registry.validate(
            DataRequest("example", "data", {"codes": TupleSubclass(["000001.SZ"])})
        )

    tuple_registry = DataSchemaRegistry()
    tuple_registry.register("example/data", [ParamSpec("codes", type=tuple)])
    assert tuple_registry.validate(
        DataRequest("example", "data", {"codes": ("000001.SZ",)})
    )["codes"] == ("000001.SZ",)
    with pytest.raises(DataContractError):
        tuple_registry.validate(
            DataRequest("example", "data", {"codes": TupleSubclass(["000001.SZ"])})
        )


def test_invalid_nested_classinfo_declaration_raises_contract_error() -> None:
    invalid_classinfo: Any = (int, ("bad",))

    with pytest.raises(DataContractError, match="declared type"):
        ParamSpec("value", type=invalid_classinfo)


def test_schema_copies_mutable_defaults_for_each_validation() -> None:
    registry = DataSchemaRegistry()
    registry.register(
        "example/data",
        [ParamSpec("options", type=dict, default={"fields": []})],
    )

    first = registry.validate(DataRequest("example", "data"))
    first["options"]["fields"].append("close")
    second = registry.validate(DataRequest("example", "data"))

    assert second == {"options": {"fields": []}}


def test_signal_intent_rejects_invalid_actions() -> None:
    with pytest.raises(ValueError, match="action"):
        SignalIntent("000001.SZ", "HOLD", "2024-01-02", "invalid")


def test_execution_request_rejects_volume_and_sizing_intent_together() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        ExecutionRequest(
            code="000001.SZ",
            action="BUY",
            time="2024-01-02",
            price=10.0,
            volume=100,
            sizing_intent=0.25,
        )
