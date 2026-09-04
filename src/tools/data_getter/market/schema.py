"""Immutable market-data requests and parameter schema validation."""

from __future__ import annotations

from datetime import datetime
from types import MappingProxyType
from dataclasses import dataclass, field
from copy import deepcopy
from typing import Any, Mapping, Optional, TypeAlias

from tools.data_getter.market.errors import DataContractError


_MISSING = object()
_NO_DEFAULT = object()


class _FrozenList(tuple[Any, ...]):
    """Tuple representation retaining that the source value was an exact list."""


class _FrozenListSubclass(tuple[Any, ...]):
    """Immutable representation of a list subclass, which is not exact ``list``."""


class _FrozenTupleSubclass(tuple[Any, ...]):
    """Immutable representation of a tuple subclass, which is not exact ``tuple``."""


ClassInfo: TypeAlias = type[Any] | tuple["ClassInfo", ...]


@dataclass(frozen=True)
class DataRequest:
    """A provider-neutral, declarative market-data request."""

    domain: str
    kind: str
    params: Mapping[str, Any] = field(default_factory=dict)
    idx: str = "001"

    def __post_init__(self) -> None:
        # Preserve list-like fields as tuples and mappings as read-only proxies.
        object.__setattr__(self, "params", _freeze(self.params))

    @property
    def scope(self) -> str:
        return f"{self.domain}/{self.kind}"

    def get(self, key: str, default: Any = None) -> Any:
        if key == "domain":
            return self.domain
        if key == "kind":
            return self.kind
        if key == "idx":
            return self.idx
        if key in ("scope", "type"):
            return self.scope
        if key == "period":
            return self.params.get("fields", default)
        if key == "trade_date":
            return self.params.get("date", default)
        return self.params.get(key, default)

    def __getitem__(self, key: str) -> Any:
        value = self.get(key, _MISSING)
        if value is _MISSING:
            raise KeyError(key)
        return value

    def items(self):
        yield "idx", self.idx
        yield "scope", self.scope
        yield from self.params.items()

    def with_params(self, **overrides: Any) -> DataRequest:
        merged = dict(self.params)
        merged.update(overrides)
        return DataRequest(self.domain, self.kind, merged, self.idx)


def coerce_data_request(query: DataRequest | Mapping[str, Any]) -> DataRequest:
    """Convert supported request declarations to one immutable request value."""
    if isinstance(query, DataRequest):
        return query
    if not isinstance(query, Mapping):
        raise TypeError(f"unsupported request: {query!r}")

    raw_params = query.get("params", {})
    if not isinstance(raw_params, Mapping):
        raise TypeError("request params must be a mapping")
    params = dict(raw_params)
    domain: Any
    kind: Any
    scope = query.get("scope")
    if scope is not None:
        if not isinstance(scope, str):
            raise ValueError("request scope must be a string")
        domain, separator, kind = scope.partition("/")
        if not separator or not domain or not kind or "/" in kind:
            raise ValueError("request scope must be a domain/kind string")
    else:
        domain = query.get("domain")
        kind = query.get("kind")
        if not isinstance(domain, str) or not isinstance(kind, str):
            legacy_type = query.get("type")
            if not isinstance(legacy_type, str):
                raise ValueError("request must contain scope or domain and kind")
            domain, separator, kind = legacy_type.partition("/")
            if not separator or not domain or not kind or "/" in kind:
                raise ValueError("request type must be a domain/kind string")
            if "date" in query:
                params.setdefault("date", query["date"])
            elif "trade_date" in query:
                params.setdefault("date", query["trade_date"])
            if "period" in query:
                params.setdefault("fields", query["period"])
            if "codes" in query:
                params.setdefault("codes", query["codes"])

    structural = {"domain", "kind", "scope", "params", "idx", "type"}
    if query.get("type") is None:
        params.update({key: value for key, value in query.items() if key not in structural})
    else:
        params.update({
            key: value
            for key, value in query.items()
            if key not in structural and key not in {"date", "trade_date", "period", "codes"}
        })
    return DataRequest(domain, kind, params, query.get("idx", "001"))


@dataclass(frozen=True)
class ResolvedRequest:
    """Validated request data with no provider-specific routing information."""

    scope: str
    params: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", _freeze(self.params))


@dataclass(frozen=True)
class ParamSpec:
    """Declaration for one request parameter."""

    name: str
    required: bool = False
    type: ClassInfo | None = None
    default: Any = _NO_DEFAULT
    desc: str = ""
    allow_none: bool = False

    def __post_init__(self) -> None:
        if self.type is not None:
            _flatten_classinfo(self.type)

    def describe(self) -> str:
        marker = (
            "required"
            if self.required
            else f"optional (default {self.default!r})"
            if self.default is not _NO_DEFAULT
            else "optional"
        )
        type_name = "any" if self.type is None else _describe_classinfo(self.type)
        return f"  {self.name:<12} {type_name:<8} {marker:<24} {self.desc}"


class DataSchemaRegistry:
    """Registry of parameter declarations keyed by ``domain/kind`` scope."""

    def __init__(self) -> None:
        self._schemas: dict[str, list[ParamSpec]] = {}

    def register(self, scope: str, params: list[ParamSpec]) -> None:
        self._schemas[scope] = list(params)

    def get(self, scope: str) -> list[ParamSpec]:
        return list(self._schemas.get(scope, []))

    def has(self, scope: str) -> bool:
        return scope in self._schemas

    def describe(self, scope: str) -> str:
        lines = [f"Parameters ({scope}):"]
        lines.extend(spec.describe() for spec in self.get(scope))
        return "\n".join(lines)

    def validate(self, request: DataRequest) -> dict[str, Any]:
        specs = {spec.name: spec for spec in self.get(request.scope)}
        params = dict(request.params)

        for spec in specs.values():
            if spec.default is not _NO_DEFAULT:
                self._validate_value(request.scope, spec, spec.default)

        missing = [
            name
            for name, spec in specs.items()
            if spec.required
            and (name not in params or (params[name] is None and not spec.allow_none))
        ]
        if missing:
            raise DataContractError(
                f"{request.scope} missing required parameters {sorted(missing)}\n"
                f"{self.describe(request.scope)}"
            )

        for name, spec in specs.items():
            if name not in params:
                if spec.default is not _NO_DEFAULT:
                    params[name] = deepcopy(spec.default)
                continue
            value = params[name]
            if value is None:
                if spec.allow_none:
                    continue
                if spec.default is not _NO_DEFAULT:
                    params[name] = deepcopy(spec.default)
                else:
                    raise DataContractError(
                        f"{request.scope} parameter {name!r} does not allow None"
                    )
                continue
            self._validate_value(request.scope, spec, value)

        self._normalize_dates(request.scope, params)
        return params

    @staticmethod
    def _validate_value(scope: str, spec: ParamSpec, value: Any) -> None:
        if value is None:
            if spec.allow_none:
                return
            raise DataContractError(
                f"{scope} parameter {spec.name!r} does not allow None"
            )
        if spec.type is not None:
            declared_types = _flatten_classinfo(spec.type)
            if type(value) in declared_types:
                return
            if list in declared_types and type(value) is _FrozenList:
                return
            type_name = _describe_classinfo(spec.type)
            raise DataContractError(
                f"{scope} parameter {spec.name!r} must be {type_name}, "
                f"got {type(value).__name__}"
            )

    @staticmethod
    def _normalize_dates(scope: str, params: dict[str, Any]) -> None:
        if "date" in params and params["date"] is not None:
            params["date"] = _normalize_day(params["date"], "date")
        if scope == "calendar/trade_cal":
            for name in ("start", "end"):
                if name in params and params[name] is not None:
                    params[name] = _normalize_day(params[name], name)


def _normalize_day(value: Any, name: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if not isinstance(value, str):
        raise DataContractError(f"{name} must be a string: {value!r}")
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        date_format = "%Y-%m-%d"
    elif len(value) == 8 and value.isdigit():
        date_format = "%Y%m%d"
    else:
        raise DataContractError(
            f"{name} must use YYYYMMDD or YYYY-MM-DD: {value!r}"
        )
    try:
        return datetime.strptime(value, date_format).strftime("%Y%m%d")
    except ValueError as exc:
        raise DataContractError(f"{name} is not a valid date: {value!r}") from exc


class RequestResolver:
    """Pure request converter and validator with no provider routing."""

    def __init__(self, registry: DataSchemaRegistry | None = None) -> None:
        self.registry = registry or DataSchemaRegistry()

    def resolve(self, query: DataRequest | Mapping[str, Any]) -> ResolvedRequest:
        try:
            request = self._to_request(query)
            params = self.registry.validate(request)
        except DataContractError:
            raise
        except (TypeError, ValueError) as exc:
            raise DataContractError(str(exc)) from exc
        return ResolvedRequest(request.scope, params)

    @staticmethod
    def _to_request(query: DataRequest | Mapping[str, Any]) -> DataRequest:
        try:
            return coerce_data_request(query)
        except (TypeError, ValueError) as exc:
            raise DataContractError(str(exc)) from exc


def _flatten_classinfo(classinfo: ClassInfo) -> tuple[type[Any], ...]:
    if isinstance(classinfo, tuple):
        flattened: list[type[Any]] = []
        for item in classinfo:
            flattened.extend(_flatten_classinfo(item))
        return tuple(flattened)
    if isinstance(classinfo, type):
        return (classinfo,)
    raise DataContractError(f"invalid declared type {classinfo!r}")


def _describe_classinfo(classinfo: ClassInfo) -> str:
    if isinstance(classinfo, tuple):
        values = ", ".join(_describe_classinfo(item) for item in classinfo)
        if len(classinfo) == 1:
            values += ","
        return f"({values})"
    return classinfo.__name__


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, _FrozenList):
        return _FrozenList(_freeze(item) for item in value)
    if isinstance(value, _FrozenListSubclass):
        return _FrozenListSubclass(_freeze(item) for item in value)
    if type(value) is list:
        return _FrozenList(_freeze(item) for item in value)
    if isinstance(value, list):
        return _FrozenListSubclass(_freeze(item) for item in value)
    if type(value) is tuple:
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return _FrozenTupleSubclass(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value
