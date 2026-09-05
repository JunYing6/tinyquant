from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from typing import Any, Mapping

from tools.data import DataBatch, DataRequest
from tools.data_getter.market.schema import DataRequest as LegacyDataRequest


def delivery_key(*parts: Any) -> str:
    return ":".join(str(part) for part in parts if part is not None and str(part))


def encode_routing(**fields: Any) -> str:
    return delivery_key(*(f"{key}={fields[key]}" for key in sorted(fields) if fields[key] is not None))


def decode_routing(key: str) -> dict[str, str] | None:
    if not isinstance(key, str) or not key:
        return None
    result: dict[str, str] = {}
    for item in key.split(":"):
        if "=" not in item:
            continue
        name, value = item.split("=", 1)
        if not name:
            return None
        result[name] = value
    return result or None


def _value(query: Mapping[str, Any], name: str, default: Any = None) -> Any:
    if name in query:
        return query[name]
    params = query.get("params", {})
    return params.get(name, default) if isinstance(params, Mapping) else default


def canonical_request(query: Any, *, delivery_key: str | None = None, default_dataset: str | None = None) -> DataRequest:
    if isinstance(query, DataRequest):
        return replace(query, delivery_key=delivery_key or query.delivery_key) if delivery_key else query
    if isinstance(query, LegacyDataRequest):
        query = {"scope": query.scope, "params": dict(query.params), "idx": query.idx}
    if not isinstance(query, Mapping):
        raise TypeError(f"unsupported request: {query!r}")
    scope = query.get("scope", query.get("type"))
    if scope is None and query.get("domain") and query.get("kind"):
        scope = f"{query['domain']}/{query['kind']}"
    if scope is None and query.get("dataset") is not None:
        dataset = query["dataset"]
    elif isinstance(scope, str):
        mapping = {
            "market/daily": "market.bar", "market/tick": "market.trade",
            "index/daily": "index.bar", "index/member": "index.member",
            "trade_data/minute": "market.bar", "trade_data/daily": "market.bar",
            "technical/indicator": "derived.technical_indicator",
        }
        dataset = mapping.get(scope, default_dataset)
        if dataset is None:
            raise ValueError(f"unsupported external dataset scope: {scope}")
    else:
        dataset = default_dataset
    if dataset == "internal/sample" or str(dataset).startswith("internal/"):
        raise ValueError("internal control frames are not external data requests")
    if not dataset:
        raise ValueError("request dataset is required")
    frequency = _value(query, "frequency")
    if scope == "trade_data/minute":
        frequency = frequency or _value(query, "period") or "1m"
    if scope == "market/daily" and any(field in (_value(query, "fields", ()) or ()) for field in ("pe", "pb", "市盈率", "市净率")):
        dataset = "market.daily_snapshot"
    anchor = _value(query, "anchor", _value(query, "trade_date", _value(query, "date")))
    if isinstance(anchor, datetime) and anchor.tzinfo is None:
        from datetime import timezone
        anchor = anchor.replace(tzinfo=timezone.utc)
    windows = _value(query, "windows")
    fields = _value(query, "fields", _value(query, "period"))
    instruments = _value(query, "instruments", _value(query, "codes"))
    routed = delivery_key or query.get("delivery_key") or query.get("idx")
    return DataRequest(dataset=dataset, frequency=frequency, anchor=anchor,
                       session_window=tuple(windows) if windows is not None else None,
                       instruments=tuple(instruments) if instruments is not None else None,
                       fields=tuple(fields) if fields is not None and not isinstance(fields, str) else ((fields,) if fields else None),
                       delivery_key=routed)


def with_routing(request: DataRequest, **fields: Any) -> DataRequest:
    existing = decode_routing(request.delivery_key or "") or {}
    existing.update({key: str(value) for key, value in fields.items() if value is not None})
    return replace(request, delivery_key=encode_routing(**existing))


def table_records(batch: Any) -> tuple:
    if not isinstance(batch, DataBatch):
        raise TypeError("expected DataBatch")
    records = batch.records
    if not isinstance(records, tuple):
        raise TypeError("DataBatch records must be read-only")
    return records
