"""Cache contract and deterministic cache-key derivation.

Task 3 of the unified data-extension interface.  :class:`DataCache` is the
provider-neutral cache boundary; :func:`cache_key` derives a stable, content
addressed key from the parts of a request that actually affect the served data,
deliberately excluding ``delivery_key``/``correlation_id`` so identical
semantic requests share one cache entry regardless of routing metadata.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Protocol, runtime_checkable

from .contracts import DataBatch, DataRequest, StreamRequest

__all__ = ["DataCache", "cache_key"]


@runtime_checkable
class DataCache(Protocol):
    """Key/value store mapping deterministic request keys to cached batches."""

    def get(self, key: str) -> DataBatch | None:
        raise NotImplementedError

    def put(self, key: str, value: DataBatch) -> None:
        raise NotImplementedError

    def invalidate(self, key: str) -> None:
        raise NotImplementedError


def _serialize(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    return value


def _identity_modes_values(request: DataRequest | StreamRequest) -> dict[str, object]:
    """Extract the request fields that determine the served data."""
    payload: dict[str, object] = {
        "dataset": request.dataset,
        "schema_version": getattr(request, "schema_version", None),
        "anchor": _serialize(getattr(request, "anchor", None)),
        "as_of": _serialize(getattr(request, "as_of", None)),
        "price_basis": getattr(request, "price_basis", None),
        "frequency": getattr(request, "frequency", None),
        "session": getattr(request, "session", None),
        "asset_type": getattr(request, "asset_type", None),
        "session_window": _serialize(getattr(request, "session_window", None)),
    }
    instruments = getattr(request, "instruments", None)
    if instruments is not None:
        payload["instruments"] = _serialize(sorted(instruments))
    fields = getattr(request, "fields", None)
    if fields is not None:
        payload["fields"] = _serialize(sorted(fields))
    event_types = getattr(request, "event_types", None)
    if event_types:
        payload["event_types"] = _serialize(sorted(event_types))
    start = getattr(request, "start", None)
    end = getattr(request, "end", None)
    if start is not None:
        payload["start"] = _serialize(start)
    if end is not None:
        payload["end"] = _serialize(end)
    filters = getattr(request, "filters", {}) or {}
    if filters:
        payload["filters"] = _serialize({str(k): v for k, v in sorted(filters.items())})
    cursor = getattr(request, "cursor", None)
    if cursor is not None:
        payload["cursor"] = cursor
    return {key: value for key, value in payload.items() if value is not None}


def cache_key(request: DataRequest | StreamRequest, source_revision: str, adapter_name: str) -> str:
    """Derive a deterministic sha256 cache key for a request."""
    payload: dict[str, object] = {
        "dataset": request.dataset,
        "adapter_name": adapter_name,
        "source_revision": source_revision,
        "schema_version": getattr(request, "schema_version", None),
        "request": _identity_modes_values(request),
    }
    payload = {key: value for key, value in payload.items() if value is not None}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()