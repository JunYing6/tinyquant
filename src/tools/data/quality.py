"""Pure validation functions over batches, events and point-in-time semantics.

Task 3 of the unified data-extension interface.  These functions are pure --
they never raise for *quality* findings unless ``strict`` is set, in which case
severe problems raise :class:`DataContractError` (or :class:`PointInTimeError`
for point-in-time violations).  Rejected rows are never silently dropped: each
is counted in :attr:`QualityReport.rejected_count` (deduplicated by record
index) and tagged with a stable warning code.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .contracts import DataBatch, DataRequest, QualityReport, QualityWarning
from .errors import DataContractError, PointInTimeError, UnsupportedDatasetError

_NON_NEGATIVE_UNITS = frozenset(
    {
        "price", "quantity", "currency", "factor", "count", "level", "revision",
        "years", "month", "year", "frequency",
    }
)

_QUERY_DIMS = frozenset(
    {
        "start", "end", "as_of", "anchor", "fields", "codes", "event_types",
        "limit", "cursor",
    }
)
_CROSS_TIME_FILTERS = frozenset(
    {
        "effective_time", "event_time", "trading_date", "available_at",
        "announcement_time", "event_date", "report_date", "observation_date",
        "rating_date",
    }
)

_WARNING_CODES = {
    "INVALID": "W_INVALID_FIELD",
    "DUP": "W_DUPLICATE_KEY",
    "PIT": "W_PIT_MISSING",
    "ORDER": "W_ORDERING",
    "SESSION": "W_SESSION",
    "TZ": "W_TIMEZONE",
    "GAP": "W_DATA_GAP",
}


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------


def _get_field(record: object, name: str) -> Any:
    if isinstance(record, Mapping):
        return record.get(name)
    return getattr(record, name, None)


def _is_missing(value: Any) -> bool:
    return value is None


def _check_type(value: Any, python_type: Any) -> bool:
    types = python_type if isinstance(python_type, tuple) else (python_type,)
    if isinstance(value, bool):
        return bool in types
    return isinstance(value, types)


def _matches_timezone(dt: datetime, target: ZoneInfo) -> bool:
    if dt.tzinfo is None:
        return True
    try:
        return dt.utcoffset() == dt.astimezone(target).utcoffset()
    except Exception:
        return True


def _pk_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        return repr(value) if not math.isnan(value) else "NaN"
    if isinstance(value, Mapping):
        return repr(tuple(sorted(value.items())))
    if isinstance(value, (list, tuple)):
        return tuple(_pk_value(item) for item in value)
    return value


# ---------------------------------------------------------------------------
# Per-record checks
# ---------------------------------------------------------------------------


def _check_fields(record: object, definition: Any) -> set[str]:
    problems: set[str] = set()
    for name in definition.required_fields:
        if _is_missing(_get_field(record, name)):
            problems.add("INVALID")
    for name, fd in definition.fields.items():
        value = _get_field(record, name)
        if _is_missing(value):
            continue
        if value is None:
            if not fd.nullable:
                problems.add("INVALID")
            continue
        if not _check_type(value, fd.python_type):
            problems.add("INVALID")
            continue
        if isinstance(value, float) and not isinstance(value, bool):
            if math.isnan(value) or math.isinf(value):
                problems.add("INVALID")
                continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if fd.unit in _NON_NEGATIVE_UNITS and value < 0:
                problems.add("INVALID")
    return problems


def _check_timezone(record: object, definition: Any, timezone: str | None) -> set[str]:
    if not timezone:
        return set()
    try:
        target = ZoneInfo(timezone)
    except Exception:
        return set()
    problems: set[str] = set()
    for name, fd in definition.fields.items():
        if fd.python_type is not datetime:
            continue
        value = _get_field(record, name)
        if isinstance(value, datetime) and value.tzinfo is not None:
            if not _matches_timezone(value, target):
                problems.add("TZ")
    return problems


def _check_session(record: object, definition: Any, session: Any) -> set[str]:
    if session is None:
        return set()
    problems: set[str] = set()
    event_time = _get_field(record, "event_time")
    if not isinstance(event_time, datetime):
        return set()
    trading_date = _get_field(record, "trading_date")
    if trading_date is not None and trading_date != session.trading_date:
        problems.add("SESSION")
    if not any(phase.start <= event_time <= phase.end for phase in session.phases):
        problems.add("SESSION")
    return problems


def _check_pit(record: object, definition: Any, as_of: datetime | None) -> set[str]:
    if not definition.point_in_time or as_of is None:
        return set()
    available_at = _get_field(record, "available_at")
    if isinstance(available_at, datetime) and available_at > as_of:
        return {"PIT"}
    return set()


def _primary_key(record: object, definition: Any) -> tuple[Any, ...]:
    return tuple(_pk_value(_get_field(record, name)) for name in definition.primary_key)


def _classify_sequence(seq: Any) -> str | tuple[str, float]:
    if seq is None:
        return "skip"
    if isinstance(seq, bool):
        return "skip"
    if isinstance(seq, (int, float)) and not isinstance(seq, bool):
        if isinstance(seq, float) and math.isnan(seq):
            return "skip"
        return ("cmp", float(seq))
    if isinstance(seq, str):
        s = seq.strip()
        if s == "" or s.upper() in ("MISSING", "NONE", "NULL"):
            return "skip"
        try:
            return ("cmp", float(s))
        except ValueError:
            return "uncomparable"
    return "uncomparable"


def _check_tick_ordering(records: Sequence[Any], definition: Any) -> dict[int, list[str]]:
    if definition.name not in ("market.trade", "market.quote"):
        return {}
    groups: dict[Any, list[int]] = {}
    for idx, record in enumerate(records):
        groups.setdefault(_get_field(record, "instrument_id"), []).append(idx)
    problems: dict[int, list[str]] = {}
    for indexes in groups.values():
        prev_time: datetime | None = None
        prev_seq: Any = None
        for idx in indexes:
            record = records[idx]
            event_time = _get_field(record, "event_time")
            if not isinstance(event_time, datetime):
                continue
            seq = _get_field(record, "sequence")
            if prev_time is not None:
                if event_time < prev_time:
                    problems.setdefault(idx, []).append("ORDER")
                elif event_time == prev_time:
                    cur = _classify_sequence(seq)
                    prev = _classify_sequence(prev_seq)
                    if cur == "skip" or prev == "skip":
                        pass
                    elif cur == "uncomparable" or prev == "uncomparable":
                        problems.setdefault(idx, []).append("ORDER")
                    elif cur[1] <= prev[1]:
                        problems.setdefault(idx, []).append("ORDER")
            prev_time = event_time
            prev_seq = seq
    return problems


def _warning_message(code: str, idx: int) -> str:
    return f"record {idx}: {_WARNING_CODES[code]} violation"


def _flush_warnings(accum: dict[str, tuple[str, set[str]]]) -> list[QualityWarning]:
    result = []
    for code, (message, keys) in sorted(accum.items()):
        result.append(
            QualityWarning(
                code=code,
                message=message,
                count=len(keys),
                severity="warning",
                sample_keys=tuple(sorted(keys)),
            )
        )
    return result


def _downgrade(
    idx: int,
    codes: set[str] | list[str],
    accum: dict[str, tuple[str, set[str]]],
    rejected: set[int],
    strict: bool,
    dataset: str,
    request_id: str,
) -> None:
    if not codes:
        return
    rejected.add(idx)
    for code in codes:
        if strict:
            if code == "PIT":
                raise PointInTimeError(_warning_message(code, idx), dataset=dataset, request_id=request_id)
            raise DataContractError(_warning_message(code, idx), dataset=dataset, request_id=request_id)
        warning_code = _WARNING_CODES[code]
        message, keys = accum.get(warning_code, (_warning_message(code, idx), set()))
        keys.add(str(idx))
        accum[warning_code] = (message, keys)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_request(request: DataRequest, definition: Any) -> None:
    """Validate that a request targets a known dataset with known fields."""
    if request.dataset != definition.name:
        raise UnsupportedDatasetError(
            f"dataset {request.dataset!r} is not described by definition {definition.name!r}",
            dataset=request.dataset,
        )
    for name in request.fields or ():
        if name not in definition.fields:
            raise DataContractError(
                f"field {name!r} is not part of dataset {definition.name!r}",
                dataset=request.dataset,
            )
    for key in request.filters:
        if key in _QUERY_DIMS or key in _CROSS_TIME_FILTERS:
            continue
        if key not in definition.filters:
            raise DataContractError(
                f"filter {key!r} is not valid for dataset {definition.name!r}",
                dataset=request.dataset,
            )


def validate_batch(
    batch: DataBatch,
    definition: Any,
    strict: bool = True,
    session: Any = None,
    timezone: str | None = None,
    as_of: datetime | None = None,
) -> QualityReport:
    """Validate every record in a batch, returning a quality report."""
    accum: dict[str, tuple[str, set[str]]] = {}
    rejected: set[int] = set()
    checked = 0
    seen_pk: set[tuple[Any, ...]] = set()
    for idx, record in enumerate(batch.records):
        checked += 1
        problems: set[str] = set()
        problems.update(_check_fields(record, definition))
        problems.update(_check_timezone(record, definition, timezone))
        problems.update(_check_session(record, definition, session))
        problems.update(_check_pit(record, definition, as_of))
        pk = _primary_key(record, definition)
        if pk in seen_pk:
            problems.add("DUP")
        else:
            seen_pk.add(pk)
        _downgrade(idx, problems, accum, rejected, strict, batch.dataset, batch.request_id)

    ordering = _check_tick_ordering(batch.records, definition)
    for idx, codes in ordering.items():
        _downgrade(idx, codes, accum, rejected, strict, batch.dataset, batch.request_id)

    warnings = _flush_warnings(accum)
    status = "warning" if warnings else "ok"
    return QualityReport(
        status=status,
        warnings=tuple(warnings),
        rejected_count=len(rejected),
        checked_count=checked,
    )


def validate_event(event: Any, definition: Any, strict: bool = True, session: Any = None, timezone: str | None = None) -> QualityReport:
    """Validate a single event record against a dataset definition."""
    accum: dict[str, tuple[str, set[str]]] = {}
    rejected: set[int] = set()
    problems: set[str] = set()
    problems.update(_check_fields(event, definition))
    problems.update(_check_timezone(event, definition, timezone))
    problems.update(_check_session(event, definition, session))
    _downgrade(0, problems, accum, rejected, strict, definition.name, "")
    warnings = _flush_warnings(accum)
    status = "warning" if warnings else "ok"
    return QualityReport(
        status=status,
        warnings=tuple(warnings),
        rejected_count=len(rejected),
        checked_count=1,
    )


def validate_point_in_time(records: Sequence[Any], as_of: datetime) -> None:
    """Raise :class:`PointInTimeError` if any record's ``available_at`` is after ``as_of``."""
    for record in records:
        available_at = _get_field(record, "available_at")
        if isinstance(available_at, datetime) and as_of is not None and available_at > as_of:
            raise PointInTimeError(
                f"available_at {available_at} is after as_of {as_of}",
                dataset=getattr(record, "dataset", None) if not isinstance(record, Mapping) else None,
            )


__all__ = [
    "validate_batch",
    "validate_event",
    "validate_point_in_time",
    "validate_request",
]
