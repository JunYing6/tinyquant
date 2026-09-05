"""Small in-memory gateway for tests, examples, and local CLI demos.

Covers the market primitives (``market.bar``, ``market.trade``,
``market.quote``), the trading calendar, and one point-in-time table dataset
(``fundamental.indicator`` by default).  Table rows are plain mappings that are
filtered by ``available_at <= as_of`` before being returned; rows without
``available_at`` are only served under a non-strict policy, and then with a
``W_PIT_MISSING`` quality warning attached to the batch.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping

from .contracts import DataGapEvent, DataSourceStateEvent, Session
from .contracts import Bar, CalendarBatch, CalendarRequest, DataBatch, DataProvenance, DataRequest, QualityReport, QualityWarning, StreamEvent, StreamRequest
from .datasets import default_catalog
from .errors import DataContractError, PointInTimeError, UnsupportedDatasetError
from .ports import AdapterDescriptor, DataPolicy, DatasetCapability, Subscription
from .quality import validate_batch, validate_request

DEFAULT_TABLE_DATASET = "fundamental.indicator"

_SUPPORTED_MARKET_DATASETS = frozenset({"market.bar", "market.trade", "market.quote"})
_TABLE_FILTERS = frozenset({"instrument_id", "asset_type", "report_date"})
_QUERY_DIM_FILTERS = frozenset({"start", "end", "as_of", "anchor", "fields", "codes", "event_types", "limit", "cursor"})


def _as_date(value: Any) -> Any:
    return value.date() if isinstance(value, datetime) else value


def _filter_values(request: DataRequest, key: str) -> frozenset[Any] | None:
    value = request.filters.get(key)
    if value is None:
        return None
    items = value if isinstance(value, tuple) else (value,)
    return frozenset(_as_date(item) for item in items)


class InMemoryGateway:
    def __init__(
        self,
        bars: Iterable[Bar] = (),
        events: Iterable[StreamEvent] = (),
        sessions: Iterable[Session] = (),
        data_policy: DataPolicy | None = None,
        table_rows: Iterable[Mapping[str, Any]] = (),
        table_dataset: str = DEFAULT_TABLE_DATASET,
    ) -> None:
        self.bars = tuple(bars)
        self.events = tuple(events)
        self._sessions = tuple(sessions)
        self.data_policy = data_policy if data_policy is not None else DataPolicy()
        self.table_dataset = table_dataset
        self._table_rows = tuple(dict(row) for row in table_rows)
        catalog = default_catalog()
        datasets = {}
        for name in ("market.bar", "market.trade", "market.quote"):
            definition = catalog.get(name)
            datasets[name] = DatasetCapability(name, ("historical", "push", "poll"), frozenset(definition.asset_types), (), tuple(definition.fields), definition.point_in_time, ordering_guarantee="global")
        try:
            table_definition = catalog.get(table_dataset)
        except KeyError as error:
            raise UnsupportedDatasetError(f"unknown table dataset {table_dataset!r}", dataset=table_dataset) from error
        datasets[table_dataset] = DatasetCapability(table_dataset, ("historical",), frozenset(table_definition.asset_types), (), tuple(table_definition.fields), table_definition.point_in_time, ordering_guarantee="global")
        calendar_definition = catalog.get("calendar.session")
        datasets["calendar.session"] = DatasetCapability("calendar.session", ("calendar",), frozenset(calendar_definition.asset_types), (), tuple(calendar_definition.fields), calendar_definition.point_in_time, ordering_guarantee="global")
        self.descriptor = AdapterDescriptor(
            name="memory", datasets=datasets, historical_modes=("historical",), realtime_modes=("push", "poll"),
            supports_point_in_time=True, supported_price_basis=frozenset(("raw", "adjusted_forward", "adjusted_backward")),
            supported_asset_types=frozenset(("equity", "index", "fund")), schema_versions=("1", "1.0"), ordering_guarantee="global",
        )
        self._sinks: dict[str, tuple[Callable[[StreamEvent], None], Callable[[StreamEvent], None]]] = {}

    @staticmethod
    def _batch(dataset: str, records: Iterable[object]) -> DataBatch:
        values = tuple(records)
        return DataBatch(request_id="memory", dataset=dataset, schema_version="1", correlation_id=None, records=values, complete=True, next_cursor=None, provenance=DataProvenance(adapter_name="memory", source_revision="1", request_fingerprint="memory", read_at=datetime.now(timezone.utc)), quality=QualityReport(status="ok", checked_count=len(values)))

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        records = tuple(session for session in self._sessions if request.start <= session.trading_date <= request.end)
        return self._batch("calendar.session", records)

    def subscribe(
        self,
        request: StreamRequest,
        sink: Callable[[StreamEvent], None],
        control_sink: Callable[[StreamEvent], None] | None = None,
    ) -> Subscription:
        subscription = Subscription(state="active")
        self._sinks[request.dataset] = (sink, control_sink or (lambda event: None))
        for event in self._matching(request):
            self._deliver_selected(request.dataset, event)
        return subscription

    def poll(self, request: StreamRequest) -> Iterator[StreamEvent]:
        for event in self._matching(request):
            yield event

    def emit(self, event: StreamEvent) -> None:
        dataset = getattr(event, "dataset", None)
        for key, (sink, control_sink) in self._sinks.items():
            if dataset is None or dataset == key or key == "market.trade" or key == "market.quote":
                self._deliver_to(key, sink, control_sink, event)

    def _deliver_selected(self, dataset: str, event: StreamEvent) -> None:
        sink, control_sink = self._sinks.get(dataset, (None, None))
        self._deliver_to(dataset, sink, control_sink, event)

    def _deliver_to(self, dataset: str, sink: Callable[[StreamEvent], None] | None, control_sink: Callable[[StreamEvent], None] | None, event: StreamEvent) -> None:
        if isinstance(event, (DataGapEvent, DataSourceStateEvent)):
            if control_sink is not None:
                control_sink(event)
        elif sink is not None:
            sink(event)

    def _matching(self, request: StreamRequest):
        for event in self.events:
            if request.instruments and getattr(event, "instrument_id", None) not in request.instruments:
                continue
            yield event

    def read(self, request: DataRequest) -> DataBatch:
        if request.dataset == self.table_dataset:
            return self._read_table(request)
        if request.dataset not in _SUPPORTED_MARKET_DATASETS:
            raise UnsupportedDatasetError(f"unsupported dataset {request.dataset!r}", dataset=request.dataset)
        records = self.bars if request.dataset == "market.bar" else tuple(event for event in self.events if type(event).__name__.lower().startswith(request.dataset.rsplit(".", 1)[1]))
        filtered = []
        for record in records:
            if request.instruments and record.instrument_id not in request.instruments:
                continue
            if request.as_of is not None and record.available_at is not None and record.available_at > request.as_of:
                continue
            if request.start is not None:
                point = record.interval_end if isinstance(record, Bar) else record.event_time
                if point < request.start:
                    continue
            if request.end is not None:
                point = record.interval_start if isinstance(record, Bar) else record.event_time
                if point > request.end:
                    continue
            if request.anchor is not None and record.trading_date != (request.anchor.date() if isinstance(request.anchor, datetime) else request.anchor):
                continue
            if request.session_window is not None:
                start, end = request.session_window
                point = record.event_time
                minute = point.hour * 100 + point.minute
                if not start <= minute <= end:
                    continue
            filtered.append(record)
        if request.fields:
            definition = default_catalog().get(request.dataset)
            unknown = set(request.fields) - set(definition.fields)
            if unknown:
                raise DataContractError(f"unknown fields: {sorted(unknown)}", dataset=request.dataset)
            if isinstance(filtered[0], Bar) if filtered else False:
                filtered = [replace(record, metadata={**record.metadata, "fields": request.fields}) for record in filtered]
        return self._batch(request.dataset, filtered)

    def _read_table(self, request: DataRequest) -> DataBatch:
        definition = default_catalog().get(request.dataset)
        validate_request(request, definition)
        unsupported = set(request.filters) - _TABLE_FILTERS - _QUERY_DIM_FILTERS
        if unsupported:
            raise DataContractError(
                f"filters {sorted(unsupported)} are not supported by the memory table adapter",
                dataset=request.dataset,
            )
        strict = self.data_policy.strict
        instrument_filter = self._instrument_filter(request)
        report_dates = _filter_values(request, "report_date")
        asset_types = _filter_values(request, "asset_type")
        selected: list[dict[str, Any]] = []
        pit_missing: list[int] = []
        for row in self._table_rows:
            if instrument_filter is not None and row.get("instrument_id") not in instrument_filter:
                continue
            if asset_types is not None and row.get("asset_type") not in asset_types:
                continue
            if report_dates is not None and _as_date(row.get("report_date")) not in report_dates:
                continue
            if request.as_of is not None:
                available_at = row.get("available_at")
                if available_at is None:
                    if strict:
                        raise PointInTimeError(
                            "record without available_at is not point-in-time visible",
                            dataset=request.dataset,
                        )
                    pit_missing.append(len(selected))
                    selected.append(row)
                    continue
                if available_at > request.as_of:
                    continue
            if request.start is not None:
                point = _as_date(row.get("report_date"))
                if point is not None and point < _as_date(request.start):
                    continue
            if request.end is not None:
                point = _as_date(row.get("report_date"))
                if point is not None and point >= _as_date(request.end):
                    continue
            selected.append(row)
        selected.sort(key=lambda row: tuple((row.get(name) is None, row.get(name)) for name in definition.ordering))
        batch = self._batch(request.dataset, selected)
        report = validate_batch(
            batch,
            definition,
            strict=strict,
            timezone=self.data_policy.timezone,
            as_of=request.as_of,
        )
        if pit_missing:
            report = QualityReport(
                status="warning",
                warnings=report.warnings
                + (
                    QualityWarning(
                        code="W_PIT_MISSING",
                        message=f"{len(pit_missing)} record(s) without available_at cannot be proven visible at as_of",
                        count=len(pit_missing),
                        severity="warning",
                        sample_keys=tuple(str(index) for index in pit_missing),
                    ),
                ),
                rejected_count=report.rejected_count + len(pit_missing),
                checked_count=report.checked_count,
            )
        return replace(batch, quality=report)

    def _instrument_filter(self, request: DataRequest) -> tuple[str, ...] | None:
        values = request.instruments
        extra = request.filters.get("instrument_id")
        if extra is None:
            return values
        extra_values = extra if isinstance(extra, tuple) else (extra,)
        if values is None:
            return tuple(extra_values)
        return tuple(code for code in values if code in set(extra_values))


__all__ = ["DEFAULT_TABLE_DATASET", "InMemoryGateway"]
