"""Small in-memory gateway for tests, examples, and local CLI demos."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Callable, Iterable, Iterator

from .contracts import DataGapEvent, DataSourceStateEvent, Session
from .contracts import Bar, CalendarBatch, CalendarRequest, DataBatch, DataProvenance, DataRequest, QualityReport, StreamEvent, StreamRequest
from .datasets import default_catalog
from .errors import DataContractError, UnsupportedDatasetError
from .ports import AdapterDescriptor, DataPolicy, DatasetCapability, Subscription


class InMemoryGateway:
    def __init__(
        self,
        bars: Iterable[Bar] = (),
        events: Iterable[StreamEvent] = (),
        sessions: Iterable[Session] = (),
        data_policy: DataPolicy | None = None,
    ) -> None:
        self.bars = tuple(bars)
        self.events = tuple(events)
        self._sessions = tuple(sessions)
        self.data_policy = data_policy if data_policy is not None else DataPolicy()
        catalog = default_catalog()
        datasets = {}
        for name in ("market.bar", "market.trade", "market.quote"):
            definition = catalog.get(name)
            datasets[name] = DatasetCapability(name, ("historical", "push", "poll"), frozenset(definition.asset_types), (), tuple(definition.fields), definition.point_in_time, ordering_guarantee="global")
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
        if request.dataset not in {"market.bar", "market.trade", "market.quote"}:
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


__all__ = ["InMemoryGateway"]