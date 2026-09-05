"""Small in-memory gateway for tests, examples, and local CLI demos."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Iterable

from .contracts import Bar, CalendarBatch, CalendarRequest, DataBatch, DataProvenance, DataRequest, QualityReport, Session, StreamEvent


class InMemoryGateway:
    def __init__(self, bars: Iterable[Bar] = (), events: Iterable[StreamEvent] = (), sessions: Iterable[Session] = ()) -> None:
        self.bars = tuple(bars)
        self.events = tuple(events)
        self._sessions = tuple(sessions)

    @staticmethod
    def _batch(dataset: str, records: Iterable[object]) -> DataBatch:
        values = tuple(records)
        return DataBatch(request_id="memory", dataset=dataset, schema_version="1", correlation_id=None, records=values, complete=True, next_cursor=None, provenance=DataProvenance(adapter_name="memory", source_revision="1", request_fingerprint="memory", read_at=datetime.now(timezone.utc)), quality=QualityReport(status="ok", checked_count=len(values)))

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        return self._batch("calendar.session", (session for session in self._sessions if request.start <= session.trading_date <= request.end))

    def read(self, request: DataRequest) -> DataBatch:
        start = request.start
        end = request.end
        if request.dataset == "market.bar":
            records = (bar for bar in self.bars if (not request.instruments or bar.instrument_id in request.instruments) and (start is None or bar.interval_end >= start) and (end is None or bar.interval_start <= end))
            return self._batch(request.dataset, records)
        if request.dataset in {"market.trade", "market.quote"}:
            records = (event for event in self.events if type(event).__name__.lower().startswith(request.dataset.rsplit(".", 1)[1]) and (not request.instruments or event.instrument_id in request.instruments) and (start is None or event.event_time >= start) and (end is None or event.event_time <= end))
            return self._batch(request.dataset, records)
        return self._batch(request.dataset, ())


__all__ = ["InMemoryGateway"]
