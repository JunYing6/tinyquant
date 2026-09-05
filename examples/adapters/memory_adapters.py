"""Credential-free data adapters built on the tinyquant ports.

This module is the reference implementation of how to turn any in-memory
``dict`` data source into tinyquant's unified data interface.

Two adapters are provided:

* :class:`MemoryHistoricalAdapter` -- implements :class:`HistoricalDataPort`
  (``read`` + ``iter``) over the ``market.bar`` dataset.
* :class:`MemoryCalendarAdapter` -- implements :class:`TradingCalendarPort`
  (``sessions``) over the ``calendar.session`` dataset.

Each adapter exposes a ``descriptor`` (:class:`AdapterDescriptor`) that lets a
real :class:`DataGateway` validate capability declarations and route requests.
The canonical record envelopes (``Bar``, ``Session``, ``DataBatch``) come from
``tools.data``; the adapters only speak those contracts, never vendor types.

``python examples/adapters/memory_adapters.py`` runs a tiny round trip through
a real :class:`DataGateway` assembled from these adapters.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:  # allow running this file directly from repo root
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from tools.data import (
    AdapterDescriptor,
    Bar,
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataBinding,
    DataGateway,
    DataPolicy,
    DataProvenance,
    DataRequest,
    DatasetCapability,
    QualityReport,
    Session,
    TradingPhase,
    default_catalog,
)


def _utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Historical adapter: dict[instrument -> list[Bar]] over market.bar
# ---------------------------------------------------------------------------


class MemoryHistoricalAdapter:
    """Adapt a ``dict`` bar source to :class:`HistoricalDataPort`.

    The source layout is ``{instrument_id: [Bar, ...]}`` (or a single
    :class:`Bar`/a flat :class:`Sequence` if ``instruments`` is not provided)
    plus an optional ``trading_dates`` sequence of ``datetime.date`` used to
    report calendar sessions.
    """

    name = "memory-historical"

    def __init__(
        self,
        bars: dict[str, Sequence[Bar]] | Sequence[Bar] | None = None,
        *,
        schema_version: str = "1.0",
        source_revision: str = "demo-1",
        timezone: str = "UTC",
    ) -> None:
        self.name = self.name
        self._bars = bars or {}
        self.schema_version = schema_version
        self.source_revision = source_revision
        self.timezone = timezone

        catalog = default_catalog()
        bar_def = catalog.get("market.bar")
        session_def = catalog.get("calendar.session")

        self.descriptor = AdapterDescriptor(
            name=self.name,
            datasets={
                "market.bar": DatasetCapability(
                    dataset="market.bar",
                    modes=("historical",),
                    asset_types=frozenset(bar_def.asset_types),
                    frequencies=("1d",),
                    fields=tuple(bar_def.fields),
                    point_in_time=bar_def.point_in_time,
                    ordering_guarantee="per_instrument",
                ),
                "calendar.session": DatasetCapability(
                    dataset="calendar.session",
                    modes=("calendar",),
                    asset_types=frozenset(session_def.asset_types),
                    frequencies=(),
                    fields=(),
                    point_in_time=False,
                ),
            },
            historical_modes=("historical",),
            realtime_modes=(),
            supports_point_in_time=bar_def.point_in_time,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset(bar_def.asset_types),
            schema_versions=(schema_version,),
            ordering_guarantee="per_instrument",
            source_revision=source_revision,
            supports_recovery=False,
        )

    # -- HistoricalDataPort -------------------------------------------------

    def _matching_bars(self, request: DataRequest) -> list[Bar]:
        catalog = default_catalog()
        bar_def = catalog.get("market.bar")
        if isinstance(self._bars, Sequence) and not isinstance(self._bars, dict):
            source_items: dict[str, list[Bar]] = {}
            for bar in self._bars:
                if bar.instrument_id is not None:
                    source_items.setdefault(bar.instrument_id, []).append(bar)
        else:
            source_items = {
                instrument: list(item if isinstance(item, Sequence) else [item])
                for instrument, item in self._bars.items()
            }

        if request.fields:
            unknown = set(request.fields) - set(bar_def.fields)
            if unknown:
                raise ValueError(f"unknown fields: {sorted(unknown)}")

        result: list[Bar] = []
        instruments = set(request.instruments or ())
        for instrument, records in source_items.items():
            if instruments and instrument not in instruments:
                continue
            for bar in records:
                if request.start is not None and bar.interval_end < request.start:
                    continue
                if request.end is not None and bar.interval_start > request.end:
                    continue
                if request.as_of is not None and bar.available_at is not None and bar.available_at > request.as_of:
                    continue
                result.append(bar)
        result.sort(key=lambda b: (b.instrument_id or "", b.interval_start))
        return result

    def _build_batch(self, records: list[Bar], request: DataRequest) -> DataBatch[Bar]:
        return DataBatch(
            request_id=request.correlation_id or "",
            dataset="market.bar",
            schema_version=self.schema_version,
            correlation_id=request.correlation_id,
            records=tuple(records),
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=self.name,
                source_revision=self.source_revision,
                request_fingerprint=request.delivery_key or "memory",
                read_at=_utc(2024, 1, 1),
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )

    def read(self, request: DataRequest) -> DataBatch[Bar]:
        return self._build_batch(self._matching_bars(request), request)

    def iter(self, request: DataRequest, chunk_size: int = 10_000) -> Iterator[DataBatch[Bar]]:
        records = self._matching_bars(request)
        for i in range(0, len(records), max(1, chunk_size)):
            yield self._build_batch(records[i : i + chunk_size], request)
        if not records:
            yield self._build_batch([], request)

    # -- TradingCalendarPort (shared with calendar adapter) ------------------

    def _matching_sessions(self, request: CalendarRequest) -> list[Session]:
        return []

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        records = tuple(self._matching_sessions(request))
        return DataBatch(
            request_id="",
            dataset="calendar.session",
            schema_version=self.schema_version,
            correlation_id=None,
            records=records,
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=self.name,
                source_revision=self.source_revision,
                request_fingerprint=request.market,
                read_at=_utc(2024, 1, 1),
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )


# ---------------------------------------------------------------------------
# Convenience builder for a self-contained data source
# ---------------------------------------------------------------------------


def build_memory_source(
    bars: Sequence[Bar],
    sessions: Sequence[Session],
    *,
    name: str = "memory",
    timezone: str = "UTC",
) -> tuple[MemoryHistoricalAdapter, CalendarAdapter]:
    """Return ``(historic, calendar)`` adapters sharing the same in-memory data.

    Prefer this over :class:`MemoryHistoricalAdapter` used alone: it keeps the
    historical and calendar ports distinct, which mirrors how external vendors
    behind different capabilities often power the same application.

    ``timezone`` must match the timezone all ``datetime`` records carry so the
    gateway's strict timezone validation accepts them (A-share data is normally
    ``Asia/Shanghai``; the bundled examples use ``UTC``).
    """
    historic = MemoryHistoricalAdapter(bars, source_revision=name, timezone=timezone)
    calendar = CalendarAdapter(sessions, source_revision=name, timezone=timezone)
    return historic, calendar


# ---------------------------------------------------------------------------
# Calendar-only adapter over a sequence of sessions
# ---------------------------------------------------------------------------


class CalendarAdapter:
    """Adapt an explicit :class:`Session` sequence to :class:`TradingCalendarPort`."""

    name = "memory-calendar"

    def __init__(
        self,
        sessions: Sequence[Session] = (),
        *,
        schema_version: str = "1.0",
        source_revision: str = "demo-cal",
        timezone: str = "UTC",
    ) -> None:
        self._sessions = tuple(sessions)
        self.schema_version = schema_version
        self.source_revision = source_revision
        self.timezone = timezone
        self.descriptor = AdapterDescriptor(
            name=self.name,
            datasets={
                "calendar.session": DatasetCapability(
                    dataset="calendar.session",
                    modes=("calendar",),
                    asset_types=frozenset({"market"}),
                    frequencies=(),
                    fields=(),
                    point_in_time=False,
                )
            },
            historical_modes=(),
            realtime_modes=(),
            supports_point_in_time=False,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"market"}),
            schema_versions=(schema_version,),
            source_revision=source_revision,
        )

    def sessions(self, request: CalendarRequest) -> CalendarBatch:
        records = tuple(
            s
            for s in self._sessions
            if request.start <= s.trading_date <= request.end
            and (request.market == s.market or request.market == "*")
        )
        return DataBatch(
            request_id="",
            dataset="calendar.session",
            schema_version=self.schema_version,
            correlation_id=None,
            records=records,
            complete=True,
            next_cursor=None,
            provenance=DataProvenance(
                adapter_name=self.name,
                source_revision=self.source_revision,
                request_fingerprint=request.market,
                read_at=_utc(2024, 1, 1),
            ),
            quality=QualityReport(status="ok", checked_count=len(records)),
        )


# ---------------------------------------------------------------------------
# Assembly helper: build one self-contained route set for a DataGateway
# ---------------------------------------------------------------------------


def make_gateway(
    bars: Sequence[Bar],
    sessions: Sequence[Session],
    *,
    dataset: str = "market.bar",
    priority: int = 1,
    timezone: str = "UTC",
) -> DataGateway:
    """Assemble a real :class:`DataGateway` from the memory adapters.

    This is the "how to wire adapters into a gateway" pattern users copy into
    their own integration packages.
    """
    historic, calendar = build_memory_source(bars, sessions, timezone=timezone)

    calendar_binding = DataBinding(dataset="calendar.session", adapter=calendar.name, priority=priority)
    bar_binding = DataBinding(dataset=dataset, adapter=historic.name, priority=priority)

    return DataGateway(
        catalog=default_catalog(),
        bindings=[
            (bar_binding, historic.descriptor, historic),
            (calendar_binding, calendar.descriptor, calendar),
        ],
        policy=DataPolicy(timezone=timezone),
    )


def _demo() -> None:
    from engines.fast import FastBacktestEngine

    def _session_of(day: int) -> Session:
        return Session(
            market="CN",
            trading_date=date(2024, 1, day),
            timezone="UTC",
            phases=(TradingPhase(name="regular", start=_utc(2024, 1, day, 9, 30), end=_utc(2024, 1, day, 15, 0), accepts_trades=True, accepts_quotes=True),),
        )

    sessions = [_session_of(2), _session_of(3)]
    bars = [
        Bar(
            schema_version="1",
            event_id=None,
            instrument_id="000001.SZ",
            asset_type="equity",
            effective_time=_utc(2024, 1, 2, 15, 0),
            event_time=_utc(2024, 1, 2, 15, 0),
            available_at=_utc(2024, 1, 2, 15, 0),
            trading_date=date(2024, 1, 2),
            source="memory",
            quality="valid",
            metadata={},
            frequency="1d",
            interval_start=_utc(2024, 1, 2, 9, 30),
            interval_end=_utc(2024, 1, 2, 15, 0),
            open=10.0,
            high=10.0,
            low=10.0,
            close=10.0,
            volume=0.0,
            turnover=0.0,
            is_complete=True,
            price_basis="raw",
        ),
        Bar(
            schema_version="1",
            event_id=None,
            instrument_id="000001.SZ",
            asset_type="equity",
            effective_time=_utc(2024, 1, 3, 15, 0),
            event_time=_utc(2024, 1, 3, 15, 0),
            available_at=_utc(2024, 1, 3, 15, 0),
            trading_date=date(2024, 1, 3),
            source="memory",
            quality="valid",
            metadata={},
            frequency="1d",
            interval_start=_utc(2024, 1, 3, 9, 30),
            interval_end=_utc(2024, 1, 3, 15, 0),
            open=10.5,
            high=10.5,
            low=10.5,
            close=10.5,
            volume=0.0,
            turnover=0.0,
            is_complete=True,
            price_basis="raw",
        ),
    ]

    gateway = make_gateway(bars, sessions)

    from trading_nodes_base.factors.base import KlineTimingFactor, TickTimingFactor
    from trading_nodes_base.factors.types import KlineBar, SignalIntent
    from trading_nodes_base.methods.base import BaseTimeSelection
    from trading_nodes_base.strategies.base import BaseStrategy

    class PassiveKlineFactor(KlineTimingFactor):
        def __init__(self) -> None:
            super().__init__("passive-kline")

        def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
            self._data_clear()
            self.sign["fit"] = True
            return []

        def on_bar(self, bar: KlineBar) -> list[SignalIntent]:
            return []

    class IntentExecutor(TickTimingFactor):
        execution_role = "intent_executor"

        def __init__(self) -> None:
            super().__init__("intent-executor")

        def get_query_lst(self, dt: object, codes: list[str] | None = None) -> list[DataRequest]:
            self._data_clear()
            self.sign["fit"] = True
            return []

    class DemoStrategy(BaseStrategy):
        supports_fast_backtest = True

        def __init__(self) -> None:
            super().__init__(
                "demo-adapters",
                timer=BaseTimeSelection("demo-timer", [PassiveKlineFactor()], [IntentExecutor()]),
            )

    engine = FastBacktestEngine(
        DemoStrategy(),
        "20240102",
        "20240103",
        mode="fast",
        data_gateway=gateway,
        progress_bar=False,
    )
    engine.run()
    stats = engine.get_stats()
    print(stats)


if __name__ == "__main__":
    _demo()


__all__ = [
    "CalendarAdapter",
    "MemoryHistoricalAdapter",
    "build_memory_source",
    "make_gateway",
]