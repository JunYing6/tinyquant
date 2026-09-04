from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import pytest

from tools.data import (
    AdapterDescriptor,
    CalendarRequest,
    DataBatch,
    DataBinding,
    DataContractError,
    DataGapError,
    DataGapEvent,
    DataGateway,
    DataPolicy,
    DataProvenance,
    DataRequest,
    DataSourceError,
    DataSourceStateEvent,
    DatasetCapability,
    QualityReport,
    RouteOptions,
    StreamRequest,
    Subscription,
    TradeTick,
    TradingPhase,
    Session,
    UnsupportedDatasetError,
    cache_key,
    default_catalog,
)


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)


CATALOG = default_catalog()
BAR_DEF = CATALOG.get("market.bar")
BAR_FIELDS = tuple(BAR_DEF.fields)


def bar_batch(records, dataset="market.bar"):
    return DataBatch(
        request_id="",
        dataset=dataset,
        schema_version="1.0",
        correlation_id=None,
        records=tuple(records),
        complete=True,
        next_cursor=None,
        provenance=DataProvenance(
            adapter_name="x",
            source_revision="x",
            request_fingerprint="k",
            read_at=utc(2024, 1, 1),
        ),
        quality=QualityReport(status="ok"),
    )


def make_bar_record(instrument="600000", i=0):
    end = utc(2024, 1, 2, 9, 30 + i)
    from tools.data import Bar

    return Bar(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=end,
        available_at=end,
        trading_date=date(2024, 1, 2),
        source="mem",
        quality="valid",
        metadata={},
        frequency="1d",
        interval_start=end - timedelta(days=1),
        interval_end=end,
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=1000.0,
        turnover=10500.0,
        is_complete=True,
        price_basis="raw",
    )


def make_tick(instrument="600000"):
    return TradeTick(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=utc(2024, 1, 2, 9, 30, 5),
        available_at=utc(2024, 1, 2, 9, 30, 5),
        trading_date=date(2024, 1, 2),
        source="mem",
        quality="valid",
        metadata={},
        event_type="trade",
        price=10.0,
        size=100.0,
        turnover=1000.0,
        side="BUY",
        sequence=1,
    )


def make_gap():
    return DataGapEvent(
        dataset="market.trade",
        instrument_id="600000",
        detected_at=utc(2024, 1, 2, 9, 31),
        from_position=None,
        to_position=None,
        recoverable=True,
        reason="stream gap",
    )


def make_state_event():
    return DataSourceStateEvent(
        dataset="market.trade",
        source="mem",
        state="reconnecting",
        occurred_at=utc(2024, 1, 2, 9, 31),
        error=None,
    )


def hist_capability(fields=BAR_FIELDS, frequencies=("1d",), asset_types=frozenset({"equity"}), pit=False):
    return DatasetCapability(
        dataset="market.bar",
        modes=("historical",),
        asset_types=asset_types,
        frequencies=frequencies,
        fields=fields,
        point_in_time=pit,
    )


def hist_descriptor(name, fields=BAR_FIELDS, pit=False, asset_types=frozenset({"equity"}), source_revision="src1"):
    return AdapterDescriptor(
        name=name,
        datasets={"market.bar": hist_capability(fields, asset_types=asset_types, pit=pit)},
        historical_modes=("historical",),
        realtime_modes=(),
        supports_point_in_time=pit,
        supported_price_basis=frozenset({"raw"}),
        supported_asset_types=asset_types,
        schema_versions=("1.0",),
        source_revision=source_revision,
        supports_recovery=False,
    )


class HistAdapter:
    """In-memory historical adapter (read + iter) over market.bar."""

    def __init__(self, name, *, batch=None, fail=None, iter_chunks=None, fields=BAR_FIELDS,
                 pit=False, asset_types=frozenset({"equity"}), source_revision="src1", empty=False):
        self.name = name
        self._batch = batch
        self._fail = fail
        self._iter_chunks = iter_chunks
        self.empty = empty
        self.read_calls = 0
        self.iter_calls = 0
        self.closed = 0
        self.descriptor = hist_descriptor(name, fields=fields, pit=pit,
                                          asset_types=asset_types, source_revision=source_revision)

    def read(self, request):
        self.read_calls += 1
        if self._fail is not None:
            raise self._fail
        if self._batch is not None:
            return self._batch
        return bar_batch([make_bar_record()]) if not self.empty else bar_batch([])

    def iter(self, request, chunk_size=10_000):
        self.iter_calls += 1
        if self._fail is not None:
            raise self._fail
        if self._iter_chunks is not None:
            return iter(self._iter_chunks)
        return iter([self.read(request)])

    def close(self):
        self.closed += 1


class CalAdapter:
    def __init__(self, name, *, batch=None, source_revision="cal1"):
        self.name = name
        self._batch = batch
        self.sessions_calls = 0
        self.descriptor = AdapterDescriptor(
            name=name,
            datasets={"calendar.session": DatasetCapability(
                dataset="calendar.session",
                modes=("calendar",),
                asset_types=frozenset({"market"}),
                frequencies=(),
                fields=(),
                point_in_time=False,
            )},
            historical_modes=(),
            realtime_modes=(),
            supports_point_in_time=False,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"market"}),
            schema_versions=("1.0",),
            source_revision=source_revision,
            supports_recovery=False,
        )

    def sessions(self, request):
        self.sessions_calls += 1
        if self._batch is not None:
            return self._batch
        phase = TradingPhase(name="open", start=utc(2024, 1, 2, 9, 30),
                             end=utc(2024, 1, 2, 15, 0), accepts_trades=True, accepts_quotes=True)
        session = Session(market=request.market, trading_date=request.start,
                          timezone=request.timezone or "Asia/Shanghai", phases=(phase,))
        return DataBatch(
            request_id="", dataset="calendar.session", schema_version="1.0",
            correlation_id=None, records=(session,), complete=True, next_cursor=None,
            provenance=DataProvenance(adapter_name="x", source_revision="x",
                                      request_fingerprint="k", read_at=utc(2024, 1, 1)),
            quality=QualityReport(status="ok"),
        )


class RTAdapter:
    def __init__(self, name, *, mode="push", dataset="market.trade", supports_recovery=False,
                 source_revision="rt1"):
        self.name = name
        self.dataset = dataset
        self.sink = None
        self.events = []
        self.subscribe_calls = 0
        self.descriptor = AdapterDescriptor(
            name=name,
            datasets={dataset: DatasetCapability(
                dataset=dataset,
                modes=(mode,),
                asset_types=frozenset({"equity"}),
                frequencies=(),
                fields=(),
                point_in_time=True,
            )},
            historical_modes=(),
            realtime_modes=(mode,) if mode in ("push", "poll") else (),
            supports_point_in_time=True,
            supported_price_basis=frozenset({"raw"}),
            supported_asset_types=frozenset({"equity"}),
            schema_versions=("1.0",),
            source_revision=source_revision,
            supports_recovery=supports_recovery,
        )

    def subscribe(self, request, sink):
        self.subscribe_calls += 1
        self.sink = sink
        return Subscription()

    def poll(self, request):
        return iter(self.events)

    def recover(self, request, from_position):
        return iter([e for e in self.events if isinstance(e, TradeTick)])

    def emit(self, event):
        self.events.append(event)
        if self.sink is not None:
            self.sink(event)


class MemoryCache:
    def __init__(self):
        self.store = {}
        self.puts = 0
        self.gets = 0

    def get(self, key):
        self.gets += 1
        return self.store.get(key)

    def put(self, key, value):
        self.puts += 1
        self.store[key] = value

    def invalidate(self, key):
        self.store.pop(key, None)


def bind(dataset, adapter, priority, allow_fallback=True, modes=()):
    return DataBinding(dataset=dataset, adapter=adapter.name, priority=priority,
                       modes=modes, allow_fallback=allow_fallback)


def make_gateway(entries, policy=None, cache=None):
    if policy is None:
        policy = DataPolicy(timezone="UTC")
    return DataGateway(CATALOG, entries, policy=policy, cache=cache)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_construction_auto_freezes_catalog():
    from tools.data import DataCatalog
    catalog = DataCatalog()
    from tools.data.datasets import _default_definitions
    for definition in _default_definitions():
        catalog.register(definition)
    assert not catalog.frozen
    gw = DataGateway(catalog, [])
    assert catalog.frozen


def test_construction_unknown_dataset_raises():
    adapter = HistAdapter("a")
    with pytest.raises(DataContractError):
        DataGateway(CATALOG, [(DataBinding(dataset="nope.nope", adapter="a", priority=1), adapter)])


def test_construction_same_priority_same_mode_raises():
    a = HistAdapter("a")
    b = HistAdapter("b")
    with pytest.raises(DataContractError):
        DataGateway(CATALOG, [
            (DataBinding(dataset="market.bar", adapter="a", priority=1), a),
            (DataBinding(dataset="market.bar", adapter="b", priority=1), b),
        ])


def test_construction_pit_consistency_raises():
    cap = DatasetCapability(dataset="market.bar", modes=("historical",),
                            asset_types=frozenset({"equity"}), frequencies=("1d",),
                            fields=BAR_FIELDS, point_in_time=True)
    desc = AdapterDescriptor(name="a", datasets={"market.bar": cap},
                             historical_modes=("historical",), realtime_modes=(),
                             supports_point_in_time=False,
                             supported_price_basis=frozenset({"raw"}),
                             supported_asset_types=frozenset({"equity"}),
                             schema_versions=("1.0",))
    with pytest.raises(DataContractError):
        DataGateway(CATALOG, [(DataBinding(dataset="market.bar", adapter="a", priority=1), desc, HistAdapter("a"))])


def test_construction_missing_capability_method_raises():
    class Broken:
        def read(self, request):
            raise NotImplementedError

    adapter = Broken()
    adapter.descriptor = hist_descriptor("a")
    with pytest.raises(DataContractError):
        DataGateway(CATALOG, [(DataBinding(dataset="market.bar", adapter="a", priority=1), adapter)])


def test_construction_recovery_requires_supports_recovery():
    cap = DatasetCapability(dataset="market.trade", modes=("recovery",),
                            asset_types=frozenset({"equity"}), frequencies=(),
                            fields=(), point_in_time=True)
    desc = AdapterDescriptor(name="a", datasets={"market.trade": cap},
                             historical_modes=(), realtime_modes=(),
                             supports_point_in_time=True,
                             supported_price_basis=frozenset({"raw"}),
                             supported_asset_types=frozenset({"equity"}),
                             schema_versions=("1.0",), supports_recovery=False)
    with pytest.raises(DataContractError):
        DataGateway(CATALOG, [(DataBinding(dataset="market.trade", adapter="a", priority=1), desc, RTAdapter("a"))])


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_priority_selection_uses_lowest_number():
    low = HistAdapter("low")
    high = HistAdapter("high")
    gw = make_gateway([
        (bind("market.bar", low, priority=2), low),
        (bind("market.bar", high, priority=1), high),
    ])
    result = gw.read(DataRequest(dataset="market.bar"))
    assert result.provenance.adapter_name == "high"


def test_explicit_adapter_routing():
    a = HistAdapter("a")
    b = HistAdapter("b")
    gw = make_gateway([
        (bind("market.bar", a, priority=1), a),
        (bind("market.bar", b, priority=2), b),
    ])
    result = gw.read(DataRequest(dataset="market.bar"), route=RouteOptions(adapter_name="b"))
    assert result.provenance.adapter_name == "b"


def test_unbound_dataset_raises():
    gw = make_gateway([(bind("market.bar", HistAdapter("a"), 1), HistAdapter("a"))])
    with pytest.raises(UnsupportedDatasetError) as exc:
        gw.read(DataRequest(dataset="market.margin"))
    assert "no dataset binding" in str(exc.value)
    assert exc.value.request_id


def test_bound_but_lacks_capability_raises():
    adapter = RTAdapter("a", mode="poll", dataset="market.bar")
    gw = make_gateway([(DataBinding(dataset="market.bar", adapter="a", priority=1), adapter)])
    with pytest.raises(UnsupportedDatasetError) as exc:
        gw.read(DataRequest(dataset="market.bar"))
    assert "lack the requested capability" in str(exc.value)


def test_explicit_adapter_does_not_match_raises():
    a = HistAdapter("a")
    b = HistAdapter("b")
    gw = make_gateway([(bind("market.bar", a, 1), a), (bind("market.bar", b, 2), b)])
    with pytest.raises(UnsupportedDatasetError) as exc:
        gw.read(DataRequest(dataset="market.bar"), route=RouteOptions(adapter_name="missing"))
    assert "does not match" in str(exc.value)


def test_request_field_not_supported_by_capability_raises():
    fields = tuple(f for f in BAR_FIELDS if f != "turnover")
    a = HistAdapter("a", fields=fields)
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    with pytest.raises(DataContractError) as exc:
        gw.read(DataRequest(dataset="market.bar", fields=("turnover",)))
    assert exc.value.request_id


def test_pit_routing_prefers_pit_adapter():
    pit = HistAdapter("pit", pit=True)
    nopit = HistAdapter("nopit", pit=False)
    gw = make_gateway([
        (bind("market.bar", nopit, 1), nopit),
        (bind("market.bar", pit, 2), pit),
    ])
    result = gw.read(DataRequest(dataset="market.bar", as_of=utc(2024, 1, 1, 12)))
    assert result.provenance.adapter_name == "pit"


# ---------------------------------------------------------------------------
# Validation ordering
# ---------------------------------------------------------------------------


def test_validate_request_runs_before_adapter():
    a = HistAdapter("a")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    with pytest.raises(DataContractError):
        gw.read(DataRequest(dataset="market.bar", fields=("bogus",)))
    assert a.read_calls == 0


def test_unknown_dataset_never_calls_adapter():
    a = HistAdapter("a")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    with pytest.raises(UnsupportedDatasetError):
        gw.read(DataRequest(dataset="not.a_dataset"))
    assert a.read_calls == 0


# ---------------------------------------------------------------------------
# Retry / fallback
# ---------------------------------------------------------------------------


def test_default_no_fallback_and_retry_until_cap():
    fail = HistAdapter("fail", fail=DataSourceError("boom", retryable=True))
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", fail, 1), fail),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=False, max_retries=2))
    with pytest.raises(DataSourceError):
        gw.read(DataRequest(dataset="market.bar"))
    assert fail.read_calls == 3
    assert backup.read_calls == 0


def test_retryable_retry_cap():
    fail = HistAdapter("fail", fail=DataSourceError("boom", retryable=True))
    gw = make_gateway([(bind("market.bar", fail, 1), fail)],
                      policy=DataPolicy(timezone="UTC", max_retries=2))
    with pytest.raises(DataSourceError):
        gw.read(DataRequest(dataset="market.bar"))
    assert fail.read_calls == 3


def test_non_retryable_not_retried():
    fail = HistAdapter("fail", fail=DataSourceError("boom", retryable=False))
    gw = make_gateway([(bind("market.bar", fail, 1), fail)],
                      policy=DataPolicy(timezone="UTC", max_retries=5))
    with pytest.raises(DataSourceError):
        gw.read(DataRequest(dataset="market.bar"))
    assert fail.read_calls == 1


def test_non_retryable_can_fallback_when_allowed():
    fail = HistAdapter("fail", fail=DataSourceError("boom", retryable=False))
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", fail, 1), fail),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True))
    result = gw.read(DataRequest(dataset="market.bar"))
    assert fail.read_calls == 1
    assert backup.read_calls == 1
    assert result.provenance.adapter_name == "backup"
    assert result.provenance.fallback_used is True


def test_fallback_when_allow_fallback_false():
    fail = HistAdapter("fail", fail=DataSourceError("boom", retryable=False))
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", fail, 1), fail),
        (bind("market.bar", backup, 2, allow_fallback=False), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True))
    with pytest.raises(DataSourceError):
        gw.read(DataRequest(dataset="market.bar"))
    assert backup.read_calls == 0


def test_contract_error_never_falls_back():
    fail = HistAdapter("fail", fail=DataContractError("bad", dataset="market.bar"))
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", fail, 1), fail),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True, max_retries=3))
    with pytest.raises(DataContractError):
        gw.read(DataRequest(dataset="market.bar"))
    assert fail.read_calls == 1
    assert backup.read_calls == 0


def test_fallback_on_empty():
    empty = HistAdapter("empty", empty=True)
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", empty, 1), empty),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True, fallback_on_empty=True))
    result = gw.read(DataRequest(dataset="market.bar"))
    assert result.provenance.adapter_name == "backup"
    assert result.provenance.fallback_used is True
    assert len(result.records) == 1


def test_fallback_on_empty_disabled_keeps_empty():
    empty = HistAdapter("empty", empty=True)
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", empty, 1), empty),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True))
    result = gw.read(DataRequest(dataset="market.bar"))
    assert result.provenance.adapter_name == "empty"
    assert len(result.records) == 0


# ---------------------------------------------------------------------------
# Provenance / request_id / wrapping
# ---------------------------------------------------------------------------


def test_provenance_contents():
    a = HistAdapter("a", source_revision="rev-9")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    request = DataRequest(dataset="market.bar")
    result = gw.read(request)
    assert result.provenance.adapter_name == "a"
    assert result.provenance.source_revision == "rev-9"
    assert result.provenance.request_fingerprint == cache_key(request, "rev-9", "a")
    assert result.provenance.fallback_used is False
    assert result.provenance.read_at.tzinfo is not None
    assert result.request_id


def test_request_id_threads_through_fallback_and_error():
    error = DataSourceError("boom", retryable=False)
    fail = HistAdapter("fail", fail=error)
    backup = HistAdapter("backup")
    gw = make_gateway([
        (bind("market.bar", fail, 1), fail),
        (bind("market.bar", backup, 2), backup),
    ], policy=DataPolicy(timezone="UTC", fallback=True))
    result = gw.read(DataRequest(dataset="market.bar"))
    assert result.request_id == error.request_id
    assert len(result.request_id) == 32


def test_non_data_error_wrapped_preserving_cause():
    inner = ValueError("kaboom")
    fail = HistAdapter("fail", fail=inner)
    gw = make_gateway([(bind("market.bar", fail, 1), fail)])
    with pytest.raises(DataSourceError) as exc:
        gw.read(DataRequest(dataset="market.bar"))
    assert exc.value.cause is inner
    assert exc.value.dataset == "market.bar"
    assert exc.value.source == "fail"
    assert exc.value.request_id
    assert exc.value.__cause__ is inner


# ---------------------------------------------------------------------------
# iterate
# ---------------------------------------------------------------------------


def test_iterate_yields_validated_batches_with_uniform_request_id():
    chunks = [bar_batch([make_bar_record(i=0)]), bar_batch([make_bar_record(i=1)])]
    a = HistAdapter("a", iter_chunks=chunks)
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    request = DataRequest(dataset="market.bar")
    results = list(gw.iterate(request))
    assert len(results) == 2
    assert len({b.request_id for b in results}) == 1
    assert all(b.provenance.adapter_name == "a" for b in results)


def test_iterate_raises_on_bad_chunk_with_request_id():
    bad = bar_batch([{"instrument_id": "600000"}])
    good = bar_batch([make_bar_record(i=0)])
    a = HistAdapter("a", iter_chunks=[good, bad])
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    it = gw.iterate(DataRequest(dataset="market.bar"))
    first = next(it)
    assert len(first.records) == 1
    with pytest.raises(DataContractError) as exc:
        next(it)
    assert exc.value.request_id == first.request_id


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_lifecycle_idempotent():
    a = HistAdapter("a")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    gw.open()
    gw.open()
    assert gw.is_open
    gw.close()
    gw.close()
    assert not gw.is_open
    assert a.closed == 1


def test_context_manager():
    a = HistAdapter("a")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    with gw as g:
        assert g is gw
        assert gw.is_open
    assert not gw.is_open
    assert a.closed == 1


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


def test_cache_hit_revalidates_and_rebuilds_provenance():
    a = HistAdapter("a", source_revision="src1")
    cache = MemoryCache()
    gw = make_gateway([(bind("market.bar", a, 1), a)], cache=cache)
    request = DataRequest(dataset="market.bar")
    first = gw.read(request)
    first_read_at = first.provenance.read_at
    assert a.read_calls == 1
    assert cache.puts == 1
    second = gw.read(request)
    assert a.read_calls == 1  # cache hit, no adapter call
    assert cache.puts == 1
    assert second.request_id != first.request_id
    assert second.provenance.request_fingerprint == first.provenance.request_fingerprint


def test_cache_strict_unknown_revision_does_not_persist():
    a = HistAdapter("a", source_revision="unknown")
    cache = MemoryCache()
    gw = make_gateway([(bind("market.bar", a, 1), a)],
                      policy=DataPolicy(timezone="UTC", strict=True), cache=cache)
    gw.read(DataRequest(dataset="market.bar"))
    assert a.read_calls == 1
    assert cache.puts == 0


# ---------------------------------------------------------------------------
# Realtime
# ---------------------------------------------------------------------------


def stream_req():
    return StreamRequest(dataset="market.trade", instruments=("600000",))


def test_subscribe_delivers_market_events_to_sink():
    a = RTAdapter("rt")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    received = []
    sub = gw.subscribe(stream_req(), received.append)
    assert sub.state == "active"
    a.emit(make_tick())
    assert len(received) == 1
    assert isinstance(received[0], TradeTick)


def test_subscribe_gap_action_raise():
    a = RTAdapter("rt")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    received = []
    gw.subscribe(stream_req(), received.append)
    with pytest.raises(DataGapError) as exc:
        a.emit(make_gap())
    assert exc.value.request_id
    assert received == []


def test_subscribe_gap_action_pause():
    a = RTAdapter("rt")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC", gap_action="pause"))
    received = []
    sub = gw.subscribe(stream_req(), received.append)
    a.emit(make_tick())
    a.emit(make_gap())
    a.emit(make_tick())
    assert sub.state == "paused"
    assert len(received) == 1


def test_subscribe_gap_action_continue_routes_to_control_sink():
    a = RTAdapter("rt")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC", gap_action="continue"))
    received = []
    control = []
    gw.subscribe(stream_req(), received.append, control_sink=control.append)
    a.emit(make_tick())
    a.emit(make_gap())
    a.emit(make_state_event())
    assert len(received) == 1
    assert isinstance(control[0], DataGapEvent)
    assert isinstance(control[1], DataSourceStateEvent)


def test_subscribe_cancelled_stops_delivery():
    a = RTAdapter("rt")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    received = []
    sub = gw.subscribe(stream_req(), received.append)
    a.emit(make_tick())
    sub.cancel()
    a.emit(make_tick())
    assert len(received) == 1


def test_poll_yields_stream_events():
    a = RTAdapter("rt", mode="poll")
    a.events = [make_tick(), make_gap()]
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    events = list(gw.poll(stream_req()))
    assert len(events) == 2


def test_recover_yields_market_events():
    a = RTAdapter("rt", mode="recovery", supports_recovery=True)
    a.events = [make_tick()]
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    events = list(gw.recover(stream_req(), from_position=None))
    assert len(events) == 1


def test_recover_unavailable():
    a = RTAdapter("rt", mode="push")
    gw = make_gateway([(DataBinding(dataset="market.trade", adapter="rt", priority=1), a)],
                      policy=DataPolicy(timezone="UTC"))
    with pytest.raises(UnsupportedDatasetError):
        list(gw.recover(stream_req(), from_position=None))


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_sessions_calendar_routing():
    a = CalAdapter("cal")
    gw = make_gateway([(DataBinding(dataset="calendar.session", adapter="cal", priority=1), a)])
    request = CalendarRequest(market="CN", start=date(2024, 1, 2), end=date(2024, 1, 5))
    result = gw.sessions(request)
    assert result.provenance.adapter_name == "cal"
    assert len(result.provenance.request_fingerprint) == 64
    assert result.request_id
    assert isinstance(result.records[0], Session)


def test_sessions_fingerprint_deterministic():
    a = CalAdapter("cal")
    gw = make_gateway([(DataBinding(dataset="calendar.session", adapter="cal", priority=1), a)])
    request = CalendarRequest(market="CN", start=date(2024, 1, 2), end=date(2024, 1, 5))
    first = gw.sessions(request).provenance.request_fingerprint
    second = gw.sessions(request).provenance.request_fingerprint
    assert first == second


def test_sessions_no_calendar_binding_raises():
    a = HistAdapter("a")
    gw = make_gateway([(bind("market.bar", a, 1), a)])
    with pytest.raises(UnsupportedDatasetError) as exc:
        gw.sessions(CalendarRequest(market="CN", start=date(2024, 1, 2), end=date(2024, 1, 5)))
    assert exc.value.dataset == "calendar.session"
