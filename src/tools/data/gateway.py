"""Catalog-driven data gateway: routing, fallback, provenance and calendar routing.

Task 4 of the unified data-extension interface.  :class:`DataGateway` is the
concrete orchestrator an application talks to.  It owns no data itself --
instead it routes a request to the right adapter using the :class:`DataCatalog`
contracts and the adapter :class:`AdapterDescriptor`/:class:`DataBinding`
capability declarations, validates every batch, threads a single ``request_id``
through a call and all of its errors, applies retry / fallback policy, and
assembles :class:`DataProvenance` so consumers can audit exactly where and how
each result was produced.

Design rules enforced here:

* Adapters are never constructed here and nothing connects to an external
  source in this module; adapters come in already built via ``bindings``.
* A :class:`DataRequest` routed through :meth:`read`/:meth:`iterate` uses the
  ``historical`` mode; :meth:`subscribe` uses ``push``; :meth:`poll`` uses
  ``poll``; :meth:`recover` uses ``recovery``.  :meth:`sessions` uses a
  dedicated calendar route keyed on ``calendar.session`` and never consults the
  ``DataRequest`` router.
* Only ``DataSourceError(retryable=True)`` is retried, and only up to
  ``policy.max_retries`` with ``policy.retry_backoff`` between attempts.
  Contract / point-in-time / quality / unsupported errors never retry and never
  fall back.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from .cache import DataCache, cache_key
from .contracts import (
    CalendarBatch,
    CalendarRequest,
    DataBatch,
    DataGapEvent,
    DataProvenance,
    DataRequest,
    DataSourceStateEvent,
    MarketEvent,
    Session,
    StreamEvent,
    StreamRequest,
)
from .datasets import DatasetDefinition
from .errors import (
    DataContractError,
    DataError,
    DataGapError,
    DataSourceError,
    PointInTimeError,
    UnsupportedDatasetError,
)
from .ports import (
    AdapterDescriptor,
    DataBinding,
    DataPolicy,
    RouteOptions,
    Subscription,
)
from .quality import validate_batch, validate_request

HISTORICAL_MODE = "historical"
PUSH_MODE = "push"
POLL_MODE = "poll"
CALENDAR_MODE = "calendar"
RECOVERY_MODE = "recovery"

_CALENDAR_DATASET = "calendar.session"

_MODE_METHODS: dict[str, tuple[str, ...]] = {
    HISTORICAL_MODE: ("read", "iter"),
    PUSH_MODE: ("subscribe",),
    POLL_MODE: ("poll",),
    CALENDAR_MODE: ("sessions",),
    RECOVERY_MODE: ("recover",),
}

_CALENDAR_REQUEST_FIELDS = ("market", "start", "end", "timezone", "include_closed")


class _Candidate:
    """A routable (dataset, mode) -> adapter mapping with its metadata."""

    __slots__ = ("binding", "descriptor", "adapter", "capability")

    def __init__(
        self,
        binding: DataBinding,
        descriptor: AdapterDescriptor,
        adapter: Any,
        capability: Any,
    ) -> None:
        self.binding = binding
        self.descriptor = descriptor
        self.adapter = adapter
        self.capability = capability


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DataGateway:
    """Orchestrates routing, validation, provenance, retry and fallback."""

    def __init__(
        self,
        catalog: Any,
        bindings: Any,
        policy: DataPolicy | None = None,
        cache: DataCache | None = None,
    ) -> None:
        self._catalog = catalog
        if not catalog.frozen:
            catalog.freeze()
        self._policy = policy if policy is not None else DataPolicy()
        self._cache = cache
        self._routes: dict[tuple[str, str], list[_Candidate]] = {}
        self._adapters: list[Any] = []
        self._subscriptions: dict[Subscription, threading.Lock] = {}
        self._open = False
        self._bind_adapter_bindings(bindings)

    # ------------------------------------------------------------------
    # Construction / binding validation
    # ------------------------------------------------------------------

    def _bind_adapter_bindings(self, bindings: Any) -> None:
        seen_priority: dict[tuple[str, str], dict[int, str]] = {}
        for entry in bindings:
            if len(entry) == 3:
                binding, descriptor, adapter = entry
            elif len(entry) == 2:
                binding, adapter = entry
                descriptor = getattr(adapter, "descriptor", None)
                if not isinstance(descriptor, AdapterDescriptor):
                    raise DataContractError(
                        "2-tuple binding requires the adapter to declare a descriptor"
                    )
            else:
                raise TypeError("binding entries must be (binding, adapter) or (binding, descriptor, adapter)")
            if not isinstance(binding, DataBinding):
                raise TypeError(f"expected DataBinding, got {type(binding).__name__}")
            if not isinstance(descriptor, AdapterDescriptor):
                raise TypeError(f"adapter descriptor must be AdapterDescriptor, got {type(descriptor).__name__}")
            if descriptor.name != binding.adapter:
                raise DataContractError(
                    f"binding adapter {binding.adapter!r} does not match descriptor name {descriptor.name!r}"
                )

            try:
                definition = self._catalog.get(binding.dataset)
            except KeyError:
                raise DataContractError(
                    f"binding references unknown dataset {binding.dataset!r}",
                    dataset=binding.dataset,
                )
            if adapter not in self._adapters:
                self._adapters.append(adapter)

            candidate_modes = set()
            for capability in descriptor.datasets.values():
                if capability.dataset != binding.dataset:
                    continue
                if capability.point_in_time != descriptor.supports_point_in_time:
                    raise DataContractError(
                        f"adapter {descriptor.name!r} capability point_in_time does not match "
                        f"descriptor supports_point_in_time for {binding.dataset}"
                    )
                self._validate_capability_methods(adapter, descriptor, capability)
                modes = set(capability.modes) & (set(binding.modes) if binding.modes else set(capability.modes))
                for mode in modes:
                    if mode not in _MODE_METHODS:
                        raise DataContractError(
                            f"adapter {descriptor.name!r} declares unknown mode {mode!r}"
                        )
                    key = (binding.dataset, mode)
                    priority_map = seen_priority.setdefault(key, {})
                    if binding.priority in priority_map:
                        raise DataContractError(
                            f"dataset {binding.dataset!r} mode {mode!r} has duplicate "
                            f"priority {binding.priority} ({priority_map[binding.priority]} and {descriptor.name})",
                            dataset=binding.dataset,
                        )
                    priority_map[binding.priority] = descriptor.name
                    self._routes.setdefault(key, []).append(
                        _Candidate(binding, descriptor, adapter, capability)
                    )
            for key, candidates in self._routes.items():
                candidates.sort(key=lambda c: c.binding.priority)

    def _validate_capability_methods(self, adapter: Any, descriptor: AdapterDescriptor, capability: Any) -> None:

        for candidate_mode, methods in _MODE_METHODS.items():
            if candidate_mode in capability.modes:
                mode = candidate_mode
                for method in methods:
                    if not hasattr(adapter, method):
                        raise DataContractError(
                            f"adapter {descriptor.name!r} declares mode {candidate_mode!r} for "
                            f"{capability.dataset} but lacks method {method!r}",
                            dataset=capability.dataset,
                        )
                if candidate_mode == RECOVERY_MODE and not descriptor.supports_recovery:
                    raise DataContractError(
                        f"adapter {descriptor.name!r} declares recovery for {capability.dataset} "
                        "but descriptor.supports_recovery is False",
                        dataset=capability.dataset,
                    )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        """Mark the gateway open.  Idempotent; never connects externally."""
        self._open = True

    def close(self) -> None:
        """Close every adapter that exposes ``close``.  Idempotent."""
        if not self._open:
            return
        for adapter in self._adapters:
            close_fn = getattr(adapter, "close", None)
            if close_fn is not None:
                close_fn()
        self._open = False

    def __enter__(self) -> "DataGateway":
        self.open()
        return self

    def __exit__(self, etype: Any, exc: Any, tb: Any) -> None:
        self.close()

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def data_policy(self) -> DataPolicy:
        return self._policy

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _validate_request(self, request: Any, definition: Any, request_id: str) -> None:
        """Contract-validate a request where the request shape supports it.

        :class:`DataRequest` carries the ``filters`` shape that
        :func:`validate_request` inspects; streaming requests
        (:class:`StreamRequest`) have no ``filters`` and are validated by
        construction, so they are skipped here.
        """
        if not isinstance(request, DataRequest):
            return
        try:
            validate_request(request, definition)
        except DataError as error:
            error.request_id = request_id
            raise

    def _invoke(self, adapter: Any, method: str, *args: Any, dataset: str, source: str, request_id: str) -> Any:
        try:
            return getattr(adapter, method)(*args)
        except DataError as error:
            error.request_id = request_id
            raise
        except Exception as exc:
            wrapped = DataSourceError(
                str(exc),
                dataset=dataset,
                source=source,
                request_id=request_id,
                cause=exc,
            )
            raise wrapped from exc

    def _build_provenance(self, descriptor: AdapterDescriptor, fingerprint: str, fallback_used: bool) -> DataProvenance:
        return DataProvenance(
            adapter_name=descriptor.name,
            source_revision=descriptor.source_revision,
            request_fingerprint=fingerprint,
            read_at=_now(),
            fallback_used=fallback_used,
        )

    def _attributes_route(
        self,
        dataset: str,
        mode: str,
        request: Any,
        opts: RouteOptions,
        definition: DatasetDefinition,
        request_id: str,
    ) -> list[_Candidate]:
        has_dataset = any(key[0] == dataset for key in self._routes)
        if not has_dataset:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {dataset}",
                dataset=dataset,
                request_id=request_id,
            )
        base = list(self._routes.get((dataset, mode), []))
        if not base:
            raise UnsupportedDatasetError(
                f"bound adapters for {dataset} lack the requested capability",
                dataset=dataset,
                request_id=request_id,
            )
        if opts.adapter_name is not None:
            base = [c for c in base if c.descriptor.name == opts.adapter_name]
            if not base:
                raise UnsupportedDatasetError(
                    f"explicit adapter {opts.adapter_name!r} does not match dataset {dataset}",
                    dataset=dataset,
                    request_id=request_id,
                )

        request_version = getattr(request, "schema_version", None)
        if request_version is not None:
            base = [c for c in base if request_version in c.descriptor.schema_versions]

        request_asset = getattr(request, "asset_type", None)
        if request_asset is not None:
            base = [
                c
                for c in base
                if request_asset in c.capability.asset_types
                and request_asset in c.descriptor.supported_asset_types
            ]

        request_frequency = getattr(request, "frequency", None)
        if request_frequency is not None:
            base = [c for c in base if request_frequency in c.capability.frequencies]

        needs_pit = getattr(request, "as_of", None) is not None or definition.point_in_time
        if needs_pit:
            base = [
                c
                for c in base
                if c.capability.point_in_time and c.descriptor.supports_point_in_time
            ]

        if not base:
            raise UnsupportedDatasetError(
                f"bound adapters for {dataset} lack the requested capability",
                dataset=dataset,
                request_id=request_id,
            )

        base.sort(key=lambda c: c.binding.priority)

        request_fields = getattr(request, "fields", None)
        if request_fields is not None:
            missing = set(request_fields) - set(base[0].capability.fields)
            if missing:
                raise DataContractError(
                    f"dataset {dataset} does not support requested fields: {sorted(missing)}",
                    dataset=dataset,
                    request_id=request_id,
                )
        return base

    def _pick_backup(self, candidates: list[_Candidate], opts: RouteOptions) -> _Candidate | None:
        if not (self._policy.fallback and opts.allow_fallback):
            return None
        for candidate in candidates[1:]:
            if candidate.binding.allow_fallback:
                return candidate
        return None

    # ------------------------------------------------------------------
    # History (read / iterate)
    # ------------------------------------------------------------------

    def _try_read(
        self,
        candidate: _Candidate,
        request: DataRequest,
        definition: DatasetDefinition,
        opts: RouteOptions,
        request_id: str,
        fallback_used: bool,
    ) -> tuple[DataBatch, bool]:
        descriptor = candidate.descriptor
        fingerprint = cache_key(request, descriptor.source_revision, descriptor.name)
        if self._cache is not None:
            cached = self._cache.get(fingerprint)
            if cached is not None:
                validate_batch(
                    cached,
                    definition,
                    strict=self._policy.strict,
                    session=None,
                    timezone=self._policy.timezone,
                    as_of=request.as_of,
                )
                provenance = self._build_provenance(descriptor, fingerprint, fallback_used)
                return cached.with_request_context(request_id, request.correlation_id, provenance), fallback_used

        batch = self._invoke(
            candidate.adapter,
            "read",
            request,
            dataset=request.dataset,
            source=descriptor.name,
            request_id=request_id,
        )
        validate_batch(
            batch,
            definition,
            strict=self._policy.strict,
            session=None,
            timezone=self._policy.timezone,
            as_of=request.as_of,
        )
        provenance = self._build_provenance(descriptor, fingerprint, fallback_used)
        assembled = batch.with_request_context(request_id, request.correlation_id, provenance)
        if self._cache is not None:
            if not (self._policy.strict and descriptor.source_revision == "unknown"):
                self._cache.put(fingerprint, assembled)
        return assembled, fallback_used

    def _execute_read(
        self,
        candidates: list[_Candidate],
        request: DataRequest,
        definition: DatasetDefinition,
        opts: RouteOptions,
        request_id: str,
    ) -> DataBatch:
        primary = candidates[0]
        attempt = 0
        while True:
            try:
                batch, fallback_used = self._try_read(primary, request, definition, opts, request_id, False)
            except DataSourceError as error:
                error.request_id = request_id
                if error.retryable and attempt < self._policy.max_retries:
                    attempt += 1
                    time.sleep(self._policy.retry_backoff)
                    continue
                backup = self._pick_backup(candidates, opts)
                if backup is None:
                    raise
                batch, fallback_used = self._try_read(backup, request, definition, opts, request_id, True)
                return batch
            except (DataContractError, PointInTimeError, UnsupportedDatasetError) as error:
                error.request_id = request_id
                raise

            if fallback_used:
                return batch
            allow_empty_switch = self._policy.fallback_on_empty or opts.fallback_on_empty
            if not batch.records and allow_empty_switch:
                backup = self._pick_backup(candidates, opts)
                if backup is not None:
                    batch, fallback_used = self._try_read(backup, request, definition, opts, request_id, True)
            return batch

    def read(self, request: DataRequest, route: RouteOptions | None = None) -> DataBatch:
        """Read a historical batch through the best-matching adapter."""
        request_id = uuid.uuid4().hex
        try:
            definition = self._catalog.get(request.dataset)
        except KeyError:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {request.dataset}",
                dataset=request.dataset,
                request_id=request_id,
            )
        opts = route if route is not None else RouteOptions()
        self._validate_request(request, definition, request_id)
        try:
            candidates = self._attributes_route(request.dataset, HISTORICAL_MODE, request, opts, definition, request_id)
            return self._execute_read(candidates, request, definition, opts, request_id)
        except DataError as error:
            error.request_id = request_id
            raise

    def iterate(self, request: DataRequest, chunk_size: int = 10_000, route: RouteOptions | None = None) -> Iterator[DataBatch]:
        """Yield validated batches chunk by chunk.

        Each chunk is validated and re-provenanced with the same ``request_id``.
        Source errors raised during iteration are wrapped and propagated with the
        request id -- they are deliberately *not* retried or re-routed per chunk
        (retry/fallback apply only to the eager :meth:`read` path).
        """
        request_id = uuid.uuid4().hex
        try:
            definition = self._catalog.get(request.dataset)
        except KeyError:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {request.dataset}",
                dataset=request.dataset,
                request_id=request_id,
            )
        opts = route if route is not None else RouteOptions()
        try:
            self._validate_request(request, definition, request_id)
            candidates = self._attributes_route(request.dataset, HISTORICAL_MODE, request, opts, definition, request_id)
            primary = candidates[0]
            descriptor = primary.descriptor
            fingerprint = cache_key(request, descriptor.source_revision, descriptor.name)
            iterator = self._invoke(
                primary.adapter,
                "iter",
                request,
                chunk_size,
                dataset=request.dataset,
                source=descriptor.name,
                request_id=request_id,
            )
            for chunk in iterator:
                try:
                    validate_batch(
                        chunk,
                        definition,
                        strict=self._policy.strict,
                        session=None,
                        timezone=self._policy.timezone,
                        as_of=request.as_of,
                    )
                    provenance = self._build_provenance(descriptor, fingerprint, False)
                    yield chunk.with_request_context(request_id, request.correlation_id, provenance)
                except DataError as error:
                    error.request_id = request_id
                    raise
        except DataError as error:
            error.request_id = request_id
            raise

    # ------------------------------------------------------------------
    # Realtime
    # ------------------------------------------------------------------

    def subscribe(
        self,
        request: StreamRequest,
        sink: Callable[[MarketEvent], None],
        route: RouteOptions | None = None,
        control_sink: Callable[[StreamEvent], None] | None = None,
    ) -> Subscription:
        """Subscribe to a push stream.

        A single per-subscription lock guards the routing decision and delivery
        state; the user sink is invoked outside the lock.  Control events
        (:class:`DataGapEvent` / :class:`DataSourceStateEvent`) follow
        ``policy.gap_action``: ``raise`` raises :class:`DataGapError` with the
        request id, ``pause`` pauses the subscription (recoverable), and
        ``continue`` routes control events to ``control_sink`` if one is given.
        Cancelled subscriptions no longer receive deliveries.  The returned
        :class:`Subscription` is the thread-safe handle shared with ``cancel``.
        """
        request_id = uuid.uuid4().hex
        try:
            definition = self._catalog.get(request.dataset)
        except KeyError:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {request.dataset}",
                dataset=request.dataset,
                request_id=request_id,
            )
        opts = route if route is not None else RouteOptions()
        try:
            self._validate_request(request, definition, request_id)
            candidates = self._attributes_route(request.dataset, PUSH_MODE, request, opts, definition, request_id)
            primary = candidates[0]
            descriptor = primary.descriptor
            subscription = Subscription()
            lock = threading.Lock()
            self._subscriptions[subscription] = lock

            def raw_sink(event: StreamEvent) -> None:
                with lock:
                    active = subscription.is_active()
                if not active:
                    return
                if isinstance(event, (DataGapEvent, DataSourceStateEvent)):
                    action = self._policy.gap_action
                    if action == "raise":
                        message = getattr(event, "reason", None) or str(event)
                        error = DataGapError(
                            message,
                            dataset=request.dataset,
                            source=descriptor.name,
                            request_id=request_id,
                        )
                        object.__setattr__(subscription, "state", "failed")
                        object.__setattr__(subscription, "error", error.message)
                        raise error
                    if action == "pause":
                        with lock:
                            if subscription.is_active():
                                object.__setattr__(subscription, "state", "paused")
                                object.__setattr__(subscription, "error", str(event))
                        return
                    # continue -> expose control events on the control sink
                    if control_sink is not None:
                        control_sink(event)
                    return
                sink(event)

            with lock:
                self._invoke(
                    primary.adapter,
                    "subscribe",
                    request,
                    raw_sink,
                    dataset=request.dataset,
                    source=descriptor.name,
                    request_id=request_id,
                )
                if subscription.is_active():
                    object.__setattr__(subscription, "state", "active")
            return subscription
        except DataError as error:
            error.request_id = request_id
            raise

    def poll(self, request: StreamRequest, route: RouteOptions | None = None) -> Iterator[StreamEvent]:
        """Pull events from a polling adapter in the configured mode."""
        request_id = uuid.uuid4().hex
        try:
            definition = self._catalog.get(request.dataset)
        except KeyError:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {request.dataset}",
                dataset=request.dataset,
                request_id=request_id,
            )
        opts = route if route is not None else RouteOptions()
        try:
            self._validate_request(request, definition, request_id)
            candidates = self._attributes_route(request.dataset, POLL_MODE, request, opts, definition, request_id)
            primary = candidates[0]
            descriptor = primary.descriptor
            iterator = self._invoke(
                primary.adapter,
                "poll",
                request,
                dataset=request.dataset,
                source=descriptor.name,
                request_id=request_id,
            )
            for event in iterator:
                yield event
        except DataError as error:
            error.request_id = request_id
            raise

    def recover(
        self,
        request: StreamRequest,
        from_position: Any,
        route: RouteOptions | None = None,
    ) -> Iterator[MarketEvent]:
        """Replay market events from a stream position."""
        request_id = uuid.uuid4().hex
        try:
            definition = self._catalog.get(request.dataset)
        except KeyError:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {request.dataset}",
                dataset=request.dataset,
                request_id=request_id,
            )
        opts = route if route is not None else RouteOptions()
        try:
            self._validate_request(request, definition, request_id)
            candidates = self._attributes_route(request.dataset, RECOVERY_MODE, request, opts, definition, request_id)
            primary = candidates[0]
            descriptor = primary.descriptor
            iterator = self._invoke(
                primary.adapter,
                "recover",
                request,
                from_position,
                dataset=request.dataset,
                source=descriptor.name,
                request_id=request_id,
            )
            for event in iterator:
                yield event
        except DataError as error:
            error.request_id = request_id
            raise

    # ------------------------------------------------------------------
    # Calendar (dedicated route)
    # ------------------------------------------------------------------

    def sessions(self, request: CalendarRequest, route: RouteOptions | None = None) -> CalendarBatch:
        """Query trading-calendar sessions via the ``calendar.session`` route.

        Calendar requests bypass the :class:`DataRequest` router entirely
        (:class:`CalendarRequest` carries no ``dataset``/``fields``).  A binding
        declaring ``calendar`` capability on ``calendar.session`` is required.
        """
        request_id = uuid.uuid4().hex
        opts = route if route is not None else RouteOptions()
        candidates = self._routes.get((_CALENDAR_DATASET, CALENDAR_MODE))
        if not candidates:
            raise UnsupportedDatasetError(
                f"no dataset binding available for {_CALENDAR_DATASET}",
                dataset=_CALENDAR_DATASET,
                request_id=request_id,
            )
        base = list(candidates)
        if opts.adapter_name is not None:
            base = [c for c in base if c.descriptor.name == opts.adapter_name]
            if not base:
                raise UnsupportedDatasetError(
                    f"explicit adapter {opts.adapter_name!r} does not match dataset {_CALENDAR_DATASET}",
                    dataset=_CALENDAR_DATASET,
                    request_id=request_id,
                )
        base.sort(key=lambda c: c.binding.priority)
        primary = base[0]
        descriptor = primary.descriptor
        try:
            batch = self._invoke(
                primary.adapter,
                "sessions",
                request,
                dataset=_CALENDAR_DATASET,
                source=descriptor.name,
                request_id=request_id,
            )
        except DataError as error:
            error.request_id = request_id
            raise
        if not isinstance(batch, DataBatch):
            raise DataContractError(
                f"calendar adapter {descriptor.name!r} did not return a CalendarBatch",
                dataset=_CALENDAR_DATASET,
                request_id=request_id,
            )
        if not isinstance(batch, DataBatch) or batch.dataset != _CALENDAR_DATASET:
            raise DataContractError(
                f"calendar adapter {descriptor.name!r} did not return a CalendarBatch",
                dataset=_CALENDAR_DATASET, request_id=request_id,
            )
        previous = None
        seen = set()
        for record in batch.records:
            if not isinstance(record, Session):
                raise DataContractError(f"calendar adapter {descriptor.name!r} returned a non-Session record", dataset=_CALENDAR_DATASET, request_id=request_id)
            if not request.start <= record.trading_date <= request.end or record.trading_date in seen:
                raise DataContractError("calendar sessions are outside range or duplicated", dataset=_CALENDAR_DATASET, request_id=request_id)
            if previous is not None and record.trading_date <= previous:
                raise DataContractError("calendar sessions must be strictly ordered", dataset=_CALENDAR_DATASET, request_id=request_id)
            previous = record.trading_date
            seen.add(record.trading_date)
            for phase in record.phases:
                if phase.start.tzinfo is None or phase.end.tzinfo is None or phase.start > phase.end:
                    raise DataContractError("calendar phase has invalid timezone or bounds", dataset=_CALENDAR_DATASET, request_id=request_id)
        fingerprint = self._calendar_fingerprint(request)
        provenance = self._build_provenance(descriptor, fingerprint, False)
        return batch.with_request_context(request_id, None, provenance)

    def _calendar_fingerprint(self, request: CalendarRequest) -> str:
        payload = {
            "market": request.market,
            "start": request.start.isoformat(),
            "end": request.end.isoformat(),
            "timezone": request.timezone,
            "include_closed": request.include_closed,
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()


__all__ = ["DataGateway"]
