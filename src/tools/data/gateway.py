"""目录驱动的数据网关：路由、回退、溯源与日历路由。

统一数据扩展接口的任务 4。:class:`DataGateway` 是应用程序实际交互的编排器。
它自身不持有任何数据——而是借助 :class:`DataCatalog` 契约、适配器的
:class:`AdapterDescriptor`/:class:`DataBinding` 能力声明，把请求路由到正确的
适配器，校验每个批次，将单个 ``request_id`` 贯穿一次调用及其所有错误，
应用重试/回退策略，并组装 :class:`DataProvenance`，使使用者能够审计每个
结果是在何处、以何种方式产出的。

此处强制遵循的设计规则：

* 本模块从不构造适配器，也不连接任何外部数据源；适配器经由 ``bindings``
  以已构建好的形式传入。
* 经 :meth:`read`/:meth:`iterate` 路由的 :class:`DataRequest` 使用
  ``historical`` 模式；:meth:`subscribe` 使用 ``push``；:meth:`poll` 使用
  ``poll``；:meth:`recover` 使用 ``recovery``。:meth:`sessions` 使用以
  ``calendar.session`` 为键的专用日历路由，绝不经过 ``DataRequest`` 路由。
* 仅 ``DataSourceError(retryable=True)`` 会被重试，且最多
  ``policy.max_retries`` 次，两次尝试之间间隔 ``policy.retry_backoff``。
  契约/时点/质量/不支持的错误从不重试，也从不回退。
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
    """一个可路由的 (dataset, mode) -> 适配器映射及其元数据。"""

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
    """编排路由、校验、溯源、重试与回退。"""

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
    # 构造 / 绑定校验
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
    # 生命周期
    # ------------------------------------------------------------------

    def open(self) -> None:
        """将网关标记为已打开。幂等；从不连接外部。"""
        self._open = True

    def close(self) -> None:
        """关闭每个暴露 ``close`` 方法的适配器。幂等。"""
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
    # 内部实现
    # ------------------------------------------------------------------

    def _validate_request(self, request: Any, definition: Any, request_id: str) -> None:
        """在请求结构支持之处对请求做契约校验。

        :class:`DataRequest` 携带 :func:`validate_request` 所检查的
        ``filters`` 结构；流式请求（:class:`StreamRequest`）没有 ``filters``，
        且已在构造时完成校验，故此处跳过。
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
    # 历史（read / iterate）
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
        """经由最匹配的适配器读取历史批次。"""
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
        """按块逐批产出经过校验的批次。

        每个块都会校验并使用相同的 ``request_id`` 重新溯源。迭代期间抛出的
        数据源错误会被包装并以请求 id 继续向上传播——它们被刻意*不*按块重试
        或重新路由（重试/回退仅适用于立即求值的 :meth:`read` 路径）。
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
    # 实时
    # ------------------------------------------------------------------

    def subscribe(
        self,
        request: StreamRequest,
        sink: Callable[[MarketEvent], None],
        route: RouteOptions | None = None,
        control_sink: Callable[[StreamEvent], None] | None = None,
    ) -> Subscription:
        """订阅推送流。

        每个订阅使用单把锁保护路由决策与投递状态；用户 sink 在锁外被调用。
        控制事件（:class:`DataGapEvent`/:class:`DataSourceStateEvent`）遵循
        ``policy.gap_action``：``raise`` 抛出带请求 id 的
        :class:`DataGapError`，``pause`` 暂停订阅（可恢复），``continue`` 则在
        提供了 ``control_sink`` 时把控制事件路由给它。已取消的订阅不再接收
        投递。返回的 :class:`Subscription` 是与 ``cancel`` 共享的线程安全句柄。
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
                    # continue -> 将控制事件暴露到控制 sink 上
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
        """按配置的模式从轮询适配器拉取事件。"""
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
        """从某个流位置回放市场事件。"""
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
    # 日历（专用路由）
    # ------------------------------------------------------------------

    def sessions(self, request: CalendarRequest, route: RouteOptions | None = None) -> CalendarBatch:
        """经由 ``calendar.session`` 路由查询交易日历会话。

        日历请求完全绕过 :class:`DataRequest` 路由（:class:`CalendarRequest`
        不携带 ``dataset``/``fields``）。需要有一个在 ``calendar.session`` 上
        声明 ``calendar`` 能力的绑定。
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
