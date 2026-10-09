"""回测/实盘的流式数据订阅登记：按 ``(kind, code)`` 引用计数哪些来源需要该流。

需求驱动数据拉取：引擎只回放/读取被订阅的 code 的流，用不到就不空转。
``kind`` 为 ``"tick"`` 或 ``"kline:<frequency>"``（见 :func:`kline_kind`）。
"""

from __future__ import annotations

from typing import Iterable, Set

KIND_TICK = "tick"


def kline_kind(frequency: str) -> str:
    """K 线流的 kind，如 ``kline:30m``。"""
    from trading_nodes_base.types import normalize_frequency

    return f"kline:{normalize_frequency(frequency)}"


class StreamRegistry:
    """按 ``(kind, code) -> 来源集合`` 跟踪订阅；某 code 无来源即视为未订阅。"""

    def __init__(self) -> None:
        self._subs: dict[tuple[str, str], set[str]] = {}

    def subscribe(self, source: str, kind: str, codes: Iterable[str]) -> None:
        for code in codes:
            if code:
                self._subs.setdefault((kind, code), set()).add(source)

    def unsubscribe(self, source: str, kind: str, codes: Iterable[str]) -> None:
        for code in codes:
            key = (kind, code)
            owners = self._subs.get(key)
            if not owners:
                continue
            owners.discard(source)
            if not owners:
                self._subs.pop(key, None)

    def drop_source(self, source: str) -> None:
        """移除某来源的全部订阅（如某策略/撮合器下线）。"""
        for key in list(self._subs):
            owners = self._subs[key]
            owners.discard(source)
            if not owners:
                self._subs.pop(key, None)

    def codes(self, kind: str) -> Set[str]:
        """被订阅的某 kind 的全部 code。"""
        return {code for (entry_kind, code) in self._subs if entry_kind == kind}

    def kinds(self) -> Set[str]:
        return {kind for (kind, _) in self._subs}

    def is_subscribed(self, kind: str, code: str) -> bool:
        return bool(self._subs.get((kind, code)))

    def clear(self) -> None:
        self._subs.clear()


class SubscriptionTracker:
    """组件持有的订阅句柄：以固定 ``source`` 向 :class:`StreamRegistry` 增减 code。"""

    def __init__(self, registry: StreamRegistry, source: str) -> None:
        self.registry = registry
        self.source = source

    def want(self, kind: str, codes: Iterable[str]) -> None:
        self.registry.subscribe(self.source, kind, codes)

    def release(self, kind: str, codes: Iterable[str]) -> None:
        self.registry.unsubscribe(self.source, kind, codes)

    def drop(self) -> None:
        self.registry.drop_source(self.source)


__all__ = ["KIND_TICK", "SubscriptionTracker", "StreamRegistry", "kline_kind"]
