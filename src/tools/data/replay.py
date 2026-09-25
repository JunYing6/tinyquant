r"""确定性的市场事件回放：时钟、排序、去重与投递。

统一数据扩展接口的任务 5。本模块将无序/重复的上游事件流转换为确定、
单调的回放。:class:`ReplayClock` 与 :class:`LiveClock` 追踪当前时刻
（自开盘起算）；:func:`sequence_events` 施加按品种或全局的
:math:`(event\_time, sequence)` 排序；:func:`deduplicate_events` 折叠重复
记录；:func:`normalize_trade_increment` 从累计成交量/成交额重建单笔成交
增量；:func:`replay_events` 仅在通过阶段、时点（point-in-time）、序列及
能力门控之后，才把每个市场事件驱动到 sink。

警告码关联
----------
回放层发现的问题在条件共享时复用 :mod:`tools.data.quality` 中的警告码字符串
—— ``W_ORDERING``、``W_DATA_GAP``、``W_PIT_MISSING`` 与 ``W_DUPLICATE_KEY``
与其 ``quality`` 对应项携带完全相同的标识符，方便使用者归并统计。回放层
特有的发现（``W_PHASE_SKIPPED``、``W_SESSION_RESET``、
``W_SEQUENCE_MISSING``）在本模块中定义了各自稳定的标识符。所有警告均通过
标准库 :mod:`warnings` 通道发出，消息以警告码开头。

序列有效性
----------
序列值必须非负：负数值 ``sequence`` 会使 :math:`(event\_time, sequence)`
排序失效（``-1`` 会排在所有 ``0..n`` 之前）。由于 :func:`sequence_events`
正是把原始适配器输出转化为有序流的环节，因此负序列无论 ``strict`` 与否都是
无条件抛出的 :class:`ValueError`——捕获它是适配器的保证，而非回放层能
掩盖的质量发现。
"""

from __future__ import annotations

import warnings
from dataclasses import replace
from datetime import datetime
from typing import Callable, Iterator

from .contracts import (
    DataGapEvent,
    DataSourceStateEvent,
    MarketEvent,
    QuoteTick,
    RegisteredEvent,
    Session,
    TradeTick,
)
from .errors import DataContractError, DataGapError, PointInTimeError

# 与 tools.data.quality 共享的警告码（相同字符串 -> 同一统计桶）。
W_ORDERING = "W_ORDERING"
W_DATA_GAP = "W_DATA_GAP"
W_PIT_MISSING = "W_PIT_MISSING"
W_DUPLICATE_KEY = "W_DUPLICATE_KEY"

# 回放层特有的警告码（本模块内稳定的标识符）。
W_PHASE_SKIPPED = "W_PHASE_SKIPPED"
W_SESSION_RESET = "W_SESSION_RESET"
W_SEQUENCE_MISSING = "W_SEQUENCE_MISSING"

_ORDERINGS: frozenset[str] = frozenset({"none", "per_instrument", "global"})
_MISSING_SEQ_WORDS = frozenset({"", "MISSING", "NONE", "NULL"})


def _is_control(event: MarketEvent | DataGapEvent | DataSourceStateEvent) -> bool:
    """控制事件（数据缺口/数据源状态）为 True，此类事件永远不会到达市场 sink。"""
    return isinstance(event, (DataGapEvent, DataSourceStateEvent))


def _event_time(event) -> datetime | None:
    """跨记录类型解析事件在时间轴上的位置。"""
    for attr in ("event_time", "detected_at", "occurred_at"):
        t = getattr(event, attr, None)
        if t is not None:
            return t
    return None


# ---------------------------------------------------------------------------
# 时钟
# ---------------------------------------------------------------------------


class ReplayClock:
    """驱动确定性回放的单调时钟。

    ``now`` 自会话开盘起算且只能向前推进：:meth:`advance` 传入早于当前时刻的
    时间戳将抛出 :class:`ValueError`，从而防止格式错误的流悄悄把时钟往回拨。
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._now = session_open(session)
        self._closed = False

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, event_time: datetime) -> None:
        """向前推进时钟；回拨将抛出 :class:`ValueError`。"""
        if event_time < self._now:
            raise ValueError(f"ReplayClock cannot move backwards: {event_time} < {self._now}")
        self._now = event_time

    def as_of(self) -> datetime:
        """返回当前时刻（同 :attr:`now`）。"""
        return self._now

    def close(self) -> None:
        """幂等地关闭时钟（空操作；为生命周期对称性预留）。"""
        self._closed = True


class LiveClock:
    """由外部观测到的时间戳推进的时钟。

    与 :class:`ReplayClock` 不同，它容忍乱序的*观测*：:meth:`update` 会把回拨
    的输入钳制住，使 :attr:`now` 保持单调而不抛出异常。
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._now = session_open(session)
        self._closed = False

    @property
    def now(self) -> datetime:
        return self._now

    def as_of(self) -> datetime:
        """返回当前时刻（同 :attr:`now`）。"""
        return self._now

    def update(self, observed_time: datetime) -> None:
        """推进到某个观测时间戳，钳制以保持单调。"""
        if observed_time > self._now:
            self._now = observed_time

    def close(self) -> None:
        """幂等地关闭时钟（空操作；为生命周期对称性预留）。"""
        self._closed = True


# ---------------------------------------------------------------------------
# 排序辅助
# ---------------------------------------------------------------------------


def _normalize_sequence(seq) -> tuple[int, object]:
    """将序列映射为可排序的 ``(class, value)`` 二元组。

    ``class`` 是整数（``0`` 缺失、``1`` 数值、``2`` 原始字符串），因此即使
    某些事件缺少序列，元组之间仍可相互比较。
    """
    if isinstance(seq, bool):
        return (0, ())
    if isinstance(seq, (int, float)):
        return (1, float(seq))
    if isinstance(seq, str):
        s = seq.strip()
        if s.upper() in _MISSING_SEQ_WORDS:
            return (0, ())
        try:
            return (1, float(s))
        except ValueError:
            return (2, s)
    return (0, ())


def _seq_sort_key(event):
    r"""事件的排序键 ``(event_time, sequence_class, sequence_value)``。

    遇到负数数值序列时抛出 :class:`ValueError`——一旦序列可能为负，
    :math:`(event\_time, sequence)` 排序所确立的顺序就不再有效，越过该界限
    即违反适配器契约。
    """
    event_time = _event_time(event)
    seq = getattr(event, "sequence", None)
    if isinstance(seq, (int, float)) and not isinstance(seq, bool) and seq < 0:
        raise ValueError(
            f"negative sequence {seq!r}: adapters must guarantee non-negative "
            "sequence so (event_time, sequence) ordering is well defined"
        )
    return (event_time, *_normalize_sequence(seq))


def _check_sequence_non_negative(event) -> None:
    """被 :func:`replay_events` 复用的硬性适配器保证检查。"""
    seq = getattr(event, "sequence", None)
    if isinstance(seq, (int, float)) and not isinstance(seq, bool) and seq < 0:
        raise ValueError(
            f"negative sequence {seq!r}: adapters must guarantee non-negative "
            "sequence so (event_time, sequence) ordering is well defined"
        )


def sequence_events(events, ordering: str = "none", strict: bool = True) -> Iterator[MarketEvent]:
    r"""对流施加 :math:`(event\_time, sequence)` 排序。

    * ``none`` —— 无排序保证：原样产出事件。
    * ``per_instrument`` —— 每个品种的事件内部有序；跨品种顺序任其自然。
    * ``global`` —— 全序列上的全序；每个市场事件都必须携带序列（控制事件
      除外）。缺失序列时，若 ``strict`` 为 True 则抛出
      :class:`DataContractError`，否则发出 ``W_SEQUENCE_MISSING`` 警告并尽力
      排序。
    """
    if ordering not in _ORDERINGS:
        raise ValueError(f"ordering must be one of none/per_instrument/global, got {ordering!r}")
    items = list(events)
    if ordering == "none":
        yield from items
        return
    if ordering == "per_instrument":
        groups: dict[object, list[object]] = {}
        seen: list[object] = []
        for event in items:
            instrument = getattr(event, "instrument_id", None)
            if instrument not in groups:
                groups[instrument] = []
                seen.append(instrument)
            groups[instrument].append(event)
        for instrument in seen:
            yield from sorted(groups[instrument], key=_seq_sort_key)
        return
    for event in items:
        if _is_control(event):
            continue
        if _normalize_sequence(getattr(event, "sequence", None))[0] == 0:
            message = (
                f"{W_SEQUENCE_MISSING}: global ordering requires a sequence for "
                f"{type(event).__name__} (sequence={getattr(event, 'sequence', None)!r})"
            )
            if strict:
                raise DataContractError(message, dataset=getattr(event, "dataset", None))
            warnings.warn(message, stacklevel=2)
    yield from sorted(items, key=_seq_sort_key)


# ---------------------------------------------------------------------------
# 去重
# ---------------------------------------------------------------------------


def deduplicate_events(events, key, strict: bool = True) -> Iterator[MarketEvent]:
    """产出唯一事件，丢弃共享 ``key`` 的后续记录。

    同键且内容完全相同的记录被静默去重。同键但*内容不一致*的两条记录在
    ``strict`` 时抛出 :class:`DataContractError`，否则发出
    ``W_DUPLICATE_KEY`` 警告并跳过后出现的记录。
    """
    seen: dict[object, object] = {}
    for event in events:
        k = key(event)
        if k in seen:
            prior = seen[k]
            if prior != event:
                message = f"{W_DUPLICATE_KEY}: duplicate key {k!r} with divergent content"
                if strict:
                    raise DataContractError(message)
                warnings.warn(message, stacklevel=2)
            continue
        seen[k] = event
        yield event


# ---------------------------------------------------------------------------
# 成交增量归一化
# ---------------------------------------------------------------------------


def normalize_trade_increment(previous: TradeTick | None, current: TradeTick) -> TradeTick:
    """根据累计字段重建单笔成交的 ``size``/``turnover`` 增量。

    当 ``current`` 携带 ``cumulative_volume``/``cumulative_turnover`` 时，单笔
    增量即为与 ``previous`` 之差。若无可比基线——首笔成交、品种不同、会话
    边界、累计值发生回退或基线缺失——则增量回退为累计值本身（即重置）并发出
    ``W_SESSION_RESET`` 警告。通过 :func:`dataclasses.replace` 返回新的
    :class:`TradeTick`；``current`` 不会被修改。
    """
    cumulative_volume = current.cumulative_volume
    cumulative_turnover = current.cumulative_turnover
    if cumulative_volume is None and cumulative_turnover is None:
        return current

    reset = False
    if previous is None:
        reset = True
    elif getattr(previous, "instrument_id", None) != current.instrument_id:
        reset = True
    elif getattr(previous, "trading_date", None) != current.trading_date:
        reset = True
    else:
        if cumulative_volume is not None:
            if previous.cumulative_volume is None:
                reset = True
            elif cumulative_volume < previous.cumulative_volume:
                reset = True
        if not reset and cumulative_turnover is not None:
            if previous.cumulative_turnover is None:
                reset = True
            elif cumulative_turnover < previous.cumulative_turnover:
                reset = True

    if reset:
        warnings.warn(
            f"{W_SESSION_RESET}: resetting trade increment for {current.instrument_id} "
            "[first tick / session boundary / cumulative regression]",
            stacklevel=2,
        )

    size = current.size if cumulative_volume is None else (
        cumulative_volume if reset else cumulative_volume - previous.cumulative_volume
    )
    turnover = current.turnover if cumulative_turnover is None else (
        cumulative_turnover if reset else cumulative_turnover - previous.cumulative_turnover
    )
    return replace(current, size=size, turnover=turnover)


# ---------------------------------------------------------------------------
# 会话辅助
# ---------------------------------------------------------------------------


def session_open(session: Session) -> datetime:
    """会话各阶段中的最早时刻。"""
    return session.open


def session_close(session: Session) -> datetime:
    """会话各阶段中的最晚时刻。"""
    return session.close


def _phase_at(session: Session | None, event_time: datetime):
    """返回包含 ``event_time`` 的阶段，缺口则返回 ``None``。"""
    if session is None or event_time is None:
        return None
    for phase in session.phases:
        if phase.start <= event_time <= phase.end:
            return phase
    return None


# ---------------------------------------------------------------------------
# 回放
# ---------------------------------------------------------------------------


def _deliver(sink: Callable[[MarketEvent], None], event: MarketEvent) -> None:
    """将事件路由到市场 sink。

    ``RegisteredEvent`` 实例只投递给显式订阅的 sink——暴露
    ``subscribed_event_types`` 属性（字符串容器）的 sink 只接收其中列出
    ``event_type`` 的 :class:`RegisteredEvent`。无 ``subscribed_event_types``
    属性的普通可调用对象接收所有市场事件。
    """
    subscribed = getattr(sink, "subscribed_event_types", None)
    if subscribed is not None and isinstance(event, RegisteredEvent):
        if event.event_type not in subscribed:
            return
    sink(event)


def replay_events(events, clock: ReplayClock | LiveClock, sink: Callable[[MarketEvent], None], strict: bool = True) -> None:
    """通过单调时钟将流回放到市场 sink。

    按流顺序处理每个事件：

    * 控制事件（:class:`DataGapEvent`/:class:`DataSourceStateEvent`）永不到达
      市场 sink。``strict`` 下缺口抛出 :class:`DataGapError`；否则发出
      ``W_DATA_GAP`` 警告并继续。
    * 时间戳落在任何会话阶段之外的市场事件（在时钟推进*之前*检查）在
      ``strict`` 下抛出 :class:`DataContractError`，否则以 ``W_PHASE_SKIPPED``
      警告跳过。
    * 负序列是硬性 :class:`ValueError`（适配器保证）。
    * 时点（point-in-time）：``available_at`` 晚于推进后的时钟时，``strict``
      下抛出 :class:`PointInTimeError`，否则以 ``W_PIT_MISSING`` 警告跳过。
      :class:`DataGapEvent` 不暴露 ``available_at``，故控制事件除外。
    * 不接受该事件类型（所在阶段上的 ``accepts_trades``/``accepts_quotes``）
      的流水线静默跳过。
    * 最终事件经由 ``_deliver`` 投递（尊重显式的
      :class:`RegisteredEvent` 订阅）。
    """
    for event in events:
        t = _event_time(event)
        if t is None:
            if strict:
                raise DataContractError(f"{type(event).__name__} has no timestamp to place on the replay timeline")
            warnings.warn(f"{W_ORDERING}: {type(event).__name__} with no timestamp skipped", stacklevel=2)
            continue

        if _is_control(event):
            clock.advance(t)
            if isinstance(event, DataGapEvent):
                gap_message = getattr(event, "reason", None) or f"data gap detected at {t}"
                if strict:
                    raise DataGapError(gap_message, dataset=getattr(event, "dataset", None))
                warnings.warn(f"{W_DATA_GAP}: {gap_message}", stacklevel=2)
            continue

        phase = _phase_at(clock.session, t)
        if phase is None:
            if strict:
                raise DataContractError(
                    f"{type(event).__name__} at {t} is outside any trading phase",
                    dataset=getattr(event, "dataset", None),
                )
            warnings.warn(
                f"{W_PHASE_SKIPPED}: skipping {type(event).__name__} at {t} outside any trading phase",
                stacklevel=2,
            )
            continue

        _check_sequence_non_negative(event)
        clock.advance(t)

        available_at = getattr(event, "available_at", None)
        if available_at is not None and available_at > clock.now:
            if strict:
                raise PointInTimeError(
                    f"available_at {available_at} is later than replay time {clock.now}"
                )
            warnings.warn(
                f"{W_PIT_MISSING}: available_at {available_at} later than replay time {clock.now}",
                stacklevel=2,
            )
            continue

        if isinstance(event, TradeTick) and not phase.accepts_trades:
            continue
        if isinstance(event, QuoteTick) and not phase.accepts_quotes:
            continue

        _deliver(sink, event)


__all__ = [
    "W_DATA_GAP",
    "W_DUPLICATE_KEY",
    "W_ORDERING",
    "W_PHASE_SKIPPED",
    "W_PIT_MISSING",
    "W_SEQUENCE_MISSING",
    "W_SESSION_RESET",
    "LiveClock",
    "ReplayClock",
    "deduplicate_events",
    "normalize_trade_increment",
    "replay_events",
    "sequence_events",
    "session_close",
    "session_open",
]