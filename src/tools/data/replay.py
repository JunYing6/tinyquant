"""Deterministic market-event replay: clocks, ordering, dedup and delivery.

Task 5 of the unified data-extension interface.  This module turns an
unordered / duplicated upstream event stream into a deterministic, monotonic
replay.  :class:`ReplayClock` and :class:`LiveClock` track the current instant
(starting at the session open); :func:`sequence_events` imposes per-instrument
or global :math:`(event\_time, sequence)` ordering; :func:`deduplicate_events`
collapses duplicate records; :func:`normalize_trade_increment` reconstructs a
single-trade delta from cumulative volume / turnover; and :func:`replay_events`
drives every market event to a sink only after phase, point-in-time, sequence
and capability gating.

Warning-code relationship
-------------------------
Replay findings reuse the warning-code strings from :mod:`tools.data.quality`
where the condition is shared -- ``W_ORDERING``, ``W_DATA_GAP``,
``W_PIT_MISSING`` and ``W_DUPLICATE_KEY`` carry the exact same identifiers as
their ``quality`` counterparts so consumers can bucket them together.  Findings
that are specific to the replay layer (``W_PHASE_SKIPPED``,
``W_SESSION_RESET``, ``W_SEQUENCE_MISSING``) have their own stable identifiers
defined in this module.  All warnings are emitted through the stdlib
:mod:`warnings` channel with the code leading the message.

Sequence validity
-----------------
Sequence values must be non-negative: a negative numeric ``sequence`` makes the
:math:`(event\_time, sequence)` ordering ill-defined (a ``-1`` would sort ahead
of every ``0..n``).  Because :func:`sequence_events` is what turns raw adapter
output into a sorted stream, a negative sequence is an unconditional
:class:`ValueError` regardless of ``strict`` -- catching it is the adapter's
guarantee, not a quality finding that a replay can paper over.
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

# Warning codes shared with tools.data.quality (same strings -> same bucket).
W_ORDERING = "W_ORDERING"
W_DATA_GAP = "W_DATA_GAP"
W_PIT_MISSING = "W_PIT_MISSING"
W_DUPLICATE_KEY = "W_DUPLICATE_KEY"

# Replay-specific warning codes (stable identifiers local to this module).
W_PHASE_SKIPPED = "W_PHASE_SKIPPED"
W_SESSION_RESET = "W_SESSION_RESET"
W_SEQUENCE_MISSING = "W_SEQUENCE_MISSING"

_ORDERINGS: frozenset[str] = frozenset({"none", "per_instrument", "global"})
_MISSING_SEQ_WORDS = frozenset({"", "MISSING", "NONE", "NULL"})


def _is_control(event: MarketEvent | DataGapEvent | DataSourceStateEvent) -> bool:
    """True for control events (gaps / source-state) that never reach a market sink."""
    return isinstance(event, (DataGapEvent, DataSourceStateEvent))


def _event_time(event) -> datetime | None:
    """Resolve the event's position on the timeline across record kinds."""
    for attr in ("event_time", "detected_at", "occurred_at"):
        t = getattr(event, attr, None)
        if t is not None:
            return t
    return None


# ---------------------------------------------------------------------------
# Clocks
# ---------------------------------------------------------------------------


class ReplayClock:
    """A monotonic clock that drives a deterministic replay.

    ``now`` starts at the session open and may only move forward:
    :meth:`advance` with a timestamp earlier than the current instant raises
    :class:`ValueError` so a malformed stream cannot silently wind the clock
    back.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._now = session_open(session)
        self._closed = False

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, event_time: datetime) -> None:
        """Move the clock forward; going backwards raises :class:`ValueError`."""
        if event_time < self._now:
            raise ValueError(f"ReplayClock cannot move backwards: {event_time} < {self._now}")
        self._now = event_time

    def as_of(self) -> datetime:
        """Return the current instant (same as :attr:`now`)."""
        return self._now

    def close(self) -> None:
        """Idempotently close the clock (no-op; reserved for lifecycle symmetry)."""
        self._closed = True


class LiveClock:
    """A clock advanced by externally observed timestamps.

    Unlike :class:`ReplayClock`, out-of-order *observations* are tolerated:
    :meth:`update` clamps backwards input so :attr:`now` stays monotonic
    without raising.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._now = session_open(session)
        self._closed = False

    @property
    def now(self) -> datetime:
        return self._now

    def update(self, observed_time: datetime) -> None:
        """Advance to an observed timestamp, clamping to stay monotonic."""
        if observed_time > self._now:
            self._now = observed_time

    def close(self) -> None:
        """Idempotently close the clock (no-op; reserved for lifecycle symmetry)."""
        self._closed = True


# ---------------------------------------------------------------------------
# Ordering / sorting helpers
# ---------------------------------------------------------------------------


def _normalize_sequence(seq) -> tuple[int, object]:
    """Map a sequence to a sortable ``(class, value)`` pair.

    ``class`` is an int (``0`` missing, ``1`` numeric, ``2`` raw string) so
    tuples remain mutually comparable even when some events lack a sequence.
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
    """Sort key ``(event_time, sequence_class, sequence_value)`` for an event.

    Raises :class:`ValueError` for a negative numeric sequence -- the ordering a
    :math:`(event\_time, sequence)` sort establishes is undefined once sequence
    can be negative, and crossing that line is an adapter-contract violation.
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
    """Hard, adapter-guarantee check reused by :func:`replay_events`."""
    seq = getattr(event, "sequence", None)
    if isinstance(seq, (int, float)) and not isinstance(seq, bool) and seq < 0:
        raise ValueError(
            f"negative sequence {seq!r}: adapters must guarantee non-negative "
            "sequence so (event_time, sequence) ordering is well defined"
        )


def sequence_events(events, ordering: str = "none", strict: bool = True) -> Iterator[MarketEvent]:
    """Impose :math:`(event\_time, sequence)` ordering over a stream.

    * ``none`` -- no ordering guarantee: yields events unchanged.
    * ``per_instrument`` -- each instrument's events are ordered internally;
      cross-instrument order is left arbitrary.
    * ``global`` -- a total order over the whole stream; every market event must
      carry a sequence (control events are exempt).  A missing sequence raises
      :class:`DataContractError` when ``strict``, else warns
      ``W_SEQUENCE_MISSING`` and sorts best-effort.
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
# Deduplication
# ---------------------------------------------------------------------------


def deduplicate_events(events, key, strict: bool = True) -> Iterator[MarketEvent]:
    """Yield unique events, dropping later records that share ``key``.

    Identical content under the same key is silently deduplicated.  Two records
    under the same key with *divergent* content raise :class:`DataContractError`
    when ``strict``, otherwise warn ``W_DUPLICATE_KEY`` and skip the later one.
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
# Trade increment normalization
# ---------------------------------------------------------------------------


def normalize_trade_increment(previous: TradeTick | None, current: TradeTick) -> TradeTick:
    """Rebuild the single-trade ``size``/``turnover`` delta from cumulative fields.

    When ``current`` carries ``cumulative_volume`` / ``cumulative_turnover``,
    the per-trade increment is the difference against ``previous``.  If there is
    no comparable baseline -- first tick, different instrument, a session
    boundary, a cumulative value that regressed, or a missing baseline -- the
    increment falls back to the cumulative value itself (a reset) and warns
    ``W_SESSION_RESET``.  Returns a new :class:`TradeTick` via
    :func:`dataclasses.replace`; ``current`` is never mutated.
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
# Session helpers
# ---------------------------------------------------------------------------


def session_open(session: Session) -> datetime:
    """The earliest instant of a session's phases."""
    return session.open


def session_close(session: Session) -> datetime:
    """The latest instant of a session's phases."""
    return session.close


def _phase_at(session: Session | None, event_time: datetime):
    """Return the phase containing ``event_time``, or ``None`` for a gap."""
    if session is None or event_time is None:
        return None
    for phase in session.phases:
        if phase.start <= event_time <= phase.end:
            return phase
    return None


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def _deliver(sink: Callable[[MarketEvent], None], event: MarketEvent) -> None:
    """Route an event to the market sink.

    ``RegisteredEvent`` instances are only delivered to sinks that explicitly
    subscribe -- a sink exposing ``subscribed_event_types`` (a container of
    strings) only receives a :class:`RegisteredEvent` whose ``event_type`` is
    listed.  Plain callables (no ``subscribed_event_types`` attribute) receive
    every market event.
    """
    subscribed = getattr(sink, "subscribed_event_types", None)
    if subscribed is not None and isinstance(event, RegisteredEvent):
        if event.event_type not in subscribed:
            return
    sink(event)


def replay_events(events, clock: ReplayClock | LiveClock, sink: Callable[[MarketEvent], None], strict: bool = True) -> None:
    """Replay a stream through a monotonic clock to a market sink.

    For each event, in stream order:

    * Control events (:class:`DataGapEvent` / :class:`DataSourceStateEvent`)
      never reach the market sink.  Under ``strict`` a gap raises
      :class:`DataGapError`; otherwise it warns ``W_DATA_GAP`` and continues.
    * A market event whose timestamp falls outside any session phase (checked
      *before* the clock advances) raises :class:`DataContractError` under
      ``strict``, else is skipped with a ``W_PHASE_SKIPPED`` warning.
    * A negative sequence is a hard :class:`ValueError` (adapter guarantee).
    * Point-in-time: an ``available_at`` later than the advanced clock raises
      :class:`PointInTimeError` under ``strict``, else is skipped with a
      ``W_PIT_MISSING`` warning.  :class:`DataGapEvent` exposes no
      ``available_at``, so control events are exempt.
    * A pipeline that does not accept the event's kind (``accepts_trades`` /
      ``accepts_quotes`` on the containing phase) skips it silently.
    * Finally the event is delivered via ``_deliver`` (honouring explicit
      :class:`RegisteredEvent` subscriptions).
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