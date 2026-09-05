"""In-memory aggregation of ticks and completed daily bars."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Callable, Mapping

from tools.data import Bar, TradeTick
from trading_nodes_base.types import KlineBar, normalize_frequency


_SESSIONS = ((9 * 60 + 30, 11 * 60 + 30), (13 * 60, 15 * 60))


@dataclass
class _Buffer:
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    end_time: str | None = None
    trade_date: date | None = None

    @classmethod
    def from_tick(
        cls,
        price: float,
        volume: float,
        amount: float,
        end_time: str | None,
        trade_date: date | None,
    ) -> _Buffer:
        return cls(price, price, price, price, volume, amount, end_time, trade_date)

    def update(self, price: float, volume: float, amount: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += volume
        self.amount += amount


class KlineAggregator:
    """Aggregate A-share session ticks into a declared K-line frequency."""

    def __init__(self, frequency: str, on_bar: Callable[[KlineBar], None]) -> None:
        self.frequency = normalize_frequency(frequency)
        self.on_bar = on_bar
        self._interval = int(self.frequency[:-1])
        self._unit = self.frequency[-1]
        self._intraday_buffers: dict[str, _Buffer] = {}
        self._daily_buffers: dict[str, _Buffer] = {}
        self._daily_windows: dict[str, deque[KlineBar]] = {}
        self._last_tick_seconds: dict[str, int] = {}
        self._last_daily_dates: dict[str, date] = {}
        self._prebuilt_mode = False

    @property
    def is_prebuilt_mode(self) -> bool:
        return self._prebuilt_mode

    def feed_trade(self, trade: TradeTick) -> None:
        if not isinstance(trade, TradeTick):
            raise TypeError("KlineAggregator.feed_trade requires TradeTick")
        if self._prebuilt_mode:
            return
        parsed = (trade.instrument_id, *self._parse_time(trade.event_time), trade.price, trade.size, trade.turnover)
        code, minute, second, trade_date, price, volume, amount = parsed
        self._feed_parsed(code, minute, second, trade_date, price, volume, amount)

    def feed_tick(self, tick: Mapping[str, Any]) -> None:
        """Temporary dict compatibility for fast/realtime; remove in Task 8/9."""
        from trading_nodes_base.market_events import market_event_from_dict
        event = market_event_from_dict(tick)
        if not isinstance(event, TradeTick):
            raise TypeError("KlineAggregator.feed_tick requires a trade dict")
        self.feed_trade(event)

    def _feed_parsed(self, code: str, minute: int, second: int, trade_date: date | None, price: float, volume: float, amount: float) -> None:
        if self._prebuilt_mode:
            return
        previous = self._last_tick_seconds.get(code)
        if previous is not None and second < previous:
            raise ValueError(f"out-of-order tick for {code}")
        self._last_tick_seconds[code] = second

        if self._unit == "d":
            buffer = self._daily_buffers.get(code)
            if buffer is not None and buffer.trade_date is not None and trade_date is not None and buffer.trade_date != trade_date:
                raise ValueError(f"cross-day tick for {code} requires flush_day")
            if buffer is None:
                self._daily_buffers[code] = _Buffer.from_tick(price, volume, amount, None, trade_date)
            else:
                buffer.update(price, volume, amount)
            return

        end_time = self._intraday_end(minute)
        buffer = self._intraday_buffers.get(code)
        if buffer is not None and buffer.end_time != end_time:
            self._emit_intraday(code)
            buffer = None
        if buffer is None:
            self._intraday_buffers[code] = _Buffer.from_tick(price, volume, amount, end_time, trade_date)
        else:
            buffer.update(price, volume, amount)

    def flush_all(self) -> None:
        if self._unit == "d" or self._prebuilt_mode:
            return
        for code in sorted(tuple(self._intraday_buffers)):
            self._emit_intraday(code)
            self._last_tick_seconds.pop(code, None)

    def flush_day(self, trade_date: Any) -> None:
        self._require_daily_target()
        if self._prebuilt_mode:
            return
        source_date = self._date_key(trade_date)
        for code in sorted(tuple(self._daily_buffers)):
            buffer = self._daily_buffers.pop(code)
            if buffer.trade_date is not None and buffer.trade_date != source_date:
                raise ValueError(f"trade_date does not match buffered date for {code}")
            self._accept_daily(
                KlineBar(
                    code=code,
                    frequency="1d",
                    end_time=trade_date,
                    open=buffer.open,
                    high=buffer.high,
                    low=buffer.low,
                    close=buffer.close,
                    volume=buffer.volume,
                    amount=buffer.amount,
                ),
                source_date,
            )
            self._last_tick_seconds.pop(code, None)

    def seed_daily(self, bars: list[KlineBar]) -> None:
        self._require_daily_target()
        for bar in bars:
            self._validate_daily_bar(bar)
            self._accept_daily(bar, self._date_key(bar.end_time), emit=False)

    def feed_daily(self, bar: Bar) -> None:
        self._require_daily_target()
        self._validate_daily_bar(bar)
        source = KlineBar(bar.instrument_id or "", bar.frequency, bar.interval_end, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.turnover)
        self._accept_daily(source, self._date_key(source.end_time))

    def feed_prebuilt_bar(self, bar: KlineBar) -> None:
        if not isinstance(bar, KlineBar):
            raise TypeError("prebuilt bar must be KlineBar")
        if bar.frequency != self.frequency:
            return
        self._prebuilt_mode = True
        self._intraday_buffers.clear()
        self._daily_buffers.clear()
        self._last_tick_seconds.clear()
        self.on_bar(bar)

    def feed_prebuilt_bars(self, bars: list[KlineBar]) -> None:
        for bar in bars:
            self.feed_prebuilt_bar(bar)

    def _emit_intraday(self, code: str) -> None:
        buffer = self._intraday_buffers.pop(code)
        self.on_bar(
            KlineBar(
                code=code,
                frequency=self.frequency,
                end_time=buffer.end_time,
                open=buffer.open,
                high=buffer.high,
                low=buffer.low,
                close=buffer.close,
                volume=buffer.volume,
                amount=buffer.amount,
            )
        )

    def _accept_daily(self, source: KlineBar, source_date: date, emit: bool = True) -> None:
        previous = self._last_daily_dates.get(source.code)
        if previous is not None and source_date <= previous:
            raise ValueError(f"daily source for {source.code} is out of order")
        window = self._daily_windows.setdefault(source.code, deque(maxlen=self._interval))
        window.append(source)
        self._last_daily_dates[source.code] = source_date
        if emit and len(window) == self._interval:
            bars = list(window)
            self.on_bar(
                KlineBar(
                    code=source.code,
                    frequency=self.frequency,
                    end_time=bars[-1].end_time,
                    open=bars[0].open,
                    high=max(bar.high for bar in bars),
                    low=min(bar.low for bar in bars),
                    close=bars[-1].close,
                    volume=sum(bar.volume for bar in bars),
                    amount=sum(bar.amount for bar in bars),
                )
            )

    def _require_daily_target(self) -> None:
        if self._unit != "d":
            raise ValueError("operation requires a daily frequency")

    def _validate_daily_bar(self, bar: Bar) -> None:
        if not isinstance(bar, Bar) or bar.frequency != "1d":
            raise ValueError("daily source must be a 1d Bar")

    def _intraday_end(self, minute: int) -> str:
        interval = self._interval * (60 if self._unit == "h" else 1)
        for start, end in _SESSIONS:
            if start <= minute < end:
                offset = minute - start
                bucket_start = start + offset // interval * interval
                bucket_end = min(bucket_start + interval, end)
                return f"{bucket_end // 60:02d}:{bucket_end % 60:02d}:00"
        raise ValueError("tick is outside the trading session")

    @classmethod
    def _parse_tick(
        cls, tick: Mapping[str, Any]
    ) -> tuple[str, int, int, date | None, float, float, float] | None:
        if not isinstance(tick, Mapping):
            return None
        code = tick.get("code")
        parsed_time = cls._parse_time(tick.get("time"))
        price, volume, amount = tick.get("price"), tick.get("volume", 0), tick.get("amount", 0)
        if not isinstance(code, str) or not code or parsed_time is None:
            return None
        if (
            not isinstance(price, (int, float))
            or isinstance(price, bool)
            or not isinstance(volume, (int, float))
            or isinstance(volume, bool)
            or not isinstance(amount, (int, float))
            or isinstance(amount, bool)
        ):
            return None
        if not cls._valid_number(price, positive=True) or not cls._valid_number(volume) or not cls._valid_number(amount):
            return None
        minute, second, trade_date = parsed_time
        if not any(start * 60 <= second <= end * 60 for start, end in _SESSIONS):
            return None
        if any(second == end * 60 for _, end in _SESSIONS):
            minute -= 1
        return code, minute, second, trade_date, float(price), float(volume), float(amount)

    @staticmethod
    def _parse_time(value: Any) -> tuple[int, int, date | None] | None:
        trade_date: date | None = None
        if isinstance(value, datetime):
            parsed = value.time()
            trade_date = value.date()
        elif isinstance(value, time):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = datetime.strptime(value, "%H:%M:%S").time()
            except ValueError:
                try:
                    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    return None
                parsed, trade_date = timestamp.time(), timestamp.date()
        else:
            return None
        minute = parsed.hour * 60 + parsed.minute
        return minute, minute * 60 + parsed.second, trade_date

    @staticmethod
    def _valid_number(value: Any, positive: bool = False) -> bool:
        if isinstance(value, bool):
            return False
        try:
            return math.isfinite(value) and (value > 0 if positive else value >= 0)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _date_key(value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            for pattern in ("%Y%m%d", "%Y-%m-%d"):
                try:
                    return datetime.strptime(value[:10], pattern).date()
                except ValueError:
                    pass
        raise ValueError("value must identify a trading date")


__all__ = ["KlineAggregator"]
