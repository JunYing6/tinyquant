from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any, Mapping

from tools.data import Bar, PriceLevel, QuoteTick, RegisteredEvent, TradeTick


def event_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.strptime(text, "%H:%M:%S").replace(tzinfo=timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def market_event_from_dict(value: Mapping[str, Any]):
    code = value.get("code") or value.get("instrument_id")
    timestamp = event_time(value.get("time") or value.get("event_time"))
    common = dict(schema_version="1", event_id=None, instrument_id=code, asset_type=None, effective_time=timestamp, event_time=timestamp, available_at=None, trading_date=timestamp.date(), source="compat", quality="valid", metadata={})
    kind = value.get("event_type") or value.get("type")
    if kind in ("quote", "quotes") or any(key in value for key in ("bid1_price", "ask1_price")):
        bids = () if value.get("bid1_price") is None else (PriceLevel(price=float(value["bid1_price"]), size=float(value.get("bid1_volume", 0)), level=1),)
        asks = () if value.get("ask1_price") is None else (PriceLevel(price=float(value["ask1_price"]), size=float(value.get("ask1_volume", 0)), level=1),)
        return QuoteTick(**common, event_type="quote", bid_levels=bids, ask_levels=asks, last_price=value.get("price"), last_size=value.get("volume"), sequence=value.get("sequence"))
    if kind in ("registered", "control"):
        return RegisteredEvent(**common, event_type=str(kind), values=dict(value))
    return TradeTick(**common, event_type="trade", price=float(value["price"]), size=float(value.get("volume", value.get("size", 0))), turnover=float(value.get("amount", value.get("turnover", 0))), side=value.get("side", "UNKNOWN"), sequence=value.get("sequence"), cumulative_volume=value.get("cumulative_volume"), cumulative_turnover=value.get("cumulative_turnover"))


def kline_bar_to_bar(value: Any) -> Bar:
    from trading_nodes_base.factors.types import KlineBar
    if isinstance(value, Bar):
        return value
    if not isinstance(value, KlineBar):
        raise TypeError("expected Bar or KlineBar")
    end = event_time(value.end_time)
    return Bar(schema_version="1", event_id=None, instrument_id=value.code, asset_type=None, effective_time=end, event_time=end, available_at=None, trading_date=end.date(), source="compat", quality="valid", metadata={}, frequency=value.frequency, interval_start=end, interval_end=end, open=value.open, high=value.high, low=value.low, close=value.close, volume=value.volume, turnover=value.amount, is_complete=True, price_basis="raw")

__all__ = ["event_time", "market_event_from_dict", "kline_bar_to_bar"]
