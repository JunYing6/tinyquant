"""Provider protocols for market data and trading calendars."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from tools.data_getter.market.schema import DataRequest


@runtime_checkable
class MarketDataProvider(Protocol):
    def fetch(self, request: DataRequest, date: str) -> Any:
        """Fetch data for one normalized request and trading date."""


@runtime_checkable
class TradingCalendarProvider(Protocol):
    def get_trade_dates(self, start: str, end: str) -> list[str]:
        """Return trading dates in the inclusive requested range."""
