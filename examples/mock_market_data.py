"""Credential-free market and calendar providers for examples."""

from __future__ import annotations

from typing import Any

from tools.data_getter.market.schema import DataRequest


class InMemoryCalendar:
    def __init__(self, dates: list[str]) -> None:
        self.dates = list(dates)

    def get_trade_dates(self, start: str, end: str) -> list[str]:
        return [date for date in self.dates if start <= date <= end]


class InMemoryMarketDataProvider:
    def __init__(
        self,
        daily: dict[str, list[dict[str, Any]]],
        ticks: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.daily = daily
        self.ticks = ticks or {}

    def fetch(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
        if request.scope == "market/daily":
            return list(self.daily.get(date, []))
        if request.scope == "market/tick":
            return list(self.ticks.get(date, []))
        return []


SAMPLE_DAILY = {
    "20240102": [
        {"code": "000001.SZ", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}
    ],
    "20240103": [
        {"code": "000001.SZ", "open": 10.5, "high": 10.5, "low": 10.5, "close": 10.5}
    ],
}
