from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from tools.data_getter.market.schema import DataRequest


CODES = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "600519.SH"]
N_DAYS = 60


def _trade_dates() -> list[str]:
    current = date(2024, 1, 2)
    dates: list[str] = []
    while len(dates) < N_DAYS:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


class InMemoryCalendar:
    def __init__(self) -> None:
        self.dates = _trade_dates()

    def get_trade_dates(self, start: str, end: str) -> list[str]:
        return [date for date in self.dates if start <= date <= end]


class InMemoryStrategyData:
    def __init__(self) -> None:
        self.calendar = InMemoryCalendar()
        self._rows: dict[str, list[dict[str, Any]]] = {}
        for date in self.calendar.dates:
            self._rows[date] = [
                {
                    "code": code,
                    "date": date,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "volume": 1000,
                    "amount": 10500.0,
                }
                for code in CODES
            ]

    def fetch(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
        if request.scope != "market/daily":
            return []
        codes = set(request.get("codes") or CODES)
        return [row for row in self._rows.get(date, []) if row["code"] in codes]


def demo_data() -> InMemoryStrategyData:
    return InMemoryStrategyData()


__all__ = ["CODES", "InMemoryCalendar", "InMemoryStrategyData", "demo_data"]