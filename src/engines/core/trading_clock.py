"""Trading-day context shared by provider-driven pipelines."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TradingDayContext:
    decision_date: str
    as_of_date: str

    def __post_init__(self) -> None:
        if not (self.decision_date.isdigit() and self.as_of_date.isdigit()):
            raise ValueError("dates must use YYYYMMDD")
        if len(self.decision_date) != 8 or len(self.as_of_date) != 8:
            raise ValueError("dates must use YYYYMMDD")
        if self.as_of_date >= self.decision_date:
            raise ValueError("as_of_date must be before decision_date")


__all__ = ["TradingDayContext"]
