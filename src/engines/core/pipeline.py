"""Gateway-backed daily and intraday strategy data pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from engines.core.trading_clock import TradingDayContext
from tools.data import DataGateway, DataRequest, StreamEvent
from trading.requests import canonical_request


class DataProviderError(RuntimeError):
    pass


class UnifiedDataPipeline:
    def __init__(self, entity: Any, data_gateway: DataGateway, adapter: Any = None) -> None:
        if data_gateway is None:
            raise ValueError("data_gateway is required")
        self.entity = entity
        self.data_gateway = data_gateway
        self.adapter = adapter
        self.clock: Any = None

    def run_daily(self, value: str | datetime | TradingDayContext) -> None:
        decision_date, _, decision_datetime = self._resolve_day(value)
        queries = list(self.entity.prepare_requirements(decision_datetime))
        iterations = 0
        while True:
            if not queries:
                queries = list(self.entity.continue_pipeline(decision_datetime))
                if not queries:
                    return
            iterations += 1
            if iterations > 1_000:
                raise RuntimeError("pipeline exceeded 1000 continuation iterations")
            for query in queries:
                request = canonical_request(query)
                as_of = self.clock.as_of() if self.clock is not None else datetime.strptime(decision_date, "%Y%m%d")
                if as_of.tzinfo is None:
                    as_of = as_of.astimezone()
                from dataclasses import replace
                request = replace(request, anchor=request.anchor or as_of.date(), as_of=request.as_of or as_of)
                try:
                    batch = self.data_gateway.read(request)
                except Exception as error:
                    raise DataProviderError(f"gateway failed for {request.dataset} on {decision_date}: {error}") from error
                self.entity.receive_data(batch, request.delivery_key)
            queries = list(self.entity.continue_pipeline(decision_datetime))

    def run_intraday(self, events: Iterable[StreamEvent]) -> None:
        if self.adapter is None:
            return
        for event in events:
            self.adapter.feed_market_event(event)

    def run(self, value: str | datetime | TradingDayContext, events: Iterable[StreamEvent] | None = None) -> None:
        self.run_daily(value)
        if events is not None:
            self.run_intraday(events)

    @staticmethod
    def _resolve_day(value: str | datetime | TradingDayContext) -> tuple[str, str, datetime]:
        if isinstance(value, TradingDayContext):
            return value.decision_date, value.as_of_date, datetime.strptime(value.decision_date, "%Y%m%d")
        if isinstance(value, datetime):
            key = value.strftime("%Y%m%d")
            return key, key, value
        if isinstance(value, str):
            try:
                parsed = datetime.strptime(value, "%Y%m%d")
            except ValueError as error:
                raise ValueError("date must use YYYYMMDD") from error
            return value, value, parsed
        raise TypeError("date must be YYYYMMDD, datetime, or TradingDayContext")


__all__ = ["DataProviderError", "UnifiedDataPipeline"]
