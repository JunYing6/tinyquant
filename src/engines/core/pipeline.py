"""Provider-driven daily and intraday strategy data pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
from typing import Any, Iterable

from engines.core.trading_clock import TradingDayContext
from tools.data import DataGateway, DataRequest
from trading.requests import canonical_request, decode_routing


class DataProviderError(RuntimeError):
    """A provider failure annotated with the request scope and date."""


class UnifiedDataPipeline:
    def __init__(self, entity: Any, data_provider: Any, adapter: Any = None) -> None:
        if data_provider is None:
            raise ValueError("data_provider is required")
        self.entity = entity
        self.data_provider = data_provider
        self.adapter = adapter

    def run_daily(self, value: str | datetime | TradingDayContext) -> None:
        decision_date, as_of_date, decision_datetime = self._resolve_day(value)
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
                legacy_query = query
                request = canonical_request(query)
                as_of = datetime.strptime(as_of_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                request = replace(
                    request,
                    anchor=request.anchor or as_of_date,
                    as_of=request.as_of or as_of,
                )
                try:
                    if hasattr(self.data_provider, "read"):
                        batch = self.data_provider.read(request)
                    else:
                        batch = self.data_provider.fetch(request, as_of_date)
                except Exception as error:
                    raise DataProviderError(f"provider failed for {getattr(legacy_query, 'scope', f"{request.dataset}/daily")} on {as_of_date}: {error}") from error
                if hasattr(self.data_provider, "read"):
                    self.entity.receive_data(batch, request.delivery_key)
                else:
                    params = {"idx": getattr(legacy_query, "idx", None) or request.delivery_key, "date": as_of_date}
                    self.entity.receive_data(params, batch)
            queries = list(self.entity.continue_pipeline(decision_datetime))

    def run_intraday(self, date: str, ticks: Iterable[dict[str, Any]]) -> None:
        if self.adapter is None:
            return
        for tick in ticks:
            self.adapter.feed_tick(tick, date)
        self.adapter.flush_day(date)

    def run(
        self,
        value: str | datetime | TradingDayContext,
        ticks: Iterable[dict[str, Any]] | None = None,
    ) -> None:
        self.run_daily(value)
        if ticks is not None:
            decision_date, _, _ = self._resolve_day(value)
            self.run_intraday(decision_date, ticks)

    @staticmethod
    def _resolve_day(value: str | datetime | TradingDayContext) -> tuple[str, str, datetime]:
        if isinstance(value, TradingDayContext):
            decision_date, as_of_date = value.decision_date, value.as_of_date
        elif isinstance(value, datetime):
            decision_date = as_of_date = value.strftime("%Y%m%d")
        elif isinstance(value, str):
            try:
                datetime.strptime(value, "%Y%m%d")
            except ValueError as error:
                raise ValueError("date must use YYYYMMDD") from error
            decision_date = as_of_date = value
        else:
            raise TypeError("date must be YYYYMMDD, datetime, or TradingDayContext")
        return decision_date, as_of_date, datetime.strptime(decision_date, "%Y%m%d")


__all__ = ["DataProviderError", "UnifiedDataPipeline"]
