"""Provider-driven historical backtest engine without persistent state."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any, Iterable, Literal, Mapping, cast

import pandas as pd  # type: ignore[import-untyped]

from engines.core.account import Account
from engines.core.fast_execution import FastExecutionAdapter
from engines.core.performance import compute_stats
from engines.core.pipeline import DataProviderError, UnifiedDataPipeline
from engines.core.slippage import SlippageModel
from engines.core.tick_matching import TickMatchingEngine
from engines.core.trading_adapter import TradingContractAdapter
from tools.data_getter.market.schema import DataRequest
from tools.data_getter.providers import MarketDataProvider, TradingCalendarProvider
from trading.factors.types import ExecutionRequest, KlineBar
from trading.market_events import kline_bar_to_bar
from trading.strategies.base import BaseStrategy
from trading.streams.base import BaseStream


_VALID_MODES = {"auto", "fast", "tick", "kline", "daily"}


class FastBacktestEngine:
    """Run a strategy or stream entirely from injected market providers."""

    def __init__(
        self,
        entity: BaseStrategy | BaseStream,
        start_date: str,
        end_date: str,
        initial_capital: float = 1_000_000,
        mode: str = "auto",
        slippage: Mapping[str, Any] | SlippageModel | None = None,
        order_cost: dict[str, float] | None = None,
        progress_bar: bool = True,
        progress_callback: Any = None,
        data_provider: MarketDataProvider | None = None,
        calendar_provider: TradingCalendarProvider | None = None,
    ) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {sorted(_VALID_MODES)}")
        if data_provider is None:
            raise ValueError("data_provider is required")
        if calendar_provider is None:
            raise ValueError("calendar_provider is required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.entity = entity
        self.start_date = self._normalize_date(start_date)
        self.end_date = self._normalize_date(end_date)
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        self.initial_capital = float(initial_capital)
        self.data_provider = data_provider
        self.calendar_provider = calendar_provider
        self.progress_bar = progress_bar
        self.progress_callback = progress_callback
        self.is_stream = isinstance(entity, BaseStream)
        self.mode = self._resolve_mode(mode)
        self.slippage = self._init_slippage(slippage)
        self.account = Account(self.initial_capital, order_cost)
        self.entity.set_account(self.account)
        self._adapter = TradingContractAdapter(self.entity)
        self._pipeline = UnifiedDataPipeline(self.entity, self.data_provider, self._adapter)
        self._fast_adapters = {
            strategy.strategy_name: FastExecutionAdapter(
                getattr(strategy, "fast_buy_timing", "open"),
                getattr(strategy, "fast_sell_timing", "open"),
            )
            for strategy in self._strategies
        }
        if self.mode == "tick":
            for strategy in self._strategies:
                strategy.tick_matcher = TickMatchingEngine(
                    strategy.account.order,
                    strategy.handle_order_event,
                )
        self.trade_dates: list[str] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.daily_positions: list[dict[str, Any]] = []

    @property
    def _strategies(self) -> list[BaseStrategy]:
        if isinstance(self.entity, BaseStream):
            return list(self.entity.strategies.values())
        return [cast(BaseStrategy, self.entity)]

    def run(self) -> None:
        dates = self.calendar_provider.get_trade_dates(self.start_date, self.end_date)
        self.trade_dates = sorted({self._normalize_date(date) for date in dates})
        for index, date in enumerate(self.trade_dates, start=1):
            if self.progress_callback is not None:
                self.progress_callback(f"Trading day {date}", index, len(self.trade_dates))
            self._run_day(date)
        for adapter in self._fast_adapters.values():
            adapter.expire_day()

    def get_stats(self) -> dict[str, float | int]:
        if not self.equity_curve:
            return {}
        return compute_stats(self.equity_curve, self.initial_capital, self.account.trade_log)

    def _run_day(self, date: str) -> None:
        self._pipeline.run_daily(date)
        if isinstance(self.entity, BaseStream):
            self.entity.on_daily({})
        price_rows = self._fetch_records(
            DataRequest(
                "market",
                "daily",
                {"date": date, "fields": ["open", "high", "low", "close"]},
                "engine_prices",
            ),
            date,
        )
        price_dict, bars = self._price_data(price_rows, date)
        if self.mode == "fast":
            self._run_fast_day(price_dict, bars)
        else:
            self._run_tick_day(date, price_dict)
        self._settle_day(price_dict)
        self._record_day(date)

    def _run_fast_day(self, price_dict: dict[str, float], bars: list[KlineBar]) -> None:
        for bar in bars:
            for strategy in self._strategies:
                requests = self._fast_adapters[strategy.strategy_name].on_source_bar(bar)
                strategy._pending_orders.extend(self._apply_slippage(requests))
            self._adapter.feed_daily_bar(kline_bar_to_bar(bar))
            for strategy in self._strategies:
                intents = list(strategy.timer.pending_intents)
                if intents:
                    self._fast_adapters[strategy.strategy_name].queue(intents)
                    strategy.timer.pending_intents.clear()
        self._price_priceless_fast_requests(price_dict)
        if isinstance(self.entity, BaseStream):
            self.entity.execute_daily(price_dict)
        else:
            cast(BaseStrategy, self.entity).daily_execute(price_dict)

    def _run_tick_day(self, date: str, price_dict: dict[str, float]) -> None:
        codes = sorted(set(price_dict) | self._active_codes())
        records = self._fetch_records(
            DataRequest("market", "tick", {"date": date, "codes": codes}, "engine_ticks"),
            date,
        )
        for tick in sorted(records, key=lambda row: str(row.get("time", ""))):
            self._adapter.feed_tick(tick, date)
        self._adapter.flush_day(date)

    def _settle_day(self, price_dict: dict[str, float]) -> None:
        if isinstance(self.entity, BaseStream):
            self.entity.daily_settle(price_dict)
        else:
            self.account.daily_summarize(price_dict)
            cast(BaseStrategy, self.entity).on_day_end()

    def _record_day(self, date: str) -> None:
        self.equity_curve.append(
            {
                "date": date,
                "trade_date": date,
                "equity": self.account.total_equity,
                "balance": self.account.balance,
            }
        )
        self.daily_positions.append(
            {"date": date, "trade_date": date, "positions": self.account.positions}
        )

    def _fetch_records(self, request: DataRequest, date: str) -> list[dict[str, Any]]:
        try:
            value = self.data_provider.fetch(request, date)
        except Exception as error:
            raise DataProviderError(
                f"provider failed for {request.scope} on {date}: {error}"
            ) from error
        return self._records(value)

    @staticmethod
    def _records(value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, pd.DataFrame):
            return [dict(row) for row in value.to_dict("records")]
        if isinstance(value, Mapping):
            records = value.get("records")
            if isinstance(records, Iterable) and not isinstance(records, (str, bytes, Mapping)):
                return [dict(row) for row in records if isinstance(row, Mapping)]
            if all(isinstance(code, str) and isinstance(price, (int, float)) for code, price in value.items()):
                return [{"code": code, "close": price} for code, price in value.items()]
            return [dict(value)]
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return [dict(row) for row in value if isinstance(row, Mapping)]
        raise TypeError("market provider must return a DataFrame, mapping, or iterable of mappings")

    @staticmethod
    def _price_data(rows: list[dict[str, Any]], date: str) -> tuple[dict[str, float], list[KlineBar]]:
        prices: dict[str, float] = {}
        bars: list[KlineBar] = []
        for row in rows:
            code = row.get("code") or row.get("ts_code") or row.get("asset")
            close = row.get("close", row.get("price"))
            if not isinstance(code, str) or not isinstance(close, (int, float)) or close <= 0:
                continue
            open_price = row.get("open", close)
            high = row.get("high", max(open_price, close))
            low = row.get("low", min(open_price, close))
            if not all(isinstance(value, (int, float)) and value > 0 for value in (open_price, high, low)):
                continue
            prices[code] = float(close)
            bars.append(
                KlineBar(
                    code=code,
                    frequency="1d",
                    end_time=date,
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=float(row.get("volume", 0.0)),
                    amount=float(row.get("amount", 0.0)),
                )
            )
        return prices, bars

    def _active_codes(self) -> set[str]:
        codes: set[str] = set()
        for strategy in self._strategies:
            codes.update(strategy.current_positions)
            codes.update(strategy.timer.stock_pool)
        return codes

    def _resolve_mode(self, requested: str) -> str:
        if requested in {"kline", "daily"}:
            requested = "fast"
        eligible = all(
            strategy.supports_fast_backtest
            and strategy.timer.fast_eligible
            and not getattr(strategy.risk_ctrl, "risk_tick_factors", [])
            for strategy in self._strategies
        )
        if requested == "auto":
            return "fast" if eligible else "tick"
        if requested == "fast" and not eligible:
            raise ValueError("mode='fast' requires fast-eligible strategy components")
        return requested

    @staticmethod
    def _normalize_date(value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("dates must be YYYYMMDD strings")
        try:
            return datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d")
        except ValueError as error:
            raise ValueError("dates must use YYYYMMDD") from error

    @staticmethod
    def _init_slippage(value: Mapping[str, Any] | SlippageModel | None) -> SlippageModel:
        if isinstance(value, SlippageModel):
            return value
        if isinstance(value, Mapping):
            model = value.get("model", "proportional")
            if model not in {"none", "proportional", "fixed"}:
                raise ValueError("slippage model must be none, proportional, or fixed")
            return SlippageModel(
                buy_slippage=value.get("buy", 0.001),
                sell_slippage=value.get("sell", 0.001),
                model=cast(Literal["none", "proportional", "fixed"], model),
            )
        return SlippageModel(model="none")

    def _apply_slippage(self, requests: list[ExecutionRequest]) -> list[ExecutionRequest]:
        adjusted: list[ExecutionRequest] = []
        for request in requests:
            if request.price > 0:
                adjusted.append(
                    replace(
                        request,
                        price=self.slippage.apply(
                            request.price,
                            cast(Literal["BUY", "SELL"], request.action),
                        ),
                    )
                )
            else:
                adjusted.append(request)
        return adjusted

    def _price_priceless_fast_requests(self, price_dict: Mapping[str, float]) -> None:
        for strategy in self._strategies:
            priced: list[ExecutionRequest] = []
            for request in strategy._pending_orders:
                if request.price > 0:
                    priced.append(request)
                    continue
                price = price_dict.get(request.code, 0.0)
                if price <= 0:
                    priced.append(request)
                    continue
                priced.append(
                    replace(
                        request,
                        price=self.slippage.apply(
                            price,
                            cast(Literal["BUY", "SELL"], request.action),
                        ),
                    )
                )
            strategy._pending_orders = priced


__all__ = ["FastBacktestEngine"]
