"""Route ticks and completed bars to governed strategies."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping

from engines.core.kline_aggregator import KlineAggregator
from tools.data import Bar, MarketEvent, QuoteTick, RegisteredEvent, TradeTick
from trading_nodes_base.types import KlineBar
from trading_nodes_base.market_events import market_event_from_dict
from trading_nodes_base.strategies import BaseStrategy


def _strategy_bar_callback(strategy: BaseStrategy):
    def on_bar(bar: KlineBar) -> None:
        strategy.on_kline_bar(bar)

    return on_bar


class TradingContractAdapter:
    """Keep Tick ownership in strategies while centralizing K-line aggregation."""

    def __init__(self, entity: Any) -> None:
        self.entity = entity
        self.strategies: tuple[BaseStrategy, ...]
        if isinstance(entity, BaseStrategy):
            self.strategies = (entity,)
        else:
            self.strategies = tuple(
                strategy
                for strategy in getattr(entity, "strategies", {}).values()
                if isinstance(strategy, BaseStrategy)
            )
        if not self.strategies:
            raise TypeError("TradingContractAdapter requires a strategy or stream")
        self._current_date: date | None = None
        self._skip_names: set[str] = set()
        self._aggregators = {
            (strategy.strategy_name, frequency): KlineAggregator(
                frequency,
                _strategy_bar_callback(strategy),
            )
            for strategy in self.strategies
            for frequency in strategy.timer.kline_frequencies
        }

    @property
    def frequencies(self) -> tuple[str, ...]:
        return tuple(sorted({frequency for _, frequency in self._aggregators}))

    @property
    def tick_factors(self) -> tuple[Any, ...]:
        return tuple(
            factor for strategy in self.strategies for factor in strategy.timer.tick_factors
        )

    def set_skip_names(self, names: set[str]) -> None:
        self._skip_names = set(names)
        if hasattr(self.entity, "set_skip_children"):
            self.entity.set_skip_children(self._skip_names)

    def feed_market_event(self, event: MarketEvent | Mapping[str, Any]) -> None:
        if isinstance(event, Mapping):
            event = market_event_from_dict(event)
        if isinstance(event, RegisteredEvent):
            return
        self._current_date = event.trading_date or self._current_date
        for (name, _), aggregator in self._aggregators.items():
            if name in self._skip_names:
                continue
            if isinstance(event, TradeTick):
                aggregator.feed_trade(event)
        if isinstance(event, TradeTick):
            if hasattr(self.entity, "on_trade"):
                self.entity.on_trade(event)
            else:
                self.entity.on_tick({"code": event.instrument_id, "time": event.event_time.strftime("%H:%M:%S"), "price": event.price, "volume": event.size, "amount": event.turnover})
        elif isinstance(event, QuoteTick):
            if hasattr(self.entity, "on_quote"):
                self.entity.on_quote(event)
        for strategy in self.strategies:
            if strategy.strategy_name in self._skip_names or strategy.tick_matcher is None:
                continue
            if hasattr(strategy.tick_matcher, "match_event"):
                matches = strategy.tick_matcher.match_event(event, self._current_date)
            else:
                matches = strategy.tick_matcher.match({"code": event.instrument_id, "time": event.event_time, "price": getattr(event, "price", None)}, self._current_date)
            for order in matches:
                strategy.handle_order_event(
                    {
                        "order_id": order.order_id,
                        "status": order.status,
                        "requested_volume": order.volume,
                        "filled_volume": order.filled_volume,
                    }
                )

    def feed_execution_tick(self, tick: Mapping[str, Any]) -> None:
        self._current_date = self._tick_date(tick) or self._current_date
        self.entity.on_tick(dict(tick))

    def feed_completed_bar(self, bar: KlineBar) -> None:
        if not isinstance(bar, KlineBar):
            raise TypeError("completed source must be KlineBar")
        if bar.frequency.endswith("d"):
            raise ValueError("daily bars must use feed_daily_bar")
        matched = False
        for strategy in self.strategies:
            if strategy.strategy_name in self._skip_names:
                continue
            if bar.frequency in strategy.timer.kline_frequencies:
                strategy.on_kline_bar(bar)
                matched = True
        if not matched:
            raise ValueError(f"unsubscribed completed frequency: {bar.frequency}")

    def feed_prebuilt_bars(self, frequency: str, bars: list[KlineBar]) -> None:
        for (name, subscribed), aggregator in self._aggregators.items():
            if name not in self._skip_names and subscribed == frequency:
                aggregator.feed_prebuilt_bars(bars)

    def feed_daily_bar(self, bar: Bar) -> None:
        if not isinstance(bar, Bar) or bar.frequency != "1d":
            raise ValueError("daily source must be a 1d Bar")
        self._current_date = self._date_key(bar.interval_end)
        for (name, frequency), aggregator in self._aggregators.items():
            if name not in self._skip_names and frequency.endswith("d"):
                aggregator.feed_daily(bar)

    def seed_daily(self, bars: list[KlineBar]) -> None:
        for (_, frequency), aggregator in self._aggregators.items():
            if frequency.endswith("d"):
                aggregator.seed_daily(bars)

    def flush_day(self, trade_date: Any) -> None:
        self._current_date = self._date_key(trade_date)
        for (name, frequency), aggregator in self._aggregators.items():
            if name in self._skip_names:
                continue
            if frequency.endswith("d"):
                aggregator.flush_day(self._current_date)
            else:
                aggregator.flush_all()
        for strategy in self.strategies:
            if strategy.tick_matcher is not None:
                strategy.tick_matcher.expire_day(self._current_date)

    def pending_intents(self) -> tuple[Any, ...]:
        return tuple(
            intent
            for strategy in self.strategies
            for intent in strategy.timer.pending_intents
        )

    def assert_no_unconsumed_intents(self) -> None:
        if self.pending_intents():
            raise RuntimeError("unconsumed SignalIntent(s)")

    def assert_no_unresolved_execution_requests(self) -> None:
        unresolved = [
            request
            for strategy in self.strategies
            for request in strategy._pending_orders
        ]
        if unresolved:
            raise RuntimeError("unresolved ExecutionRequest(s)")

    @classmethod
    def _tick_date(cls, tick: Mapping[str, Any]) -> date | None:
        time_value = tick.get("time")
        if isinstance(time_value, datetime):
            return time_value.date()
        value = tick.get("trade_date") or tick.get("date")
        return cls._date_key(value) if value is not None else None

    @staticmethod
    def _date_key(value: Any) -> date | None:
        if value is None:
            return None
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
        raise ValueError("trading date must identify a calendar date")


__all__ = ["TradingContractAdapter"]
