"""Provider-neutral multi-strategy stream base."""

from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

import pandas as pd  # type: ignore[import-untyped]

from tools.data import DataBatch, DataRequest
from trading_nodes_base.requests import canonical_request, decode_routing, with_routing

if TYPE_CHECKING:
    from trading_nodes_base.minds.base import BaseMind
    from trading_nodes_base.strategies.base import BaseStrategy


_MAX_RETURN_HISTORY = 504


class BaseStream:
    def __init__(self, stream_name: str, strategies: List[BaseStrategy], mind: BaseMind) -> None:
        self.stream_name = stream_name
        self.mind = mind
        names = [strategy.strategy_name for strategy in strategies]
        if len(names) != len(set(names)):
            raise ValueError("duplicate strategy_name values are not allowed")
        self.strategies: Dict[str, BaseStrategy] = {
            strategy.strategy_name: strategy for strategy in strategies
        }
        self.shadow_accounts: Dict[str, Any] = {}
        self.real_account: Any = None
        self._skip_children: set[str] = set()
        self._current_date: Optional[datetime] = None
        self._live_order_executor: Optional[Callable[[List[tuple]], None]] = None
        self._substrategy_return_history: Dict[str, deque[float]] = {}
        self._prev_substrategy_equity: Dict[str, float] = {}
        self.mind.set_stream(self)

    def set_live_order_executor(self, executor: Optional[Callable[[List[tuple]], None]]) -> None:
        self._live_order_executor = executor

    @staticmethod
    def _new_shadow_account(account: Any, money: float) -> Any:
        account_type = type(account)
        try:
            return account_type(money=money)
        except TypeError:
            return account_type(money)

    def set_account(self, account: Any) -> None:
        self.real_account = account
        self.shadow_accounts.clear()
        self._prev_substrategy_equity.clear()
        initial = account.total_equity / max(len(self.strategies), 1)
        for name, strategy in self.strategies.items():
            shadow = self._new_shadow_account(account, initial)
            strategy.set_account(shadow)
            self.shadow_accounts[name] = shadow
            self._prev_substrategy_equity[name] = initial

    def get_return_history(self, strategy_name: str, lookback: Optional[int] = None) -> Optional[pd.Series]:
        history = self._substrategy_return_history.get(strategy_name)
        if not history:
            return None
        series = pd.Series(list(history))
        return series.tail(lookback).reset_index(drop=True) if lookback and lookback > 0 else series.reset_index(drop=True)

    def prepare_requirements(self, date: datetime) -> List[DataRequest]:
        self._current_date = date
        queries: List[DataRequest] = []
        for strategy in self.strategies.values():
            for query in strategy.prepare_requirements(date):
                queries.append(with_routing(canonical_request(query), source_strategy=strategy.strategy_name))
        return queries

    @staticmethod
    def _normalize_query(query: Any) -> DataRequest:
        return canonical_request(query)

    def receive_data(self, batch: Any, delivery_key: Any = None) -> None:
        if isinstance(batch, dict) and delivery_key is not None and not isinstance(delivery_key, str):
            name = batch.get("source_strategy")
            if name in self.strategies:
                self.strategies[name].receive_data(batch, delivery_key)
            return
        sign = {**(decode_routing(delivery_key or getattr(batch, "request_id", "") or "") or {})}
        name = sign.get("source_strategy")
        if name in self.strategies:
            self.strategies[name].receive_data(batch, delivery_key)

    def continue_pipeline(self, date: datetime) -> List[DataRequest]:
        queries: List[DataRequest] = []
        for name, strategy in self.strategies.items():
            for query in strategy.continue_pipeline(date):
                queries.append(with_routing(canonical_request(query), source_strategy=name))
        return queries

    def on_daily(self, market_data: Optional[Dict[str, Any]] = None) -> None:
        for strategy in self.strategies.values():
            if strategy.strategy_name not in self._skip_children and not strategy._daily_pipeline_triggered:
                strategy._run_daily_pipeline()
        performance = self._get_strategies_performance()
        self.mind.current_weights = self.mind._validate_weights(self.mind.calculate_weights(market_data or {}, performance))

    def _get_strategies_performance(self) -> Dict[str, Dict[str, float]]:
        performance: Dict[str, Dict[str, float]] = {}
        for name, account in self.shadow_accounts.items():
            if name in self._skip_children:
                continue
            values = {"total_equity": account.total_equity, "balance": account.balance, "position_value": account.total_equity - account.balance}
            performance[name] = values
            self.mind.update_performance(name, values)
        return performance

    def _mirror_multiplier(self, strategy_name: str) -> float:
        return self.mind.current_weights.get(strategy_name, 0.0) * max(len(self.strategies), 1)

    def set_skip_children(self, names: set[str]) -> None:
        self._skip_children = set(names)

    def on_tick(self, tick: Dict[str, Any]) -> None:
        before = {name: dict(account.positions) for name, account in self.shadow_accounts.items()}
        for name, strategy in self.strategies.items():
            if name not in self._skip_children:
                strategy.on_tick(tick)
        orders = self._calc_net_orders(before, tick)
        if orders:
            self._submit_real_orders(orders)

    def _calc_net_orders(self, before: Dict[str, Dict[str, int]], tick: Dict[str, Any]) -> List[tuple]:
        net_deltas = self._net_weighted_deltas(before)
        return [
            (self._current_date, tick.get("time"), code, tick.get("price", 0), volume, "n")
            for code, volume in sorted(net_deltas.items())
            if volume
        ]

    def _net_weighted_deltas(
        self, before: Dict[str, Dict[str, int]]
    ) -> Dict[str, int]:
        weighted_deltas: Dict[str, float] = {}
        for name, account in self.shadow_accounts.items():
            if name in self._skip_children:
                continue
            multiplier = self._mirror_multiplier(name)
            if multiplier <= 0:
                continue
            old = before[name]
            for code in set(old) | set(account.positions):
                delta = account.positions.get(code, 0) - old.get(code, 0)
                weighted_deltas[code] = weighted_deltas.get(code, 0.0) + delta * multiplier
        return {
            code: round(volume)
            for code, volume in weighted_deltas.items()
            if round(volume)
        }

    def _submit_real_orders(self, orders: List[tuple]) -> None:
        if self._live_order_executor is not None:
            self._live_order_executor(orders)
        elif self.real_account is not None:
            self.real_account.order(orders)
        else:
            raise RuntimeError("Stream account is not configured")

    def daily_settle(self, price_dict: Dict[str, float]) -> None:
        if self.real_account is None:
            raise RuntimeError("Stream account is not configured")
        if hasattr(self.real_account, "daily_summarize"):
            self.real_account.daily_summarize(price_dict)
        for name, account in self.shadow_accounts.items():
            if name not in self._skip_children and hasattr(account, "daily_summarize"):
                account.daily_summarize(price_dict)
        self._update_substrategy_returns()
        for name, strategy in self.strategies.items():
            if name not in self._skip_children:
                strategy.on_day_end()

    def execute_daily(self, price_dict: Dict[str, float]) -> None:
        if self.real_account is None:
            raise RuntimeError("Stream account is not configured")
        before = {
            name: dict(account.positions)
            for name, account in self.shadow_accounts.items()
        }
        for name, strategy in self.strategies.items():
            if name not in self._skip_children:
                strategy.daily_execute(price_dict)

        net_deltas = self._net_weighted_deltas(before)

        orders = [
            (
                self._current_date,
                "15:00:00",
                code,
                price_dict.get(code, 0),
                volume,
                "n",
            )
            for code, volume in sorted(net_deltas.items())
            if volume and price_dict.get(code, 0) > 0
        ]
        if orders:
            self._submit_real_orders(orders)

    def _update_substrategy_returns(self) -> None:
        for name, account in self.shadow_accounts.items():
            if name in self._skip_children:
                continue
            history = self._substrategy_return_history.setdefault(name, deque(maxlen=_MAX_RETURN_HISTORY))
            current = account.total_equity
            previous = self._prev_substrategy_equity.get(name)
            if previous and previous > 0:
                history.append((current - previous) / previous)
            self._prev_substrategy_equity[name] = current


__all__ = ["BaseStream"]
