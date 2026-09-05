"""Provider-neutral selector, timer, and risk-control bases."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional, Set
from collections.abc import Sequence

import pandas as pd  # type: ignore[import-untyped]

from tools.data import DataBatch, DataRequest
from trading_nodes_base.requests import canonical_request, with_routing
from trading_nodes_base.factors import (
    BaseFactor,
    BinarySelectionFactor,
    FloatSelectionFactor,
    KlineTimingFactor,
    RiskKlineFactor,
    RiskTickFactor,
    TickTimingFactor,
    validate_selection_output,
    winsorize_and_zscore,
)
from trading_nodes_base.types import KlineBar, RiskSignal, SignalIntent, ExecutionRequest, normalize_frequency


@dataclass(frozen=True)
class RiskDecision:
    trading_allowed: bool = True
    target_position_ratio: float = 1.0
    force_liquidate: bool = False


@dataclass(frozen=True)
class RiskLayerResult:
    triggered: bool = False
    target_position_ratio: float = 1.0
    trading_allowed: bool = True
    force_liquidate: bool = False
    reason: str = ""


class StrategyContext:
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def has(self, key: str) -> bool:
        return key in self._data

    def clear(self) -> None:
        self._data.clear()

    def __repr__(self) -> str:
        return f"<StrategyContext keys={len(self._data)}>"


def _selection_kind_from_identity(factor: BaseFactor) -> str:
    if isinstance(factor, BinarySelectionFactor) and not isinstance(factor, FloatSelectionFactor):
        return "selection_binary"
    if isinstance(factor, FloatSelectionFactor) and not isinstance(factor, BinarySelectionFactor):
        return "selection_float"
    raise TypeError(f"{type(factor).__name__} must inherit exactly one selection base")


class BaseComponent(ABC):
    """Internal shared factor routing and context-injection implementation."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.all_factors: List[BaseFactor] = []
        self._idy_map: Dict[int, BaseFactor] = {}
        self._next_idy = 0
        self._next_request_id = 0
        self._next_generation = 0
        self.context: Optional[StrategyContext] = None

    @property
    def strategy_name(self) -> str:
        return self.name

    def set_context(self, context: StrategyContext) -> None:
        self.context = context
        for factor in self.all_factors:
            factor.context = context

    def get_queries(self, date: Any) -> List[Any]:
        queries: List[Any] = []
        for index, factor in enumerate(self.all_factors):
            factor._current_date = date
            self._begin_factor_generation(factor)
            factor_queries = factor.get_query_lst(date)
            self._idy_map[index] = factor
            queries.extend(
                self._route_factor_queries(
                    index, factor, factor_queries, begin_generation=False
                )
            )
            factor._mark_generation_ready()
        return queries

    def _begin_factor_generation(
        self, factor: BaseFactor, history: bool = False
    ) -> Any:
        self._next_generation += 1
        return factor._begin_request_generation(
            token=f"{self.name}:generation:{self._next_generation}",
            history=history,
        )

    def _route_factor_queries(
        self,
        index: int,
        factor: BaseFactor,
        factor_queries: Sequence[Any],
        history: bool = False,
        begin_generation: bool = True,
    ) -> List[Any]:
        self._idy_map[index] = factor
        if begin_generation:
            self._begin_factor_generation(factor, history=history)
        queries: List[Any] = []
        for query in factor_queries:
            request_id = f"{self.name}:{self._next_request_id}"
            self._next_request_id += 1
            factor._register_request(request_id, history=history)
            generation = (
                factor._active_history_generation
                if history
                else factor._active_generation
            )
            request = with_routing(canonical_request(query),
                idy=index,
                request_id=request_id,
                generation=generation,
                **({"request_channel": "history"} if history else {}),
            )
            queries.append(request)
        factor._mark_generation_ready(history=history)
        return queries

    def check_data(self) -> bool:
        return all(
            factor._is_ready()
            for factor in self.all_factors
        )

    def receive_data(self, sign: dict, data: Any) -> None:
        factor = self._idy_map.get(int(sign.get("idy", -1)))
        if factor is not None:
            is_history = (
                sign.get("request_channel") == "history"
                or sign.get("source") == "timer_history"
            )
            if is_history and "request_channel" not in sign:
                sign = {**sign, "request_channel": "history"}
            expected_request_ids = (
                factor._history_expected_request_ids
                if is_history
                else factor._expected_request_ids
            )
            request_id = sign.get("request_id")
            legacy_request_id = request_id
            generation = (
                factor._active_history_generation
                if is_history
                else factor._active_generation
            )
            if request_id not in expected_request_ids and request_id is not None:
                status = factor._history_request_status if is_history else factor._request_status
                pending = [key for key in expected_request_ids if not status.get(key, False)]
                if len(pending) == 1 and str(request_id) == self.name:
                    if is_history:
                        sign = {**sign, "request_id": pending[0]}
                        request_id = pending[0]
                    else:
                        legacy_request_id = request_id
                        expected_request_ids.append(str(request_id))
                        sign = {**sign, "request_id": pending[0]}
                        request_id = pending[0]
                else:
                    return
            supplied_generation = sign.get("generation")
            if supplied_generation is not None and supplied_generation != generation:
                if not (str(generation).startswith(f"{supplied_generation}:") and request_id in expected_request_ids):
                    return
                sign = {**sign, "generation": generation}
            factor.receive_data(sign, data)
            status = factor._history_request_status if is_history else factor._request_status
            if legacy_request_id is not None:
                legacy_key = str(legacy_request_id)
                if is_history:
                    status[legacy_key] = True
                elif legacy_key not in status:
                    status[legacy_key] = True

    def _reset_factor_state(self) -> None:
        self._idy_map.clear()
        self._next_idy = 0
        for factor in self.all_factors:
            factor._cancel_request_generation()
            factor._cancel_request_generation(history=True)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.name}>"


class BaseStockPicking(BaseComponent):
    STATE_IDLE = "IDLE"
    STATE_AND_QUERY = "AND_QUERY"
    STATE_OR_QUERY = "OR_QUERY"
    STATE_FLOAT_QUERY = "FLOAT_QUERY"
    STATE_DONE = "DONE"
    VIRTUAL_PASS_COL = "__virtual_pass__"

    def __init__(
        self,
        strategy_name: str,
        and_factors: List[BinarySelectionFactor],
        or_factors: List[BinarySelectionFactor],
        float_factors: List[FloatSelectionFactor],
        weights: Optional[Dict[str, float]] = None,
    ) -> None:
        super().__init__(strategy_name)
        self._validate_factor_slot("AND", and_factors, BinarySelectionFactor)
        self._validate_factor_slot("OR", or_factors, BinarySelectionFactor)
        self._validate_factor_slot("FLOAT", float_factors, FloatSelectionFactor)
        if {id(f) for f in and_factors + or_factors} & {id(f) for f in float_factors}:
            raise ValueError("a factor instance cannot occupy logical and FLOAT slots")
        self.and_factors = list(and_factors)
        self.or_factors = list(or_factors)
        self.float_factors = list(float_factors)
        self.all_factors = [*self.and_factors, *self.or_factors, *self.float_factors]
        self.factor_weights = weights or self._init_weights()
        self._state = self.STATE_IDLE
        self._current_date: Any = None
        self._valid_codes: Optional[Set[str]] = None
        self.factor_matrix = pd.DataFrame()

    @staticmethod
    def _validate_factor_slot(slot: str, factors: Sequence[BaseFactor], expected_type: type) -> None:
        expected_kind = "selection_binary" if expected_type is BinarySelectionFactor else "selection_float"
        for factor in factors:
            if _selection_kind_from_identity(factor) != expected_kind:
                raise TypeError(f"{slot} factor {type(factor).__name__} must be {expected_type.__name__}")

    def _init_weights(self) -> Dict[str, float]:
        if not self.float_factors:
            return {self.VIRTUAL_PASS_COL: 1.0}
        return {factor.factor_name: 1.0 / len(self.float_factors) for factor in self.float_factors}

    def reset_state(self, date: Any) -> None:
        self._state = self.STATE_IDLE
        self._current_date = date
        self._valid_codes = None
        self.factor_matrix = pd.DataFrame()
        self._reset_factor_state()

    def get_queries(self, date: Any) -> List[Any]:
        if self._state == self.STATE_DONE:
            self.reset_state(date)
        self._current_date = date
        if self._state != self.STATE_IDLE:
            if self._is_current_stage_ready():
                self._process_current_stage()
            else:
                return []
        return self._generate_next_stage_queries()

    def _generate_next_stage_queries(self) -> List[Any]:
        if self._state == self.STATE_IDLE:
            self._state = self.STATE_AND_QUERY
            queries = self._build_queries(self.and_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_OR_QUERY
                queries = self._build_queries(self.or_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_FLOAT_QUERY
                queries = self._build_queries(self.float_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_DONE
            return queries
        if self._state == self.STATE_AND_QUERY:
            if self._is_pool_empty():
                self._state = self.STATE_DONE
                return []
            self._state = self.STATE_OR_QUERY
            queries = self._build_queries(self.or_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_FLOAT_QUERY
                queries = self._build_queries(self.float_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_DONE
            return queries
        if self._state == self.STATE_OR_QUERY:
            if self._is_pool_empty():
                self._state = self.STATE_DONE
                return []
            self._state = self.STATE_FLOAT_QUERY
            queries = self._build_queries(self.float_factors)
            if not queries:
                if not self._process_queryless_stage():
                    return []
                self._state = self.STATE_DONE
            return queries
        if self._state == self.STATE_FLOAT_QUERY:
            self._state = self.STATE_DONE
        return []

    def _process_queryless_stage(self) -> bool:
        if not self._is_current_stage_ready():
            return False
        self._process_current_stage()
        return True

    def _is_pool_empty(self) -> bool:
        return self._valid_codes is not None and not self._valid_codes

    def check_data(self) -> bool:
        return self._state == self.STATE_DONE

    def is_done(self) -> bool:
        return self._state == self.STATE_DONE

    def _build_queries(self, factors: Sequence[BaseFactor]) -> List[Any]:
        queries: List[Any] = []
        for factor in factors:
            factor._current_date = self._current_date
            self._begin_factor_generation(factor)
            factor_queries = factor.get_query_lst(
                self._current_date,
                codes=list(self._valid_codes) if self._valid_codes is not None else None,
            )
            for query in factor_queries:
                index = self._next_idy
                self._next_idy += 1
                queries.extend(
                    self._route_factor_queries(
                        index, factor, [query], begin_generation=False
                    )
                )
            factor._mark_generation_ready()
        return queries

    def _get_current_stage_factors(self) -> List[BaseFactor]:
        if self._state == self.STATE_AND_QUERY:
            return list(self.and_factors)
        if self._state == self.STATE_OR_QUERY:
            return list(self.or_factors)
        if self._state == self.STATE_FLOAT_QUERY:
            return list(self.float_factors)
        return []

    def _is_current_stage_ready(self) -> bool:
        return all(
            factor._is_ready()
            for factor in self._get_current_stage_factors()
        )

    def _process_current_stage(self) -> None:
        if self._state == self.STATE_AND_QUERY:
            passed = self._calc_logical_factors(self.and_factors, "AND")
            if passed is not None:
                self._valid_codes = passed
        elif self._state == self.STATE_OR_QUERY:
            passed = self._calc_logical_factors(self.or_factors, "OR")
            if passed is not None:
                self._valid_codes = passed if self._valid_codes is None else self._valid_codes.intersection(passed)
        elif self._state == self.STATE_FLOAT_QUERY:
            self._calc_float_factors()

    def _calc_logical_factors(self, factors: List[BinarySelectionFactor], mode: str) -> Optional[Set[str]]:
        if not factors:
            return None
        result: Optional[Set[str]] = None
        for factor in factors:
            values = factor.calculate(date=self._current_date)
            validate_selection_output(factor.factor_name, "selection_binary", self._current_date, values)
            codes = set(values[values.astype(float).round(5) == 1].index)
            result = codes if result is None else (result & codes if mode == "AND" else result | codes)
        return result or set()

    def _calc_float_factors(self) -> None:
        if not self._valid_codes:
            self.factor_matrix = pd.DataFrame()
            return
        self.factor_matrix = pd.DataFrame(index=list(self._valid_codes))
        if not self.float_factors:
            self.factor_matrix[self.VIRTUAL_PASS_COL] = 1.0
            return
        for factor in self.float_factors:
            values = factor.calculate(date=self._current_date, codes=self._valid_codes)
            validate_selection_output(factor.factor_name, "selection_float", self._current_date, values)
            self.factor_matrix[factor.factor_name] = values

    def score_stocks(self, factor_matrix: Optional[pd.DataFrame] = None) -> pd.Series:
        matrix = factor_matrix if factor_matrix is not None else self.factor_matrix
        if matrix.empty:
            return pd.Series(dtype=float)
        processed = matrix.copy()
        for factor in self.float_factors:
            if factor.factor_name in processed:
                processed[factor.factor_name] = winsorize_and_zscore(processed[factor.factor_name])
        scores = pd.Series(0.0, index=processed.index)
        for name, weight in self.factor_weights.items():
            if name in processed:
                scores += processed[name].fillna(0) * weight
        return scores

    @abstractmethod
    def select_stocks(self, date: Any) -> pd.DataFrame:
        raise NotImplementedError


class BaseTimeSelection(BaseComponent):
    def __init__(
        self,
        name: str,
        kline_factors: Optional[List[KlineTimingFactor]] = None,
        tick_factors: Optional[List[TickTimingFactor]] = None,
    ) -> None:
        super().__init__(name)
        self.kline_factors = list(kline_factors or [])
        self.tick_factors = list(tick_factors or [])
        self._validate_governed_factors()
        self.all_factors = [*self.kline_factors, *self.tick_factors]
        self.pending_intents: List[SignalIntent] = []
        self.stock_pool: List[str] = []
        self.selector_pool: List[str] = []
        self.position_pool: List[str] = []
        self._selected_pool: List[str] = []
        self.positions: Dict[str, Any] = {}
        self.current_time: Optional[str] = None
        self.account_info: Optional[Dict[str, Any]] = None

    def _validate_governed_factors(self) -> None:
        if not self.tick_factors:
            raise ValueError("timer requires at least one TickTimingFactor")
        if not all(isinstance(f, KlineTimingFactor) for f in self.kline_factors):
            raise TypeError("kline_factors must contain KlineTimingFactor instances")
        if not all(isinstance(f, TickTimingFactor) for f in self.tick_factors):
            raise TypeError("tick_factors must contain TickTimingFactor instances")
        emitted = set().union(*(f.emitted_actions for f in self.kline_factors)) if self.kline_factors else set()
        accepted = set().union(*(f.accepted_intent_actions for f in self.tick_factors))
        missing = emitted - accepted
        if missing:
            raise ValueError(f"missing Tick execution coverage for actions: {sorted(missing)}")
        self.emitted_actions = frozenset(emitted)
        self.accepted_intent_actions = frozenset(accepted)

    @property
    def kline_frequencies(self) -> List[str]:
        return sorted({normalize_frequency(type(f).frequency) for f in self.kline_factors})

    @property
    def has_tick_execution_factor(self) -> bool:
        return bool(self.tick_factors)

    @property
    def fast_blockers(self) -> List[str]:
        return [f.factor_name for f in self.tick_factors if f.execution_role != "intent_executor"]

    @property
    def fast_eligible(self) -> bool:
        return bool(self.kline_factors) and not self.fast_blockers

    def set_stock_pool(self, pool: List[str]) -> None:
        self.selector_pool = list(dict.fromkeys(pool))
        self.position_pool = list(dict.fromkeys(self.positions))
        self._selected_pool = list(self.selector_pool)
        self.stock_pool = list(dict.fromkeys([*self.selector_pool, *self.position_pool]))

    @property
    def monitored_codes(self) -> List[str]:
        selected = set(self._selected_pool)
        return [code for code in self.stock_pool if code not in selected]

    def get_history_requirements(self, date: Any, codes: Optional[List[str]] = None) -> List[Any]:
        requested = list(codes if codes is not None else self.stock_pool)
        requirements: List[Any] = []
        for index, factor in enumerate(self.kline_factors):
            if not normalize_frequency(type(factor).frequency).endswith("d"):
                continue
            state = factor._snapshot_request_state()
            try:
                factor_queries = factor.get_history_requirements(date, requested)
            finally:
                factor._restore_request_state(state)
            requirements.extend(
                self._route_factor_queries(index, factor, factor_queries, history=True)
            )
        return requirements

    def update_positions(self, positions: Dict[str, Any]) -> None:
        self.positions = positions

    def on_orders_executed(self, executed_orders: List[Dict[str, Any]]) -> None:
        pass

    def on_daily(self, pool: List[str], positions: Dict[str, Any], account_info: Optional[Dict[str, Any]] = None, extra_data: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        for factor in self.all_factors:
            factor.reset_targets()
        self.update_positions(positions)
        self.set_stock_pool(pool)
        self.account_info = account_info
        self._extra_data = extra_data
        self._daily_logic()

    def kline_bar_input(self, bar: KlineBar) -> List[SignalIntent]:
        if not isinstance(bar, KlineBar):
            raise TypeError(f"kline_bar_input expected KlineBar, got {type(bar).__name__}")
        queued: List[SignalIntent] = []
        for factor in self.kline_factors:
            if normalize_frequency(type(factor).frequency) != bar.frequency or not factor.is_target_code(bar.code):
                continue
            output = self._validate_factor_output(factor, factor.on_bar(bar), SignalIntent)
            queued.extend(replace(item, metadata={**dict(item.metadata), "origin_factor": factor.factor_name}) for item in output)
        self.pending_intents.extend(queued)
        return queued

    def tick_input(self, tick: Dict[str, Any]) -> List[ExecutionRequest]:
        code = tick.get("code")
        self.current_time = tick.get("time")
        if not code:
            return []
        remaining = list(self.pending_intents)
        requests: List[ExecutionRequest] = []
        for factor in self.tick_factors:
            if not factor.in_window(self.current_time):
                continue
            matching = [item for item in remaining if item.code == code and item.action in factor.accepted_intent_actions]
            if not matching and not factor.is_target_code(code):
                continue
            outputs = self._validate_factor_output(factor, factor.on_tick(tick, tuple(matching)), ExecutionRequest)
            requests.extend(outputs)
            available_intents = list(matching)
            for request in outputs:
                for index, intent in enumerate(available_intents):
                    if request.code == intent.code and request.action == intent.action:
                        available_intents.pop(index)
                        if intent in remaining:
                            remaining.remove(intent)
                        break
        self.pending_intents = remaining
        return requests

    @staticmethod
    def _validate_factor_output(factor: BaseFactor, output: Any, expected_type: type) -> List[Any]:
        if not isinstance(output, Sequence) or isinstance(output, (str, bytes)):
            raise TypeError(f"{factor.factor_name} expected a sequence of {expected_type.__name__}")
        for index, value in enumerate(output):
            if not isinstance(value, expected_type):
                raise TypeError(f"{factor.factor_name} output[{index}] expected {expected_type.__name__}")
        return list(output)

    def assert_no_unconsumed_intents(self) -> None:
        if self.pending_intents:
            raise RuntimeError("unconsumed SignalIntent(s) at day end")

    def _daily_logic(self) -> None:
        pass


class BaseRiskControl(BaseComponent):
    def __init__(
        self,
        name: str,
        daily_factors: Optional[List[BaseFactor]] = None,
        tick_factors: Optional[List[BaseFactor]] = None,
        risk_kline_factors: Optional[List[RiskKlineFactor]] = None,
        risk_tick_factors: Optional[List[RiskTickFactor]] = None,
        risk_event_factors: Optional[List[BaseFactor]] = None,
        risk_float_factors: Optional[List[BaseFactor]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(name)
        self.params = dict(params or {})
        self.daily_factors = list(daily_factors or [])
        self.tick_factors = list(tick_factors or [])
        self.risk_kline_factors = list(risk_kline_factors or [])
        self.risk_tick_factors = list(risk_tick_factors or [])
        self.risk_event_factors = list(risk_event_factors or [])
        self.risk_float_factors = list(risk_float_factors or [])
        if not all(isinstance(f, RiskKlineFactor) for f in self.risk_kline_factors):
            raise TypeError("risk_kline_factors must contain RiskKlineFactor instances")
        if not all(isinstance(f, RiskTickFactor) for f in self.risk_tick_factors):
            raise TypeError("risk_tick_factors must contain RiskTickFactor instances")
        self.all_factors = [*self.daily_factors, *self.tick_factors, *self.risk_event_factors, *self.risk_float_factors, *self.risk_kline_factors, *self.risk_tick_factors]
        self.target_position_ratio = 1.0
        self.is_trading_allowed = True
        self.risk_decision = RiskDecision()
        self.daily_factor_data: Dict[str, Any] = {}

    def _calculate_daily_factors(self) -> None:
        for factor in self.daily_factors + self.risk_event_factors + self.risk_float_factors:
            self.daily_factor_data[factor.factor_name] = factor.calculate()

    def on_pre_market(self) -> RiskDecision:
        self._calculate_daily_factors()
        emergency = self._normalize_layer_result(self.evaluate_emergency(self.daily_factor_data))
        if emergency.triggered:
            return self._apply_layer_result(replace(emergency, force_liquidate=True, target_position_ratio=0.0, trading_allowed=False))
        base = self._normalize_layer_result(self.evaluate_base(self.daily_factor_data))
        dynamic = self.evaluate_dynamic(self.daily_factor_data, base) if len(inspect.signature(self.evaluate_dynamic).parameters) >= 2 else self.evaluate_dynamic(self.daily_factor_data)
        return self._apply_layer_result(self._normalize_layer_result(dynamic))

    @staticmethod
    def _normalize_layer_result(result: Any) -> RiskLayerResult:
        if isinstance(result, RiskLayerResult):
            return result
        if isinstance(result, bool):
            return RiskLayerResult(triggered=result)
        if isinstance(result, (int, float)):
            return RiskLayerResult(target_position_ratio=float(result))
        raise TypeError("risk layer must return RiskLayerResult")

    def _apply_layer_result(self, result: RiskLayerResult) -> RiskDecision:
        ratio = max(0.0, min(1.0, result.target_position_ratio))
        self.target_position_ratio = ratio
        self.is_trading_allowed = result.trading_allowed
        self.risk_decision = RiskDecision(result.trading_allowed, ratio, result.force_liquidate)
        return self.risk_decision

    def evaluate_emergency(self, daily_factor_data: Dict[str, Any]) -> RiskLayerResult:
        return RiskLayerResult()

    def evaluate_base(self, daily_factor_data: Dict[str, Any]) -> RiskLayerResult:
        return RiskLayerResult()

    def evaluate_dynamic(self, daily_factor_data: Dict[str, Any], current: Optional[RiskLayerResult] = None) -> RiskLayerResult:
        return current or RiskLayerResult()

    def on_daily(self) -> RiskDecision:
        return self.on_pre_market()

    def get_tick_codes(self) -> Set[str]:
        codes: Set[str] = set()
        for factor in self.risk_tick_factors:
            codes.update(getattr(factor, "_target_codes", set()))
        return codes

    def on_bar(self, bar: KlineBar) -> RiskDecision:
        signals: List[RiskSignal] = []
        for factor in self.risk_kline_factors:
            output = factor.on_bar(bar)
            signals.extend(output if isinstance(output, list) else [output])
        self._update_decision_from_signals(signals)
        return self.risk_decision

    def on_tick(self, tick: Dict[str, Any]) -> RiskDecision:
        signals: List[RiskSignal] = []
        for factor in self.risk_tick_factors:
            if factor.in_window(tick.get("time")):
                output = factor.on_tick(tick)
                signals.extend(output if isinstance(output, list) else [output])
        self._update_decision_from_signals(signals)
        return self.risk_decision

    def tick_input(self, tick: Dict[str, Any]) -> RiskDecision:
        return self.on_tick(tick)

    def _update_decision_from_signals(self, signals: List[RiskSignal]) -> None:
        for signal in signals:
            if not signal.triggered:
                continue
            if signal.risk_kind == "event" and (signal.value is None or signal.value):
                self.is_trading_allowed = False
            elif signal.risk_kind == "float" and signal.value is not None:
                self.target_position_ratio = max(0.0, min(1.0, signal.value))
        self.risk_decision = RiskDecision(self.is_trading_allowed, self.target_position_ratio, self.risk_decision.force_liquidate)


__all__ = ["BaseStockPicking", "BaseTimeSelection", "BaseRiskControl", "RiskDecision", "RiskLayerResult", "StrategyContext"]
