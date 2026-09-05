"""Provider-neutral factor bases and governed output contracts."""

from __future__ import annotations

from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd  # type: ignore[import-untyped]

from trading_nodes_base.types import (
    ExecutionRequest,
    KlineBar,
    RiskSignal,
    SignalIntent,
    normalize_frequency,
)


class _GovernedFactorMeta(ABCMeta):
    _PROTECTED_KIND_ATTRIBUTES = frozenset({"factor_kind", "_fixed_factor_kind"})

    @staticmethod
    def _is_governed(cls: type) -> bool:
        return any(
            "_fixed_factor_kind" in base.__dict__ for base in cls.__mro__
        )

    def __setattr__(cls, name: str, value: Any) -> None:
        if name in _GovernedFactorMeta._PROTECTED_KIND_ATTRIBUTES and _GovernedFactorMeta._is_governed(cls):
            raise TypeError(f"{name} is immutable on governed factor classes")
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        if name in _GovernedFactorMeta._PROTECTED_KIND_ATTRIBUTES and _GovernedFactorMeta._is_governed(cls):
            raise TypeError(f"{name} is immutable on governed factor classes")
        super().__delattr__(name)


class BaseFactor(ABC, metaclass=_GovernedFactorMeta):
    """Base lifecycle for factors that consume routed data or context."""

    output_schema = "series"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        fixed_kind = next(
            (
                base.__dict__["_fixed_factor_kind"]
                for base in cls.__mro__[1:]
                if "_fixed_factor_kind" in base.__dict__
            ),
            None,
        )
        if fixed_kind is not None and (
            "factor_kind" in cls.__dict__ or "_fixed_factor_kind" in cls.__dict__
        ):
            raise TypeError(
                "factor_kind is fixed by the governed base and cannot be declared "
                f"on {cls.__name__}"
            )

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "factor_kind":
            raise AttributeError("factor_kind is fixed by the governed factor base")
        super().__setattr__(name, value)

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.factor_name = factor_name
        self.params = dict(params or {})
        self.context: Any = None
        self._received_data: Dict[str, Any] = {}
        self._history_received_data: Dict[str, Any] = {}
        self._request_status: Dict[str, bool] = {}
        self._history_request_status: Dict[str, bool] = {}
        self._fit_request_status: Dict[str, bool] = {}
        self._history_fit_request_status: Dict[str, bool] = {}
        self._expected_request_ids: List[str] = []
        self._history_expected_request_ids: List[str] = []
        self.sign: Dict[str, bool] = {"fit": False, "data": False}
        self._current_date: Any = None
        self._generation_counter = 0
        self._active_generation: Optional[int] = None
        self._active_history_generation: Optional[int] = None
        self._consumed_generation: Optional[int] = None
        self._legacy_generation = False

    def __getattr__(self, name: str) -> Any:
        params = self.__dict__.get("params", {})
        if name in params:
            return params[name]
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    @abstractmethod
    def get_query_lst(
        self, date: Any, codes: Optional[List[str]] = None
    ) -> List[Any]:
        """Return declarative data requests for the given date."""

    def receive_data(self, sign: dict, data: Any) -> None:
        request_id = sign.get("request_id")
        is_history = (
            sign.get("request_channel") == "history"
            or request_id in self._history_expected_request_ids
        )
        expected_request_ids = (
            self._history_expected_request_ids
            if is_history
            else self._expected_request_ids
        )
        request_status = (
            self._history_request_status if is_history else self._request_status
        )
        fit_request_status = (
            self._history_fit_request_status
            if is_history
            else self._fit_request_status
        )
        received_data = (
            self._history_received_data if is_history else self._received_data
        )
        active_generation = (
            self._active_history_generation
            if is_history
            else self._active_generation
        )
        if active_generation is None:
            return
        generation = sign.get("generation")
        if generation is not None and generation != active_generation:
            return
        if request_id is not None and request_id not in expected_request_ids:
            return
        if request_id is None:
            if not expected_request_ids and (is_history or not self._legacy_generation):
                return
            pending = [
                key for key in expected_request_ids if not request_status.get(key, False)
            ]
            if expected_request_ids and not pending:
                return
            request_id = pending[0] if pending else str(sign.get("idx", "001"))
        key = str(request_id)
        if request_status.get(key, False) or fit_request_status.get(key, False):
            return
        received_data[key] = data
        if sign.get("fit_sign", False):
            fit_request_status[key] = True
            if not is_history:
                self.sign["fit"] = all(self._fit_request_status.values())
        else:
            request_status[key] = True
            if not is_history and self._expected_request_ids:
                self.sign["data"] = all(
                    self._request_status.get(expected, False)
                    for expected in self._expected_request_ids
                )
            elif not is_history:
                self.sign["data"] = all(self._request_status.values())

    def _register_request(self, request_id: str, history: bool = False) -> None:
        expected_request_ids = (
            self._history_expected_request_ids
            if history
            else self._expected_request_ids
        )
        if request_id not in expected_request_ids:
            expected_request_ids.append(request_id)

    def _begin_request_generation(
        self, token: Any = None, history: bool = False
    ) -> Any:
        self._generation_counter += 1
        generation = self._generation_counter if token is None else token
        if history:
            self._history_received_data.clear()
            self._history_request_status.clear()
            self._history_fit_request_status.clear()
            self._history_expected_request_ids.clear()
            self._active_history_generation = generation
        else:
            self._received_data.clear()
            self._request_status.clear()
            self._fit_request_status.clear()
            self._expected_request_ids.clear()
            self.sign = {"fit": False, "data": False}
            self._active_generation = generation
            self._legacy_generation = token is None
        return generation

    def _cancel_request_generation(self, history: bool = False) -> None:
        if history:
            self._history_received_data.clear()
            self._history_request_status.clear()
            self._history_fit_request_status.clear()
            self._history_expected_request_ids.clear()
            self._active_history_generation = None
            return
        self._received_data.clear()
        self._request_status.clear()
        self._fit_request_status.clear()
        self._expected_request_ids.clear()
        self._active_generation = None
        self._legacy_generation = False
        self.sign = {"fit": False, "data": False}

    def _mark_generation_ready(self, history: bool = False) -> None:
        if history:
            return
        self.sign["data"] = not self._expected_request_ids or all(
            self._request_status.get(request_id, False)
            for request_id in self._expected_request_ids
        )

    def _is_ready(self) -> bool:
        return bool(
            self._active_generation is not None
            and self.sign.get("fit")
            and self.sign.get("data")
            and all(
                self._request_status.get(request_id, False)
                for request_id in self._expected_request_ids
            )
        )

    def _snapshot_request_state(self) -> tuple[Any, ...]:
        return (
            dict(self._received_data),
            dict(self._history_received_data),
            dict(self._request_status),
            dict(self._history_request_status),
            dict(self._fit_request_status),
            dict(self._history_fit_request_status),
            list(self._expected_request_ids),
            list(self._history_expected_request_ids),
            dict(self.sign),
            self._current_date,
            self._active_generation,
            self._active_history_generation,
            self._consumed_generation,
            self._legacy_generation,
        )

    def _restore_request_state(self, state: tuple[Any, ...]) -> None:
        (
            received_data,
            history_received_data,
            request_status,
            history_request_status,
            fit_request_status,
            history_fit_request_status,
            expected_request_ids,
            history_expected_request_ids,
            sign,
            current_date,
            active_generation,
            active_history_generation,
            consumed_generation,
            legacy_generation,
        ) = state
        self._received_data.clear()
        self._received_data.update(received_data)
        self._history_received_data.clear()
        self._history_received_data.update(history_received_data)
        self._request_status.clear()
        self._request_status.update(request_status)
        self._history_request_status.clear()
        self._history_request_status.update(history_request_status)
        self._fit_request_status.clear()
        self._fit_request_status.update(fit_request_status)
        self._history_fit_request_status.clear()
        self._history_fit_request_status.update(history_fit_request_status)
        self._expected_request_ids[:] = expected_request_ids
        self._history_expected_request_ids[:] = history_expected_request_ids
        self.sign.clear()
        self.sign.update(sign)
        self._current_date = current_date
        self._active_generation = active_generation
        self._active_history_generation = active_history_generation
        self._consumed_generation = consumed_generation
        self._legacy_generation = legacy_generation

    def _data_clear(self) -> None:
        """Prepare a legacy direct factor request without affecting history."""
        if self._active_generation is None:
            self._begin_request_generation()
            return
        if self._legacy_generation:
            self._begin_request_generation()
            return
        self._received_data.clear()
        self._request_status.clear()
        self._fit_request_status.clear()
        self._expected_request_ids.clear()
        self.sign = {"fit": False, "data": False}

    def calculate(
        self, date: Optional[Any] = None, codes: Optional[Set[str]] = None
    ) -> pd.Series:
        if date is not None:
            self._current_date = date
        if not self._is_ready():
            raise ValueError(f"Data not ready for factor {self.factor_name}")

        result = self._calculate_internal(self._received_data)
        self._clear_after_calculate()
        result = self._normalize_index(result)
        kind = getattr(type(self), "factor_kind", None)
        if kind in {"selection_binary", "selection_float"}:
            validate_selection_output(
                self.factor_name, kind, self._current_date, result
            )
        elif kind in {"risk_event", "risk_float"}:
            validate_risk_output(self.factor_name, kind, self._current_date, result)
        if codes is not None:
            result = result[result.index.isin(codes)]
        return result

    def _calculate_internal(self, data_cache: Dict[str, Any]) -> pd.Series:
        return pd.Series(dtype=float)

    def _clear_after_calculate(self) -> None:
        self._consumed_generation = self._active_generation
        self._cancel_request_generation()

    @staticmethod
    def _normalize_index(result: Any) -> Any:
        if not isinstance(result, pd.Series):
            return result
        if isinstance(result.index, pd.MultiIndex):
            if "trade_date" in result.index.names:
                result = result.droplevel("trade_date")
            else:
                result = result.to_frame(name="value").iloc[:, -1]
        if result.index.name in (None, "code", "asset"):
            result.index.name = "ts_code"
        return result

    def on_tick(self, tick: Dict[str, Any]) -> Optional[Any]:
        return None

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.factor_name}>"


class BinarySelectionFactor(BaseFactor):
    _fixed_factor_kind = "selection_binary"
    factor_kind = "selection_binary"
    output_schema = "binary_series"


class FloatSelectionFactor(BaseFactor):
    _fixed_factor_kind = "selection_float"
    factor_kind = "selection_float"
    output_schema = "float_series"


class RiskEventFactor(BaseFactor):
    _fixed_factor_kind = "risk_event"
    factor_kind = "risk_event"
    output_schema = "binary_series"


class RiskFloatFactor(BaseFactor):
    _fixed_factor_kind = "risk_float"
    factor_kind = "risk_float"
    output_schema = "float_series"


class StreamFactor(BaseFactor):
    """Common target-code state for live K-line and tick factors."""

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        self._target_codes: Set[str] = set()
        self._targets_configured = False

    def set_targets(self, codes: Set[str]) -> None:
        self._target_codes = set(codes)
        self._targets_configured = True

    def is_target_code(self, code: str) -> bool:
        return not self._targets_configured or code in self._target_codes

    def reset_targets(self) -> None:
        self._target_codes.clear()
        self._targets_configured = False

    def _snapshot_request_state(self) -> tuple[Any, ...]:
        return super()._snapshot_request_state() + (
            set(self._target_codes),
            self._targets_configured,
        )

    def _restore_request_state(self, state: tuple[Any, ...]) -> None:
        super()._restore_request_state(state[:-2])
        self._target_codes = set(state[-2])
        self._targets_configured = state[-1]


class KlineTimingFactor(StreamFactor):
    _fixed_factor_kind = "timing_kline"
    factor_kind = "timing_kline"
    output_schema = "signal_intent"
    frequency = "1d"
    emitted_actions: frozenset[str] = frozenset()
    lookback_days: Optional[int] = None

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        normalize_frequency(type(self).frequency)

    @property
    def history_lookback_days(self) -> int:
        declared = type(self).lookback_days
        if declared is not None:
            return int(declared)
        return int(normalize_frequency(type(self).frequency)[:-1])

    def get_history_requirements(
        self, date: Any, codes: Optional[List[str]] = None
    ) -> List[Any]:
        return self.get_query_lst(date, codes=codes)

    def on_bar(self, bar: KlineBar) -> List[SignalIntent]:
        return []


class TickTimingFactor(StreamFactor):
    _fixed_factor_kind = "timing_tick"
    factor_kind = "timing_tick"
    output_schema = "execution_request"
    accepted_intent_actions: frozenset[str] = frozenset()
    tick_window: Optional[Tuple[str, str]] = None
    execution_role = "market_tick"

    def in_window(self, tick_time: Optional[str]) -> bool:
        if not self.tick_window:
            return True
        if not tick_time:
            return False
        start, end = self.tick_window
        return start <= tick_time <= end

    def on_tick(
        self, tick: Dict[str, Any], intents: Sequence[SignalIntent] = ()
    ) -> List[ExecutionRequest]:
        return []


class RiskKlineFactor(StreamFactor):
    _fixed_factor_kind = "risk_kline"
    factor_kind = "risk_kline"
    output_schema = "risk_signal"

    def on_bar(self, bar: KlineBar) -> List[RiskSignal]:
        return []


class RiskTickFactor(StreamFactor):
    _fixed_factor_kind = "risk_tick"
    factor_kind = "risk_tick"
    output_schema = "risk_signal"
    tick_window: Optional[Tuple[str, str]] = None

    def in_window(self, tick_time: Optional[str]) -> bool:
        if not self.tick_window:
            return True
        if not tick_time:
            return False
        start, end = self.tick_window
        return start <= tick_time <= end

    def on_tick(self, tick: Dict[str, Any]) -> List[RiskSignal]:
        return []


def winsorize_and_zscore(series: pd.Series, n_std: float = 3.0) -> pd.Series:
    if series.isnull().all():
        return series
    mean, std = series.mean(), series.std()
    if std == 0 or np.isnan(std):
        return series * 0
    clipped = series.clip(mean - n_std * std, mean + n_std * std)
    return (clipped - clipped.mean()) / clipped.std()


def validate_selection_output(
    factor_name: str, factor_kind: str, date: Any, values: Any
) -> pd.Series:
    if not isinstance(values, pd.Series):
        raise TypeError(
            f"{factor_name} returned {type(values).__name__}; expected pd.Series on {date}"
        )
    non_null = values.dropna()
    if factor_kind == "selection_binary":
        invalid = non_null[~non_null.isin([0, 1, False, True])]
        if not invalid.empty:
            raise ValueError(f"{factor_name} produced non-binary values on {date}")
    elif factor_kind == "selection_float":
        dtype = values.dtype
        if (
            not pd.api.types.is_numeric_dtype(dtype)
            or pd.api.types.is_bool_dtype(dtype)
            or pd.api.types.is_datetime64_any_dtype(dtype)
            or pd.api.types.is_timedelta64_dtype(dtype)
            or pd.api.types.is_complex_dtype(dtype)
        ):
            raise TypeError(f"{factor_name} produced invalid values on {date}")
        if not np.isfinite(non_null.to_numpy(dtype=float)).all():
            raise ValueError(f"{factor_name} produced non-finite values on {date}")
    else:
        raise ValueError(f"unknown factor_kind {factor_kind!r} for {factor_name}")
    return values


def validate_risk_output(
    factor_name: str, factor_kind: str, date: Any, values: Any
) -> pd.Series:
    if not isinstance(values, pd.Series):
        raise TypeError(
            f"{factor_name} returned {type(values).__name__}; expected pd.Series on {date}"
        )
    non_null = values.dropna()
    if factor_kind == "risk_event":
        invalid = non_null[~non_null.isin([0, 1, False, True])]
        if not invalid.empty:
            raise ValueError(f"{factor_name} produced non-binary risk events on {date}")
    elif factor_kind == "risk_float":
        if not pd.api.types.is_numeric_dtype(values.dtype) or pd.api.types.is_bool_dtype(values.dtype):
            raise TypeError(f"{factor_name} produced non-numeric risk values on {date}")
        if not np.isfinite(non_null.to_numpy(dtype=float)).all():
            raise ValueError(f"{factor_name} produced non-finite risk values on {date}")
    else:
        raise ValueError(f"unknown risk factor_kind {factor_kind!r} for {factor_name}")
    return values


__all__ = [
    "BaseFactor",
    "BinarySelectionFactor",
    "FloatSelectionFactor",
    "StreamFactor",
    "KlineTimingFactor",
    "TickTimingFactor",
    "RiskEventFactor",
    "RiskFloatFactor",
    "RiskKlineFactor",
    "RiskTickFactor",
    "validate_risk_output",
    "validate_selection_output",
    "winsorize_and_zscore",
]
