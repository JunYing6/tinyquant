"""提供者无关的因子基类与受治理的输出契约。"""

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
    """消费被路由数据或上下文信息的因子的基础生命周期。"""

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
        self._node_prefix: str = ""
        self._received_data: Dict[str, Any] = {}
        self._history_received_data: Dict[str, Any] = {}
        self._request_status: Dict[str, bool] = {}
        self._history_request_status: Dict[str, bool] = {}
        self._expected_request_ids: List[str] = []
        self._history_expected_request_ids: List[str] = []
        self.sign: Dict[str, bool] = {"data": False}
        self._current_date: Any = None
        self._generation_counter = 0
        self._active_generation: Optional[int] = None
        self._active_history_generation: Optional[int] = None
        self._consumed_generation: Optional[int] = None
        self._legacy_generation = False
        # 数据需求/目标代码（择时器、风控器统一按此聚合与分发；流式/声明式通用）。
        self._target_codes: Set[str] = set()
        self._targets_configured = False
        self._needed: Set[str] = set()
        self._on_needs_change: Optional[Any] = None

    def __getattr__(self, name: str) -> Any:
        params = self.__dict__.get("params", {})
        if name in params:
            return params[name]
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def set_node_prefix(self, prefix: str) -> None:
        self._node_prefix = prefix

    def set_needs_callback(self, callback: Optional[Any]) -> None:
        self._on_needs_change = callback

    def set_needed(self, codes: Optional[Sequence[str]]) -> None:
        self._needed = set(codes or ())
        self._target_codes = set(self._needed)
        self._targets_configured = bool(self._needed)
        self._notify_needs_change()

    def drop_required_codes(self, code: str) -> None:
        if code not in self._needed:
            return
        self._needed.discard(code)
        self._target_codes.discard(code)
        if not self._target_codes:
            self._targets_configured = False
        self._notify_needs_change()

    def _notify_needs_change(self) -> None:
        if self._on_needs_change is not None:
            self._on_needs_change(self)

    def required_codes(
        self,
        pool: Optional[List[str]] = None,
        positions: Optional[Dict[str, Any]] = None,
        account_info: Optional[Dict[str, Any]] = None,
        **extra: Any,
    ) -> List[str]:
        """数据需求声明钩子：返回本因子需要获取 tick/K 线数据的代码集合。

        基类默认返回 ``set_needed`` 声明的代码（未声明则返回 []）；子类可覆写。
        因子可通过 ``drop_required_codes`` 在日内缩减自身需求，择时器据此重聚合。
        """
        return list(self._needed)

    def is_target_code(self, code: str) -> bool:
        return not self._targets_configured or code in self._target_codes

    def _set_target_codes(self, codes: Optional[List[str]]) -> None:
        self._target_codes = set(codes or ())
        self._targets_configured = bool(codes)

    @property
    def state(self) -> Any:
        if self.context is None:
            raise RuntimeError(f"factor {self.factor_name!r} context not injected; call set_context first")
        key = f"{self._node_prefix}:{self.factor_name}" if self._node_prefix else self.factor_name
        return self.context.node(key)

    @abstractmethod
    def get_query_lst(
        self, date: Any, codes: Optional[List[str]] = None
    ) -> List[Any]:
        """返回针对给定日期的声明式数据请求。"""

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
        if request_status.get(key, False):
            return
        received_data[key] = data
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
            self._history_expected_request_ids.clear()
            self._active_history_generation = generation
        else:
            self._received_data.clear()
            self._request_status.clear()
            self._expected_request_ids.clear()
            self.sign = {"data": False}
            self._active_generation = generation
            self._legacy_generation = token is None
        return generation

    def _cancel_request_generation(self, history: bool = False) -> None:
        if history:
            self._history_received_data.clear()
            self._history_request_status.clear()
            self._history_expected_request_ids.clear()
            self._active_history_generation = None
            return
        self._received_data.clear()
        self._request_status.clear()
        self._expected_request_ids.clear()
        self._active_generation = None
        self._legacy_generation = False
        self.sign = {"data": False}

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
            list(self._expected_request_ids),
            list(self._history_expected_request_ids),
            dict(self.sign),
            self._current_date,
            self._active_generation,
            self._active_history_generation,
            self._consumed_generation,
            self._legacy_generation,
            set(self._target_codes),
            self._targets_configured,
        )

    def _restore_request_state(self, state: tuple[Any, ...]) -> None:
        (
            received_data,
            history_received_data,
            request_status,
            history_request_status,
            expected_request_ids,
            history_expected_request_ids,
            sign,
            current_date,
            active_generation,
            active_history_generation,
            consumed_generation,
            legacy_generation,
            target_codes,
            targets_configured,
        ) = state
        self._received_data.clear()
        self._received_data.update(received_data)
        self._history_received_data.clear()
        self._history_received_data.update(history_received_data)
        self._request_status.clear()
        self._request_status.update(request_status)
        self._history_request_status.clear()
        self._history_request_status.update(history_request_status)
        self._expected_request_ids[:] = expected_request_ids
        self._history_expected_request_ids[:] = history_expected_request_ids
        self.sign.clear()
        self.sign.update(sign)
        self._current_date = current_date
        self._active_generation = active_generation
        self._active_history_generation = active_history_generation
        self._consumed_generation = consumed_generation
        self._legacy_generation = legacy_generation
        self._target_codes = set(target_codes)
        self._targets_configured = targets_configured

    def _data_clear(self) -> None:
        """准备一次不影响历史数据的传统直连因子请求。"""
        if self._active_generation is None:
            self._begin_request_generation()
            return
        if self._legacy_generation:
            self._begin_request_generation()
            return
        self._received_data.clear()
        self._request_status.clear()
        self._expected_request_ids.clear()
        self.sign = {"data": False}

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

    def stream_requirement(
        self, codes: Optional[Sequence[str]] = None
    ) -> Optional[Tuple[str, Optional[str], List[str]]]:
        """数据供给方式声明：流式因子返回 ``(kind, frequency, codes)``，声明式返回 ``None``。"""
        return None

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.factor_name}>"


# --- 二级：数据供给方式 -------------------------------------------------------
# BaseFactor → DeclaredFactor(声明式取数) / StreamFactor(流式订阅)。
# 三级：DeclaredFactor 下为内容(选股/非日内 kline/风控日频)；StreamFactor 下为流种类
# (tick / 日内 kline)。四级：流种类下的角色叶子(择时 / 风控)。


class DeclaredFactor(BaseFactor):
    """二级：声明式取数因子（``get_query_lst`` → ``receive_data``），不做流订阅。"""

    def stream_requirement(
        self, codes: Optional[Sequence[str]] = None
    ) -> Optional[Tuple[str, Optional[str], List[str]]]:
        return None


class StreamFactor(BaseFactor):
    """二级：流式因子——声明自身需要的流（``stream_type``）与代码池。

    择时器/风控器据此汇总需求表；``stream_type`` 为 ``"tick"``（无频率）或 ``"kline"``
    （带 ``frequency``）。子类只需声明 ``stream_type``（及 kline 的 ``frequency``）。
    """

    stream_type: str = ""
    tick_window: Optional[Tuple[str, str]] = None

    def get_query_lst(
        self, date: Any = None, codes: Optional[List[str]] = None
    ) -> List[Any]:
        """流式因子不做声明式取数（数据来自流）。"""
        return []

    def stream_requirement(
        self, codes: Optional[Sequence[str]] = None
    ) -> Optional[Tuple[str, Optional[str], List[str]]]:
        """向择时器/风控器报需求：``(kind, frequency, codes)``；tick 无频率。"""
        if not self.stream_type:
            return None
        frequency = (
            normalize_frequency(type(self).frequency)
            if self.stream_type == "kline"
            else None
        )
        return (self.stream_type, frequency, list(codes or ()))

    def in_window(self, tick_time: Optional[str]) -> bool:
        if not self.tick_window:
            return True
        if not tick_time:
            return False
        start, end = self.tick_window
        return start <= tick_time <= end


class BinarySelectionFactor(DeclaredFactor):
    _fixed_factor_kind = "selection_binary"
    factor_kind = "selection_binary"
    output_schema = "binary_series"


class FloatSelectionFactor(DeclaredFactor):
    _fixed_factor_kind = "selection_float"
    factor_kind = "selection_float"
    output_schema = "float_series"


class RiskEventFactor(DeclaredFactor):
    _fixed_factor_kind = "risk_event"
    factor_kind = "risk_event"
    output_schema = "binary_series"


class RiskFloatFactor(DeclaredFactor):
    _fixed_factor_kind = "risk_float"
    factor_kind = "risk_float"
    output_schema = "float_series"


class TickStreamFactor(StreamFactor):
    """三级：择时·tick 流式因子（逐 tick 产出 SignalIntent）。"""

    _fixed_factor_kind = "timing_tick"
    factor_kind = "timing_tick"
    output_schema = "signal_intent"
    stream_type = "tick"

    def on_tick(
        self, tick: Dict[str, Any], intents: Sequence[SignalIntent] = ()
    ) -> List[SignalIntent]:
        return []


class IntradayKlineFactor(StreamFactor):
    """三级：择时·日内 kline 流式因子（``frequency`` < 1d）。"""

    _fixed_factor_kind = "timing_kline"
    factor_kind = "timing_kline"
    output_schema = "signal_intent"
    stream_type = "kline"
    frequency = "30m"
    emitted_actions: frozenset[str] = frozenset()

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        if normalize_frequency(type(self).frequency).endswith("d"):
            raise ValueError(
                f"{type(self).__name__} requires an intraday frequency (<1d), "
                f"got {type(self).frequency!r}"
            )

    def on_bar(self, bar: KlineBar) -> List[SignalIntent]:
        return []


class NonIntradayKlineFactor(DeclaredFactor):
    """三级：择时·非日内 kline 因子（声明式取历史，``frequency`` 以 d 结尾）。

    声明式契约：``get_history_requirements`` 请求以当日为锚、含 ``lookback`` 的日线窗口；
    引擎在**盘前**取回后（``as_of`` 为盘前，故窗口到上一交易日），由 :meth:`daily_signal`
    计算信号——不再依赖 ``on_bar`` 流式累积，因而无暖机、无窗口前注入。
    """

    _fixed_factor_kind = "timing_kline"
    factor_kind = "timing_kline"
    output_schema = "signal_intent"
    frequency = "1d"
    emitted_actions: frozenset[str] = frozenset()
    lookback_days: Optional[int] = None
    history_fields: tuple[str, ...] = ("close",)

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        frequency = normalize_frequency(type(self).frequency)
        if not frequency.endswith("d"):
            raise ValueError(
                f"{type(self).__name__} requires a daily frequency (>=1d), "
                f"got {type(self).frequency!r}"
            )
        int(frequency[:-1])

    @property
    def history_lookback_days(self) -> int:
        declared = type(self).lookback_days
        if declared is not None:
            return int(declared)
        return int(normalize_frequency(type(self).frequency)[:-1])

    def get_query_lst(self, date: Any = None, codes: Optional[List[str]] = None) -> List[Any]:
        """非日内因子默认不声明主动查询（走默认窗口）。子类可覆写以自定义取数。"""
        return []

    def get_history_requirements(
        self, date: Any, codes: Optional[List[str]] = None
    ) -> List[Any]:
        """声明式数据需求：优先用子类的显式 ``get_query_lst``；否则以 ``date`` 为锚、
        向前 ``history_lookback_days`` 天的日线窗口。"""
        declared = self.get_query_lst(date, codes=codes)
        if declared:
            return declared
        window = self.history_lookback_days
        code_list = list(codes or ())
        if window <= 0 or not code_list:
            return []
        return [{
            "dataset": "market.bar",
            "anchor": date,
            "windows": (-window, 0),
            "codes": code_list,
            "fields": tuple(self.history_fields),
            "frequency": normalize_frequency(type(self).frequency),
        }]

    def history_bars(self, date: Any) -> Dict[str, List[Any]]:
        """把盘前取回的窗口历史按标的聚成 ``{code: [bar,...]}``（按时间升序）。"""
        by_code: Dict[str, List[Any]] = {}
        for payload in self._history_received_data.values():
            records = getattr(payload, "records", payload)
            for record in records or ():
                code = getattr(record, "instrument_id", None)
                if code is None and isinstance(record, dict):
                    code = record.get("instrument_id")
                if not code:
                    continue
                by_code.setdefault(str(code), []).append(record)
        for bars in by_code.values():
            bars.sort(key=lambda b: getattr(b, "interval_end", None) or getattr(b, "close", None))
        return by_code

    def daily_signal(self, date: Any) -> tuple[Dict[str, float], List[SignalIntent]]:
        """盘前计算：返回 ``(参考价 {code: last_close}, [SignalIntent])``。子类覆写。"""
        return {}, []

    def on_bar(self, bar: KlineBar) -> List[SignalIntent]:
        return []


class RiskNonIntradayKlineFactor(DeclaredFactor):
    """三级：风控·非日内 kline 因子（声明式取历史，``frequency`` 以 d 结尾）。

    声明式契约与择时非日内因子一致：盘前经 ``get_history_requirements`` 取回日线窗口
    （``as_of`` 为盘前，故到上一交易日），由 :meth:`daily_risk` 计算风控信号。
    """

    _fixed_factor_kind = "risk_kline"
    factor_kind = "risk_kline"
    output_schema = "risk_signal"
    frequency = "1d"
    lookback_days: Optional[int] = None
    history_fields: tuple[str, ...] = ("high", "low", "close")

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        frequency = normalize_frequency(type(self).frequency)
        if not frequency.endswith("d"):
            raise ValueError(
                f"{type(self).__name__} requires a daily frequency (>=1d), "
                f"got {type(self).frequency!r}"
            )
        int(frequency[:-1])

    def get_query_lst(self, date: Any = None, codes: Optional[List[str]] = None) -> List[Any]:
        return []

    @property
    def history_lookback_days(self) -> int:
        declared = type(self).lookback_days
        if declared is not None:
            return int(declared)
        return int(normalize_frequency(type(self).frequency)[:-1])

    def get_history_requirements(
        self, date: Any, codes: Optional[List[str]] = None
    ) -> List[Any]:
        """声明式数据需求：以 ``date`` 为锚、向前 ``history_lookback_days`` 天的日线窗口。"""
        declared = self.get_query_lst(date, codes=codes)
        if declared:
            return declared
        window = self.history_lookback_days
        code_list = list(codes or ())
        if window <= 0 or not code_list:
            return []
        return [{
            "dataset": "market.bar",
            "anchor": date,
            "windows": (-window, 0),
            "codes": code_list,
            "fields": tuple(self.history_fields),
            "frequency": normalize_frequency(type(self).frequency),
        }]

    def history_bars(self, date: Any) -> Dict[str, List[Any]]:
        """把盘前取回的窗口历史按标的聚成 ``{code: [bar,...]}``（按时间升序）。"""
        by_code: Dict[str, List[Any]] = {}
        for payload in self._history_received_data.values():
            records = getattr(payload, "records", payload)
            for record in records or ():
                code = getattr(record, "instrument_id", None)
                if code is None and isinstance(record, dict):
                    code = record.get("instrument_id")
                if not code:
                    continue
                by_code.setdefault(str(code), []).append(record)
        for bars in by_code.values():
            bars.sort(key=lambda b: getattr(b, "interval_end", None) or getattr(b, "close", None))
        return by_code

    def daily_risk(self, date: Any) -> List[RiskSignal]:
        """盘前从声明式历史计算风控信号。子类覆写。"""
        return []

    def on_bar(self, bar: KlineBar) -> List[RiskSignal]:
        return []


class RiskTickFactor(StreamFactor):
    """三级：风控·tick 流式因子。"""

    _fixed_factor_kind = "risk_tick"
    factor_kind = "risk_tick"
    output_schema = "risk_signal"
    stream_type = "tick"

    def on_tick(self, tick: Dict[str, Any]) -> List[RiskSignal]:
        return []


class RiskIntradayKlineFactor(StreamFactor):
    """三级：风控·日内 kline 流式因子（``frequency`` < 1d）。"""

    _fixed_factor_kind = "risk_kline"
    factor_kind = "risk_kline"
    output_schema = "risk_signal"
    stream_type = "kline"
    frequency = "30m"

    def __init__(self, factor_name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(factor_name, params)
        if normalize_frequency(type(self).frequency).endswith("d"):
            raise ValueError(
                f"{type(self).__name__} requires an intraday frequency (<1d), "
                f"got {type(self).frequency!r}"
            )

    def on_bar(self, bar: KlineBar) -> List[RiskSignal]:
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
    "DeclaredFactor",
    "StreamFactor",
    "BinarySelectionFactor",
    "FloatSelectionFactor",
    "TickStreamFactor",
    "IntradayKlineFactor",
    "NonIntradayKlineFactor",
    "RiskEventFactor",
    "RiskFloatFactor",
    "RiskTickFactor",
    "RiskIntradayKlineFactor",
    "RiskNonIntradayKlineFactor",
    "validate_risk_output",
    "validate_selection_output",
    "winsorize_and_zscore",
]
