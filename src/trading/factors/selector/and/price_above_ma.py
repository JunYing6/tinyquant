from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from trading.factors.base import BinarySelectionFactor


class PriceAboveMaSelectionFactor(BinarySelectionFactor):
    def __init__(self, window: int = 20) -> None:
        super().__init__("price-above-ma")
        self.window = window
        self._values: dict[str, list[float]] = {}

    def get_query_lst(self, date: Any, codes: list[str] | None = None) -> list[dict]:
        self._data_clear()
        self.sign["fit"] = True
        return [{"type": "market/daily", "date": date, "codes": codes or [], "period": ["close"]}]

    def _calculate_internal(self, data_cache: dict) -> pd.Series:
        values: dict[str, float] = {}
        for payload in data_cache.values():
            rows = payload.to_dict("records") if hasattr(payload, "to_dict") else payload
            for row in rows or []:
                code = row.get("code") or row.get("ts_code")
                close = row.get("close")
                if code and isinstance(close, (int, float)):
                    history = self._values.setdefault(code, [])
                    history.append(float(close))
                    history[:] = history[-self.window:]
                    values[code] = float(len(history) >= self.window and close > sum(history) / len(history))
        return pd.Series(values, dtype=float)


__all__ = ["PriceAboveMaSelectionFactor"]
