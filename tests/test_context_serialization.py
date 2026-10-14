"""上下文序列化适配层：DataFrame/ndarray/日期/自定义对象 -> 可 JSON 载荷 -> 还原。"""

from __future__ import annotations

from datetime import date, datetime, time

from trading_nodes_base.methods import TradingContext
from trading_nodes_base.serialization import encode, decode


def test_json_ready_passthrough() -> None:
    value = {"a": [1, 2], "b": "x", "c": None}
    assert encode(value) == value
    assert decode(encode(value)) == value


def test_datetime_round_trip() -> None:
    for value in (datetime(2024, 1, 2, 9, 30), date(2024, 1, 2), time(9, 30)):
        decoded = decode(encode(value))
        assert decoded == value


def test_dataframe_round_trip() -> None:
    import pandas as pd  # noqa: PLC0415

    frame = pd.DataFrame({"a": [1, 2], "b": [3.5, 4.5]}, index=["x", "y"])
    decoded = decode(encode(frame))
    assert isinstance(decoded, pd.DataFrame)
    assert decoded.equals(frame)


def test_series_round_trip() -> None:
    import pandas as pd  # noqa: PLC0415

    series = pd.Series([1, 2, 3], index=["a", "b", "c"], name="s")
    decoded = decode(encode(series))
    assert isinstance(decoded, pd.Series)
    assert decoded.name == series.name
    assert decoded.to_dict() == series.to_dict()


def test_ndarray_round_trip() -> None:
    import numpy as np  # noqa: PLC0415

    array = np.array([[1, 2], [3, 4]], dtype="float32")
    decoded = decode(encode(array))
    assert isinstance(decoded, np.ndarray)
    assert decoded.dtype == array.dtype
    assert (decoded == array).all()


def test_context_segment_round_trips_complex_values() -> None:
    import pandas as pd  # noqa: PLC0415

    context = TradingContext()
    node = context.node("n")
    frame = pd.DataFrame({"v": [1, 2, 3]})
    node.set("df", frame)
    node.set("ts", datetime(2024, 6, 30, 15, 0))

    restored = node.get("df")
    assert isinstance(restored, pd.DataFrame) and restored.equals(frame)
    assert node.get("ts") == datetime(2024, 6, 30, 15, 0)

    # save/restore 全程：encoded 载荷 => 再读回
    raw = context.node("n")._data
    assert raw["ts"] == {"__ctx__": "datetime", "value": "2024-06-30T15:00:00"}
    assert raw["df"]["__ctx__"] == "dataframe"


class _Marker:
    def __init__(self, n: int) -> None:
        self.n = n

    def to_context(self):
        return {"n": self.n}

    @classmethod
    def from_context(cls, payload):
        return cls(payload["n"])


def test_custom_object_hook_round_trip() -> None:
    decoded = decode(encode(_Marker(42)))
    assert isinstance(decoded, _Marker) and decoded.n == 42