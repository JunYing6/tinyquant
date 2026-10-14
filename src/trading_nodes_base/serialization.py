"""共享上下文的值序列化适配层。

``ContextSegment`` 存任意值（``pd.DataFrame`` / ``np.ndarray`` / 日期 / 自定义对象
等），但持久化后端只存 JSON 字符串。本模块提供 :func:`encode` / :func:`decode`
在"内存值 <-> 可 JSON 化的载荷"之间转换：非标量用 ``__ctx__`` 类型标记包裹，
纯 JSON 类型原样透传（向前/向后兼容）。

pandas/numpy 是可选依赖：仅在用到对应类型时惰性 import，避免 import core 即拉起。
自定义对象约定实现 :meth:`to_context`（返回可 JSON 化载荷）与
:meth:`from_context`（类方法，由载荷重建）两个钩子。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Dict

_MARK = "__ctx__"


def encode(value: Any) -> Any:
    """把任意值转成可 JSON 化的载荷；标量/纯结构原样，复杂类型打 ``__ctx__`` 标记。

    容器（list/dict）会**递归**处理元素——datetime 等嵌在 dict 值里也能转。
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, _dt.datetime):
        return {"__ctx__": "datetime", "value": value.isoformat()}
    if isinstance(value, _dt.date):
        return {"__ctx__": "date", "value": value.isoformat()}
    if isinstance(value, _dt.time):
        return {"__ctx__": "time", "value": value.isoformat()}
    # 惰性 import：仅在出现该类时拉起依赖
    df = _try_type("pandas", "DataFrame")
    sr = _try_type("pandas", "Series")
    np_types = _try_module("numpy")
    if df is not None and isinstance(value, df):
        return {"__ctx__": "dataframe", "data": value.to_dict("split")}
    if sr is not None and isinstance(value, sr):
        return {"__ctx__": "series", "name": str(value.name or ""), "index": value.index.tolist(), "data": value.tolist()}
    if np_types is not None and isinstance(value, np_types.ndarray):
        return {"__ctx__": "ndarray", "dtype": str(value.dtype), "data": value.tolist()}
    if hasattr(value, "to_context") and callable(value.to_context) and hasattr(type(value), "from_context"):
        return {
            "__ctx__": "custom",
            "__module__": type(value).__module__,
            "__qualname__": type(value).__qualname__,
            "payload": value.to_context(),
        }
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    if isinstance(value, dict):
        return {str(key): encode(item) for key, item in value.items()}
    raise TypeError(f"context value of type {type(value).__name__} is not serializable")


def decode(value: Any) -> Any:
    """把 encode 的载荷还原成原值；无 ``__ctx__`` 标记时原样返回（兼容旧数据）。

    容器（list/dict）递归处理，还原嵌套的带标记值。
    """
    if isinstance(value, list):
        return [decode(item) for item in value]
    if not isinstance(value, dict):
        return value
    if _MARK not in value:
        return {str(key): decode(item) for key, item in value.items()}
    kind = value[_MARK]
    if kind == "datetime":
        return _dt.datetime.fromisoformat(value["value"])
    if kind == "date":
        return _dt.date.fromisoformat(value["value"])
    if kind == "time":
        return _dt.time.fromisoformat(value["value"])
    if kind == "dataframe":
        import pandas as pd  # noqa: PLC0415
        payload = value["data"]
        return pd.DataFrame(payload["data"], columns=payload["columns"], index=payload["index"])
    if kind == "series":
        import pandas as pd  # noqa: PLC0415
        return pd.Series(value["data"], index=value["index"], name=value["name"])
    if kind == "ndarray":
        import numpy as np  # noqa: PLC0415
        return np.array(value["data"], dtype=np.dtype(value["dtype"]))
    if kind == "custom":
        return _from_custom(value)
    raise TypeError(f"unknown context value tag {kind!r}")


def _from_custom(value: dict) -> Any:
    import importlib  # noqa: PLC0415
    module = importlib.import_module(value["__module__"])
    cls = getattr(module, value["__qualname__"])
    if not hasattr(cls, "from_context"):
        raise TypeError(f"custom context type {cls.__name__} lacks from_context")
    return cls.from_context(value["payload"])


def _is_json_ready(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int, float, str)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_json_ready(item) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _is_json_ready(item) for key, item in value.items()
        )
    return False


def _try_module(name: str) -> Any:
    import importlib  # noqa: PLC0415
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


def _try_type(module_name: str, type_name: str) -> Any:
    module = _try_module(module_name)
    if module is None:
        return None
    return getattr(module, type_name, None)


__all__ = ["encode", "decode"]