"""数据扩展接口的统一错误层次结构。

统一数据扩展接口的任务 3。数据层抛出的每个错误都是 :class:`DataError` 的
子类，携带结构化上下文（``dataset``/``source``/``request_id``）以及机器可读的
:meth:`DataError.as_dict` 负载。错误的 ``cause`` 仅保留在进程内——它被刻意
排除在 :meth:`DataError.as_dict` 之外，使错误能够在进程边界之间保持可序列化。
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

__all__ = [
    "DataContractError",
    "DataError",
    "DataGapError",
    "DataSourceError",
    "DataUnavailableError",
    "PointInTimeError",
    "UnsupportedDatasetError",
]


class DataError(RuntimeError):
    """携带结构化数据层上下文的基础错误。

    ``request_id`` 在未提供时自动生成（``uuid4().hex``），使每个错误都能被
    端到端追踪。``cause`` 作为异常的 ``__cause__`` 保留在进程内，但不被
    :meth:`as_dict` 序列化。
    """

    def __init__(
        self,
        message: str,
        *,
        dataset: str | None = None,
        source: str | None = None,
        request_id: str = "",
        retryable: bool = False,
        partial: bool = False,
        cause: BaseException | None = None,
    ) -> None:
        self.message = message
        self.dataset = dataset
        self.source = source
        self.request_id = request_id if request_id else uuid4().hex
        self.retryable = bool(retryable)
        self.partial = bool(partial)
        self.cause = cause
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause

    def as_dict(self) -> dict[str, Any]:
        """序列化错误，省略仅限进程内使用的 ``cause``。"""
        return {
            "error_type": type(self).__name__,
            "message": self.message,
            "dataset": self.dataset,
            "source": self.source,
            "request_id": self.request_id,
            "retryable": self.retryable,
            "partial": self.partial,
        }


class UnsupportedDatasetError(DataError):
    """所请求的数据集不被数据层支持。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)


class DataContractError(DataError):
    """数据违反了声明的契约（模式/质量）。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)


class DataUnavailableError(DataError):
    """数据源暂时不可用；重试是有意义的。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=True, **kwargs)


class DataSourceError(DataError):
    """数据源抛出了传输/处理错误（默认可重试）。"""

    def __init__(self, message: str, retryable: bool = True, **kwargs: Any) -> None:
        super().__init__(message, retryable=retryable, **kwargs)


class DataGapError(DataError):
    """在流中检测到数据缺口；重试是有意义的。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=True, **kwargs)


class PointInTimeError(DataError):
    """违反时点（point-in-time）语义（例如 available_at > as_of）。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)
