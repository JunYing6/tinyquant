"""Unified error hierarchy for the data-extension interface.

Task 3 of the unified data-extension interface.  Every error surfaced by the
data layer is a :class:`DataError` subclass carrying structured context
(``dataset``/``source``/``request_id``) plus a machine-readable
:meth:`DataError.as_dict` payload.  The ``cause`` of an error is kept in-process
only -- it is intentionally excluded from :meth:`DataError.as_dict` so errors
stay serializable across process boundaries.
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
    """Base error carrying structured data-layer context.

    ``request_id`` is auto-generated (``uuid4().hex``) when not supplied so
    every error can be traced end to end.  ``cause`` is retained as the
    exception ``__cause__`` in-process but is not serialized by :meth:`as_dict`.
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
        """Serialize the error, omitting the in-process-only ``cause``."""
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
    """The requested dataset is not supported by the data layer."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)


class DataContractError(DataError):
    """The data violates a declared contract (schema / quality)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)


class DataUnavailableError(DataError):
    """The data source is temporarily unavailable; retry is meaningful."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=True, **kwargs)


class DataSourceError(DataError):
    """A data source raised a transport/processing error (retryable by default)."""

    def __init__(self, message: str, retryable: bool = True, **kwargs: Any) -> None:
        super().__init__(message, retryable=retryable, **kwargs)


class DataGapError(DataError):
    """A data gap was detected in a stream; retry is meaningful."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=True, **kwargs)


class PointInTimeError(DataError):
    """Point-in-time semantics were violated (e.g. available_at > as_of)."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.pop("retryable", None)
        super().__init__(message, retryable=False, **kwargs)
