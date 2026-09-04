"""Errors raised by market data contracts and providers."""


class DataAccessError(RuntimeError):
    """Base error for market data access failures."""


class DataContractError(DataAccessError, ValueError):
    """Raised when a data request violates its declared contract."""


class DataUnavailable(DataAccessError):
    """Raised when requested market data is unavailable."""
