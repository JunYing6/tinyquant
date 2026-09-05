"""Credential-free market data for examples."""

from tools.data import InMemoryGateway


__all__ = ["InMemoryGateway"]


SAMPLE_DAILY = {
    "20240102": [
        {"code": "000001.SZ", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}
    ],
    "20240103": [
        {"code": "000001.SZ", "open": 10.5, "high": 10.5, "low": 10.5, "close": 10.5}
    ],
}
