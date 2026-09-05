"""Credential-free example data adapters."""

from .memory_adapters import (
    CalendarAdapter,
    MemoryHistoricalAdapter,
    build_memory_source,
    make_gateway,
)

__all__ = [
    "CalendarAdapter",
    "MemoryHistoricalAdapter",
    "build_memory_source",
    "make_gateway",
]
