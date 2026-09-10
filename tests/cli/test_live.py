from __future__ import annotations

import pytest

from tinyquant_cli.live import (
    EXECUTOR_LABELS,
    QUOTE_SOURCE_LABELS,
    resolve_route,
)
from tinyquant_cli.wizard import WizardError


def test_resolve_route_by_number_and_key() -> None:
    assert resolve_route(QUOTE_SOURCE_LABELS, "1") == "mock"
    assert resolve_route(QUOTE_SOURCE_LABELS, "mock") == "mock"
    assert resolve_route(EXECUTOR_LABELS, "gm") == "gm"


def test_resolve_route_by_label() -> None:
    assert resolve_route(QUOTE_SOURCE_LABELS, "本地模拟行情") == "mock"


def test_resolve_route_rejects_invalid() -> None:
    with pytest.raises(WizardError):
        resolve_route(QUOTE_SOURCE_LABELS, "9")
    with pytest.raises(WizardError):
        resolve_route(EXECUTOR_LABELS, "不存在")


def test_route_label_maps() -> None:
    assert set(QUOTE_SOURCE_LABELS) == {"mock", "jvquant"}
    assert set(EXECUTOR_LABELS) == {"paper", "gm"}