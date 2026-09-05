from __future__ import annotations

import sys


def test_public_base_imports() -> None:
    from trading.factors.base import BaseFactor
    from trading.methods.base import BaseRiskControl, BaseStockPicking, BaseTimeSelection
    from trading.minds.base import BaseMind
    from trading.strategies.base import BaseStrategy
    from trading.streams.base import BaseStream

    assert all(
        (
            BaseFactor,
            BaseStockPicking,
            BaseTimeSelection,
            BaseRiskControl,
            BaseStrategy,
            BaseStream,
            BaseMind,
        )
    )


def test_core_imports_do_not_load_excluded_runtime_modules() -> None:
    import engines
    import tools.data
    import tools.real_trade.providers
    import tools.trade.providers
    import trading

    forbidden = {
        "duck" + "db",
        "engines.study",
        "engines.research",
        "gui",
        "registry",
        "tu" + "share",
        "gm",
    }

    assert not forbidden.intersection(sys.modules)
