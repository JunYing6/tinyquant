from __future__ import annotations

import sys

from pathlib import Path


def test_release_uses_trading_nodes_base_namespace() -> None:
    root = Path(__file__).parents[1]

    assert (root / "src" / "trading_nodes_base").is_dir()
    assert not (root / "src" / "trading").exists()

    from trading_nodes_base.factors.base import BaseFactor
    from trading_nodes_base.methods.base import BaseRiskControl, BaseStockPicking, BaseTimeSelection
    from trading_nodes_base.minds.base import BaseMind
    from trading_nodes_base.strategies.base import BaseStrategy
    from trading_nodes_base.streams.base import BaseStream

    assert all(
        (
            BaseFactor,
            BaseRiskControl,
            BaseStockPicking,
            BaseTimeSelection,
            BaseMind,
            BaseStrategy,
            BaseStream,
        )
    )


def test_public_base_imports() -> None:
    from trading_nodes_base.factors.base import BaseFactor
    from trading_nodes_base.methods.base import BaseRiskControl, BaseStockPicking, BaseTimeSelection
    from trading_nodes_base.minds.base import BaseMind
    from trading_nodes_base.strategies.base import BaseStrategy
    from trading_nodes_base.streams.base import BaseStream

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
    import tools.trade.providers
    import trading_nodes_base

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
