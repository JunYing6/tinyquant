"""Backtest command handler."""

from __future__ import annotations

from rich.console import Console

from tinyquant_cli.loading import FactoryContractError, load_backtest_factory
from tinyquant_cli.render import render_backtest, render_error
from tinyquant_cli.runtime import SessionState
from engines.fast import FastBacktestEngine


def run_backtest(
    console: Console,
    state: SessionState,
    factory_path: str,
    start: str,
    end: str,
    capital: float,
    mode: str,
) -> int:
    try:
        entity, data_gateway = load_backtest_factory(factory_path)
        engine = FastBacktestEngine(
            entity,
            start,
            end,
            initial_capital=capital,
            mode=mode,
            data_gateway=data_gateway,
            progress_bar=False,
        )
        engine.run()
        stats = engine.get_stats()
        if not stats:
            raise RuntimeError("backtest produced no equity curve")
        state.update_backtest(stats)
        account = engine.account
        render_backtest(console, stats, engine.equity_curve, account.positions)
        return 0
    except FactoryContractError as error:
        render_error(console, str(error))
        return 2
    except Exception as error:
        render_error(console, f"{type(error).__name__}: {error}")
        return 1
