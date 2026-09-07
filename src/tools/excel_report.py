"""Export a finished :class:`FastBacktestEngine` run as an Excel report."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from tools.excel_generator import ExcelReportGenerator, StreamExcelReportGenerator

DEFAULT_OUTPUT_DIRNAME = "excel_reports"
ENV_OUTPUT_DIR = "TINYQUANT_EXCEL_DIR"

# engine stats keys -> (report label, percent scale)
_STAT_LABELS: dict[str, tuple[str, float]] = {
    "trading_days": ("交易天数", 1.0),
    "total_return": ("总收益率 (%)", 100.0),
    "max_drawdown": ("最大回撤 (%)", 100.0),
    "annualized_return": ("年化收益率 (%)", 100.0),
    "annualized_volatility": ("年化波动率 (%)", 100.0),
    "sharpe": ("夏普比率", 1.0),
    "win_rate": ("胜率 (%)", 100.0),
    "final_equity": ("最终权益", 1.0),
    "trade_count": ("总交易次数", 1.0),
}


def default_output_dir() -> Path:
    """Excel report output folder: ``$TINYQUANT_EXCEL_DIR`` or ``<cwd>/excel_reports``."""
    override = os.environ.get(ENV_OUTPUT_DIR)
    return Path(override) if override else Path.cwd() / DEFAULT_OUTPUT_DIRNAME


def _localized_stats(stats: dict[str, float | int]) -> dict[str, Any]:
    return {
        label: value * scale
        for key, (label, scale) in _STAT_LABELS.items()
        if (value := stats.get(key)) is not None
    }


def export_backtest_excel(
    engine: Any,
    output_dir: str | Path | None = None,
    strategy_name: str | None = None,
) -> Path:
    """Write the engine's finished backtest as an Excel workbook.

    Args:
        engine: a run instance of ``FastBacktestEngine`` (equity curve populated).
        output_dir: target folder; defaults to :func:`default_output_dir`.
        strategy_name: file/report name override; defaults to the entity's
            ``strategy_name`` attribute or its class name.

    Returns:
        The written report path.
    """
    name = (
        strategy_name
        or getattr(engine.entity, "strategy_name", None)
        or type(engine.entity).__name__
    )
    directory = Path(output_dir) if output_dir is not None else default_output_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    common = {
        "equity_curve": list(engine.equity_curve),
        "daily_positions": list(engine.daily_positions),
        "trade_log": engine.account.trade_log,
        "stats_dict": _localized_stats(engine.get_stats()),
        "strategy_name": name,
        "start_date": engine.start_date,
        "end_date": engine.end_date,
        "initial_capital": engine.initial_capital,
    }
    if getattr(engine, "is_stream", False):
        generator = StreamExcelReportGenerator(
            **common,
            member_curve=list(engine.member_curve),
            member_trade_logs={
                member: getattr(account, "trade_log", {})
                for member, account in engine.entity.shadow_accounts.items()
            },
        )
    else:
        generator = ExcelReportGenerator(**common)

    save_path = directory / f"{name}_{engine.start_date}_{engine.end_date}_{timestamp}.xlsx"
    return Path(generator.generate(str(save_path)))


__all__ = ["DEFAULT_OUTPUT_DIRNAME", "ENV_OUTPUT_DIR", "default_output_dir", "export_backtest_excel"]
