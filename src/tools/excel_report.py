"""将已完成的 :class:`FastBacktestEngine` 运行结果导出为 Excel 报告。"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from tools.excel_generator import ExcelReportGenerator, StreamExcelReportGenerator

DEFAULT_OUTPUT_DIRNAME = "excel_reports"
ENV_OUTPUT_DIR = "TINYQUANT_EXCEL_DIR"

# engine 统计键 -> (报告标签, 百分比缩放系数)
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
    """Excel 报告输出文件夹：``$TINYQUANT_EXCEL_DIR`` 或 ``<cwd>/excel_reports``。"""
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
    """将 engine 已完成的回测写入 Excel 工作簿。

    Args:
        engine: 一个 ``FastBacktestEngine`` 运行实例（含填充好的权益曲线）。
        output_dir: 目标文件夹；默认使用 :func:`default_output_dir`。
        strategy_name: 文件/报告名称覆盖；默认为实体的 ``strategy_name``
            属性或类名。

    Returns:
        生成的报告路径。
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
