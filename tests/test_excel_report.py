"""Excel report export tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from openpyxl import load_workbook

from engines.fast import FastBacktestEngine
from tools.data import Bar, DataRequest, InMemoryGateway, Session, TradingPhase
from tools.excel_generator import ExcelReportGenerator, StreamExcelReportGenerator
from tools.excel_report import (
    _localized_stats,
    default_output_dir,
    export_backtest_excel,
)
from trading_nodes_base.minds import BaseMind
from trading_nodes_base.methods import BaseTimeSelection
from trading_nodes_base.strategies import BaseStrategy
from trading_nodes_base.streams import BaseStream
from trading_nodes_base.types import ExecutionMode, ExecutionRequest
from trading_nodes_base.factors import KlineTimingFactor, TickTimingFactor


def _instant(day: str, clock: str = "15:00:00") -> datetime:
    return datetime.strptime(f"{day} {clock}", "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _bar(day: str, price: float, code: str = "000001.SZ") -> Bar:
    instant = _instant(day)
    return Bar(schema_version="1", event_id=None, instrument_id=code, asset_type="equity", effective_time=instant, event_time=instant, available_at=instant, trading_date=instant.date(), source="test", quality="valid", metadata={}, frequency="1d", interval_start=instant, interval_end=instant, open=price, high=price, low=price, close=price, volume=0, turnover=0, is_complete=True, price_basis="raw")


def _session(day: str) -> Session:
    phase = TradingPhase(name="regular", start=_instant(day, "09:00:00"), end=_instant(day, "15:00:00"), accepts_trades=True, accepts_quotes=True)
    return Session(market="CN", trading_date=_instant(day).date(), timezone="UTC", phases=(phase,))


def _gateway(bars: list[Bar]) -> InMemoryGateway:
    days = ["20240102", "20240103"]
    return InMemoryGateway(bars=bars, events=[], sessions=[_session(day) for day in days])


class PassiveQueryFactor(KlineTimingFactor):
    def __init__(self) -> None:
        super().__init__("passive-query")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []

    def on_bar(self, bar: object) -> list:
        return []


class PassiveIntentExecutor(TickTimingFactor):
    execution_role = "intent_executor"
    accepted_intent_actions = frozenset({"BUY"})

    def __init__(self) -> None:
        super().__init__("passive-executor")

    def get_query_lst(self, date: object, codes: list[str] | None = None) -> list[DataRequest]:
        self._data_clear()
        self.sign["fit"] = True
        return []


class BuyingFastStrategy(BaseStrategy):
    supports_fast_backtest = True

    def __init__(self) -> None:
        timer = BaseTimeSelection("buy-timer", [PassiveQueryFactor()], [PassiveIntentExecutor()])
        super().__init__("buy-fast", timer=timer)
        self._queued_once = False

    def _run_daily_pipeline(self) -> None:
        super()._run_daily_pipeline()
        if not self._queued_once:
            self._pending_orders.append(
                ExecutionRequest("000001.SZ", "BUY", "15:00:00", price=0, volume=100, mode=ExecutionMode.MARKET)
            )
            self._queued_once = True


def _run_engine() -> FastBacktestEngine:
    bars = [_bar("20240102", 10.0), _bar("20240103", 11.0)]
    engine = FastBacktestEngine(BuyingFastStrategy(), "20240102", "20240103", initial_capital=100_000, mode="fast", data_gateway=_gateway(bars), progress_bar=False)
    engine.run()
    return engine


class EqualMind(BaseMind):
    def calculate_weights(self, market_data: dict[str, Any], strategies_performance: dict[str, dict[str, float]]) -> dict[str, float]:
        return {name: 1.0 for name in strategies_performance}


class NamedBuyingFastStrategy(BuyingFastStrategy):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.strategy_name = name


def _run_stream_engine() -> tuple[FastBacktestEngine, list[Bar]]:
    bars = [_bar("20240102", 10.0), _bar("20240103", 11.0)]
    stream = BaseStream("组合A", [NamedBuyingFastStrategy("s1"), NamedBuyingFastStrategy("s2")], EqualMind())
    engine = FastBacktestEngine(stream, "20240102", "20240103", initial_capital=100_000, mode="fast", data_gateway=_gateway(bars), progress_bar=False)
    engine.run()
    return engine, bars


def test_export_backtest_excel_writes_workbook(tmp_path) -> None:
    engine = _run_engine()

    path = export_backtest_excel(engine, output_dir=tmp_path)

    assert path.exists()
    assert path.parent == tmp_path
    assert path.name.startswith("buy-fast_20240102_20240103_")
    workbook = load_workbook(path)
    assert workbook.sheetnames == ["策略概览", "权益曲线", "交易记录", "持仓明细", "月度收益"]
    overview = workbook["策略概览"]
    assert overview["A1"].value == "tinyquant 回测报告"
    assert overview["B4"].value == "buy-fast"
    equity = workbook["权益曲线"]
    assert equity["A2"].value == "20240102"
    assert equity["B3"].value == pytest.approx(engine.equity_curve[-1]["equity"], abs=0.01)
    positions = workbook["持仓明细"]
    assert positions["B2"].value == "000001.SZ"
    assert positions["D2"].value == pytest.approx(10.0, abs=0.01)


def test_export_uses_default_output_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TINYQUANT_EXCEL_DIR", str(tmp_path))
    assert default_output_dir() == tmp_path

    engine = _run_engine()
    path = export_backtest_excel(engine)

    assert path.parent == tmp_path
    assert path.exists()


def test_localized_stats_scales_percent_keys() -> None:
    stats = {"total_return": 0.12, "sharpe": 1.5, "win_rate": 0.6, "trading_days": 2, "final_equity": 112000.0}

    localized = _localized_stats(stats)

    assert localized["总收益率 (%)"] == pytest.approx(12.0)
    assert localized["胜率 (%)"] == pytest.approx(60.0)
    assert localized["夏普比率"] == pytest.approx(1.5)
    assert localized["交易天数"] == 2
    assert localized["最终权益"] == pytest.approx(112000.0)


class _HookProbe(ExcelReportGenerator):
    def __init__(self, *args, member_count: int = 0, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.member_count = member_count

    def _report_title(self) -> str:
        return "tinyquant 组合回测报告"

    def _create_sheets(self, wb) -> None:
        super()._create_sheets(wb)
        wb.create_sheet("探测Sheet")


def test_generator_hooks_allow_title_override_and_sheet_extension(tmp_path) -> None:
    engine = _run_engine()
    gen = _HookProbe(
        equity_curve=list(engine.equity_curve), daily_positions=list(engine.daily_positions),
        trade_log=engine.account.trade_log, stats_dict=_localized_stats(engine.get_stats()),
        strategy_name="probe", start_date=engine.start_date, end_date=engine.end_date,
        initial_capital=engine.initial_capital, member_count=2,
    )
    save_path = tmp_path / "probe.xlsx"
    gen.generate(str(save_path))

    workbook = load_workbook(save_path)
    assert workbook["策略概览"]["A1"].value == "tinyquant 组合回测报告"
    assert workbook.sheetnames[-1] == "探测Sheet"


def test_stream_report_adds_member_breakdown_sheets(tmp_path) -> None:
    engine, _ = _run_stream_engine()
    gen = StreamExcelReportGenerator(
        equity_curve=list(engine.equity_curve), daily_positions=list(engine.daily_positions),
        trade_log=engine.account.trade_log, stats_dict=_localized_stats(engine.get_stats()),
        strategy_name="组合A", start_date=engine.start_date, end_date=engine.end_date,
        initial_capital=engine.initial_capital,
        member_curve=list(engine.member_curve),
        member_trade_logs={name: getattr(acc, "trade_log", {}) for name, acc in engine.entity.shadow_accounts.items()},
    )
    save_path = tmp_path / "stream.xlsx"
    gen.generate(str(save_path))

    workbook = load_workbook(save_path)
    assert workbook.sheetnames == ["策略概览", "权益曲线", "交易记录", "持仓明细", "月度收益", "成员策略汇总", "成员权益曲线", "权重演变"]
    assert workbook["策略概览"]["A1"].value == "tinyquant 组合回测报告"
    summary = workbook["成员策略汇总"]
    assert {summary["A2"].value, summary["A3"].value} == {"s1", "s2"}
    assert summary["E2"].value is not None
    member_eq = workbook["成员权益曲线"]
    assert member_eq["A2"].value == "20240102"
    assert member_eq["B2"].value is not None
