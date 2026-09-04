from __future__ import annotations

from collections import deque
from typing import Any

from trading.factors.risk.kline.atr_stop import AtrStopRiskFactor
from trading.factors.types import KlineBar, RiskSignal
from trading.methods.base import BaseRiskControl, RiskDecision


class AtrRiskControl(BaseRiskControl):
    def __init__(self, window: int = 14, multiple: float = 2.0) -> None:
        super().__init__("atr-risk", risk_kline_factors=[AtrStopRiskFactor(window, multiple)])

    def on_pre_market(self) -> RiskDecision:
        self.risk_decision = RiskDecision(True, 1.0, False)
        self.is_trading_allowed = True
        self.target_position_ratio = 1.0
        return self.risk_decision

    def on_bar(self, bar: KlineBar) -> RiskDecision:
        signals = self.risk_kline_factors[0].on_bar(bar)
        if any(signal.triggered for signal in signals):
            self.risk_decision = RiskDecision(False, 0.0, True)
            self.is_trading_allowed = False
            self.target_position_ratio = 0.0
        return self.risk_decision
