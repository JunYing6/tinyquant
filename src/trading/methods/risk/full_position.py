from __future__ import annotations

from typing import Any

from trading.methods.base import BaseRiskControl, RiskDecision


class FullPositionRisk(BaseRiskControl):
    def __init__(self) -> None:
        super().__init__("full-position-risk")

    def on_pre_market(self) -> RiskDecision:
        self.risk_decision = RiskDecision(True, 1.0, False)
        self.is_trading_allowed = True
        self.target_position_ratio = 1.0
        return self.risk_decision

    def on_daily(self) -> RiskDecision:
        return self.on_pre_market()


__all__ = ["FullPositionRisk"]
