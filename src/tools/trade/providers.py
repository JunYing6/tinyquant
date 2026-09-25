"""订单执行与券商快照的 Provider 协议。"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class TradeExecutor(Protocol):
    def connect(self) -> None:
        """打开券商连接。"""

    def buy(self, symbol: str, volume: int, **kwargs: Any) -> Any:
        """提交买入订单。"""

    def sell(self, symbol: str, volume: int, **kwargs: Any) -> Any:
        """提交卖出订单。"""

    def get_positions(self) -> list[Any]:
        """返回券商持仓。"""

    def get_account(self) -> Any:
        """返回券商账户快照。"""

    def disconnect(self) -> None:
        """关闭券商连接。"""
