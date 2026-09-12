<div align="center">

# tinyquant

**tinyquant是一个轻量化的量化框架。相较于传统的策略框架，tinyquant将多策略通过mind动态调整权重进行组合以获得更好的普适性，同时解决以往单一策略过于臃肿问题。**

[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.1.x-blue.svg)](pyproject.toml)

</div>

## 项目范围

tinyquant 包含七个公开的量化组件基类：

- `BaseFactor`
- `BaseStockPicking`
- `BaseTimeSelection`
- `BaseRiskControl`
- `BaseStrategy`
- `BaseStream`
- `BaseMind`

七个基类全部从 `trading_nodes_base` 命名空间导入：

```python
from trading_nodes_base import (
    BaseFactor,
    BaseMind,
    BaseRiskControl,
    BaseStockPicking,
    BaseStrategy,
    BaseStream,
    BaseTimeSelection,
)
```

tinyquant 只发布核心运行时与通用 `tq` CLI，**不内置任何具体策略、Mind、Stream 或数据适配器**。具体因子、选股器、择时器、风控、策略、Mind、Stream 和真实数据适配器属于外部用户项目（例如 `tinyquant-workspace`），通过工作区的注册表（默认 `trading_nodes.backtests`）向 CLI 提供回测/实盘入口。

## 安装

tinyquant 需要 Python 3.11 或更高版本。

```bash
python -m pip install "tinyquant[cli]"
```

源码开发环境：

```bash
python -m pip install -e ".[dev]"
```

## CLI 工作台

`tq` 命令在交互式 REPL 和一次性执行模式下支持相同的处理器：

```bash
tq help       # 命令帮助
tq doctor     # 运行时诊断
tq backtest   # 交互式回测向导
tq live       # 交互式实盘向导
```

REPL 同时接受 `command` 和 `/command` 格式，支持自动补全，保存本地命令历史，并将诊断信息和回测摘要以紧凑的量化工作台表格形式呈现。

### 回测向导 `tq backtest`

依次选择回测类型（策略/Stream）、具体回测项目、开始日期、结束日期、初始资金、运行模式，以及是否导出 Excel，随后实时显示回测进度。

可运行的回测清单由用户项目提供：模块（默认 `trading_nodes.backtests`，可用环境变量 `TINYQUANT_BACKTEST_MODULE` 覆盖）暴露一个 `BACKTESTS` 列表，每项为：

```python
{
    "name": "展示名称",
    "kind": "strategy",  # 或 "stream"
    "factory": "module:factory",  # 无参工厂，返回 (BaseStrategy | BaseStream, DataGateway)
}
```

### 实盘向导 `tq live`

依次选择策略/Stream、实时行情源（mock/jvquant）、券商执行器（paper/gm）、初始资金，然后启动 `RealTimeTradeEngine`。行情源与执行器清单来自用户项目 `trading_nodes/live`（可用环境变量 `TINYQUANT_LIVE_MODULE` 覆盖）。

## Excel 回测报告

`tq backtest` 向导默认不导出 Excel，仅在向导末尾选择"是否导出 Excel"为 `y` 时生成工作簿，包含五个 Sheet：策略概览、权益曲线（含基准线与权益走势图）、交易记录、持仓明细、月度收益（含月度收益图）。Stream 报告在此基础上增加成员策略汇总、成员权益曲线、权重演变三个 Sheet。

输出目录的解析顺序：

1. 环境变量 `TINYQUANT_EXCEL_DIR`；
2. 使用默认目录 `excel_reports/`（输出时不存在会自动创建）。

文件名格式为 `{策略名}_{开始日期}_{结束日期}_{时间戳}.xlsx`。

代码中可以对任意跑完的引擎直接导出：

```python
from tools.excel_report import export_backtest_excel

engine.run()
report_path = export_backtest_excel(engine)  # -> Path
```

## 数据扩展架构

`tools.data` 是唯一数据层：契约定义标准 `Bar`、`Session`、`DataBatch` 和请求；`HistoricalDataPort`/`TradingCalendarPort`/`RealtimeDataPort` 是适配器端口，`DataGateway` 根据 `default_catalog()` 与 `DataBinding` 路由、校验、审计来源并应用策略。

默认 Catalog 注册 37 个数据集。接入新数据源时，在独立集成包（例如用户项目 `tinyquant-workspace`）中实现端口，不要把供应商类型传入引擎。实时数据走 `RealtimeDataPort`（`subscribe`/`poll`），通过 `DataGateway` 的 push/poll 模式路由。

## 用 DataGateway 运行

```python
from engines.fast import FastBacktestEngine

strategy = ...
gateway = build_workspace_gateway()  # 来自用户项目的真实数据适配器
engine = FastBacktestEngine(strategy, "20240102", "20241231", mode="auto", data_gateway=gateway)
engine.run()
print(engine.get_stats())
```

实盘引擎同样接收统一的 `(strategy_or_stream, data_gateway, trade_executor)` 入口；实时行情由网关的 `subscribe`/`poll` 提供，券商下单由注入的 `TradeExecutor` 负责。不引入真实供应商或凭据，供应商和券商执行器应放在用户集成项目中。

`fast` 模式使用已完成的日 K 线，并在下一个数据源 K 线执行交易意图。`tick` 模式将提供者推送的行情通过内存撮合器处理。`auto` 模式仅在策略声明兼容组件时选择快速模式。

## 实盘执行

```python
from engines.realtime import RealTimeTradeEngine

engine = RealTimeTradeEngine(
    strategy,
    data_gateway=gateway,
    trade_executor=trade_executor,
    initial_capital=1_000_000,
)
engine.start()
```

实盘执行不会重试被拒绝的订单，而是记录失败信息，并从注入的执行器同步本地运行时账户。

## 版本与分支

- `main`：稳定分支（1.1.x），仅收 bugfix、随版本发布。
- `dev/1.2.0`：开发线，承载架构演进与新功能；稳定后合入主版本。
- 1.1.x 修复以 `release/1.1` 为出口（或 tag），bugfix 单向回流到 `dev/1.2.0`，避免破坏稳定线。
