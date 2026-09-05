<div align="center">

# tinyquant

**tinyquant是一个轻量化的量化框架。相较于传统的策略框架，tinyquant将多策略通过mind动态调整权重进行组合以获得更好的普适性，同时解决以往单一策略过于臃肿问题。**

[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.1.0-blue.svg)](pyproject.toml)

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

1.1 只发布核心运行时与通用 `tq` CLI，**不内置任何具体策略、Mind、Stream 或数据适配器**。具体因子、选股器、择时器、风控、策略、Mind、Stream 和真实数据适配器属于外部用户项目（例如 `tinyquant-workspace`），通过 `main:build_backtest` 工厂向 CLI 提供 `(BaseStrategy | BaseStream, DataGateway)`。

## 安装

tinyquant 需要 Python 3.11 或更高版本。

```bash
python -m pip install tinyquant
```

源码开发环境：

```bash
python -m pip install -e ".[dev]"
```

## CLI 工作台

可选的量化工作台需要从核心运行时中单独安装：

```bash
python -m pip install "tinyquant[cli]"
tq
```

`tq` 命令在交互式 REPL 和一次性执行模式下支持相同的处理器：

```bash
tq help
tq examples list
tq examples run backtest
tq doctor
tq backtest run package.module:function --start 20240102 --end 20241231
```

REPL 同时接受 `command` 和 `/command` 格式，支持自动补全，保存本地命令历史，并将诊断信息和回测摘要以紧凑的量化工作台表格形式呈现。`help`、`examples`、`doctor` 和 `backtest` 是 1.1 版本的完整命令集；CLI 不包含研究、注册表、下载、持久化结果、提供者配置或实盘交易命令。

`tq backtest run` 会加载一个无参数的 Python 工厂函数，该函数必须准确返回：

```python
(BaseStrategy | BaseStream, DataGateway)
```

工厂函数负责创建策略并组装数据网关。CLI 参数只能覆盖开始日期、结束日期、资金和运行模式；数据供应商选择、重试和数据质量策略由 `DataGateway` 负责。

## 数据扩展架构

`tools.data` 是唯一数据层：契约定义标准 `Bar`、`Session`、`DataBatch` 和请求，`HistoricalDataPort`/`TradingCalendarPort` 是适配器端口，`DataGateway` 根据 `default_catalog()` 与 `DataBinding` 路由、校验、审计来源并应用策略。

默认 Catalog 注册 37 个数据集。接入新数据源时，在独立集成包或 `examples/adapters/` 中实现端口，不要把供应商类型传入引擎：

```python
class VendorBars:
    descriptor = AdapterDescriptor(...)

    def read(self, request: DataRequest) -> DataBatch[Bar]:
        return DataBatch(...)

    def iter(self, request: DataRequest, chunk_size=10_000):
        yield self.read(request)
```

实现还应按请求过滤记录、返回完整的标准批次，并在 `descriptor` 声明 dataset/mode/schema/capability。日历适配器实现 `sessions(CalendarRequest) -> CalendarBatch`。完整的无凭据示例见 `examples/adapters/memory_adapters.py`。

## 用 DataGateway 运行

```python
from engines.fast import FastBacktestEngine
from examples.adapters.memory_adapters import make_gateway

strategy = ...
gateway = make_gateway(bars, sessions)
engine = FastBacktestEngine(strategy, "20240102", "20241231", mode="auto", data_gateway=gateway)
engine.run()
print(engine.get_stats())
```

实盘引擎同样只接收 `(strategy_or_stream, data_gateway)` 的统一数据入口；实时适配器可在同一 Gateway 上提供 `subscribe`/`poll`。不引入真实供应商或凭据，供应商和券商执行器应放在独立集成包中。

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

## 示例

```bash
python examples/in_memory_backtest.py
python examples/in_memory_live.py
```

两个示例都是有限运行、无需凭据，并且可以安全地在本地执行。
