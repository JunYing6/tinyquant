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

在1.1 内置了9个strategy3个mind，1一个stream

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
(BaseStrategy | BaseStream, MarketDataProvider, TradingCalendarProvider)
```

工厂函数负责创建策略并选择提供者。CLI 参数只能覆盖开始日期、结束日期、资金和运行模式。tinyquant 不会选择数据供应商、不读取本地数据库，也不会重试失败的提供者请求。

## 提供者

核心版本不提供默认的市场数据、行情或券商实现。请提供实现以下协议的适配器：

- `MarketDataProvider.fetch(request, date)`：获取历史数据和策略日数据。
- `TradingCalendarProvider.get_trade_dates(start, end)`：获取回测交易日历。
- `QuoteProvider.subscribe(codes, on_tick)`、`start()` 和 `stop()`：获取实时行情数据。
- `TradeExecutor.connect()`、`buy()`、`sell()`、账户快照、持仓和 `disconnect()`：执行实盘交易。

`examples/` 下的示例在内存中实现了所有提供者。真实的数据供应商和券商适配器应放在独立的集成包中。

## 回测

```python
from engines.fast import FastBacktestEngine

engine = FastBacktestEngine(
    strategy,
    "20240102",
    "20241231",
    data_provider=market_provider,
    calendar_provider=calendar_provider,
    mode="auto",
)
engine.run()
print(engine.get_stats())
```

`fast` 模式使用已完成的日 K 线，并在下一个数据源 K 线执行交易意图。`tick` 模式将提供者推送的行情通过内存撮合器处理。`auto` 模式仅在策略声明兼容组件时选择快速模式。

## 实盘执行

```python
from engines.realtime import RealTimeTradeEngine

engine = RealTimeTradeEngine(
    strategy,
    quote_provider=quote_provider,
    trade_executor=trade_executor,
    data_provider=market_provider,  # 仅当策略需要日线数据时才需要
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
