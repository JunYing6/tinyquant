# 数据扩展与适配器

## 分层

`tools.data` 是 tinyquant 的唯一数据层：

1. **契约**：`DataRequest`、`CalendarRequest`、`Bar`、`Session` 和 `DataBatch` 定义供应商无关的数据形状。
2. **Port**：`HistoricalDataPort.read/iter`、`TradingCalendarPort.sessions`（以及实时 Port）是外部适配器实现的边界。
3. **Adapter**：把数据库、文件或供应商 SDK 转换为标准契约，并通过 `descriptor` 声明能力。
4. **DataGateway**：以 `default_catalog()` 的 37 个数据集为目录，使用 `DataBinding` 路由请求，执行校验、来源审计、重试和回退。
5. **Engine/CLI**：只依赖 `DataGateway`，不依赖旧的 provider 协议或供应商类型。

## 实现历史数据适配器

适配器需要实现 `read` 和 `iter`，并暴露 `AdapterDescriptor`：

```python
class MyBars:
    descriptor = AdapterDescriptor(
        name="my-bars",
        datasets={"market.bar": DatasetCapability(
            dataset="market.bar",
            modes=("historical",),
            asset_types=frozenset({"equity"}),
            frequencies=("1d",),
            fields=("instrument_id", "open", "high", "low", "close", "volume", "turnover"),
            point_in_time=False,
        )},
        historical_modes=("historical",),
        realtime_modes=(),
        supports_point_in_time=False,
        supported_price_basis=frozenset({"raw"}),
        supported_asset_types=frozenset({"equity"}),
        schema_versions=("1.0",),
    )

    def read(self, request: DataRequest) -> DataBatch[Bar]:
        records = ...
        return DataBatch(..., dataset="market.bar", records=records, complete=True, ...)

    def iter(self, request: DataRequest, chunk_size=10_000):
        yield self.read(request)
```

供应商字段应在适配器内部转换，不应泄漏到 `Bar`。时间字段必须带时区，数值字段应使用 Catalog 声明的 Python 类型。日历适配器实现 `sessions(request) -> CalendarBatch` 并声明 `calendar.session`。

## 组装 Gateway

```python
from tools.data import DataBinding, DataGateway, DataPolicy, default_catalog

gateway = DataGateway(
    catalog=default_catalog(),
    bindings=[
        (DataBinding("market.bar", "my-bars", priority=1), bars_adapter),
        (DataBinding("calendar.session", "my-calendar", priority=1), calendar_adapter),
    ],
    policy=DataPolicy(timezone="Asia/Shanghai"),
)
```

随后将 Gateway 传给 `FastBacktestEngine` 或 `RealTimeTradeEngine`。CLI 工厂契约也是 `(strategy_or_stream, data_gateway)`：

```python
def build():
    return strategy, gateway
```

## 无凭据示例

- `examples/adapters/memory_adapters.py`：完整的内存 `HistoricalDataPort`/`TradingCalendarPort` 示例和真实 `DataGateway` 组装。
- `examples/in_memory_backtest.py`：通过适配器组装 Gateway 后运行有限回测。
- `examples/in_memory_live.py`：使用 `tools.data.memory.InMemoryGateway` 和内存交易执行器运行有限实盘会话。

```powershell
$env:PYTHONPATH="src"
python examples/in_memory_backtest.py
python examples/in_memory_live.py
python -m pytest tests/test_data_adapters_demo.py tests/cli -q
```

这些示例不读取凭据、不访问真实供应商，也不需要本地数据库。
