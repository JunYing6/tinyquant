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

## 真实数据适配器

适配器属于用户项目（例如 `tinyquant-workspace`），通过 `main:build_backtest` 工厂组装并注入 `DataGateway`。tinyquant 1.1 不内置、不发布任何无凭据的内存示例适配器；本地数据库（duckdb/parquet）、tushare 下载与券商执行器等真实实现都在用户项目中完成。
