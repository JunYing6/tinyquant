from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from tools.data import (
    DataBatch,
    DataContractError,
    DataProvenance,
    DataRequest,
    PointInTimeError,
    QualityReport,
    Session,
    TradeTick,
    TradingPhase,
    UnsupportedDatasetError,
    default_catalog,
    validate_batch,
    validate_event,
    validate_point_in_time,
    validate_request,
)

CATALOG = default_catalog()


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)


def make_batch(dataset: str, records: list[object], request_id: str = "r1") -> DataBatch:
    return DataBatch(
        request_id=request_id,
        dataset=dataset,
        schema_version="1.0",
        correlation_id=None,
        records=tuple(records),
        complete=True,
        next_cursor=None,
        provenance=DataProvenance(
            adapter_name="adapter",
            source_revision="r1",
            request_fingerprint="fp",
            read_at=utc(2024, 1, 1, 12),
        ),
        quality=QualityReport(status="ok"),
    )


def metric_record(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "instrument_id": "600000",
        "trading_date": date(2024, 1, 1),
        "asset_type": "equity",
    }
    base.update(over)
    return base


def tick(instrument: str, event_time: datetime, sequence: int | str) -> TradeTick:
    return TradeTick(
        schema_version="1.0",
        event_id=None,
        instrument_id=instrument,
        asset_type="equity",
        effective_time=None,
        event_time=event_time,
        available_at=None,
        trading_date=date(2024, 1, 1),
        source="test",
        quality="valid",
        metadata={},
        event_type="trade",
        price=10.0,
        size=1.0,
        turnover=10.0,
        side="BUY",
        sequence=sequence,
    )


def relaxed_trade_definition() -> object:
    """A market.trade definition that accepts numeric-string sequence values."""
    from tools.data import DatasetDefinition, FieldDefinition

    base = CATALOG.get("market.trade")
    fields = dict(base.fields)
    fields["sequence"] = FieldDefinition(name="sequence", python_type=(int, str), nullable=False, unit="count")
    return DatasetDefinition(
        name=base.name,
        schema_version=base.schema_version,
        fields=fields,
        required_fields=base.required_fields,
        filters=base.filters,
        primary_key=base.primary_key,
        ordering=base.ordering,
        time_fields=base.time_fields,
        point_in_time=base.point_in_time,
        asset_types=base.asset_types,
        status=base.status,
        revision_key=base.revision_key,
    )


# ---------------------------------------------------------------------------
# validate_request
# ---------------------------------------------------------------------------


def test_validate_request_ok_and_unknown_dataset() -> None:
    request = DataRequest(dataset="market.bar")
    validate_request(request, CATALOG.get("market.bar"))
    with pytest.raises(UnsupportedDatasetError):
        validate_request(DataRequest(dataset="market.bar"), CATALOG.get("market.daily_metric"))


def test_validate_request_rejects_unknown_field() -> None:
    with pytest.raises(DataContractError):
        validate_request(
            DataRequest(dataset="market.bar", fields=("bogus_field",)),
            CATALOG.get("market.bar"),
        )


def test_validate_request_rejects_unknown_filter() -> None:
    with pytest.raises(DataContractError):
        validate_request(
            DataRequest(dataset="market.bar", filters={"bogus": 1}),
            CATALOG.get("market.bar"),
        )


# ---------------------------------------------------------------------------
# validate_batch
# ---------------------------------------------------------------------------


def test_validate_batch_ok() -> None:
    batch = make_batch(
        "market.daily_metric",
        [metric_record(), metric_record(instrument_id="600001")],
    )
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True)
    assert report.status == "ok"
    assert report.rejected_count == 0
    assert report.checked_count == 2
    assert report.warnings == ()


def test_validate_batch_missing_required_field() -> None:
    bad = metric_record()
    del bad["trading_date"]
    batch = make_batch("market.daily_metric", [bad])
    with pytest.raises(DataContractError):
        validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True)
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.status == "warning"
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)


def test_validate_batch_wrong_type() -> None:
    batch = make_batch("market.daily_metric", [metric_record(instrument_id=123)])
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)


def test_validate_batch_negative_quantity_unit() -> None:
    batch = make_batch("market.daily_metric", [metric_record(total_share=-5.0)])
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)


def test_validate_batch_nan_field() -> None:
    batch = make_batch("market.daily_metric", [metric_record(pe_ttm=float("nan"))])
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)


def test_validate_batch_duplicate_primary_key() -> None:
    batch = make_batch("market.daily_metric", [metric_record(), metric_record()])
    with pytest.raises(DataContractError):
        validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True)
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1  # only the duplicate row counted
    assert any(w.code == "W_DUPLICATE_KEY" for w in report.warnings)


def test_validate_batch_pit_violation() -> None:
    good = metric_record(available_at=utc(2024, 1, 1, 10))
    batch_ok = make_batch("market.daily_metric", [good])
    assert validate_batch(batch_ok, CATALOG.get("market.daily_metric"), as_of=utc(2024, 1, 1, 12)).status == "ok"

    bad = metric_record(available_at=utc(2024, 1, 1, 15))
    batch = make_batch("market.daily_metric", [bad])
    with pytest.raises(PointInTimeError):
        validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True, as_of=utc(2024, 1, 1, 12))
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False, as_of=utc(2024, 1, 1, 12))
    assert report.rejected_count == 1
    assert any(w.code == "W_PIT_MISSING" for w in report.warnings)


def test_validate_batch_strict_raises_contract_error() -> None:
    bad = metric_record()
    del bad["trading_date"]
    batch = make_batch("market.daily_metric", [bad])
    with pytest.raises(DataContractError, match="INVALID"):
        validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True)
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.status == "warning"
    assert report.rejected_count == 1


def test_validate_batch_session_consistency() -> None:
    phase = TradingPhase(
        name="cont",
        start=utc(2024, 1, 1, 9, 30),
        end=utc(2024, 1, 1, 11, 30),
        accepts_trades=True,
        accepts_quotes=True,
    )
    session = Session(market="cn", trading_date=date(2024, 1, 1), timezone="Asia/Shanghai", phases=(phase,))
    inside = tick("600000", utc(2024, 1, 1, 10, 0), 1)
    report = validate_batch(make_batch("market.trade", [inside]), CATALOG.get("market.trade"), strict=False, session=session)
    assert report.status == "ok"

    outside = tick("600000", utc(2024, 1, 1, 14, 0), 1)
    report = validate_batch(make_batch("market.trade", [outside]), CATALOG.get("market.trade"), strict=False, session=session)
    assert any(w.code == "W_SESSION" for w in report.warnings)


def test_validate_batch_timezone_mismatch() -> None:
    batch = make_batch("market.daily_metric", [metric_record(available_at=utc(2024, 1, 1, 10))])
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False, timezone="Asia/Shanghai")
    assert any(w.code == "W_TIMEZONE" for w in report.warnings)
    matched = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False, timezone="UTC")
    assert matched.status == "ok"


# ---------------------------------------------------------------------------
# Ticket ordering
# ---------------------------------------------------------------------------


def test_validate_batch_tick_ordering_ok() -> None:
    records = [
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 1),
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 2),
        tick("600000", utc(2024, 1, 1, 9, 30, 1), 1),
        tick("600001", utc(2024, 1, 1, 9, 30, 2), 5),
    ]
    report = validate_batch(make_batch("market.trade", records), CATALOG.get("market.trade"), strict=False)
    assert report.status == "ok"


def test_validate_batch_tick_ordering_violation() -> None:
    records = [
        tick("600000", utc(2024, 1, 1, 9, 30, 1), 2),
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 1),
    ]
    report = validate_batch(make_batch("market.trade", records), CATALOG.get("market.trade"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_ORDERING" for w in report.warnings)


def test_validate_batch_tick_ordering_same_time_sequence() -> None:
    records = [
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 10),
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 5),
    ]
    report = validate_batch(make_batch("market.trade", records), CATALOG.get("market.trade"), strict=False)
    assert any(w.code == "W_ORDERING" for w in report.warnings)


def test_validate_batch_tick_ordering_numeric_string_normalized() -> None:
    records = [
        tick("600000", utc(2024, 1, 1, 9, 30, 0), "1"),
        tick("600000", utc(2024, 1, 1, 9, 30, 0), 2),
    ]
    report = validate_batch(make_batch("market.trade", records), relaxed_trade_definition(), strict=False)
    assert report.status == "ok"


# ---------------------------------------------------------------------------
# rejected_count dedup
# ---------------------------------------------------------------------------


def test_validate_batch_rejected_count_deduplicates_record() -> None:
    record = metric_record(total_share=-5.0, pe_ttm=float("nan"))
    batch = make_batch("market.daily_metric", [record])
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)


def test_validate_batch_warning_counts_aggregate() -> None:
    batch = make_batch(
        "market.daily_metric",
        [
            metric_record(instrument_id="600000"),
            metric_record(instrument_id="600000"),
        ],
    )
    report = validate_batch(batch, CATALOG.get("market.daily_metric"), strict=False)
    duplicate = next(w for w in report.warnings if w.code == "W_DUPLICATE_KEY")
    assert duplicate.count == 1


# ---------------------------------------------------------------------------
# validate_event / validate_point_in_time
# ---------------------------------------------------------------------------


def test_validate_event_ok_and_invalid() -> None:
    good = validate_event(metric_record(), CATALOG.get("market.daily_metric"), strict=False)
    assert good.status == "ok"
    bad = metric_record(instrument_id=123)
    report = validate_event(bad, CATALOG.get("market.daily_metric"), strict=False)
    assert report.rejected_count == 1
    assert any(w.code == "W_INVALID_FIELD" for w in report.warnings)
    with pytest.raises(DataContractError):
        validate_event(bad, CATALOG.get("market.daily_metric"), strict=True)


def test_validate_point_in_time() -> None:
    as_of = utc(2024, 1, 1, 12)
    ok_records = [metric_record(available_at=utc(2024, 1, 1, 10))]
    validate_point_in_time(ok_records, as_of)
    bad_records = [metric_record(available_at=utc(2024, 1, 1, 15))]
    with pytest.raises(PointInTimeError):
        validate_point_in_time(bad_records, as_of)


def test_validate_batch_request_id_on_error() -> None:
    batch = make_batch("market.daily_metric", [metric_record(instrument_id=123)], request_id="trace-42")
    with pytest.raises(DataContractError) as exc_info:
        validate_batch(batch, CATALOG.get("market.daily_metric"), strict=True)
    assert exc_info.value.request_id == "trace-42"
    assert exc_info.value.dataset == "market.daily_metric"