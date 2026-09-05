"""In-memory gateway point-in-time table dataset acceptance tests.

Design-spec acceptance criterion 3: at least one in-memory adapter serves
``market.bar`` / ``market.trade`` / ``market.quote`` / ``calendar.session`` and
one point-in-time table dataset whose records respect ``available_at <= as_of``
visibility under the shared request/result contracts.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from tools.data import (
    DataContractError,
    DataPolicy,
    DataRequest,
    InMemoryGateway,
    PointInTimeError,
    UnsupportedDatasetError,
)

TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2024, 4, 30, 15, 0, tzinfo=TZ)


def _row(
    instrument_id: str = "000001.SZ",
    report_date: date = date(2024, 3, 31),
    available_at: datetime | None = AS_OF,
    revision: int = 1,
) -> dict:
    return {
        "instrument_id": instrument_id,
        "asset_type": "equity",
        "report_date": report_date,
        "available_at": available_at,
        "source": "memory",
        "quality": "valid",
        "metadata": {},
        "revision": revision,
        "roe": 12.5,
    }


def test_descriptor_covers_market_and_pit_table_datasets() -> None:
    gateway = InMemoryGateway()

    assert set(gateway.descriptor.datasets) == {
        "market.bar",
        "market.trade",
        "market.quote",
        "calendar.session",
        "fundamental.indicator",
    }
    table_capability = gateway.descriptor.datasets["fundamental.indicator"]
    assert table_capability.point_in_time is True
    assert set(table_capability.modes) == {"historical"}
    assert set(gateway.descriptor.datasets["calendar.session"].modes) == {"calendar"}


def test_unknown_table_dataset_is_rejected_at_construction() -> None:
    with pytest.raises(UnsupportedDatasetError):
        InMemoryGateway(table_dataset="not.a.dataset")


def test_record_published_at_as_of_is_visible() -> None:
    gateway = InMemoryGateway(table_rows=(_row(revision=1),))

    batch = gateway.read(DataRequest(dataset="fundamental.indicator", as_of=AS_OF))

    assert batch.complete is True
    assert batch.next_cursor is None
    assert [record["revision"] for record in batch.records] == [1]
    assert batch.provenance.adapter_name == "memory"
    assert batch.quality.status == "ok"


def test_records_published_after_as_of_are_filtered() -> None:
    future = _row(revision=2, available_at=datetime(2024, 5, 6, 9, 0, tzinfo=TZ))
    gateway = InMemoryGateway(table_rows=(_row(revision=1), future))

    batch = gateway.read(DataRequest(dataset="fundamental.indicator", as_of=AS_OF))

    assert [record["revision"] for record in batch.records] == [1]


def test_rows_are_ordered_by_primary_key_and_keep_visible_revisions() -> None:
    rows = (
        _row(instrument_id="600000.SH", report_date=date(2024, 3, 31), revision=1),
        _row(instrument_id="000001.SZ", report_date=date(2024, 6, 30), revision=1),
        _row(instrument_id="000001.SZ", report_date=date(2024, 3, 31), revision=2),
    )
    gateway = InMemoryGateway(table_rows=rows)

    batch = gateway.read(DataRequest(dataset="fundamental.indicator", as_of=AS_OF))

    assert [
        (record["instrument_id"], record["report_date"], record["revision"])
        for record in batch.records
    ] == [
        ("000001.SZ", date(2024, 3, 31), 2),
        ("000001.SZ", date(2024, 6, 30), 1),
        ("600000.SH", date(2024, 3, 31), 1),
    ]


def test_strict_mode_rejects_records_without_available_at() -> None:
    gateway = InMemoryGateway(table_rows=(_row(available_at=None),))

    with pytest.raises(PointInTimeError):
        gateway.read(DataRequest(dataset="fundamental.indicator", as_of=AS_OF))


def test_non_strict_mode_returns_pit_missing_warning() -> None:
    gateway = InMemoryGateway(
        table_rows=(_row(available_at=None), _row(revision=2)),
        data_policy=DataPolicy(strict=False),
    )

    batch = gateway.read(DataRequest(dataset="fundamental.indicator", as_of=AS_OF))

    assert batch.quality.status == "warning"
    assert "W_PIT_MISSING" in {warning.code for warning in batch.quality.warnings}
    assert batch.quality.rejected_count == 1
    assert len(batch.records) == 2


def test_start_is_inclusive_and_end_is_exclusive() -> None:
    rows = (
        _row(report_date=date(2024, 3, 31), revision=1),
        _row(report_date=date(2024, 6, 30), revision=1),
    )
    gateway = InMemoryGateway(table_rows=rows)

    batch = gateway.read(
        DataRequest(
            dataset="fundamental.indicator",
            start=date(2024, 3, 31),
            end=date(2024, 6, 30),
        )
    )

    assert [record["report_date"] for record in batch.records] == [date(2024, 3, 31)]


def test_report_date_and_instrument_filters_apply() -> None:
    rows = (
        _row(instrument_id="000001.SZ", report_date=date(2024, 3, 31)),
        _row(instrument_id="000001.SZ", report_date=date(2024, 6, 30)),
        _row(instrument_id="600000.SH", report_date=date(2024, 3, 31)),
    )
    gateway = InMemoryGateway(table_rows=rows)

    batch = gateway.read(
        DataRequest(
            dataset="fundamental.indicator",
            filters={"report_date": (date(2024, 3, 31),), "instrument_id": "000001.SZ"},
        )
    )

    assert [record["report_date"] for record in batch.records] == [date(2024, 3, 31)]


def test_explicit_empty_instruments_return_empty_batch() -> None:
    gateway = InMemoryGateway(table_rows=(_row(),))

    batch = gateway.read(DataRequest(dataset="fundamental.indicator", instruments=()))

    assert batch.records == ()
    assert batch.complete is True


def test_unknown_field_and_unsupported_filter_are_rejected() -> None:
    gateway = InMemoryGateway(table_rows=(_row(),))

    with pytest.raises(DataContractError):
        gateway.read(
            DataRequest(dataset="fundamental.indicator", fields=("not_a_field",))
        )
    with pytest.raises(DataContractError):
        gateway.read(
            DataRequest(dataset="fundamental.indicator", filters={"available_at": AS_OF})
        )


def test_unbound_table_dataset_is_rejected() -> None:
    gateway = InMemoryGateway(table_rows=(_row(),))

    with pytest.raises(UnsupportedDatasetError):
        gateway.read(
            DataRequest(dataset="analyst.rating", fields=("rating",), as_of=AS_OF)
        )
