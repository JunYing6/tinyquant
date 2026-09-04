from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import MappingProxyType

import pytest

from tools.data import (
    DataCatalog,
    DatasetDefinition,
    FieldDefinition,
    default_catalog,
)


EXPECTED_DATASETS = {
    "calendar.session",
    "instrument.master",
    "industry.membership",
    "market.bar",
    "market.daily_snapshot",
    "market.daily_metric",
    "market.trade",
    "market.quote",
    "corporate.action",
    "market.money_flow",
    "market.margin",
    "market.margin_detail",
    "market.breadth",
    "market.northbound",
    "market.northbound_summary",
    "market.shibor",
    "market.yield_curve",
    "index.bar",
    "index.member",
    "fund.bar",
    "fund.portfolio",
    "fundamental.indicator",
    "fundamental.consensus",
    "fundamental.income",
    "fundamental.balance",
    "fundamental.cashflow",
    "event.forecast",
    "event.holder_trade",
    "event.top_holder",
    "event.holder_number",
    "event.block_trade",
    "event.pledge",
    "event.equity_incentive",
    "event.private_placement",
    "analyst.rating",
    "macro.indicator",
    "derived.technical_indicator",
}


def make_definition(name: str = "example.dataset", schema_version: str = "1.0") -> DatasetDefinition:
    field = FieldDefinition("instrument_id", str, unit="identifier")
    return DatasetDefinition(
        name=name,
        schema_version=schema_version,
        fields={field.name: field},
        required_fields=frozenset({field.name}),
        filters={"instrument_id": field},
        primary_key=(field.name,),
        ordering=(field.name,),
        time_fields={},
        point_in_time=False,
        asset_types=frozenset({"equity"}),
        status="available",
    )


def test_default_catalog_contains_exactly_all_first_release_datasets() -> None:
    catalog = default_catalog()
    assert set(catalog.list()) == EXPECTED_DATASETS
    assert len(catalog.list()) == len(EXPECTED_DATASETS)


def test_default_catalog_is_frozen_and_exposes_relationships() -> None:
    catalog = default_catalog()
    assert catalog.frozen
    assert catalog.get("market.daily_snapshot").composition == (
        "market.bar",
        "market.daily_metric",
    )
    assert catalog.get("index.bar").view_of == "market.bar"
    assert catalog.get("fund.bar").view_of == "market.bar"
    assert catalog.get("derived.technical_indicator").status == "derived"
    assert catalog.get("market.breadth").status == "derived"
    assert catalog.get("event.private_placement").status == "contract_only"
    assert catalog.get("event.equity_incentive").status == "contract_only"
    assert catalog.get("analyst.rating").status == "contract_only"


def test_catalog_rejects_duplicate_names_and_all_changes_after_freeze() -> None:
    catalog = DataCatalog()
    definition = make_definition()
    catalog.register(definition)
    with pytest.raises(ValueError):
        catalog.register(definition)
    catalog.freeze()
    with pytest.raises(RuntimeError):
        catalog.register(make_definition("new.dataset"))
    with pytest.raises(RuntimeError):
        catalog.register(make_definition(schema_version="2.0"), replace=True)


def test_catalog_allows_only_explicit_replacement_before_freeze() -> None:
    catalog = DataCatalog()
    original = make_definition()
    replacement = make_definition(schema_version="2.0")
    catalog.register(original)
    catalog.register(replacement, replace=True)
    assert catalog.get(original.name, "2.0") is replacement
    with pytest.raises(KeyError):
        catalog.get(original.name, "1.0")


def test_dataset_definition_freezes_nested_mappings_and_sequences() -> None:
    definition = DatasetDefinition(
        name="example.dataset",
        schema_version="1.0",
        fields={
            "value": FieldDefinition("value", float, nullable=True, unit="currency"),
            "event_time": FieldDefinition("event_time", str, unit="datetime"),
        },
        required_fields={"value"},
        filters={"value": FieldDefinition("value", float, unit="currency")},
        primary_key=["value"],
        ordering=["value"],
        time_fields={"event_time": "event"},
        point_in_time=True,
        asset_types={"equity"},
        status="available",
    )
    assert isinstance(definition.fields, MappingProxyType)
    assert isinstance(definition.filters, MappingProxyType)
    assert isinstance(definition.time_fields, MappingProxyType)
    assert isinstance(definition.required_fields, frozenset)
    assert isinstance(definition.primary_key, tuple)
    assert isinstance(definition.ordering, tuple)
    assert isinstance(definition.asset_types, frozenset)
    with pytest.raises(TypeError):
        definition.fields["other"] = definition.fields["value"]
    with pytest.raises(TypeError):
        definition.filters["other"] = definition.filters["value"]
    with pytest.raises(FrozenInstanceError):
        definition.name = "other.dataset"


@pytest.mark.parametrize(
    "changes",
    [
        {"name": ""},
        {"required_fields": frozenset({"missing"})},
        {"primary_key": ("missing",)},
        {"filters": {"missing": FieldDefinition("missing", str, unit="text")}},
        {"asset_types": frozenset()},
        {"status": "bogus"},
        {"revision_key": "not_a_field"},
        {
            "fields": {
                "left": FieldDefinition("same", str, unit="text"),
                "right": FieldDefinition("same", str, unit="text"),
            }
        },
    ],
)
def test_dataset_definition_rejects_invalid_definitions(changes: dict[str, object]) -> None:
    values: dict[str, object] = {
        "name": "example.dataset",
        "schema_version": "1.0",
        "fields": {"value": FieldDefinition("value", float, unit="currency")},
        "required_fields": frozenset({"value"}),
        "filters": {},
        "primary_key": ("value",),
        "ordering": ("value",),
        "time_fields": {},
        "point_in_time": False,
        "asset_types": frozenset({"equity"}),
        "status": "available",
    }
    values.update(changes)
    with pytest.raises(ValueError):
        DatasetDefinition(**values)  # type: ignore[arg-type]


def test_catalog_rejects_unknown_relationships_and_cycles_on_freeze() -> None:
    catalog = DataCatalog()
    catalog.register(make_definition("a.dataset"))
    bad = DatasetDefinition(
        name="b.dataset",
        schema_version="1.0",
        fields={"instrument_id": FieldDefinition("instrument_id", str, unit="identifier")},
        required_fields=frozenset({"instrument_id"}),
        filters={},
        primary_key=("instrument_id",),
        ordering=("instrument_id",),
        time_fields={},
        point_in_time=False,
        asset_types=frozenset({"equity"}),
        status="available",
        view_of="a.dataset",
    )
    catalog.register(bad)
    # a.dataset is fine, b.view_of points to a (exists) -> no error
    catalog.freeze()
    assert catalog.frozen

    # cycle
    c = DataCatalog()
    a1 = make_definition("x.dataset")
    b1 = make_definition("y.dataset")
    a2 = DatasetDefinition(**{**a1.__dict__, "composition": ("y.dataset",)})
    b2 = DatasetDefinition(**{**b1.__dict__, "composition": ("x.dataset",)})
    c.register(a2)
    c.register(b2)
    with pytest.raises(ValueError):
        c.freeze()


def test_catalog_manifest_uses_standard_fields_units_and_aliases() -> None:
    catalog = default_catalog()
    bar = catalog.get("market.bar")
    indicator = catalog.get("fundamental.indicator")
    income = catalog.get("fundamental.income")
    tick = catalog.get("market.trade")
    quote = catalog.get("market.quote")
    assert {"instrument_id", "trading_date", "effective_time", "available_at"} <= set(bar.fields)
    assert bar.fields["close"].unit == "price"
    assert bar.fields["volume"].unit == "quantity"
    assert bar.fields["turnover"].unit == "currency"
    assert bar.fields["pct_chg"].unit == "percentage_points"
    assert "ts_code" in bar.fields["instrument_id"].aliases
    assert "vol" in bar.fields["volume"].aliases
    assert "amount" in bar.fields["turnover"].aliases
    assert indicator.fields["roe"].unit == "percentage_points"
    assert income.fields["total_revenue"].unit == "currency"
    assert {"price", "size", "sequence"} <= set(tick.fields)
    assert {"bid_levels", "ask_levels", "sequence"} <= set(quote.fields)
    assert indicator.revision_key == "revision"


def test_all_default_fields_declare_units_and_nullability() -> None:
    catalog = default_catalog()
    for name in catalog.list():
        definition = catalog.get(name)
        for field in definition.fields.values():
            assert isinstance(field.nullable, bool)
            assert field.unit
