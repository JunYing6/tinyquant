"""Provider-neutral dataset catalog and field definitions.

A :class:`DatasetDefinition` describes what one logical dataset looks like --
its fields, units, primary key, ordering, time semantics and point-in-time
requirements.  :func:`default_catalog` registers the 37 first-release datasets
and freezes them so a run observes one consistent set of contracts.

Field units follow these conventions:
  * ``currency``     -- money in the asset base unit (yuan for A-shares)
  * ``quantity``     -- shares / units in the base unit
  * ``percentage_points`` -- e.g. ``5.2`` means ``5.2%``
  * ``price``        -- price in the asset base unit
  * old vendor names (``ts_code``/``code``/``stock_code``/``pr``/``vol``/
    ``amount``) are kept only as ``aliases``, never as canonical field names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, Literal, Mapping

DatasetStatus = Literal[
    "available", "contract_only", "provisional", "derived", "internal"
]

# Query-parameter dimensions that may appear in ``filters`` without being
# dataset fields themselves (request controls, not record columns).
_QUERY_DIMS = frozenset(
    {
        "start", "end", "as_of", "anchor", "fields", "codes", "event_types",
        "limit", "cursor",
    }
)

# Cross-domain time dimensions allowed in ``filters`` regardless of whether a
# particular dataset emits them as columns.
_CROSS_TIME_FILTERS = frozenset(
    {
        "effective_time", "event_time", "trading_date", "available_at",
        "announcement_time", "event_date", "report_date", "observation_date",
        "rating_date",
    }
)


def _freeze(value: Any) -> Any:
    """Recursively freeze mappings and sequences into read-only structures."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(value)
    return value


@dataclass(frozen=True)
class FieldDefinition:
    """Definition of one field in a dataset."""

    name: str
    python_type: type | tuple[type, ...]
    nullable: bool = False
    unit: str | None = None
    precision: int | None = None
    description: str = ""
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("field name must not be empty")
        if self.name != self.name.strip():
            raise ValueError(f"field name must not have leading/trailing space: {self.name!r}")
        if self.precision is not None and (isinstance(self.precision, bool) or not isinstance(self.precision, int) or self.precision < 0):
            raise ValueError("precision must be a non-negative integer")


@dataclass(frozen=True)
class DatasetDefinition:
    """What one dataset looks like."""

    name: str
    schema_version: str
    fields: Mapping[str, FieldDefinition]
    required_fields: frozenset[str]
    filters: Mapping[str, FieldDefinition]
    primary_key: tuple[str, ...]
    ordering: tuple[str, ...]
    time_fields: Mapping[str, str]
    point_in_time: bool
    asset_types: frozenset[str]
    status: DatasetStatus
    view_of: str | None = None
    composition: tuple[str, ...] = ()
    extensions: frozenset[str] = frozenset()
    revision_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("dataset name must not be empty")
        if self.name != self.name.strip():
            raise ValueError(f"dataset name must not have leading/trailing space: {self.name!r}")
        if self.status not in ("available", "contract_only", "provisional", "derived", "internal"):
            raise ValueError(f"invalid dataset status: {self.status!r}")
        if not self.asset_types:
            raise ValueError(f"{self.name}: asset_types must not be empty")

        object.__setattr__(self, "fields", _freeze(self.fields))
        object.__setattr__(self, "filters", _freeze(self.filters))
        object.__setattr__(self, "required_fields", frozenset(self.required_fields))
        object.__setattr__(self, "primary_key", tuple(self.primary_key))
        object.__setattr__(self, "ordering", tuple(self.ordering))
        object.__setattr__(self, "time_fields", _freeze(self.time_fields))
        object.__setattr__(self, "asset_types", frozenset(self.asset_types))
        object.__setattr__(self, "composition", tuple(self.composition))
        object.__setattr__(self, "extensions", frozenset(self.extensions))

        fields = set(self.fields)
        if not self.required_fields.issubset(fields):
            raise ValueError(f"{self.name}: required_fields not a subset of fields: {sorted(self.required_fields - fields)}")
        if not set(self.primary_key).issubset(fields):
            raise ValueError(f"{self.name}: primary_key not a subset of fields: {set(self.primary_key) - fields}")
        if not set(self.ordering).issubset(fields):
            raise ValueError(f"{self.name}: ordering not a subset of fields: {set(self.ordering) - fields}")
        missing_filters = set(self.filters) - fields - _QUERY_DIMS - _CROSS_TIME_FILTERS
        if missing_filters and not missing_filters.issubset(_FIELDS):
            raise ValueError(f"{self.name}: filters not a subset of fields or known dims: {sorted(missing_filters)}")
        missing_time = set(self.time_fields) - fields
        if missing_time:
            raise ValueError(f"{self.name}: time_fields keys not a subset of fields: {sorted(missing_time)}")
        if self.revision_key is not None and self.revision_key not in fields:
            raise ValueError(f"{self.name}: revision_key {self.revision_key!r} not a field")


class DataCatalog:
    """Registry of dataset definitions, frozen after construction."""

    def __init__(self) -> None:
        self._definitions: dict[str, DatasetDefinition] = {}
        self._frozen = False

    def register(self, definition: DatasetDefinition, replace: bool = False) -> None:
        if not isinstance(replace, bool):
            raise TypeError("replace must be a bool")
        if self._frozen:
            raise RuntimeError("data catalog is frozen")
        if definition.name in self._definitions and not replace:
            raise ValueError(f"dataset already registered: {definition.name}")
        self._definitions[definition.name] = definition

    def get(self, name: str, schema_version: str | None = None) -> DatasetDefinition:
        if not isinstance(name, str) or not name.strip():
            raise KeyError(name)
        if name not in self._definitions:
            raise KeyError(name)
        definition = self._definitions[name]
        if schema_version is not None:
            if not isinstance(schema_version, str):
                raise TypeError("schema_version must be a string or None")
            if not schema_version:
                raise KeyError(name)
            if definition.schema_version != schema_version:
                raise KeyError(f"{name!r} has no schema version {schema_version!r}")
        return definition

    def list(self) -> tuple[str, ...]:
        return tuple(self._definitions)

    def freeze(self) -> None:
        if self._frozen:
            return
        self._validate_relations()
        self._frozen = True

    @property
    def frozen(self) -> bool:
        return self._frozen

    @classmethod
    def default(cls) -> "DataCatalog":
        catalog = cls()
        for definition in _default_definitions():
            catalog.register(definition)
        catalog.freeze()
        return catalog

    def _validate_relations(self) -> None:
        names = set(self._definitions)
        for name, definition in self._definitions.items():
            if definition.view_of is not None and definition.view_of not in names:
                raise ValueError(f"{name}: view_of {definition.view_of!r} is not registered")
            for target in definition.composition:
                if target not in names:
                    raise ValueError(f"{name}: composition member {target!r} is not registered")
        # cycle / self-reference detection via DFS over view_of and composition
        visiting: set[str] = set()
        done: set[str] = set()

        def visit(node: str, path: set[str]) -> None:
            if node in done:
                return
            if node in visiting:
                raise ValueError(f"dataset relation cycle detected: {' -> '.join(sorted(path))}")
            visiting.add(node)
            definition = self._definitions[node]
            for target in (list(definition.composition) + ([definition.view_of] if definition.view_of else [])):
                if target in self._definitions:
                    visit(target, path | {target})
            visiting.discard(node)
            done.add(node)

        for name in names:
            visit(name, {name})


def default_catalog() -> DataCatalog:
    """Return a fresh, frozen catalog containing the release manifest."""
    return DataCatalog.default()


def _field(
    name: str,
    python_type: type | tuple[type, ...],
    unit: str,
    *,
    nullable: bool = True,
    aliases: tuple[str, ...] = (),
    description: str | None = None,
) -> FieldDefinition:
    return FieldDefinition(
        name=name,
        python_type=python_type,
        nullable=nullable,
        unit=unit,
        description=description or name,
        aliases=tuple(aliases),
    )


# ---------------------------------------------------------------------------
# Canonical field registry
# ---------------------------------------------------------------------------

_FIELDS: dict[str, FieldDefinition] = {}


def _add_fields(
    names: tuple[str, ...],
    python_type: type | tuple[type, ...],
    unit: str,
    *,
    nullable: bool = True,
    aliases: dict[str, tuple[str, ...]] | None = None,
) -> None:
    for name in names:
        if name in _FIELDS:
            raise RuntimeError(f"duplicate catalog field: {name}")
        _FIELDS[name] = _field(name, python_type, unit, nullable=nullable, aliases=(aliases or {}).get(name, ()))


def _manifest(*names: str) -> dict[str, FieldDefinition]:
    result: dict[str, FieldDefinition] = {}
    for name in names:
        if name not in _FIELDS:
            raise RuntimeError(f"unknown catalog field: {name}")
        result[name] = _FIELDS[name]
    return result


def _combine(*groups: tuple[str, ...]) -> tuple[str, ...]:
    result: list[str] = []
    for group in groups:
        for name in group:
            if name not in result:
                result.append(name)
    return tuple(result)


# cross-domain identifiers
_add_fields(
    (
        "instrument_id", "asset_type", "source", "quality", "market", "timezone",
        "symbol", "name", "exchange", "status", "industry_id", "holder_name",
        "holder_type", "in_de", "type", "action_type", "side", "session",
        "frequency", "price_basis", "event_id", "action_id", "index_id",
        "fund_id", "report_id", "rating", "term", "curve_id", "curve_type",
        "indicator", "parameter_hash", "pledgee", "pledge_status",
    ),
    str,
    "text",
    nullable=True,
    aliases={
        "instrument_id": ("ts_code", "code", "stock_code"),
        "index_id": ("index_code",),
        "fund_id": ("fund_code",),
    },
)
_FIELDS["instrument_id"] = _field("instrument_id", str, "identifier", nullable=False, aliases=("ts_code", "code", "stock_code"))
_FIELDS["asset_type"] = _field("asset_type", str, "category", nullable=False)
_FIELDS["event_id"] = _field("event_id", str, "identifier", nullable=False)
_FIELDS["action_id"] = _field("action_id", str, "identifier", nullable=False)
_FIELDS["frequency"] = _field("frequency", str, "frequency", nullable=False)
_FIELDS["side"] = _field("side", str, "category", nullable=False)
_FIELDS["sequence"] = _field("sequence", int, "count", nullable=False)

_add_fields(
    (
        "trading_date", "report_date", "event_date", "valid_from", "valid_to",
        "listed_date", "delisted_date", "effective_date", "rating_date",
        "observation_date", "pledge_start_date", "pledge_end_date",
    ),
    date,
    "date",
    aliases={
        "trading_date": ("trade_date",),
        "report_date": ("end_date",),
        "valid_from": ("in_date",),
        "valid_to": ("out_date",),
    },
)
_FIELDS["trading_date"] = _field("trading_date", date, "date", nullable=False, aliases=("trade_date",))
_FIELDS["report_date"] = _field("report_date", date, "date", nullable=False, aliases=("end_date",))
_FIELDS["observation_month"] = _field("observation_month", str, "month", nullable=False, aliases=("month",))
_FIELDS["fiscal_year"] = _field("fiscal_year", int, "year", nullable=False)

_add_fields(
    (
        "effective_time", "event_time", "available_at", "announcement_time",
        "interval_start", "interval_end",
    ),
    datetime,
    "datetime",
    aliases={
        "event_time": ("time",),
        "available_at": ("ann_date",),
        "announcement_time": ("ann_date",),
    },
)
_FIELDS["interval_start"] = _field("interval_start", datetime, "datetime", nullable=False)
_FIELDS["interval_end"] = _field("interval_end", datetime, "datetime", nullable=False)

_add_fields(("metadata",), Mapping, "mapping", nullable=True)
_add_fields(("phases", "bid_levels", "ask_levels"), tuple, "structure", nullable=False)
_add_fields(
    ("is_complete", "is_st", "is_suspended", "is_limit_up", "is_limit_down", "is_limit_break"),
    bool,
    "boolean",
    nullable=False,
)

_add_fields(
    (
        "open", "high", "low", "close", "pre_close", "up_limit", "down_limit",
        "price", "last_price", "last_size", "issue_price", "exercise_price",
        "avg_price", "change",
    ),
    float,
    "price",
    nullable=True,
    aliases={"price": ("pr",)},
)
for _n in ("open", "high", "low", "close", "pre_close"):
    _FIELDS[_n] = _field(_n, float, "price", nullable=False)
_FIELDS["price"] = _field("price", float, "price", nullable=False, aliases=("pr",))
_FIELDS["last_size"] = _field("last_size", float, "quantity")

_add_fields(
    (
        "volume", "size", "cumulative_volume", "total_share", "float_share",
        "free_share", "issue_shares", "incentive_shares", "change_vol",
        "after_share", "hold_amount", "hold_shares", "net_buy_shares",
        "quantity", "rqmcl", "rqchl",
    ),
    float,
    "quantity",
    nullable=True,
    aliases={"volume": ("vol",), "size": ("vol",), "cumulative_volume": ("total_vol",), "quantity": ("amount",)},
)
_FIELDS["size"] = _field("size", float, "quantity", nullable=False, aliases=("vol",))
_FIELDS["cumulative_volume"] = _field("cumulative_volume", float, "quantity", aliases=("total_vol",))
_FIELDS["volume"] = _field("volume", float, "quantity", nullable=False, aliases=("vol",))
_FIELDS["sequence"] = _field("sequence", int, "count", nullable=False)

_add_fields(
    (
        "turnover", "cumulative_turnover", "total_mv", "circ_mv", "market_value",
        "hold_market_cap", "net_buy", "rzmre", "rzye", "rqye", "rzrqye",
        "rzche", "up_amount", "down_amount", "total_amount", "buy_sm_amount",
        "sell_sm_amount", "buy_md_amount", "sell_md_amount", "buy_lg_amount",
        "sell_lg_amount", "buy_elg_amount", "sell_elg_amount", "net_mf_amount",
        "net_profit_min", "net_profit_max", "total_revenue", "revenue",
        "total_cogs", "oper_exp", "operate_profit", "total_profit",
        "n_income_attr_p", "n_income", "int_income", "comm_income",
        "n_commis_income", "invest_income", "ass_invest_income",
        "assets_impair_loss", "non_oper_income", "non_oper_exp", "income_tax",
        "biz_tax_surchg", "sell_exp", "admin_exp", "fin_exp", "rd_exp",
        "profit_dedt", "total_assets", "total_liab",
        "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int",
        "total_cur_assets", "total_cur_liab", "total_nca", "total_ncl",
        "money_cap", "notes_receiv", "accounts_receiv", "inventories",
        "fix_assets", "intan_assets", "goodwill", "lt_rec", "defer_tax_assets",
        "lt_borr", "st_borr", "notes_payable", "oth_payable", "oth_cur_liab",
        "adv_receipts", "payroll_payable", "taxes_payable", "int_payable",
        "defer_tax_liab", "minority_int", "free_cashflow", "n_cashflow_act",
        "n_cashflow_inv_act", "n_cash_flows_fnc_act", "c_cash_equ_end_period",
        "c_fr_sale_sg", "c_pay_acq_const_fiolta", "c_cash_equ_beg_period",
        "n_incr_cash_cash_equ", "eps_mean", "np_mean", "value",
    ),
    float,
    "currency",
    aliases={"turnover": ("amount",), "cumulative_turnover": ("amount",), "market_value": ("mkv",)},
)
_FIELDS["turnover"] = _field("turnover", float, "currency", nullable=False, aliases=("amount",))
_FIELDS["net_buy"] = _field("net_buy", float, "currency", nullable=False)
_FIELDS["value"] = _field("value", float, "value", nullable=False)
_FIELDS["eps_mean"] = _field("eps_mean", float, "currency")
_FIELDS["np_mean"] = _field("np_mean", float, "currency")

_add_fields(
    (
        "pct_chg", "turnover_rate_f", "dv_ttm", "net_pct_main", "change_ratio",
        "after_ratio", "hold_ratio", "stk_mkv_ratio", "stk_float_ratio",
        "weight", "premium_rate", "exercise_roe", "base_year_roe",
        "exercise_deducted_roe", "base_year_deducted_roe", "p_change_min",
        "p_change_max", "roe", "roe_waa", "roe_dt", "roe_yearly", "roa",
        "roic", "roa2_yearly", "grossprofit_margin", "netprofit_margin",
        "op_of_gr", "or_yoy", "netprofit_yoy", "basic_eps_yoy", "dt_eps_yoy",
        "debt_to_assets", "debt_to_eqt", "pledge_ratio", "rate", "yield",
    ),
    float,
    "percentage_points",
)
_FIELDS["pct_chg"] = _field("pct_chg", float, "percentage_points", nullable=False)
_FIELDS["weight"] = _field("weight", float, "percentage_points", nullable=False)
_FIELDS["rate"] = _field("rate", float, "percentage_points", nullable=False)
_FIELDS["yield"] = _field("yield", float, "percentage_points", nullable=False)

_add_fields(
    (
        "volume_ratio", "pe_ttm", "pb", "ps_ttm", "current_ratio", "quick_ratio",
        "cash_ratio", "assets_to_eqt", "assets_turn", "ar_turn", "adj_factor",
    ),
    float,
    "ratio",
)
_FIELDS["adj_factor"] = _field("adj_factor", float, "factor")

_add_fields(("bps", "eps", "ocfps", "total_revenue_ps", "basic_eps", "diluted_eps"), float, "currency")
_FIELDS["eps"] = _field("eps", float, "currency")

_add_fields(("holder_number", "level", "curve_term", "revision"), (int, float), "count")
_FIELDS["level"] = _field("level", int, "level")
_FIELDS["curve_term"] = _field("curve_term", float, "years")
_FIELDS["curve_type"] = _field("curve_type", str, "category")
_FIELDS["revision"] = _field("revision", int, "revision")
_FIELDS["parameter_hash"] = _field("parameter_hash", str, "identifier")
_FIELDS["term"] = _field("term", str, "term")
_FIELDS["holder_number"] = _field("holder_number", int, "count")

_add_fields(
    (
        "start", "end", "as_of", "anchor", "fields", "codes", "event_types",
        "limit", "cursor",
    ),
    (str, date, datetime, tuple, int),
    "filter",
)
_FIELDS["start"] = _field("start", (str, date, datetime), "date")
_FIELDS["end"] = _field("end", (str, date, datetime), "date")
_FIELDS["as_of"] = _field("as_of", (date, datetime), "datetime")
_FIELDS["anchor"] = _field("anchor", (date, datetime), "datetime")
_FIELDS["fields"] = _field("fields", tuple, "projection")
_FIELDS["codes"] = _field("codes", tuple, "identifier")
_FIELDS["event_types"] = _field("event_types", tuple, "category")
_FIELDS["limit"] = _field("limit", int, "count")
_FIELDS["cursor"] = _field("cursor", str, "cursor")

_add_fields(("is_new", "is_index", "flag", "listed_status", "fiscal_period", "action_value", "pledge_amount", "p_change", "forecast_type"), str, "text")
_FIELDS["is_new"] = _field("is_new", bool, "boolean")
_FIELDS["is_index"] = _field("is_index", bool, "boolean")
_FIELDS["flag"] = _field("flag", int, "count")
_FIELDS["listed_status"] = _field("listed_status", str, "category")
_FIELDS["action_value"] = _field("action_value", float, "value")
_FIELDS["pledge_amount"] = _field("pledge_amount", float, "quantity")
_FIELDS["p_change"] = _field("p_change", float, "percentage_points")
_FIELDS["forecast_type"] = _field("forecast_type", str, "category")


def _dataset(
    name: str,
    fields: tuple[str, ...],
    required_fields: tuple[str, ...],
    filters: tuple[str, ...],
    primary_key: tuple[str, ...],
    ordering: tuple[str, ...],
    time_fields: dict[str, str],
    *,
    point_in_time: bool,
    asset_types: frozenset[str],
    status: DatasetStatus = "available",
    view_of: str | None = None,
    composition: tuple[str, ...] = (),
    extensions: frozenset[str] = frozenset(),
    revision_key: str | None = None,
) -> DatasetDefinition:
    return DatasetDefinition(
        name=name,
        schema_version="1.0",
        fields=_manifest(*fields),
        required_fields=frozenset(required_fields),
        filters=_manifest(*filters),
        primary_key=primary_key,
        ordering=ordering,
        time_fields=time_fields,
        point_in_time=point_in_time,
        asset_types=asset_types,
        status=status,
        view_of=view_of,
        composition=composition,
        extensions=extensions,
        revision_key=revision_key,
    )


_INSTRUMENT_COMMON = (
    "instrument_id", "asset_type", "trading_date", "effective_time", "event_time",
    "available_at", "source", "quality", "metadata",
)
_EVENT_COMMON = (
    "event_id", "instrument_id", "asset_type", "effective_time", "event_time",
    "available_at", "source", "quality", "metadata",
)
_TIME_FIELDS = {"trading_date": "session_date", "available_at": "publication_time"}
_INSTRUMENT_FILTERS = (
    "instrument_id", "asset_type", "trading_date", "effective_time", "event_time",
    "available_at", "start", "end", "as_of", "source", "quality",
)
_EVENT_FILTERS = (
    "instrument_id", "asset_type", "event_date", "report_date", "effective_time",
    "event_time", "available_at", "start", "end", "as_of", "event_types", "source",
)
_BAR_FIELDS = _combine(
    _INSTRUMENT_COMMON,
    (
        "frequency", "interval_start", "interval_end", "open", "high", "low",
        "close", "volume", "turnover", "pre_close", "pct_chg", "is_complete",
        "price_basis",
    ),
)
_BAR_REQUIRED = (
    "instrument_id", "asset_type", "frequency", "interval_start", "interval_end",
    "open", "high", "low", "close", "volume", "turnover", "is_complete", "price_basis",
)
_DAILY_METRIC_FIELDS = (
    "instrument_id", "asset_type", "trading_date", "available_at", "source",
    "quality", "metadata", "is_st", "is_suspended", "is_limit_up",
    "is_limit_down", "is_limit_break", "turnover_rate_f", "volume_ratio",
    "pe_ttm", "pb", "ps_ttm", "dv_ttm", "total_share", "float_share",
    "free_share", "total_mv", "circ_mv", "up_limit", "down_limit", "adj_factor", "name",
)
_FINANCIAL_INDICATOR_FIELDS = (
    "roe", "roe_waa", "roe_dt", "roe_yearly", "roa", "roic", "roa2_yearly",
    "grossprofit_margin", "netprofit_margin", "op_of_gr", "current_ratio",
    "quick_ratio", "cash_ratio", "debt_to_assets", "debt_to_eqt",
    "assets_to_eqt", "bps", "eps", "ocfps", "total_revenue_ps", "assets_turn",
    "ar_turn", "or_yoy", "netprofit_yoy", "profit_dedt", "basic_eps_yoy",
    "dt_eps_yoy",
)
_INCOME_FIELDS = (
    "total_revenue", "revenue", "total_cogs", "oper_exp", "operate_profit",
    "total_profit", "n_income_attr_p", "n_income", "int_income", "comm_income",
    "n_commis_income", "invest_income", "ass_invest_income",
    "assets_impair_loss", "non_oper_income", "non_oper_exp", "income_tax",
    "biz_tax_surchg", "sell_exp", "admin_exp", "fin_exp", "rd_exp",
    "basic_eps", "diluted_eps",
)
_BALANCE_FIELDS = (
    "total_assets", "total_liab", "total_hldr_eqy_exc_min_int",
    "total_hldr_eqy_inc_min_int", "total_cur_assets", "total_cur_liab",
    "total_nca", "total_ncl", "money_cap", "notes_receiv", "accounts_receiv",
    "inventories", "fix_assets", "intan_assets", "goodwill", "lt_rec",
    "defer_tax_assets", "lt_borr", "st_borr", "notes_payable", "oth_payable",
    "oth_cur_liab", "adv_receipts", "payroll_payable", "taxes_payable",
    "int_payable", "defer_tax_liab", "minority_int", "total_share",
)
_CASHFLOW_FIELDS = (
    "free_cashflow", "c_fr_sale_sg", "c_pay_acq_const_fiolta",
    "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act",
    "c_cash_equ_end_period", "c_cash_equ_beg_period", "n_incr_cash_cash_equ",
)


def _default_definitions() -> tuple[DatasetDefinition, ...]:
    equity_index_fund = frozenset({"equity", "index", "fund"})
    broad_assets = frozenset({"equity", "index", "fund", "bond", "future", "option"})
    return (
        _dataset("calendar.session",
                 ("market", "trading_date", "timezone", "phases", "source", "quality", "metadata"),
                 ("market", "trading_date", "timezone", "phases"),
                 ("market", "trading_date"),
                 ("market", "trading_date"), ("trading_date",),
                 {"trading_date": "session_date"},
                 point_in_time=False, asset_types=frozenset({"market"})),
        _dataset("instrument.master",
                 ("instrument_id", "symbol", "name", "asset_type", "exchange", "status",
                  "listed_date", "delisted_date", "valid_from", "valid_to", "available_at",
                  "source", "quality", "metadata"),
                 ("instrument_id", "symbol", "name", "asset_type", "exchange", "status", "listed_date"),
                 ("instrument_id", "asset_type", "available_at", "as_of", "source"),
                 ("instrument_id", "valid_from"), ("instrument_id", "valid_from"),
                 {"valid_from": "validity_start", "valid_to": "validity_end", "available_at": "publication_time"},
                 point_in_time=True, asset_types=broad_assets),
        _dataset("industry.membership",
                 ("industry_id", "instrument_id", "level", "valid_from", "valid_to", "available_at", "source", "quality", "metadata"),
                 ("industry_id", "instrument_id", "level", "valid_from", "valid_to"),
                 ("industry_id", "instrument_id", "level", "start", "end", "as_of"),
                 ("industry_id", "instrument_id", "valid_from"), ("industry_id", "instrument_id", "valid_from"),
                 {"valid_from": "membership_start", "valid_to": "membership_end", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("market.bar", _BAR_FIELDS, _BAR_REQUIRED,
                 _combine(_INSTRUMENT_FILTERS, ("frequency", "price_basis", "fields")),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 {"event_time": "interval_end", "trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=False, asset_types=equity_index_fund),
        _dataset("market.daily_snapshot", _combine(_BAR_FIELDS, _DAILY_METRIC_FIELDS),
                 ("instrument_id", "trading_date"),
                 _combine(_INSTRUMENT_FILTERS, ("fields",)),
                 ("instrument_id", "trading_date"), ("instrument_id", "trading_date"),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=equity_index_fund,
                 composition=("market.bar", "market.daily_metric")),
        _dataset("market.daily_metric", _DAILY_METRIC_FIELDS,
                 ("instrument_id", "trading_date"),
                 _combine(_INSTRUMENT_FILTERS, ("fields",)),
                 ("instrument_id", "trading_date"), ("instrument_id", "trading_date"),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=equity_index_fund),
        _dataset("market.trade", _combine(_INSTRUMENT_COMMON, ("price", "size", "turnover", "cumulative_volume", "cumulative_turnover", "side", "sequence", "session")),
                 ("instrument_id", "event_time", "trading_date", "price", "size", "turnover", "sequence"),
                 _combine(_INSTRUMENT_FILTERS, ("start", "end", "session", "fields")),
                 ("instrument_id", "event_time", "sequence"), ("instrument_id", "event_time", "sequence"),
                 {"event_time": "trade_time", "trading_date": "session_date", "available_at": "feed_time"},
                 point_in_time=True, asset_types=equity_index_fund),
        _dataset("market.quote", _combine(_INSTRUMENT_COMMON, ("bid_levels", "ask_levels", "last_price", "last_size", "sequence", "session")),
                 ("instrument_id", "event_time", "trading_date", "bid_levels", "ask_levels", "sequence"),
                 _combine(_INSTRUMENT_FILTERS, ("start", "end", "session", "fields")),
                 ("instrument_id", "event_time", "sequence"), ("instrument_id", "event_time", "sequence"),
                 {"event_time": "quote_time", "trading_date": "session_date", "available_at": "feed_time"},
                 point_in_time=True, asset_types=equity_index_fund),
        _dataset("corporate.action", _combine(_EVENT_COMMON, ("action_id", "action_type", "effective_date", "action_value", "total_share")),
                 ("action_id", "instrument_id", "action_type", "effective_date"),
                 _EVENT_FILTERS,
                 ("action_id",), ("available_at", "effective_date"),
                 {"effective_time": "action_effective_time", "available_at": "publication_time"},
                 point_in_time=True, asset_types=equity_index_fund),
        _dataset("market.money_flow", _combine(_INSTRUMENT_COMMON, ("buy_sm_amount", "sell_sm_amount", "buy_md_amount", "sell_md_amount", "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount", "net_mf_amount", "net_pct_main")),
                 ("instrument_id", "trading_date"),
                 _combine(_INSTRUMENT_FILTERS, ("fields",)),
                 ("instrument_id", "trading_date"), ("instrument_id", "trading_date"),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("market.margin",
                 ("trading_date", "available_at", "source", "quality", "metadata", "rzmre", "rzye", "rqye", "rzrqye"),
                 ("trading_date", "rzmre", "rzye", "rqye", "rzrqye"),
                 ("trading_date", "start", "end", "as_of", "fields"),
                 ("trading_date",), ("trading_date",),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"market"})),
        _dataset("market.margin_detail",
                 ("instrument_id", "trading_date", "available_at", "source", "quality", "metadata", "rzmre", "rzche", "rqmcl", "rqchl", "rqye", "rzrqye"),
                 ("instrument_id", "trading_date", "rzmre", "rzche", "rqmcl", "rqchl", "rqye", "rzrqye"),
                 _combine(_INSTRUMENT_FILTERS, ("fields",)),
                 ("instrument_id", "trading_date"), ("instrument_id", "trading_date"),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("market.breadth",
                 ("trading_date", "available_at", "source", "quality", "metadata", "up_amount", "down_amount", "total_amount"),
                 ("trading_date", "up_amount", "down_amount", "total_amount"),
                 ("trading_date", "start", "end", "as_of", "fields"),
                 ("trading_date",), ("trading_date",),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"market"}), status="derived"),
        _dataset("market.northbound", _combine(_INSTRUMENT_COMMON, ("net_buy", "net_buy_shares", "hold_shares", "hold_market_cap", "close", "pct_chg")),
                 ("instrument_id", "trading_date", "net_buy", "net_buy_shares", "hold_shares", "hold_market_cap"),
                 _combine(_INSTRUMENT_FILTERS, ("fields",)),
                 ("instrument_id", "trading_date"), ("instrument_id", "trading_date"),
                 _TIME_FIELDS, point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("market.northbound_summary",
                 ("trading_date", "available_at", "source", "quality", "metadata", "net_buy", "net_buy_shares", "turnover", "value"),
                 ("trading_date",),
                 ("trading_date", "start", "end", "as_of", "fields"),
                 ("trading_date",), ("trading_date",),
                 {"trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"market"})),
        _dataset("market.shibor",
                 ("observation_date", "term", "rate", "available_at", "source", "quality", "metadata"),
                 ("observation_date", "term", "rate"),
                 ("observation_date", "start", "end", "as_of", "term", "fields"),
                 ("observation_date", "term"), ("observation_date", "term"),
                 {"observation_date": "observation_time", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"market"})),
        _dataset("market.yield_curve",
                 ("curve_id", "curve_type", "curve_term", "observation_date", "yield", "available_at", "source", "quality", "metadata"),
                 ("curve_id", "curve_type", "curve_term", "observation_date", "yield"),
                 ("curve_id", "curve_type", "curve_term", "observation_date", "start", "end", "as_of", "fields"),
                 ("curve_id", "curve_type", "curve_term", "observation_date"),
                 ("curve_id", "curve_type", "curve_term", "observation_date"),
                 {"observation_date": "observation_time", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"bond"})),
        _dataset("index.bar", _BAR_FIELDS, _BAR_REQUIRED,
                 _combine(_INSTRUMENT_FILTERS, ("frequency", "price_basis", "fields")),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 {"event_time": "interval_end", "trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=False, asset_types=frozenset({"index"}), view_of="market.bar"),
        _dataset("index.member",
                 ("index_id", "instrument_id", "asset_type", "effective_date", "weight", "available_at", "source", "quality", "metadata"),
                 ("index_id", "instrument_id", "effective_date", "weight"),
                 ("index_id", "instrument_id", "effective_date", "start", "end", "as_of", "fields"),
                 ("index_id", "instrument_id", "effective_date"), ("index_id", "instrument_id", "effective_date"),
                 {"effective_date": "membership_date", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"index"})),
        _dataset("fund.bar", _BAR_FIELDS, _BAR_REQUIRED,
                 _combine(_INSTRUMENT_FILTERS, ("frequency", "price_basis", "fields")),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 ("instrument_id", "frequency", "interval_end", "price_basis"),
                 {"event_time": "interval_end", "trading_date": "session_date", "available_at": "publication_time"},
                 point_in_time=False, asset_types=frozenset({"fund"}), view_of="market.bar"),
        _dataset("fund.portfolio",
                 ("fund_id", "instrument_id", "asset_type", "report_date", "available_at", "market_value", "quantity", "weight", "stk_mkv_ratio", "stk_float_ratio", "source", "quality", "metadata"),
                 ("fund_id", "instrument_id", "report_date", "market_value", "quantity", "weight"),
                 ("fund_id", "instrument_id", "report_date", "start", "end", "as_of", "fields"),
                 ("fund_id", "instrument_id", "report_date", "available_at"),
                 ("fund_id", "instrument_id", "report_date", "available_at"),
                 {"report_date": "reporting_period", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"fund", "equity"})),
        _dataset("fundamental.indicator",
                 _combine(("instrument_id", "asset_type", "report_date", "effective_time", "available_at", "source", "quality", "metadata", "revision"), _FINANCIAL_INDICATOR_FIELDS),
                 ("instrument_id", "report_date"),
                 _combine(("instrument_id", "asset_type", "report_date", "start", "end", "as_of", "fields"), ("available_at",)),
                 ("instrument_id", "report_date", "revision"), ("instrument_id", "report_date", "revision"),
                 {"effective_time": "reporting_period", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), revision_key="revision"),
        _dataset("fundamental.consensus",
                 ("instrument_id", "asset_type", "fiscal_year", "available_at", "eps_mean", "np_mean", "source", "quality", "metadata"),
                 ("instrument_id", "fiscal_year", "eps_mean", "np_mean"),
                 ("instrument_id", "fiscal_year", "start", "end", "as_of", "fields"),
                 ("instrument_id", "fiscal_year", "available_at"), ("instrument_id", "fiscal_year", "available_at"),
                 {"available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("fundamental.income",
                 _combine(("instrument_id", "asset_type", "report_date", "effective_time", "available_at", "source", "quality", "metadata", "revision"), _INCOME_FIELDS),
                 ("instrument_id", "report_date"),
                 _combine(("instrument_id", "asset_type", "report_date", "start", "end", "as_of", "fields"), ("available_at",)),
                 ("instrument_id", "report_date", "revision"), ("instrument_id", "report_date", "revision"),
                 {"effective_time": "reporting_period", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), revision_key="revision"),
        _dataset("fundamental.balance",
                 _combine(("instrument_id", "asset_type", "report_date", "effective_time", "available_at", "source", "quality", "metadata", "revision"), _BALANCE_FIELDS),
                 ("instrument_id", "report_date"),
                 _combine(("instrument_id", "asset_type", "report_date", "start", "end", "as_of", "fields"), ("available_at",)),
                 ("instrument_id", "report_date", "revision"), ("instrument_id", "report_date", "revision"),
                 {"effective_time": "reporting_period", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), revision_key="revision"),
        _dataset("fundamental.cashflow",
                 _combine(("instrument_id", "asset_type", "report_date", "effective_time", "available_at", "source", "quality", "metadata", "revision"), _CASHFLOW_FIELDS),
                 ("instrument_id", "report_date"),
                 _combine(("instrument_id", "asset_type", "report_date", "start", "end", "as_of", "fields"), ("available_at",)),
                 ("instrument_id", "report_date", "revision"), ("instrument_id", "report_date", "revision"),
                 {"effective_time": "reporting_period", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), revision_key="revision"),
        _dataset("event.forecast", _combine(_EVENT_COMMON, ("event_date", "report_date", "type", "p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "announcement_time")),
                 ("event_id", "instrument_id", "event_date", "report_date", "type"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "event_date", "event_id"),
                 {"event_date": "forecast_period", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.holder_trade", _combine(_EVENT_COMMON, ("holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share", "valid_from", "valid_to", "announcement_time")),
                 ("event_id", "instrument_id", "holder_name", "holder_type", "in_de", "change_vol"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "event_id"),
                 {"valid_from": "trade_period_start", "valid_to": "trade_period_end", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.top_holder", _combine(_EVENT_COMMON, ("report_date", "holder_name", "hold_amount", "hold_ratio", "announcement_time")),
                 ("event_id", "instrument_id", "report_date", "holder_name", "hold_amount", "hold_ratio"),
                 _EVENT_FILTERS,
                 ("instrument_id", "report_date", "holder_name", "available_at"),
                 ("available_at", "instrument_id", "report_date", "holder_name"),
                 {"report_date": "reporting_period", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.holder_number", _combine(_EVENT_COMMON, ("report_date", "holder_number", "announcement_time")),
                 ("event_id", "instrument_id", "report_date", "holder_number"),
                 _EVENT_FILTERS,
                 ("instrument_id", "report_date", "available_at"), ("available_at", "instrument_id", "report_date"),
                 {"report_date": "reporting_period", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.block_trade", _combine(_EVENT_COMMON, ("event_date", "price", "quantity", "turnover", "announcement_time")),
                 ("event_id", "instrument_id", "event_date", "price", "quantity", "turnover"),
                 _EVENT_FILTERS, ("event_id",), ("event_date", "available_at", "event_id"),
                 {"event_date": "trade_date", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.pledge", _combine(_EVENT_COMMON, ("event_date", "valid_from", "valid_to", "quantity", "pledgee", "pledge_status", "pledge_ratio", "announcement_time")),
                 ("event_id", "instrument_id", "event_date", "quantity"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "event_date", "event_id"),
                 {"event_date": "pledge_event_date", "valid_from": "pledge_start", "valid_to": "pledge_end", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"})),
        _dataset("event.equity_incentive", _combine(_EVENT_COMMON, ("event_date", "issue_price", "issue_shares", "exercise_price", "premium_rate", "incentive_shares", "exercise_roe", "base_year_roe", "exercise_deducted_roe", "base_year_deducted_roe", "total_share", "announcement_time")),
                 ("event_id", "instrument_id", "event_date"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "event_date", "event_id"),
                 {"event_date": "incentive_event_date", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), status="contract_only"),
        _dataset("event.private_placement", _combine(_EVENT_COMMON, ("event_date", "issue_price", "issue_shares", "announcement_time")),
                 ("event_id", "instrument_id", "event_date"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "event_date", "event_id"),
                 {"event_date": "placement_event_date", "available_at": "announcement_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), status="contract_only"),
        _dataset("analyst.rating", _combine(_EVENT_COMMON, ("rating", "report_id", "rating_date", "announcement_time")),
                 ("event_id", "instrument_id", "rating", "report_id", "rating_date"),
                 _EVENT_FILTERS, ("event_id",), ("available_at", "rating_date", "event_id"),
                 {"rating_date": "rating_date", "available_at": "report_publication_time"},
                 point_in_time=True, asset_types=frozenset({"equity"}), status="contract_only"),
        _dataset("macro.indicator",
                 ("indicator", "observation_month", "observation_date", "value", "rate", "revision", "available_at", "source", "quality", "metadata"),
                 ("indicator", "observation_month", "value"),
                 ("indicator", "observation_month", "start", "end", "as_of", "fields"),
                 ("indicator", "observation_month", "revision"), ("indicator", "observation_month", "revision"),
                 {"observation_month": "effective_month", "available_at": "publication_time"},
                 point_in_time=True, asset_types=frozenset({"macro"})),
        _dataset("derived.technical_indicator",
                 _combine(("instrument_id", "asset_type", "trading_date", "effective_time", "available_at", "source", "quality", "metadata"), ("indicator", "value", "parameter_hash")),
                 ("instrument_id", "trading_date", "indicator", "value"),
                 _combine(_INSTRUMENT_FILTERS, ("indicator", "fields")),
                 ("instrument_id", "trading_date", "indicator", "parameter_hash"),
                 ("instrument_id", "trading_date", "indicator", "parameter_hash"),
                 {"trading_date": "input_session_date", "available_at": "input_as_of"},
                 point_in_time=True, asset_types=equity_index_fund, status="derived"),
    )


__all__ = [
    "DataCatalog",
    "DatasetDefinition",
    "DatasetStatus",
    "FieldDefinition",
    "default_catalog",
]
