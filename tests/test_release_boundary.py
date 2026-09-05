from __future__ import annotations

import importlib.util
from pathlib import Path
import tomllib


def test_package_metadata_is_tinyquant_1_1() -> None:
    root = Path(__file__).parents[1]
    with (root / "pyproject.toml").open("rb") as file:
        metadata = tomllib.load(file)["project"]

    assert metadata["name"] == "tinyquant"
    assert metadata["version"] == "1.1.0"


def test_release_contains_only_runtime_data_paths() -> None:
    root = Path(__file__).parents[1]
    source_files = [path for path in (root / "src").rglob("*.py")]

    assert source_files
    assert not list(root.rglob("*." + "duck" + "db"))
    assert not list(root.rglob("*.parquet"))
    assert not any(path.name.startswith("test_") for path in source_files)


def test_data_contract_package_present_and_legacy_request_protocol_removed() -> None:
    assert importlib.util.find_spec("tools.data") is not None
    assert importlib.util.find_spec("tools.data_getter") is None


def test_core_sources_reference_no_vendor_or_storage_backends() -> None:
    root = Path(__file__).parents[1]
    banned_tokens = ("akshare", "duckdb", "tushare")

    source_files = sorted((root / "src").rglob("*.py"))
    assert source_files
    for path in source_files:
        text = path.read_text(encoding="utf-8").lower()
        for token in banned_tokens:
            assert token not in text, f"{path.name} must not reference {token!r}"


def test_release_does_not_ship_concrete_user_nodes() -> None:
    root = Path(__file__).parents[1]
    source = root / "src"

    assert not (source / "trading").exists()
    assert not (source / "trading_nodes_base" / "strategies" / "simple_strategies.py").exists()
    assert not (source / "trading_nodes_base" / "streams" / "multi_strategy.py").exists()
    assert not (source / "trading_nodes_base" / "minds" / "weighting.py").exists()
    assert not (source / "trading_nodes_base" / "methods" / "selector" / "fixed.py").exists()
    assert not (root / "examples" / "multi_strategy").exists()
