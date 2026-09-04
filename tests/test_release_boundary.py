from __future__ import annotations

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
