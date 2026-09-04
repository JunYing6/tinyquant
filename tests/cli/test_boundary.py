from __future__ import annotations

import importlib
import sys


def test_cli_import_uses_unique_tinyquant_namespace() -> None:
    module = importlib.import_module("tinyquant_cli.app")

    assert callable(module.main)
    assert "tinyquant_cli" in str(module.__file__)


def test_cli_import_does_not_load_excluded_runtime_modules() -> None:
    importlib.import_module("tinyquant_cli.app")

    forbidden = {"duck" + "db", "tushare", "gm", "gui", "registry"}
    assert not forbidden.intersection(sys.modules)
