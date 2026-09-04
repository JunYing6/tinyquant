from __future__ import annotations

import tomllib
from pathlib import Path

from tinyquant_cli import app


ROOT = Path(__file__).parents[2]


def test_pyproject_declares_optional_cli_extra_and_console_script() -> None:
    with (ROOT / "pyproject.toml").open("rb") as file:
        document = tomllib.load(file)
    project = document["project"]

    assert project["optional-dependencies"]["cli"] == [
        "prompt_toolkit>=3.0",
        "rich>=13.0",
    ]
    assert project["scripts"]["tq"] == "tinyquant_cli.app:main"
    assert "tinyquant_cli*" in document["tool"]["setuptools"]["packages"]["find"]["include"]


def test_main_explains_missing_cli_extra(monkeypatch, capsys) -> None:
    monkeypatch.setattr(app, "_cli_dependencies_available", lambda: False)

    code = app.main(["help"])

    assert code == 2
    assert 'pip install "tinyquant[cli]"' in capsys.readouterr().err
