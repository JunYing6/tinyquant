from __future__ import annotations

import sys
from pathlib import Path

from tinyquant_cli import app


def test_one_shot_help_uses_registered_handler(capsys) -> None:
    assert app.main(["help"]) == 0
    assert "tinyquant" in capsys.readouterr().out


def test_one_shot_unknown_command_returns_nonzero(capsys) -> None:
    assert app.main(["exampls"]) == 2
    assert "examples" in capsys.readouterr().err


def test_one_shot_invalid_arguments_returns_nonzero(capsys) -> None:
    assert app.main(["backtest", "run", "tinyquant_cli.demos:build_demo_backtest"]) == 2

    assert "Invalid command arguments" in capsys.readouterr().err


def test_group_command_without_subcommand_shows_help(capsys) -> None:
    assert app.main(["examples"]) == 0
    assert "Examples" in capsys.readouterr().out


def test_group_command_backtest_without_subcommand_shows_help(capsys) -> None:
    assert app.main(["backtest"]) == 0
    assert "Run" in capsys.readouterr().out


def test_cli_runtime_root_takes_import_precedence(monkeypatch, tmp_path) -> None:
    app_file = tmp_path / "tinyquant_cli" / "app.py"
    app_file.parent.mkdir()
    app_file.touch()
    monkeypatch.setattr(app, "__file__", str(app_file))
    monkeypatch.setattr(sys, "path", ["old-project-src", str(tmp_path)])

    app._ensure_runtime_precedence()

    assert sys.path[0] == str(tmp_path)
    assert sys.path.count(str(tmp_path)) == 1
