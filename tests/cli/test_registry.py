from __future__ import annotations

import argparse

import pytest

from tinyquant_cli.registry import Command, Registry


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--value", default="default")


def _handler(args: argparse.Namespace) -> int:
    return 7 if args.value == "run" else 3


def test_registry_resolves_alias_builds_parser_and_dispatches() -> None:
    registry = Registry()
    registry.register(Command("doctor", "Diagnostics", "Check runtime", ["diag"], _configure, _handler))

    parser = registry.build_parser("tq")
    args = parser.parse_args(["diag", "--value", "run"])

    command = registry.resolve("diag")
    assert command is not None
    assert command.name == "doctor"
    assert registry.dispatch(args) == 7


def test_registry_suggests_close_command() -> None:
    registry = Registry()
    registry.register(Command("examples", "Examples", "Run bundled examples", [], None, lambda args: 0))

    assert registry.suggest("exampls") == "examples"


def test_registry_rejects_duplicate_alias() -> None:
    registry = Registry()
    registry.register(Command("help", "Run", "Show help", ["?"], None, lambda args: 0))

    with pytest.raises(ValueError, match="alias"):
        registry.register(Command("doctor", "Diagnostics", "Check runtime", ["?"], None, lambda args: 0))
