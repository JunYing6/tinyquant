from __future__ import annotations

from tinyquant_cli.repl import classify_line, completion_candidates


def test_repl_normalizes_slash_and_bare_commands() -> None:
    assert classify_line("/doctor") == ("command", ["doctor"])
    assert classify_line("doctor") == ("command", ["doctor"])
    assert classify_line("exit") == ("exit", [])
    assert classify_line("   ") == ("empty", [])


def test_completion_lists_command_alias_and_flag() -> None:
    candidates = completion_candidates("bt run --")

    assert "--start" in candidates
    assert "--end" in candidates
