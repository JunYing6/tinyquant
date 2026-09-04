"""Interactive prompt-toolkit shell backed by the shared command registry."""

from __future__ import annotations

import argparse
import shlex
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.completion.base import CompleteEvent
from rich.console import Console

from tinyquant_cli.registry import Registry
from tinyquant_cli.render import render_error, render_welcome
from tinyquant_cli.runtime import SessionState


def classify_line(line: str) -> tuple[str, list[str]]:
    text = line.strip()
    if not text:
        return "empty", []
    if text.startswith("/"):
        text = text[1:].lstrip()
    try:
        tokens = shlex.split(text)
    except ValueError:
        return "command", [text]
    if not tokens:
        return "empty", []
    if tokens[0].lower() in {"exit", "quit"}:
        return "exit", []
    if tokens[0].lower() in {"help", "?"}:
        return "help", tokens[1:]
    return "command", tokens


def completion_candidates(line: str) -> list[str]:
    from tinyquant_cli.commands import build_registry

    registry = build_registry(Console(), SessionState())
    registry.build_parser("tq")
    text = line.lstrip()
    if text.startswith("/"):
        text = text[1:].lstrip()
    try:
        tokens = shlex.split(text)
    except ValueError:
        return []
    trailing_space = text.endswith(" ")
    current = "" if trailing_space or not tokens else tokens[-1]
    prior = tokens[:-1] if tokens and not trailing_space else tokens
    if not prior:
        root_candidates = [command.name for command in registry.commands()]
        root_candidates.extend(alias for command in registry.commands() for alias in command.aliases)
        return sorted(value for value in root_candidates if value.startswith(current))

    command = registry.resolve(prior[0])
    if command is None:
        return []
    parser = registry.parser_for(command.name)
    if parser is None:
        return []
    for token in prior[1:]:
        matched = False
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction) and token in action.choices:
                parser = action.choices[token]
                matched = True
                break
        if not matched:
            continue
    candidates: list[str] = []
    for action in parser._actions:
        candidates.extend(action.option_strings)
        choices = getattr(action, "choices", None)
        if choices:
            candidates.extend(str(choice) for choice in choices)
    return sorted(set(value for value in candidates if value.startswith(current)))


class _Completer(Completer):
    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterable[Completion]:
        line = document.text_before_cursor
        word = line.rsplit(" ", 1)[-1]
        for candidate in completion_candidates(line):
            yield Completion(candidate, start_position=-len(word))


def _toolbar(state: SessionState) -> str:
    parts = [f"Python {sys.version_info.major}.{sys.version_info.minor}", f"cwd {Path.cwd()}"]
    if state.last_final_equity is not None:
        parts.append(f"return {state.last_total_return:.2%}")
        parts.append(f"drawdown {state.last_max_drawdown:.2%}")
    return " | ".join(parts)


def run_repl(console: Console, state: SessionState, registry: Registry) -> int:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory

    render_welcome(console, {"python": sys.version.split()[0], "core": "ready", "cli": "ready"})
    session: Any = PromptSession(
        history=FileHistory(str(Path.home() / ".tinyquant_history")),
        completer=_Completer(),
        bottom_toolbar=lambda: _toolbar(state),
    )
    parser = registry.build_parser("tq")
    while True:
        try:
            line = session.prompt("tq> ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            console.print("Goodbye", style="cyan")
            return 0
        kind, tokens = classify_line(line)
        if kind == "empty":
            continue
        if kind == "exit":
            console.print("Goodbye", style="cyan")
            return 0
        if kind == "help":
            tokens = ["help", *tokens]
        command = registry.resolve(tokens[0]) if tokens else None
        if command is None:
            suggestion = registry.suggest(tokens[0]) if tokens else None
            message = f"Unknown command: {tokens[0] if tokens else ''}"
            if suggestion:
                message += f"; did you mean: {suggestion}"
            render_error(console, message)
            continue
        try:
            arguments = parser.parse_args(tokens)
            registry.dispatch(arguments)
        except SystemExit as error:
            if error.code not in (0, None):
                render_error(console, "Invalid command arguments")
        except KeyboardInterrupt:
            continue
        except Exception as error:
            render_error(console, f"{type(error).__name__}: {error}")
