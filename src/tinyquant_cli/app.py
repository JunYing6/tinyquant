from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path


def _ensure_runtime_precedence() -> None:
    runtime_root = str(Path(__file__).resolve().parents[1])
    while runtime_root in sys.path:
        sys.path.remove(runtime_root)
    sys.path.insert(0, runtime_root)


_ensure_runtime_precedence()


def _cli_dependencies_available() -> bool:
    try:
        import prompt_toolkit  # noqa: F401
        import rich  # noqa: F401
    except ImportError:
        return False
    return True


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not _cli_dependencies_available():
        print('tinyquant CLI requires: pip install "tinyquant[cli]"', file=sys.stderr)
        return 2

    from rich.console import Console

    from tinyquant_cli.commands import build_registry
    from tinyquant_cli.registry import ParserError
    from tinyquant_cli.repl import run_repl
    from tinyquant_cli.runtime import SessionState

    console = Console()
    state = SessionState()
    registry = build_registry(console, state)
    if not args:
        return run_repl(console, state, registry)
    command = registry.resolve(args[0])
    if command is None:
        suggestion = registry.suggest(args[0])
        message = f"unknown command: {args[0]}"
        if suggestion:
            message += f"; did you mean: {suggestion}"
        print(message, file=sys.stderr)
        return 2
    try:
        return registry.dispatch(registry.build_parser("tq").parse_args(args))
    except ParserError as error:
        print(f"Invalid command arguments: {error}", file=sys.stderr)
        return 2
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else 2
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
