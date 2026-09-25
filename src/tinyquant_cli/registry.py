"""面向单次执行与交互式 tinyquant CLI 的共享命令注册表。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from difflib import get_close_matches
from typing import Callable, NoReturn


ArgConfigurer = Callable[[argparse.ArgumentParser], None]
Handler = Callable[[argparse.Namespace], int]


class ParserError(ValueError):
    """命令行语法错误时抛出，但不会终止进程。"""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ParserError(message)


@dataclass(frozen=True)
class Command:
    name: str
    category: str
    description: str
    aliases: list[str] = field(default_factory=list)
    configure: ArgConfigurer | None = None
    handler: Handler | None = None


class Registry:
    def __init__(self) -> None:
        self._commands: dict[str, Command] = {}
        self._parsers: dict[str, argparse.ArgumentParser] = {}

    def register(self, command: Command) -> None:
        if command.name in self._commands:
            raise ValueError(f"重复的命令: {command.name}")
        occupied = set(self._commands)
        occupied.update(alias for registered in self._commands.values() for alias in registered.aliases)
        if command.name in occupied or any(alias in occupied for alias in command.aliases):
            raise ValueError(f"命令别名冲突: {command.aliases}")
        self._commands[command.name] = command

    def commands(self) -> list[Command]:
        return [self._commands[name] for name in sorted(self._commands)]

    def resolve(self, name: str) -> Command | None:
        if name in self._commands:
            return self._commands[name]
        return next(
            (command for command in self._commands.values() if name in command.aliases),
            None,
        )

    def suggest(self, name: str) -> str | None:
        candidates = [command.name for command in self._commands.values()]
        candidates.extend(alias for command in self._commands.values() for alias in command.aliases)
        matches = get_close_matches(name, candidates, n=1, cutoff=0.5)
        return matches[0] if matches else None

    def build_parser(self, prog: str = "tq") -> argparse.ArgumentParser:
        parser = _ArgumentParser(prog=prog, description="tinyquant 命令行")
        subparsers = parser.add_subparsers(
            dest="command", required=True, parser_class=_ArgumentParser
        )
        for command in self.commands():
            child = subparsers.add_parser(
                command.name,
                aliases=command.aliases,
                help=command.description,
            )
            child.set_defaults(_command=command)
            if command.configure is not None:
                command.configure(child)
            self._parsers[command.name] = child
        return parser

    def parser_for(self, name: str) -> argparse.ArgumentParser | None:
        command = self.resolve(name)
        return self._parsers.get(command.name) if command else None

    def dispatch(self, args: argparse.Namespace) -> int:
        command: Command = args._command
        if command.handler is None:
            raise RuntimeError(f"命令没有处理器: {command.name}")
        return int(command.handler(args))


__all__ = ["ArgConfigurer", "Command", "Handler", "ParserError", "Registry"]
