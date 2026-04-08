"""
Command pattern and builder utilities.

Provides command object pattern, command queue,
and chain of responsibility implementations.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, TypeVar


T = TypeVar("T")


class Command(ABC, Generic[T]):
    """Base command interface."""

    @abstractmethod
    def execute(self) -> T:
        """Execute the command."""
        pass

    def undo(self) -> None:
        """Undo the command (optional)."""
        pass


class SimpleCommand(Command[T]):
    """Simple command wrapping a callable."""

    def __init__(self, execute_fn: Callable[[], T]):
        self._execute = execute_fn

    def execute(self) -> T:
        return self._execute()


class CommandWithUndo(Command[T]):
    """Command with undo support."""

    def __init__(
        self,
        execute_fn: Callable[[], T],
        undo_fn: Callable[[], None],
    ):
        self._execute = execute_fn
        self._undo = undo_fn

    def execute(self) -> T:
        return self._execute()

    def undo(self) -> None:
        self._undo()


class MacroCommand(Command[list[Any]]):
    """Command that executes multiple commands."""

    def __init__(self, commands: list[Command]):
        self.commands = commands
        self._results: list[Any] = []

    def execute(self) -> list[Any]:
        self._results = []
        for cmd in self.commands:
            self._results.append(cmd.execute())
        return self._results

    def undo(self) -> None:
        for cmd in reversed(self.commands):
            cmd.undo()


class CommandQueue:
    """Queue of commands for batch execution."""

    def __init__(self):
        self._queue: list[Command] = []
        self._lock = threading.Lock()

    def add(self, command: Command) -> None:
        with self._lock:
            self._queue.append(command)

    def execute_all(self) -> list[Any]:
        """Execute all queued commands in order."""
        results = []
        with self._lock:
            queue = list(self._queue)
            self._queue.clear()
        for cmd in queue:
            results.append(cmd.execute())
        return results

    def clear(self) -> None:
        with self._lock:
            self._queue.clear()

    @property
    def size(self) -> int:
        return len(self._queue)


class AsyncCommand(Command[T]):
    """Command that executes asynchronously."""

    def __init__(self, coro_fn: Callable[[], Any]):
        self._coro_fn = coro_fn

    def execute(self) -> Any:
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self._coro_fn())
        finally:
            loop.close()


class CommandHistory:
    """Command history with undo/redo support."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._undo_stack: list[Command] = []
        self._redo_stack: list[Command] = []
        self._lock = threading.Lock()

    def execute(self, command: Command) -> Any:
        """Execute command and add to history."""
        result = command.execute()
        with self._lock:
            self._undo_stack.append(command)
            self._redo_stack.clear()
            if len(self._undo_stack) > self.max_size:
                self._undo_stack.pop(0)
        return result

    def undo(self) -> bool:
        """Undo last command."""
        with self._lock:
            if not self._undo_stack:
                return False
            command = self._undo_stack.pop()
            self._redo_stack.append(command)
        try:
            command.undo()
            return True
        except NotImplementedError:
            return False

    def redo(self) -> bool:
        """Redo last undone command."""
        with self._lock:
            if not self._redo_stack:
                return False
            command = self._redo_stack.pop()
            self._undo_stack.append(command)
        try:
            command.execute()
            return True
        except Exception:
            return False

    @property
    def can_undo(self) -> bool:
        return bool(self._undo_stack)

    @property
    def can_redo(self) -> bool:
        return bool(self._redo_stack)


class ChainOfResponsibility(ABC):
    """Base handler in chain of responsibility pattern."""

    def __init__(self):
        self._next_handler: ChainOfResponsibility | None = None

    def set_next(self, handler: "ChainOfResponsibility") -> "ChainOfResponsibility":
        self._next_handler = handler
        return handler

    def handle(self, request: Any) -> Any:
        if self._next_handler:
            return self._next_handler.handle(request)
        return None


def create_command(func: Callable[[], T]) -> Command[T]:
    """Factory to create command from callable."""
    return SimpleCommand(func)
