"""Command Handler Action Module.

Command pattern implementation with undo/redo support.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Generic, TypeVar
import uuid

T = TypeVar("T")


class CommandStatus(Enum):
    """Command execution status."""
    PENDING = "pending"
    EXECUTED = "executed"
    FAILED = "failed"
    UNDONE = "undone"


@dataclass
class CommandResult:
    """Result of command execution."""
    success: bool
    output: Any = None
    error: str | None = None
    execution_time: float = 0.0


class Command(ABC, Generic[T]):
    """Abstract command interface."""

    def __init__(self, command_id: str | None = None) -> None:
        self.command_id = command_id or str(uuid.uuid4())
        self.status = CommandStatus.PENDING
        self.created_at = datetime.now(timezone.utc)

    @abstractmethod
    async def execute(self) -> T:
        """Execute the command."""
        pass

    @abstractmethod
    async def undo(self) -> T:
        """Undo the command."""
        pass

    @abstractmethod
    async def redo(self) -> T:
        """Redo the command (typically same as execute)."""
        pass


class SyncCommand(Command[T]):
    """Command wrapper for synchronous functions."""

    def __init__(
        self,
        execute_fn: Callable[[], T],
        undo_fn: Callable[[], T] | None = None,
        command_id: str | None = None
    ) -> None:
        super().__init__(command_id)
        self.execute_fn = execute_fn
        self.undo_fn = undo_fn or execute_fn

    async def execute(self) -> T:
        return self.execute_fn()

    async def undo(self) -> T:
        return self.undo_fn()

    async def redo(self) -> T:
        return await self.execute()


class AsyncCommand(Command[T]):
    """Command wrapper for async functions."""

    def __init__(
        self,
        execute_fn: Callable[[], Any],
        undo_fn: Callable[[], Any] | None = None,
        command_id: str | None = None
    ) -> None:
        super().__init__(command_id)
        self.execute_fn = execute_fn
        self.undo_fn = undo_fn or execute_fn

    async def execute(self) -> T:
        result = self.execute_fn()
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def undo(self) -> T:
        result = self.undo_fn()
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def redo(self) -> T:
        return await self.execute()


class MacroCommand(Command[list[T]]):
    """Composite command that executes multiple commands."""

    def __init__(self, commands: list[Command], command_id: str | None = None) -> None:
        super().__init__(command_id)
        self.commands = commands

    async def execute(self) -> list[T]:
        results = []
        for cmd in self.commands:
            result = await cmd.execute()
            results.append(result)
        return results

    async def undo(self) -> list[T]:
        results = []
        for cmd in reversed(self.commands):
            result = await cmd.undo()
            results.append(result)
        return results

    async def redo(self) -> list[T]:
        return await self.execute()


class CommandHistory:
    """Command history with undo/redo support."""

    def __init__(self, max_history: int = 100) -> None:
        self.max_history = max_history
        self._undo_stack: deque[Command] = deque(maxlen=max_history)
        self._redo_stack: deque[Command] = deque(maxlen=max_history)
        self._lock = asyncio.Lock()

    async def execute(self, command: Command) -> CommandResult:
        """Execute a command and add to history."""
        import time
        start = time.monotonic()
        try:
            result = await command.execute()
            command.status = CommandStatus.EXECUTED
            async with self._lock:
                self._undo_stack.append(command)
                self._redo_stack.clear()
            return CommandResult(
                success=True,
                output=result,
                execution_time=time.monotonic() - start
            )
        except Exception as e:
            command.status = CommandStatus.FAILED
            return CommandResult(
                success=False,
                error=str(e),
                execution_time=time.monotonic() - start
            )

    async def undo(self) -> CommandResult | None:
        """Undo the last command."""
        import time
        start = time.monotonic()
        async with self._lock:
            if not self._undo_stack:
                return None
            command = self._undo_stack.pop()
        try:
            result = await command.undo()
            command.status = CommandStatus.UNDONE
            async with self._lock:
                self._redo_stack.append(command)
            return CommandResult(
                success=True,
                output=result,
                execution_time=time.monotonic() - start
            )
        except Exception as e:
            return CommandResult(
                success=False,
                error=str(e),
                execution_time=time.monotonic() - start
            )

    async def redo(self) -> CommandResult | None:
        """Redo the last undone command."""
        import time
        start = time.monotonic()
        async with self._lock:
            if not self._redo_stack:
                return None
            command = self._redo_stack.pop()
        try:
            result = await command.redo()
            command.status = CommandStatus.EXECUTED
            async with self._lock:
                self._undo_stack.append(command)
            return CommandResult(
                success=True,
                output=result,
                execution_time=time.monotonic() - start
            )
        except Exception as e:
            return CommandResult(
                success=False,
                error=str(e),
                execution_time=time.monotonic() - start
            )

    def can_undo(self) -> bool:
        return len(self._undo_stack) > 0

    def can_redo(self) -> bool:
        return len(self._redo_stack) > 0
