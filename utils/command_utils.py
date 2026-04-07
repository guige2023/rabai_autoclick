"""
Command Pattern & Undo/Redo System

Provides command pattern implementation with full undo/redo support,
macro recording, and command history management.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class CommandStatus(Enum):
    """Execution status of a command."""
    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    UNDONE = auto()


@dataclass
class CommandResult(Generic[T]):
    """Result of command execution."""
    success: bool
    data: T | None = None
    error: str | None = None
    execution_time: float = 0.0
    timestamp: float = field(default_factory=time.time)

    @property
    def failed(self) -> bool:
        return not self.success


class Command(ABC, Generic[T]):
    """Abstract base class for all commands."""

    def __init__(self, description: str = ""):
        self.description = description or self.__class__.__name__
        self._status = CommandStatus.PENDING
        self._execution_time: float = 0.0
        self._error: str | None = None
        self._result: T | None = None

    @property
    def status(self) -> CommandStatus:
        return self._status

    @property
    def can_undo(self) -> bool:
        """Most commands can be undone except special ones."""
        return True

    @abstractmethod
    def execute(self) -> CommandResult[T]:
        """Execute the command. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def undo(self) -> CommandResult[None]:
        """Undo the command. Must be implemented by subclasses."""
        pass

    def redo(self) -> CommandResult[T]:
        """Redo the command - re-executes the command."""
        return self.execute()

    def __repr__(self) -> str:
        return f"<Command: {self.description} [{self._status.name}]>"


class MacroCommand(Command[None]):
    """A command that contains multiple sub-commands."""

    def __init__(self, commands: Sequence[Command] | None = None, description: str = ""):
        super().__init__(description or f"Macro with {len(commands or [])} commands")
        self.commands: list[Command] = list(commands) if commands else []
        self._executed_commands: list[Command] = []
        self._undone_commands: list[Command] = []

    def add(self, command: Command) -> None:
        """Add a command to the macro."""
        self.commands.append(command)

    def remove(self, command: Command) -> bool:
        """Remove a command from the macro."""
        try:
            self.commands.remove(command)
            return True
        except ValueError:
            return False

    def execute(self) -> CommandResult[None]:
        """Execute all commands in sequence."""
        self._status = CommandStatus.EXECUTING
        start = time.time()
        self._executed_commands.clear()
        self._undone_commands.clear()

        for cmd in self.commands:
            result = cmd.execute()
            if result.failed:
                self._status = CommandStatus.FAILED
                self._error = f"Command '{cmd.description}' failed: {result.error}"
                return CommandResult(
                    success=False,
                    error=self._error,
                    execution_time=time.time() - start,
                )
            self._executed_commands.append(cmd)

        self._status = CommandStatus.COMPLETED
        self._execution_time = time.time() - start
        return CommandResult(success=True, execution_time=self._execution_time)

    def undo(self) -> CommandResult[None]:
        """Undo all executed commands in reverse order."""
        self._status = CommandStatus.EXECUTING
        start = time.time()

        for cmd in reversed(self._executed_commands):
            if not cmd.can_undo:
                continue
            result = cmd.undo()
            if result.failed:
                self._status = CommandStatus.FAILED
                self._error = f"Undo failed for '{cmd.description}': {result.error}"
                return CommandResult(success=False, error=self._error)
            self._undone_commands.append(cmd)

        self._status = CommandStatus.UNDONE
        return CommandResult(success=True, execution_time=time.time() - start)


class UndoableCommand(Command[T]):
    """Base class for commands that support undo via stored previous state."""

    def __init__(self, description: str = ""):
        super().__init__(description)
        self._previous_state: Any = None

    @abstractmethod
    def _do_execute(self) -> T:
        """Actual execution logic. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _do_undo(self) -> None:
        """Actual undo logic. Must be implemented by subclasses."""
        pass

    def execute(self) -> CommandResult[T]:
        """Execute and capture state for undo."""
        self._status = CommandStatus.EXECUTING
        start = time.time()

        try:
            self._previous_state = self._capture_state()
            self._result = self._do_execute()
            self._status = CommandStatus.COMPLETED
            return CommandResult(
                success=True,
                data=self._result,
                execution_time=time.time() - start,
            )
        except Exception as e:
            self._status = CommandStatus.FAILED
            self._error = str(e)
            return CommandResult(success=False, error=self._error, execution_time=time.time() - start)

    def undo(self) -> CommandResult[None]:
        """Restore previous state."""
        if not self.can_undo or self._previous_state is None:
            return CommandResult(success=False, error="Cannot undo")

        self._status = CommandStatus.EXECUTING
        start = time.time()

        try:
            self._do_undo()
            self._status = CommandStatus.UNDONE
            return CommandResult(success=True, execution_time=time.time() - start)
        except Exception as e:
            self._status = CommandStatus.FAILED
            return CommandResult(success=False, error=str(e), execution_time=time.time() - start)

    def _capture_state(self) -> Any:
        """Override to capture state before execution."""
        return None


class NoOpCommand(Command[None]):
    """A command that does nothing."""

    def __init__(self):
        super().__init__("NoOp")

    def execute(self) -> CommandResult[None]:
        self._status = CommandStatus.COMPLETED
        return CommandResult(success=True)

    def undo(self) -> CommandResult[None]:
        return CommandResult(success=True)


@dataclass
class CommandHistoryEntry:
    """Single entry in command history."""
    command: Command
    timestamp: float = field(default_factory=time.time)
    index: int = 0


class CommandHistory:
    """Manages command history with undo/redo support."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._history: list[CommandHistoryEntry] = []
        self._current_index: int = -1
        self._undone_stack: list[CommandHistoryEntry] = []
        self._on_execute_callbacks: list[Callable[[Command], None]] = []
        self._on_undo_callbacks: list[Callable[[Command], None]] = []
        self._on_redo_callbacks: list[Callable[[Command], None]] = []

    @property
    def can_undo(self) -> bool:
        return self._current_index >= 0

    @property
    def can_redo(self) -> bool:
        return len(self._undone_stack) > 0

    @property
    def current_index(self) -> int:
        return self._current_index

    @property
    def total_commands(self) -> int:
        return len(self._history)

    def execute(self, command: Command) -> CommandResult:
        """Execute a command and add it to history."""
        result = command.execute()

        if result.success:
            # Clear any redo history when new command is executed
            self._undone_stack.clear()

            entry = CommandHistoryEntry(command=command, index=len(self._history))
            self._history.append(entry)
            self._current_index = len(self._history) - 1

            # Trim if exceeds max_size
            if len(self._history) > self.max_size:
                excess = len(self._history) - self.max_size
                self._history = self._history[excess:]
                self._current_index = max(0, self._current_index - excess)

            for cb in self._on_execute_callbacks:
                cb(command)

        return result

    def undo(self) -> CommandResult[None] | None:
        """Undo the last command."""
        if not self.can_undo:
            return None

        entry = self._history[self._current_index]
        result = entry.command.undo()

        if result.success:
            self._current_index -= 1
            self._undone_stack.append(entry)
            for cb in self._on_undo_callbacks:
                cb(entry.command)

        return result

    def redo(self) -> CommandResult | None:
        """Redo the last undone command."""
        if not self.can_redo:
            return None

        entry = self._undone_stack.pop()
        result = entry.command.redo()

        if result.success:
            self._current_index += 1
            for cb in self._on_redo_callbacks:
                cb(entry.command)

        return result

    def get_commands(self) -> list[Command]:
        """Get all commands in history order."""
        return [e.command for e in self._history]

    def get_undone_commands(self) -> list[Command]:
        """Get commands in the redo stack."""
        return [e.command for e in self._undone_stack]

    def on_execute(self, callback: Callable[[Command], None]) -> None:
        """Register callback for command execution."""
        self._on_execute_callbacks.append(callback)

    def on_undo(self, callback: Callable[[Command], None]) -> None:
        """Register callback for undo."""
        self._on_undo_callbacks.append(callback)

    def on_redo(self, callback: Callable[[Command], None]) -> None:
        """Register callback for redo."""
        self._on_redo_callbacks.append(callback)

    def clear(self) -> None:
        """Clear all history."""
        self._history.clear()
        self._undone_stack.clear()
        self._current_index = -1


class RecordingCommandManager:
    """Manager for recording and playback of command sequences."""

    def __init__(self, history: CommandHistory | None = None):
        self.history = history or CommandHistory()
        self._is_recording = False
        self._recording: list[Command] = []
        self._recorded_macros: dict[str, MacroCommand] = {}

    @property
    def is_recording(self) -> bool:
        return self._is_recording

    def start_recording(self) -> None:
        """Start recording commands."""
        self._is_recording = True
        self._recording.clear()

    def stop_recording(self, macro_name: str = "") -> MacroCommand:
        """Stop recording and return a macro command."""
        self._is_recording = False
        macro = MacroCommand(
            commands=self._recording,
            description=macro_name or f"Recorded Macro {len(self._recorded_macros) + 1}",
        )
        name = macro_name or f"macro_{len(self._recorded_macros) + 1}"
        self._recorded_macros[name] = macro
        return macro

    def cancel_recording(self) -> None:
        """Cancel current recording."""
        self._is_recording = False
        self._recording.clear()

    def execute_recording(self, macro_name: str) -> CommandResult[None] | None:
        """Execute a previously recorded macro."""
        macro = self._recorded_macros.get(macro_name)
        if not macro:
            return None
        return self.history.execute(macro)

    def get_recorded_macro(self, name: str) -> MacroCommand | None:
        """Get a recorded macro by name."""
        return self._recorded_macros.get(name)

    def list_recorded_macros(self) -> list[str]:
        """List all recorded macro names."""
        return list(self._recorded_macros.keys())

    def delete_macro(self, name: str) -> bool:
        """Delete a recorded macro."""
        if name in self._recorded_macros:
            del self._recorded_macros[name]
            return True
        return False
