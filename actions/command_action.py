"""Command action module for RabAI AutoClick.

Provides command pattern implementation:
- Command: Base command interface
- ConcreteCommand: Specific command implementations
- CommandQueue: Queue for command execution
- CommandHistory: Track command execution history
- MacroCommand: Combine multiple commands
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
import uuid
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Command(ABC):
    """Abstract command interface."""

    @abstractmethod
    def execute(self) -> Any:
        """Execute the command."""
        pass

    @abstractmethod
    def undo(self) -> Any:
        """Undo the command."""
        pass

    @abstractmethod
    def redo(self) -> Any:
        """Redo the command."""
        pass


@dataclass
class CommandResult:
    """Result of command execution."""
    command_id: str
    success: bool
    result: Any = None
    error: str = ""
    execution_time: float = 0.0


class ConcreteCommand(Command):
    """Concrete command implementation."""

    def __init__(
        self,
        command_id: str,
        execute_fn: Callable[[], Any],
        undo_fn: Optional[Callable[[], Any]] = None,
        redo_fn: Optional[Callable[[], Any]] = None,
        label: str = "",
    ):
        self.command_id = command_id
        self._execute_fn = execute_fn
        self._undo_fn = undo_fn
        self._redo_fn = redo_fn
        self._label = label
        self._last_result: Any = None

    def execute(self) -> Any:
        """Execute the command."""
        try:
            self._last_result = self._execute_fn()
            return self._last_result
        except Exception as e:
            raise RuntimeError(f"Command execution failed: {e}") from e

    def undo(self) -> Any:
        """Undo the command."""
        if self._undo_fn is None:
            raise RuntimeError(f"Command {self.command_id} does not support undo")
        return self._undo_fn()

    def redo(self) -> Any:
        """Redo the command."""
        if self._redo_fn is not None:
            return self._redo_fn()
        return self.execute()


class MacroCommand(Command):
    """Command that groups multiple commands."""

    def __init__(self, command_id: str, label: str = ""):
        self.command_id = command_id
        self._label = label
        self._commands: List[Command] = []
        self._executed_results: List[Any] = []

    def add_command(self, command: Command) -> None:
        """Add a command to the macro."""
        self._commands.append(command)

    def remove_command(self, command: Command) -> bool:
        """Remove a command from the macro."""
        try:
            self._commands.remove(command)
            return True
        except ValueError:
            return False

    def execute(self) -> List[Any]:
        """Execute all commands in sequence."""
        self._executed_results = []
        for cmd in self._commands:
            result = cmd.execute()
            self._executed_results.append(result)
        return self._executed_results

    def undo(self) -> List[Any]:
        """Undo all commands in reverse order."""
        results = []
        for cmd in reversed(self._commands):
            result = cmd.undo()
            results.append(result)
        return results

    def redo(self) -> List[Any]:
        """Redo all commands in order."""
        self._executed_results = []
        for cmd in self._commands:
            result = cmd.redo()
            self._executed_results.append(result)
        return self._executed_results


class ConditionalCommand(Command):
    """Command with conditional execution."""

    def __init__(
        self,
        command_id: str,
        condition_fn: Callable[[], bool],
        execute_fn: Callable[[], Any],
        undo_fn: Optional[Callable[[], Any]] = None,
        else_fn: Optional[Callable[[], Any]] = None,
    ):
        self.command_id = command_id
        self._condition_fn = condition_fn
        self._execute_fn = execute_fn
        self._undo_fn = undo_fn
        self._else_fn = else_fn
        self._executed_branch: Optional[str] = None
        self._last_result: Any = None

    def execute(self) -> Any:
        """Execute based on condition."""
        if self._condition_fn():
            self._executed_branch = "then"
            self._last_result = self._execute_fn()
        elif self._else_fn:
            self._executed_branch = "else"
            self._last_result = self._else_fn()
        else:
            self._executed_branch = "skipped"
            self._last_result = None
        return self._last_result

    def undo(self) -> Any:
        """Undo based on executed branch."""
        if self._undo_fn and self._executed_branch == "then":
            return self._undo_fn()
        return None

    def redo(self) -> Any:
        """Redo the command."""
        return self.execute()


class CommandQueue:
    """Queue for command execution."""

    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        self._queue: List[Command] = []
        self._history: List[CommandResult] = []

    def enqueue(self, command: Command) -> str:
        """Add command to queue."""
        self._queue.append(command)
        return command.command_id

    def enqueue_priority(self, command: Command) -> str:
        """Add command to front of queue."""
        self._queue.insert(0, command)
        return command.command_id

    def execute_next(self) -> Optional[CommandResult]:
        """Execute next command in queue."""
        if not self._queue:
            return None

        command = self._queue.pop(0)
        return self._execute_command(command)

    def execute_all(self) -> List[CommandResult]:
        """Execute all queued commands."""
        results = []
        while self._queue:
            result = self.execute_next()
            if result:
                results.append(result)
        return results

    def clear(self) -> int:
        """Clear the queue."""
        count = len(self._queue)
        self._queue.clear()
        return count

    def get_queue_size(self) -> int:
        """Get number of queued commands."""
        return len(self._queue)

    def _execute_command(self, command: Command) -> CommandResult:
        """Execute a single command."""
        import time
        start = time.time()

        try:
            result = command.execute()
            success = True
            error = ""
        except Exception as e:
            result = None
            success = False
            error = str(e)

        duration = time.time() - start
        cmd_result = CommandResult(
            command_id=command.command_id,
            success=success,
            result=result,
            error=error,
            execution_time=duration,
        )

        self._history.append(cmd_result)
        if len(self._history) > self.max_history:
            self._history.pop(0)

        return cmd_result


class CommandHistory:
    """Track command execution history."""

    def __init__(self, max_history: int = 500):
        self.max_history = max_history
        self._history: List[CommandResult] = []

    def add(self, result: CommandResult) -> None:
        """Add result to history."""
        self._history.append(result)
        if len(self._history) > self.max_history:
            self._history.pop(0)

    def get_all(self) -> List[CommandResult]:
        """Get all history."""
        return self._history.copy()

    def get_successful(self) -> List[CommandResult]:
        """Get successful commands."""
        return [r for r in self._history if r.success]

    def get_failed(self) -> List[CommandResult]:
        """Get failed commands."""
        return [r for r in self._history if not r.success]

    def get_by_id(self, command_id: str) -> Optional[CommandResult]:
        """Get command by ID."""
        for r in reversed(self._history):
            if r.command_id == command_id:
                return r
        return None

    def clear(self) -> None:
        """Clear history."""
        self._history.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics."""
        total = len(self._history)
        successful = sum(1 for r in self._history if r.success)
        failed = total - successful
        total_time = sum(r.execution_time for r in self._history)
        return {
            "total": total,
            "successful": successful,
            "failed": failed,
            "avg_time": total_time / total if total > 0 else 0,
        }


class CommandAction(BaseAction):
    """Command pattern action."""
    action_type = "command"
    display_name = "命令模式"
    description = "命令模式和队列执行"

    def __init__(self):
        super().__init__()
        self._queue = CommandQueue()
        self._history = CommandHistory()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "execute":
                return self._execute_command(params)
            elif operation == "enqueue":
                return self._enqueue_command(params)
            elif operation == "execute_next":
                return self._execute_next()
            elif operation == "execute_all":
                return self._execute_all()
            elif operation == "macro":
                return self._create_macro(params)
            elif operation == "conditional":
                return self._create_conditional(params)
            elif operation == "history":
                return self._get_history()
            elif operation == "stats":
                return self._get_stats()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Command error: {str(e)}")

    def _execute_command(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a command directly."""
        cmd_id = params.get("command_id", str(uuid.uuid4()))
        execute_fn = params.get("execute_fn")
        undo_fn = params.get("undo_fn")
        redo_fn = params.get("redo_fn")
        label = params.get("label", "")

        if not callable(execute_fn):
            return ActionResult(success=False, message="execute_fn is required")

        cmd = ConcreteCommand(
            command_id=cmd_id,
            execute_fn=execute_fn,
            undo_fn=undo_fn,
            redo_fn=redo_fn,
            label=label,
        )

        try:
            result = cmd.execute()
            cmd_result = CommandResult(command_id=cmd_id, success=True, result=result)
            self._history.add(cmd_result)
            return ActionResult(success=True, message="Command executed", data={"result": result})
        except Exception as e:
            cmd_result = CommandResult(command_id=cmd_id, success=False, error=str(e))
            self._history.add(cmd_result)
            return ActionResult(success=False, message=f"Command failed: {e}")

    def _enqueue_command(self, params: Dict[str, Any]) -> ActionResult:
        """Enqueue a command."""
        cmd_id = params.get("command_id", str(uuid.uuid4()))
        execute_fn = params.get("execute_fn")
        priority = params.get("priority", False)

        if not callable(execute_fn):
            return ActionResult(success=False, message="execute_fn is required")

        cmd = ConcreteCommand(command_id=cmd_id, execute_fn=execute_fn)

        if priority:
            self._queue.enqueue_priority(cmd)
        else:
            self._queue.enqueue(cmd)

        return ActionResult(
            success=True,
            message=f"Command enqueued (priority={priority})",
            data={"command_id": cmd_id, "queue_size": self._queue.get_queue_size()},
        )

    def _execute_next(self) -> ActionResult:
        """Execute next queued command."""
        result = self._queue.execute_next()
        if not result:
            return ActionResult(success=False, message="Queue is empty")

        self._history.add(result)
        return ActionResult(
            success=result.success,
            message="Command executed" if result.success else f"Command failed: {result.error}",
            data={
                "command_id": result.command_id,
                "result": result.result,
                "error": result.error,
                "execution_time": result.execution_time,
            },
        )

    def _execute_all(self) -> ActionResult:
        """Execute all queued commands."""
        results = self._queue.execute_all()
        for r in results:
            self._history.add(r)

        successful = sum(1 for r in results if r.success)
        return ActionResult(
            success=successful == len(results),
            message=f"Executed {successful}/{len(results)} commands",
            data={"results": [{"id": r.command_id, "success": r.success} for r in results]},
        )

    def _create_macro(self, params: Dict[str, Any]) -> ActionResult:
        """Create and optionally execute a macro command."""
        commands = params.get("commands", [])
        label = params.get("label", "Macro")
        execute_now = params.get("execute", False)

        macro_id = str(uuid.uuid4())
        macro = MacroCommand(command_id=macro_id, label=label)

        for cmd_data in commands:
            cmd = ConcreteCommand(
                command_id=cmd_data.get("id", str(uuid.uuid4())),
                execute_fn=cmd_data.get("execute_fn", lambda: None),
            )
            macro.add_command(cmd)

        if execute_now:
            results = macro.execute()
            return ActionResult(success=True, message=f"Macro executed: {label}", data={"results": results})

        return ActionResult(success=True, message=f"Macro created: {label}", data={"macro_id": macro_id})

    def _create_conditional(self, params: Dict[str, Any]) -> ActionResult:
        """Create a conditional command."""
        cmd_id = params.get("command_id", str(uuid.uuid4()))
        condition_fn = params.get("condition_fn")
        execute_fn = params.get("execute_fn")
        else_fn = params.get("else_fn")

        if not callable(condition_fn) or not callable(execute_fn):
            return ActionResult(success=False, message="condition_fn and execute_fn are required")

        cmd = ConditionalCommand(
            command_id=cmd_id,
            condition_fn=condition_fn,
            execute_fn=execute_fn,
            else_fn=else_fn,
        )

        try:
            result = cmd.execute()
            return ActionResult(success=True, message="Conditional executed", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Conditional failed: {e}")

    def _get_history(self) -> ActionResult:
        """Get command history."""
        return ActionResult(
            success=True,
            message=f"{len(self._history.get_all())} commands in history",
            data={"history": [{"id": r.command_id, "success": r.success, "time": r.execution_time} for r in self._history.get_all()]},
        )

    def _get_stats(self) -> ActionResult:
        """Get command statistics."""
        return ActionResult(
            success=True,
            message="Command statistics",
            data=self._history.get_stats(),
        )
