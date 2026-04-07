"""Automation runner utilities for RabAI AutoClick.

Provides:
- Automation runner
- Step execution
- Error handling
"""

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class RunnerStatus(Enum):
    """Runner status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class StepResult:
    """Result of a step execution."""
    step_name: str
    success: bool
    duration: float = 0
    error: Optional[str] = None
    screenshot: Optional[str] = None


@dataclass
class RunnerResult:
    """Result of automation run."""
    success: bool
    status: RunnerStatus
    step_results: List[StepResult] = field(default_factory=list)
    total_duration: float = 0
    error: Optional[str] = None


class Step:
    """A single automation step."""

    def __init__(
        self,
        name: str,
        action: Callable[[], Any],
        condition: Optional[Callable[[], bool]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        retry: int = 0,
        timeout: float = 0,
    ) -> None:
        """Initialize step.

        Args:
            name: Step name.
            action: Action to execute.
            condition: Optional condition to check before execution.
            on_error: Optional error handler.
            retry: Number of retries on failure.
            timeout: Timeout in seconds.
        """
        self.name = name
        self.action = action
        self.condition = condition
        self.on_error = on_error
        self.retry = retry
        self.timeout = timeout


class AutomationRunner:
    """Run automation workflows."""

    def __init__(self) -> None:
        """Initialize runner."""
        self._status = RunnerStatus.IDLE
        self._steps: List[Step] = []
        self._current_step = 0
        self._results: List[StepResult] = []
        self._pause_event = threading.Event()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def add_step(self, step: Step) -> "AutomationRunner":
        """Add step to runner.

        Args:
            step: Step to add.

        Returns:
            Self for chaining.
        """
        self._steps.append(step)
        return self

    def step(
        self,
        name: str,
        action: Callable[[], Any],
        **kwargs,
    ) -> "AutomationRunner":
        """Add step using fluent interface.

        Args:
            name: Step name.
            action: Action to execute.
            **kwargs: Additional step options.

        Returns:
            Self for chaining.
        """
        self._steps.append(Step(name=name, action=action, **kwargs))
        return self

    def run(self) -> RunnerResult:
        """Run automation.

        Returns:
            Runner result.
        """
        with self._lock:
            self._status = RunnerStatus.RUNNING
            self._results.clear()
            self._current_step = 0
            self._stop_event.clear()
            self._pause_event.clear()

        start_time = time.time()
        all_success = True

        try:
            while self._current_step < len(self._steps):
                if self._stop_event.is_set():
                    self._status = RunnerStatus.STOPPED
                    break

                self._pause_event.wait()

                step = self._steps[self._current_step]
                result = self._execute_step(step)
                self._results.append(result)

                if not result.success:
                    all_success = False
                    if step.on_error:
                        try:
                            step.on_error(Exception(result.error))
                        except Exception:
                            pass
                    if step.retry == 0:
                        break

                self._current_step += 1

            if self._status == RunnerStatus.RUNNING:
                self._status = RunnerStatus.IDLE

        except Exception as e:
            all_success = False
            self._status = RunnerStatus.STOPPED

        duration = time.time() - start_time

        return RunnerResult(
            success=all_success,
            status=self._status,
            step_results=self._results,
            total_duration=duration,
        )

    def _execute_step(self, step: Step) -> StepResult:
        """Execute a single step.

        Args:
            step: Step to execute.

        Returns:
            Step result.
        """
        start_time = time.time()

        # Check condition
        if step.condition:
            try:
                if not step.condition():
                    return StepResult(
                        step_name=step.name,
                        success=True,
                        duration=time.time() - start_time,
                    )
            except Exception as e:
                return StepResult(
                    step_name=step.name,
                    success=False,
                    duration=time.time() - start_time,
                    error=f"Condition failed: {e}",
                )

        # Execute with retry
        last_error = None
        for attempt in range(step.retry + 1):
            try:
                if step.timeout > 0:
                    # Would need threading for timeout
                    pass
                step.action()
                return StepResult(
                    step_name=step.name,
                    success=True,
                    duration=time.time() - start_time,
                )
            except Exception as e:
                last_error = e
                if attempt < step.retry:
                    time.sleep(0.1 * (attempt + 1))

        return StepResult(
            step_name=step.name,
            success=False,
            duration=time.time() - start_time,
            error=str(last_error),
        )

    def pause(self) -> None:
        """Pause execution."""
        with self._lock:
            if self._status == RunnerStatus.RUNNING:
                self._status = RunnerStatus.PAUSED
                self._pause_event.clear()

    def resume(self) -> None:
        """Resume execution."""
        with self._lock:
            if self._status == RunnerStatus.PAUSED:
                self._status = RunnerStatus.RUNNING
                self._pause_event.set()

    def stop(self) -> None:
        """Stop execution."""
        with self._lock:
            self._status = RunnerStatus.STOPPED
            self._stop_event.set()
            self._pause_event.set()

    @property
    def status(self) -> RunnerStatus:
        """Get runner status."""
        return self._status

    @property
    def current_step(self) -> int:
        """Get current step index."""
        return self._current_step

    def get_results(self) -> List[StepResult]:
        """Get step results so far."""
        return self._results.copy()


class StepBuilder:
    """Build steps for automation."""

    @staticmethod
    def create(name: str, action: Callable[[], Any]) -> Step:
        """Create a step.

        Args:
            name: Step name.
            action: Action function.

        Returns:
            Created step.
        """
        return Step(name=name, action=action)

    @staticmethod
    def click(x: int, y: int, name: Optional[str] = None) -> Step:
        """Create a click step.

        Args:
            x: X coordinate.
            y: Y coordinate.
            name: Optional step name.

        Returns:
            Click step.
        """
        def action():
            from utils.mouse import MouseSimulator
            MouseSimulator.click(x, y)

        return Step(
            name=name or f"Click at ({x}, {y})",
            action=action,
        )

    @staticmethod
    def type_text(text: str, name: Optional[str] = None) -> Step:
        """Create a type text step.

        Args:
            text: Text to type.
            name: Optional step name.

        Returns:
            Type text step.
        """
        def action():
            from utils.clipboard import Clipboard
            Clipboard.set_text(text)

        return Step(
            name=name or f"Type: {text[:20]}",
            action=action,
        )

    @staticmethod
    def wait(duration: float, name: Optional[str] = None) -> Step:
        """Create a wait step.

        Args:
            duration: Wait duration in seconds.
            name: Optional step name.

        Returns:
            Wait step.
        """
        def action():
            time.sleep(duration)

        return Step(
            name=name or f"Wait {duration}s",
            action=action,
        )


class RunnerContext:
    """Context passed to automation steps."""

    def __init__(self) -> None:
        """Initialize context."""
        self._data: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def set(self, key: str, value: Any) -> None:
        """Set context value.

        Args:
            key: Key name.
            value: Value.
        """
        with self._lock:
            self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get context value.

        Args:
            key: Key name.
            default: Default if not found.

        Returns:
            Value or default.
        """
        with self._lock:
            return self._data.get(key, default)

    def clear(self) -> None:
        """Clear context."""
        with self._lock:
            self._data.clear()
