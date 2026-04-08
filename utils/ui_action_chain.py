"""UI action chain builder for complex automation workflows.

Provides a fluent builder API for chaining UI actions (click, type, wait, etc.)
into composable automation sequences.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar


class ActionType(Enum):
    """Types of UI actions."""
    CLICK = auto()
    RIGHT_CLICK = auto()
    DOUBLE_CLICK = auto()
    HOVER = auto()
    TYPE = auto()
    PRESS_KEY = auto()
    WAIT = auto()
    WAIT_FOR_ELEMENT = auto()
    WAIT_FOR_STATE = auto()
    SCROLL = auto()
    DRAG = auto()
    DROP = auto()
    CUSTOM = auto()


@dataclass
class ActionStep:
    """A single step in an action chain.

    Attributes:
        step_id: Unique identifier for this step.
        action_type: The type of action.
        target: Target selector or element reference.
        value: Action parameter (text, keys, coordinates, etc.).
        options: Additional action options.
        before_hook: Optional callable run before this step.
        after_hook: Optional callable run after this step.
        condition: Optional condition for conditional execution.
        retry_count: Number of retries on failure.
        timeout: Timeout in seconds for this step.
        description: Human-readable description of this step.
    """
    action_type: ActionType
    target: str = ""
    value: Any = None
    options: dict[str, Any] = field(default_factory=dict)
    before_hook: Optional[Callable[[], None]] = None
    after_hook: Optional[Callable[[], Any]] = None
    condition: Optional[Callable[[], bool]] = None
    retry_count: int = 3
    timeout: float = 10.0
    description: str = ""
    step_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def should_execute(self) -> bool:
        """Return True if the condition allows execution."""
        if self.condition is None:
            return True
        try:
            return self.condition()
        except Exception:
            return False

    def run_before_hook(self) -> None:
        """Run the before hook if defined."""
        if self.before_hook:
            self.before_hook()

    def run_after_hook(self) -> Any:
        """Run the after hook if defined."""
        if self.after_hook:
            return self.after_hook()
        return None


@dataclass
class ActionChainResult:
    """Result of executing an action chain or step.

    Attributes:
        success: Whether the action succeeded.
        step_id: ID of the step that was executed.
        result: Return value from the action.
        error: Error message if failed.
        duration: Execution time in seconds.
        timestamp: When execution completed.
    """
    success: bool
    step_id: str = ""
    result: Any = None
    error: str = ""
    duration: float = 0.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class ActionChain:
    """A named chain of action steps.

    Attributes:
        name: Human-readable name for this chain.
        steps: Ordered list of action steps.
        on_error: Error handling mode ('stop', 'skip', 'retry', 'continue').
        default_timeout: Default timeout for all steps.
        default_retry: Default retry count for all steps.
        metadata: Additional chain metadata.
    """
    name: str
    steps: list[ActionStep] = field(default_factory=list)
    on_error: str = "stop"
    default_timeout: float = 10.0
    default_retry: int = 3
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    def add_step(self, step: ActionStep) -> ActionStep:
        """Append a step and return it."""
        self.steps.append(step)
        return step

    @property
    def step_count(self) -> int:
        """Return the number of steps."""
        return len(self.steps)

    @property
    def is_empty(self) -> bool:
        """Return True if chain has no steps."""
        return len(self.steps) == 0


class UIActionChainBuilder:
    """Fluent builder for UI action chains.

    Provides a chainable API for building complex automation sequences.

    Usage:
        chain = (
            UIActionChainBuilder("login")
            .click("#username")
            .type("user@example.com")
            .click("#password")
            .type("password123")
            .click("#submit")
            .wait(1.0)
            .build()
        )
    """

    def __init__(self, name: str = "") -> None:
        """Initialize builder with optional chain name."""
        self._chain = ActionChain(name=name)

    def click(
        self,
        target: str,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a left-click action."""
        step = ActionStep(
            action_type=ActionType.CLICK,
            target=target,
            options=options,
            description=description or f"Click {target}",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def right_click(
        self,
        target: str,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a right-click action."""
        step = ActionStep(
            action_type=ActionType.RIGHT_CLICK,
            target=target,
            options=options,
            description=description or f"Right-click {target}",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def double_click(
        self,
        target: str,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a double-click action."""
        step = ActionStep(
            action_type=ActionType.DOUBLE_CLICK,
            target=target,
            options=options,
            description=description or f"Double-click {target}",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def hover(
        self,
        target: str,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a hover action."""
        step = ActionStep(
            action_type=ActionType.HOVER,
            target=target,
            options=options,
            description=description or f"Hover {target}",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def type(
        self,
        text: str,
        target: str = "",
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a type/keyboard input action."""
        step = ActionStep(
            action_type=ActionType.TYPE,
            target=target,
            value=text,
            options=options,
            description=description or f"Type '{text}'" + (f" in {target}" if target else ""),
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def press_key(
        self,
        key: str,
        target: str = "",
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a key press action."""
        step = ActionStep(
            action_type=ActionType.PRESS_KEY,
            target=target,
            value=key,
            options=options,
            description=description or f"Press {key}" + (f" on {target}" if target else ""),
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def wait(
        self,
        seconds: float,
        description: str = "",
    ) -> UIActionChainBuilder:
        """Add a wait/delay action."""
        step = ActionStep(
            action_type=ActionType.WAIT,
            value=seconds,
            description=description or f"Wait {seconds}s",
            timeout=seconds + 5.0,
            retry_count=1,
        )
        self._chain.add_step(step)
        return self

    def wait_for(
        self,
        target: str,
        timeout: float = 10.0,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a wait-for-element action."""
        step = ActionStep(
            action_type=ActionType.WAIT_FOR_ELEMENT,
            target=target,
            options=options,
            description=description or f"Wait for {target}",
            timeout=timeout,
            retry_count=1,
        )
        self._chain.add_step(step)
        return self

    def scroll(
        self,
        dx: float,
        dy: float,
        target: str = "",
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a scroll action."""
        step = ActionStep(
            action_type=ActionType.SCROLL,
            target=target,
            value=(dx, dy),
            options=options,
            description=description or f"Scroll ({dx}, {dy})",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def drag(
        self,
        source: str,
        target: str,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a drag action."""
        step = ActionStep(
            action_type=ActionType.DRAG,
            target=source,
            value=target,
            options=options,
            description=description or f"Drag {source} to {target}",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def custom(
        self,
        action_type: ActionType,
        target: str = "",
        value: Any = None,
        description: str = "",
        **options: Any,
    ) -> UIActionChainBuilder:
        """Add a custom action."""
        step = ActionStep(
            action_type=action_type,
            target=target,
            value=value,
            options=options,
            description=description,
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def then(
        self,
        callback: Callable[[], Any],
        description: str = "",
    ) -> UIActionChainBuilder:
        """Add a callback/assertion step."""
        step = ActionStep(
            action_type=ActionType.CUSTOM,
            value=callback,
            description=description or "Custom callback",
            timeout=self._chain.default_timeout,
            retry_count=self._chain.default_retry,
        )
        self._chain.add_step(step)
        return self

    def when(
        self,
        condition: Callable[[], bool],
    ) -> UIActionChainBuilder:
        """Add a conditional step (must follow an action step)."""
        if self._chain.steps:
            self._chain.steps[-1].condition = condition
        return self

    def with_timeout(self, timeout: float) -> UIActionChainBuilder:
        """Set default timeout for subsequent steps."""
        self._chain.default_timeout = timeout
        return self

    def with_retry(self, count: int) -> UIActionChainBuilder:
        """Set default retry count for subsequent steps."""
        self._chain.default_retry = count
        return self

    def on_error(self, mode: str) -> UIActionChainBuilder:
        """Set error handling mode: 'stop', 'skip', 'retry', 'continue'."""
        self._chain.on_error = mode
        return self

    def build(self) -> ActionChain:
        """Build and return the action chain."""
        return self._chain


# Executor for action chains
class ActionChainExecutor:
    """Executes action chains with logging and error handling."""

    def __init__(self) -> None:
        """Initialize executor."""
        self._handlers: dict[ActionType, Callable[[ActionStep], Any]] = {}
        self._on_step_callbacks: list[Callable[[ActionStep, ActionChainResult], None]] = []

    def register_handler(
        self,
        action_type: ActionType,
        handler: Callable[[ActionStep], Any],
    ) -> None:
        """Register a handler function for an action type."""
        self._handlers[action_type] = handler

    def execute(self, chain: ActionChain) -> list[ActionChainResult]:
        """Execute an action chain and return results for each step.

        Handles errors according to the chain's on_error setting.
        """
        results: list[ActionChainResult] = []

        for step in chain.steps:
            if not step.should_execute():
                results.append(ActionChainResult(
                    success=True,
                    step_id=step.step_id,
                    result="skipped (condition)",
                ))
                continue

            step.run_before_hook()
            result = self._execute_step(step)
            step.run_after_hook()
            results.append(result)
            self._on_step(step, result)

            if not result.success and chain.on_error == "stop":
                break

        return results

    def _execute_step(self, step: ActionStep) -> ActionChainResult:
        """Execute a single step with retries."""
        start = time.time()
        last_error = ""

        for attempt in range(step.retry_count):
            try:
                handler = self._handlers.get(step.action_type)
                if handler:
                    result = handler(step)
                else:
                    result = None

                duration = time.time() - start
                return ActionChainResult(
                    success=True,
                    step_id=step.step_id,
                    result=result,
                    duration=duration,
                )
            except Exception as e:
                last_error = str(e)
                if attempt < step.retry_count - 1:
                    time.sleep(0.2)

        duration = time.time() - start
        return ActionChainResult(
            success=False,
            step_id=step.step_id,
            error=last_error,
            duration=duration,
        )

    def on_step(
        self,
        callback: Callable[[ActionStep, ActionChainResult], None],
    ) -> None:
        """Register a callback for step completion."""
        self._on_step_callbacks.append(callback)

    def _on_step(
        self,
        step: ActionStep,
        result: ActionChainResult,
    ) -> None:
        """Notify step callbacks."""
        for cb in self._on_step_callbacks:
            try:
                cb(step, result)
            except Exception:
                pass
