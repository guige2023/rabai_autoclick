"""Element action utilities.

This module provides utilities for executing and validating
actions on UI elements.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, TypeVar, Protocol
from dataclasses import dataclass, field
from enum import Enum, auto
import time

T = TypeVar("T")


class ActionType(Enum):
    """Types of UI element actions."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    HOVER = auto()
    TYPE = auto()
    PRESS_KEY = auto()
    SCROLL = auto()
    DRAG = auto()
    DROP = auto()
    SELECT = auto()
    CHECK = auto()
    EXPAND = auto()
    COLLAPSE = auto()
    CUSTOM = auto()


@dataclass
class ActionResult:
    """Result of an element action."""
    success: bool
    action_type: ActionType
    element_id: str
    timestamp: float
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return self.metadata.get("duration_ms", 0.0)


@dataclass
class ElementAction:
    """A single action to perform on an element."""
    action_type: ActionType
    element_id: str
    params: Dict[str, Any] = field(default_factory=dict)
    timeout_ms: float = 5000.0
    retry_count: int = 0


@dataclass
class ActionSequence:
    """A sequence of element actions."""
    actions: List[ElementAction] = field(default_factory=list)
    name: Optional[str] = None

    def add(self, action: ElementAction) -> None:
        self.actions.append(action)

    def is_empty(self) -> bool:
        return len(self.actions) == 0


class ActionExecutor:
    """Executes actions on UI elements."""

    def __init__(self) -> None:
        self._handlers: Dict[ActionType, Callable[..., bool]] = {}
        self._results: List[ActionResult] = []
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default action handlers."""
        self._handlers[ActionType.CLICK] = self._default_click
        self._handlers[ActionType.DOUBLE_CLICK] = self._default_double_click
        self._handlers[ActionType.RIGHT_CLICK] = self._default_right_click
        self._handlers[ActionType.HOVER] = self._default_hover
        self._handlers[ActionType.TYPE] = self._default_type
        self._handlers[ActionType.SCROLL] = self._default_scroll

    def register_handler(
        self,
        action_type: ActionType,
        handler: Callable[..., bool],
    ) -> None:
        """Register a custom handler for an action type."""
        self._handlers[action_type] = handler

    def execute(self, action: ElementAction) -> ActionResult:
        """Execute a single action.

        Args:
            action: Action to execute.

        Returns:
            ActionResult with success status.
        """
        start = time.perf_counter()
        handler = self._handlers.get(action.action_type)

        if not handler:
            return ActionResult(
                success=False,
                action_type=action.action_type,
                element_id=action.element_id,
                timestamp=start,
                error=f"No handler for action type {action.action_type.name}",
            )

        try:
            success = handler(action)
            duration_ms = (time.perf_counter() - start) * 1000
            result = ActionResult(
                success=success,
                action_type=action.action_type,
                element_id=action.element_id,
                timestamp=start,
                metadata={"duration_ms": duration_ms},
            )
        except Exception as e:
            result = ActionResult(
                success=False,
                action_type=action.action_type,
                element_id=action.element_id,
                timestamp=start,
                error=str(e),
            )

        self._results.append(result)
        return result

    def execute_sequence(self, seq: ActionSequence) -> List[ActionResult]:
        """Execute a sequence of actions.

        Args:
            seq: ActionSequence to execute.

        Returns:
            List of ActionResults.
        """
        results: List[ActionResult] = []
        for action in seq.actions:
            result = self.execute(action)
            results.append(result)
            if not result.success:
                break
        return results

    def _default_click(self, action: ElementAction) -> bool:
        return True

    def _default_double_click(self, action: ElementAction) -> bool:
        return True

    def _default_right_click(self, action: ElementAction) -> bool:
        return True

    def _default_hover(self, action: ElementAction) -> bool:
        return True

    def _default_type(self, action: ElementAction) -> bool:
        return True

    def _default_scroll(self, action: ElementAction) -> bool:
        return True

    @property
    def results(self) -> List[ActionResult]:
        """Get all execution results."""
        return self._results.copy()


def validate_action_result(result: ActionResult) -> bool:
    """Validate that an action result indicates success.

    Args:
        result: ActionResult to validate.

    Returns:
        True if successful.
    """
    return result.success


__all__ = [
    "ActionType",
    "ActionResult",
    "ElementAction",
    "ActionSequence",
    "ActionExecutor",
    "validate_action_result",
]
