"""
Keyboard Navigation Utilities.

Utilities for simulating keyboard-based navigation through UI elements,
including Tab traversal, arrow key navigation, and focus movement.

Usage:
    from utils.keyboard_navigation import KeyboardNavController, TabNavStrategy

    controller = KeyboardNavController(bridge)
    controller.set_strategy(TabNavStrategy.FORWARD)
    next_element = controller.navigate()
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import deque

if TYPE_CHECKING:
    from utils.accessibility_bridge import AccessibilityBridge


class NavDirection(Enum):
    """Navigation direction for keyboard traversal."""
    FORWARD = auto()
    BACKWARD = auto()
    UP = auto()
    DOWN = auto()
    LEFT = auto()
    RIGHT = auto()
    IN = auto()
    OUT = auto()


class NavRole(Enum):
    """Expected role for navigation targets."""
    BUTTON = "button"
    LINK = "link"
    TEXT_FIELD = "text_field"
    CHECK_BOX = "check_box"
    RADIO_BUTTON = "radio_button"
    MENU_ITEM = "menu_item"
    TAB = "tab"
    ANY = "any"


@dataclass
class NavStep:
    """A single keyboard navigation step."""
    direction: NavDirection
    element: Optional[Dict[str, Any]] = None
    key: Optional[str] = None
    modifiers: List[str] = field(default_factory=list)
    distance: int = 1

    def __repr__(self) -> str:
        return f"NavStep({self.direction.name}, element={self.element.get('title') if self.element else None})"


@dataclass
class NavPath:
    """A path of navigation steps to reach a target element."""
    steps: List[NavStep]
    target: Optional[Dict[str, Any]] = None
    estimated_length: int = 0

    def __len__(self) -> int:
        return len(self.steps)


class KeyboardNavController:
    """
    Control keyboard-based navigation through UI elements.

    Supports Tab navigation, arrow key traversal, and custom
    navigation strategies based on element roles and positions.

    Example:
        controller = KeyboardNavController(bridge)
        controller.set_filter(role=NavRole.BUTTON)
        while True:
            result = controller.navigate()
            if result is None:
                break
            print(f"Navigated to: {result.get('title')}")
    """

    def __init__(self, bridge: "AccessibilityBridge") -> None:
        """
        Initialize the keyboard navigation controller.

        Args:
            bridge: An AccessibilityBridge instance.
        """
        self._bridge = bridge
        self._current: Optional[Dict[str, Any]] = None
        self._history: List[NavStep] = []
        self._max_history = 50
        self._role_filter: Optional[NavRole] = None
        self._enabled_filter: Callable[[Dict[str, Any]], bool] = lambda _: True
        self._direction = NavDirection.FORWARD

    def set_filter(
        self,
        role: Optional[NavRole] = None,
        enabled_only: bool = True,
    ) -> None:
        """
        Set filters for navigable elements.

        Args:
            role: Only navigate to elements with this role.
            enabled_only: Only navigate to enabled elements.
        """
        self._role_filter = role
        if enabled_only:
            self._enabled_filter = lambda e: e.get("enabled", True)
        else:
            self._enabled_filter = lambda _: True

    def set_direction(self, direction: NavDirection) -> None:
        """Set the navigation direction."""
        self._direction = direction

    def navigate(
        self,
        steps: int = 1,
        wrap_around: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform one or more navigation steps.

        Args:
            steps: Number of key presses to simulate.
            wrap_around: Whether to wrap at the end of the UI.

        Returns:
            The element reached after navigation, or None.
        """
        for _ in range(steps):
            element = self._do_step()
            if element is None and wrap_around:
                element = self._wrap_to_start()
            if element is None:
                return self._current
            self._current = element
        return self._current

    def _do_step(self) -> Optional[Dict[str, Any]]:
        """Execute a single navigation step."""
        key = self._direction_to_key()
        self._bridge._send_key(key)  # type: ignore
        import time
        time.sleep(0.05)
        return self._get_current_focus()

    def _direction_to_key(self) -> str:
        """Map NavDirection to the corresponding key."""
        mapping = {
            NavDirection.FORWARD: "tab",
            NavDirection.BACKWARD: "tab",
            NavDirection.UP: "up",
            NavDirection.DOWN: "down",
            NavDirection.LEFT: "left",
            NavDirection.RIGHT: "right",
        }
        return mapping.get(self._direction, "tab")

    def _get_current_focus(self) -> Optional[Dict[str, Any]]:
        """Get the currently focused element."""
        try:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return None
            tree = self._bridge.build_accessibility_tree(app)
            return self._find_focused(tree)
        except Exception:
            return None

    def _find_focused(
        self,
        tree: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Recursively find the focused element in a tree."""
        if tree.get("focused") or tree.get("selected"):
            if self._matches_filter(tree):
                return tree

        for child in tree.get("children", []):
            result = self._find_focused(child)
            if result:
                return result
        return None

    def _matches_filter(self, element: Dict[str, Any]) -> bool:
        """Check if an element matches the current filters."""
        if not self._enabled_filter(element):
            return False
        if self._role_filter and self._role_filter != NavRole.ANY:
            if element.get("role") != self._role_filter.value:
                return False
        return True

    def _wrap_to_start(self) -> Optional[Dict[str, Any]]:
        """Wrap navigation to the beginning."""
        for _ in range(10):
            self._bridge._send_key("shift+tab")
        import time
        time.sleep(0.1)
        return self._get_current_focus()

    def navigate_to(
        self,
        target_title: str,
        role: Optional[NavRole] = None,
        max_steps: int = 50,
    ) -> Tuple[bool, int]:
        """
        Navigate to a specific element by title.

        Args:
            target_title: Title of the element to reach.
            role: Optional role filter.
            max_steps: Maximum number of steps before giving up.

        Returns:
            Tuple of (success, steps_taken).
        """
        if role:
            self._role_filter = role

        for step in range(max_steps):
            current = self.navigate(steps=1, wrap_around=False)
            if current and current.get("title") == target_title:
                return True, step + 1

        return False, max_steps

    def get_nav_path(
        self,
        from_element: Dict[str, Any],
        to_element: Dict[str, Any],
    ) -> NavPath:
        """
        Calculate a navigation path between two elements.

        This is a heuristic based on spatial positions.

        Args:
            from_element: Starting element.
            to_element: Target element.

        Returns:
            NavPath with estimated steps.
        """
        steps: List[NavStep] = []

        from_rect = from_element.get("rect", {})
        to_rect = to_element.get("rect", {})

        fx = from_rect.get("x", 0) + from_rect.get("width", 0) / 2
        fy = from_rect.get("y", 0) + from_rect.get("height", 0) / 2
        tx = to_rect.get("x", 0) + to_rect.get("width", 0) / 2
        ty = to_rect.get("y", 0) + to_rect.get("height", 0) / 2

        dx = tx - fx
        dy = ty - fy

        if abs(dx) > abs(dy):
            direction = NavDirection.RIGHT if dx > 0 else NavDirection.LEFT
        else:
            direction = NavDirection.DOWN if dy > 0 else NavDirection.UP

        est_steps = int(max(abs(dx), abs(dy)) / 20)

        return NavPath(
            steps=[NavStep(direction=direction, distance=est_steps)],
            target=to_element,
            estimated_length=est_steps,
        )


class TabNavStrategy:
    """Predefined Tab navigation strategies."""

    @staticmethod
    def forward() -> Callable[[KeyboardNavController], Optional[Dict[str, Any]]]:
        """Return a strategy that navigates forward with Tab."""
        def apply(controller: KeyboardNavController) -> Optional[Dict[str, Any]]:
            controller.set_direction(NavDirection.FORWARD)
            return controller.navigate()
        return apply

    @staticmethod
    def backward() -> Callable[[KeyboardNavController], Optional[Dict[str, Any]]]:
        """Return a strategy that navigates backward with Shift+Tab."""
        def apply(controller: KeyboardNavController) -> Optional[Dict[str, Any]]:
            controller.set_direction(NavDirection.BACKWARD)
            return controller.navigate()
        return apply
