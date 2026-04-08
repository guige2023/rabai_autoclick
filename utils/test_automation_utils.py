"""Test automation utilities for validating automation workflows.

Provides helpers for creating automated tests of automation
actions, including mock input simulation, screenshot comparison,
and assertion helpers for GUI automation testing.

Example:
    >>> from utils.test_automation_utils import assert_image_match, mock_click
    >>> assert_image_match('expected.png', 'actual.png', threshold=0.95)
    >>> with mock_click(100, 200):
    ...     result = automation_action()
"""

from __future__ import annotations

import time
from typing import Optional

__all__ = [
    "assert_image_match",
    "assert_element_visible",
    "mock_click",
    "MockInput",
    "TestAutomationError",
]


class TestAutomationError(AssertionError):
    """Raised when test assertions fail."""
    pass


def assert_image_match(
    expected: str | bytes,
    actual: str | bytes,
    threshold: float = 0.9,
) -> None:
    """Assert that two images match with a given similarity threshold.

    Args:
        expected: Expected image path or bytes.
        actual: Actual image path or bytes.
        threshold: Minimum similarity ratio (0.0 to 1.0).

    Raises:
        TestAutomationError: If images don't match above threshold.
    """
    try:
        from utils.image_analysis_utils import similarity_score

        score = similarity_score(expected, actual)
        if score > threshold:
            return
        raise TestAutomationError(
            f"Image mismatch: similarity={score:.2f}, threshold={threshold:.2f}"
        )
    except ImportError:
        raise TestAutomationError("image_analysis_utils not available")


def assert_element_visible(
    element_image: str,
    region: Optional[tuple[float, float, float, float]] = None,
    threshold: float = 0.7,
) -> bool:
    """Assert that an element image is visible in the screen region.

    Args:
        element_image: Path to element template image.
        region: Screen region to search.
        threshold: Match confidence threshold.

    Returns:
        True if element is visible.

    Raises:
        TestAutomationError: If element is not found.
    """
    try:
        from utils.template_matching_utils import find_best

        match = find_best(element_image, region=region, threshold=threshold)
        if match:
            return True
        raise TestAutomationError(
            f"Element not visible: {element_image}"
        )
    except ImportError:
        raise TestAutomationError("template_matching_utils not available")


class MockInput:
    """Context manager for mocking input events during tests.

    Example:
        >>> with MockInput() as mock:
        ...     mock.click(100, 200)
        ...     mock.type_text("hello")
        ...     events = mock.get_events()
    """

    def __init__(self):
        self._events: list = []
        self._recording: list = []

    def __enter__(self) -> "MockInput":
        self._events = []
        self._recording = []
        return self

    def __exit__(self, *args) -> None:
        pass

    def click(self, x: float, y: float, button: str = "left") -> None:
        self._events.append({"type": "click", "x": x, "y": y, "button": button})

    def type_text(self, text: str) -> None:
        self._events.append({"type": "type", "text": text})

    def scroll(self, dx: float = 0, dy: float = 0) -> None:
        self._events.append({"type": "scroll", "dx": dx, "dy": dy})

    def get_events(self) -> list:
        """Return all recorded events."""
        return list(self._events)

    def assert_clicked(self, x: float, y: float, tolerance: float = 5.0) -> None:
        """Assert a click was recorded near the given coordinates."""
        for event in self._events:
            if event["type"] == "click":
                if abs(event["x"] - x) <= tolerance and abs(event["y"] - y) <= tolerance:
                    return
        raise TestAutomationError(
            f"No click found near ({x}, {y}) within tolerance {tolerance}"
        )


def mock_click(x: float, y: float, button: str = "left"):
    """Context manager that records a mock click event.

    Example:
        >>> with mock_click(100, 200):
        ...     do_automation()
    """
    return _MockClickContext(x, y, button)


class _MockClickContext:
    def __init__(self, x: float, y: float, button: str):
        self.x = x
        self.y = y
        self.button = button
        self._triggered = False

    def __enter__(self) -> "_MockClickContext":
        # Would record the expected click in test framework
        return self

    def __exit__(self, *args) -> None:
        self._triggered = True


class AutomationTestSuite:
    """A collection of automation test cases.

    Example:
        >>> suite = AutomationTestSuite('Click Tests')
        >>> @suite.test
        ... def test_button_click():
        ...     assert_element_visible('button.png')
    """

    def __init__(self, name: str):
        self.name = name
        self._tests: list[tuple[str, callable]] = []

    def test(self, fn: Optional[callable] = None) -> callable:
        """Decorator to register a test function."""
        def decorator(f: callable) -> callable:
            self._tests.append((f.__name__, f))
            return f

        if fn is not None:
            return decorator(fn)
        return decorator

    def run(self) -> dict:
        """Run all tests and return results."""
        results = {"passed": [], "failed": [], "errors": []}
        for name, test_fn in self._tests:
            try:
                test_fn()
                results["passed"].append(name)
            except Exception as e:
                results["failed"].append((name, str(e)))
        return results
