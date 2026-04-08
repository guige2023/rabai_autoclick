"""Visual assertion utilities for RabAI AutoClick.

Provides:
- Visual assertion helpers
- Screenshot comparison
- Region validation
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)


class VisualAssertion:
    """A visual assertion for testing."""

    def __init__(
        self,
        name: str,
        region: Tuple[int, int, int, int],
        expected: Any,
        comparator: Optional[Callable] = None,
    ) -> None:
        self.name = name
        self.region = region
        self.expected = expected
        self.comparator = comparator or _default_compare
        self.passed = False
        self.message = ""


def _default_compare(actual: Any, expected: Any) -> Tuple[bool, str]:
    """Default comparison function."""
    if actual == expected:
        return True, "Match"
    return False, f"Mismatch: expected {expected}, got {actual}"


class VisualAssertionSuite:
    """Collection of visual assertions."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._assertions: List[VisualAssertion] = []

    def add(
        self,
        name: str,
        region: Tuple[int, int, int, int],
        expected: Any,
        comparator: Optional[Callable] = None,
    ) -> "VisualAssertionSuite":
        """Add an assertion.

        Returns:
            Self for chaining.
        """
        assertion = VisualAssertion(name, region, expected, comparator)
        self._assertions.append(assertion)
        return self

    def run(self, screenshot: Any) -> Dict[str, Any]:
        """Run all assertions.

        Args:
            screenshot: Screenshot to validate.

        Returns:
            Results dict.
        """
        results = {
            "name": self.name,
            "total": len(self._assertions),
            "passed": 0,
            "failed": 0,
            "assertions": [],
        }

        for assertion in self._assertions:
            x, y, w, h = assertion.region
            region_data = _extract_region(screenshot, x, y, w, h)
            passed, msg = assertion.comparator(region_data, assertion.expected)
            assertion.passed = passed
            assertion.message = msg

            results["assertions"].append({
                "name": assertion.name,
                "passed": passed,
                "message": msg,
            })

            if passed:
                results["passed"] += 1
            else:
                results["failed"] += 1

        return results


def _extract_region(
    screenshot: Any,
    x: int,
    y: int,
    w: int,
    h: int,
) -> Any:
    """Extract a region from screenshot."""
    return screenshot


__all__ = [
    "VisualAssertion",
    "VisualAssertionSuite",
]
