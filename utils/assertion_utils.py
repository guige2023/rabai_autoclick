"""Assertion and Verification Utilities.

Provides assertions and verification for UI automation testing.
Supports soft assertions, custom assertion messages, and verification chains.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class AssertionLevel(Enum):
    """Severity level for assertions."""

    ERROR = auto()
    WARNING = auto()
    INFO = auto()


@dataclass
class AssertionResult:
    """Result of an assertion.

    Attributes:
        passed: Whether the assertion passed.
        level: Severity level of the assertion.
        message: Description of the assertion.
        expected: Expected value.
        actual: Actual value.
        details: Additional details.
    """

    passed: bool
    level: AssertionLevel
    message: str
    expected: Any = None
    actual: Any = None
    details: dict = field(default_factory=dict)


@dataclass
class AssertionError(Exception):
    """Custom assertion error.

    Attributes:
        result: AssertionResult that caused the error.
    """

    result: AssertionResult

    def __str__(self) -> str:
        """String representation."""
        return f"{self.result.level.name}: {self.result.message}"


class Assertion:
    """Base assertion class.

    Example:
        assertion = Assertion("Value should be positive", level=AssertionLevel.WARNING)
        result = assertion.evaluate(value=5)
    """

    def __init__(
        self,
        message: str,
        level: AssertionLevel = AssertionLevel.ERROR,
    ):
        """Initialize the assertion.

        Args:
            message: Description of what is being asserted.
            level: Severity level.
        """
        self.message = message
        self.level = level

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate the assertion.

        Args:
            **context: Assertion context values.

        Returns:
            AssertionResult.
        """
        raise NotImplementedError

    def check(self, **context: Any) -> None:
        """Evaluate assertion and raise if failed.

        Args:
            **context: Assertion context values.

        Raises:
            AssertionError: If assertion fails.
        """
        result = self.evaluate(**context)
        if not result.passed:
            raise AssertionError(result)


class IsEqualAssertion(Assertion):
    """Asserts that two values are equal."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate equality assertion."""
        expected = context.get("expected")
        actual = context.get("actual")

        passed = actual == expected
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=expected,
            actual=actual,
        )


class IsNotEqualAssertion(Assertion):
    """Asserts that two values are not equal."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate inequality assertion."""
        expected = context.get("expected")
        actual = context.get("actual")

        passed = actual != expected
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=f"not {expected}",
            actual=actual,
        )


class IsTrueAssertion(Assertion):
    """Asserts that a condition is true."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate truth assertion."""
        actual = context.get("actual")

        passed = bool(actual)
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=True,
            actual=actual,
        )


class IsFalseAssertion(Assertion):
    """Asserts that a condition is false."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate falsity assertion."""
        actual = context.get("actual")

        passed = not bool(actual)
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=False,
            actual=actual,
        )


class IsNoneAssertion(Assertion):
    """Asserts that a value is None."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate None assertion."""
        actual = context.get("actual")

        passed = actual is None
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=None,
            actual=actual,
        )


class IsNotNoneAssertion(Assertion):
    """Asserts that a value is not None."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate not None assertion."""
        actual = context.get("actual")

        passed = actual is not None
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected="not None",
            actual=actual,
        )


class ContainsAssertion(Assertion):
    """Asserts that a value contains a substring or element."""

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate contains assertion."""
        container = context.get("container", "")
        item = context.get("item")

        try:
            passed = item in container
        except TypeError:
            passed = False

        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=item,
            actual=container,
        )


class MatchesPatternAssertion(Assertion):
    """Asserts that a value matches a regex pattern."""

    def __init__(
        self,
        message: str,
        pattern: str,
        level: AssertionLevel = AssertionLevel.ERROR,
    ):
        """Initialize the pattern assertion.

        Args:
            message: Description.
            pattern: Regex pattern to match.
            level: Severity level.
        """
        super().__init__(message, level)
        self.pattern = pattern

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate pattern match assertion."""
        import re
        actual = str(context.get("actual", ""))

        passed = bool(re.match(self.pattern, actual))
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=self.pattern,
            actual=actual,
        )


class InRangeAssertion(Assertion):
    """Asserts that a value is within a range."""

    def __init__(
        self,
        message: str,
        min_value: float,
        max_value: float,
        level: AssertionLevel = AssertionLevel.ERROR,
    ):
        """Initialize the range assertion.

        Args:
            message: Description.
            min_value: Minimum acceptable value.
            max_value: Maximum acceptable value.
            level: Severity level.
        """
        super().__init__(message, level)
        self.min_value = min_value
        self.max_value = max_value

    def evaluate(self, **context: Any) -> AssertionResult:
        """Evaluate range assertion."""
        actual = float(context.get("actual", 0))

        passed = self.min_value <= actual <= self.max_value
        return AssertionResult(
            passed=passed,
            level=self.level,
            message=self.message,
            expected=f"{self.min_value} <= x <= {self.max_value}",
            actual=actual,
        )


class AssertionChain:
    """Chain of assertions with soft failure.

    Example:
        chain = AssertionChain()
        chain.that(actual).is_equal(expected).is_not_none().check()
    """

    def __init__(self, fail_fast: bool = True):
        """Initialize the assertion chain.

        Args:
            fail_fast: Stop on first failure.
        """
        self.fail_fast = fail_fast
        self._results: list[AssertionResult] = []
        self._last_value: Any = None

    def that(self, value: Any) -> "AssertionChain":
        """Set the value to assert against.

        Args:
            value: Value to test.

        Returns:
            Self for chaining.
        """
        self._last_value = value
        return self

    def is_equal(self, expected: Any, message: str = "") -> "AssertionChain":
        """Assert equality."""
        result = IsEqualAssertion(message or f"Expected {expected}").evaluate(
            expected=expected, actual=self._last_value
        )
        return self._add_result(result)

    def is_not_none(self, message: str = "") -> "AssertionChain":
        """Assert not None."""
        result = IsNotNoneAssertion(message or "Value should not be None").evaluate(
            actual=self._last_value
        )
        return self._add_result(result)

    def is_none(self, message: str = "") -> "AssertionChain":
        """Assert None."""
        result = IsNoneAssertion(message or "Value should be None").evaluate(
            actual=self._last_value
        )
        return self._add_result(result)

    def is_true(self, message: str = "") -> "AssertionChain":
        """Assert true."""
        result = IsTrueAssertion(message or "Value should be true").evaluate(
            actual=self._last_value
        )
        return self._add_result(result)

    def is_false(self, message: str = "") -> "AssertionChain":
        """Assert false."""
        result = IsFalseAssertion(message or "Value should be false").evaluate(
            actual=self._last_value
        )
        return self._add_result(result)

    def contains(self, item: Any, message: str = "") -> "AssertionChain":
        """Assert contains."""
        result = ContainsAssertion(message or f"Should contain {item}").evaluate(
            container=self._last_value, item=item
        )
        return self._add_result(result)

    def in_range(
        self,
        min_val: float,
        max_val: float,
        message: str = "",
    ) -> "AssertionChain":
        """Assert in range."""
        result = InRangeAssertion(
            message or f"Should be between {min_val} and {max_val}",
            min_val,
            max_val,
        ).evaluate(actual=self._last_value)
        return self._add_result(result)

    def _add_result(self, result: AssertionResult) -> "AssertionChain":
        """Add a result to the chain.

        Args:
            result: AssertionResult to add.

        Returns:
            Self for chaining.
        """
        self._results.append(result)
        if self.fail_fast and not result.passed:
            raise AssertionError(result)
        return self

    def check(self, level: AssertionLevel = AssertionLevel.ERROR) -> None:
        """Check all assertions and raise if any failed.

        Args:
            level: Minimum level to consider failure.

        Raises:
            AssertionError: If any assertions failed.
        """
        failures = [r for r in self._results if not r.passed and r.level == level]
        if failures:
            raise AssertionError(failures[0])

    def get_results(self) -> list[AssertionResult]:
        """Get all assertion results.

        Returns:
            List of results.
        """
        return self._results

    def passed(self) -> bool:
        """Check if all assertions passed.

        Returns:
            True if all passed.
        """
        return all(r.passed for r in self._results)
