"""
Property-based testing utilities using Hypothesis.

Provides strategies for generating test data, property assertions,
stateful testing, and regression test generation.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar

try:
    from hypothesis import given, strategies as st, settings, assume, Phase, example
    from hypothesis.stateful import RuleSystem, Rule, StateMachine
except ImportError:
    raise ImportError("hypothesis is required: pip install hypothesis")

import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
A = TypeVar("A")
B = TypeVar("B")


@dataclass
class StrategyConfig:
    """Configuration for Hypothesis strategies."""
    max_examples: int = 100
    deadline: Optional[int] = None
    max_shrinks: int = 500
    database: Optional[str] = None


class DataStrategies:
    """Pre-built Hypothesis strategies for common data types."""

    @staticmethod
    def bounded_int(min_val: int = -1000, max_val: int = 1000) -> Any:
        return st.integers(min_value=min_val, max_value=max_val)

    @staticmethod
    def bounded_float(min_val: float = -1e6, max_val: float = 1e6) -> Any:
        return st.floats(min_value=min_val, max_value=max_val, allow_nan=False, allow_infinity=False)

    @staticmethod
    def text(max_size: int = 100, exclude_chars: str = "") -> Any:
        return st.text(min_size=0, max_size=max_size, alphabet=st.characters(exclude_characters=exclude_chars))

    @staticmethod
    def ascii_string(max_size: int = 100) -> Any:
        return st.text(min_size=0, max_size=max_size, alphabet=st.characters(categories=["Ls", "Nd", "Ps", "Pe", "Sm", "So"]))

    @staticmethod
    def email() -> Any:
        return st.emails()

    @staticmethod
    def url() -> Any:
        return st.urls()

    @staticmethod
    def ipv4() -> Any:
        return st.ipv4s()

    @staticmethod
    def uuid4() -> Any:
        return st.uuids()

    @staticmethod
    def datetime(min_year: int = 1900, max_year: int = 2100) -> Any:
        return st.datetimes(min_year=min_year, max_year=max_year)

    @staticmethod
    def lists_of(elements: Any, min_size: int = 0, max_size: int = 50) -> Any:
        return st.lists(elements, min_size=min_size, max_size=max_size)

    @staticmethod
    def dictionaries_of(keys: Any, values: Any, min_size: int = 0, max_size: int = 50) -> Any:
        return st.dictionaries(keys=keys, values=values, min_size=min_size, max_size=max_size)

    @staticmethod
    def one_of(options: list[Any]) -> Any:
        return st.one_of(options)

    @staticmethod
    def tuples_of(*strategies: Any) -> Any:
        return st.tuples(*strategies)

    @staticmethod
    def fixed_dictionaries(template: dict[str, Any]) -> Any:
        return st.fixed_dictionaries(template)

    @staticmethod
    def recursive(base: Any, expand: Callable[[Any], Any], max_depth: int = 5) -> Any:
        return st.recursive(base, expand, max_depth=max_depth)

    @staticmethod
    def json_object(max_size: int = 50) -> Any:
        return st.dictionaries(
            keys=st.text(max_size=20),
            values=st.one_of([
                st.text(max_size=50),
                st.integers(),
                st.floats(),
                st.booleans(),
                st.lists(st.text(max_size=20)),
            ]),
            max_size=max_size,
        )

    @staticmethod
    def sql_select_query() -> Any:
        return st.builds(
            lambda cols, tbl, cond: f"SELECT {','.join(cols)} FROM {tbl} WHERE {cond}",
            st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=5),
            st.text(min_size=1, max_size=20),
            st.text(min_size=0, max_size=50),
        )


@dataclass
class PropertyTestResult:
    """Result of a property-based test run."""
    is_passed: bool = True
    num_examples: int = 0
    num_failed: int = 0
    failed_examples: list[dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None


class PropertyTester:
    """Helper for running property-based tests."""

    def __init__(self, config: Optional[StrategyConfig] = None) -> None:
        self.config = config or StrategyConfig()

    def test_inverse_property(
        self,
        serialize: Callable[[Any], Any],
        deserialize: Callable[[Any], Any],
        generator: Any,
    ) -> PropertyTestResult:
        """Test that serialize(deserialize(x)) == x."""
        @given(generator)
        @settings(
            max_examples=self.config.max_examples,
            deadline=self.config.deadline,
            max_shrinks=self.config.max_shrinks,
        )
        def prop(x: Any) -> None:
            try:
                serialized = serialize(x)
                deserialized = deserialize(serialized)
                assert deserialized == x, f"Round-trip failed: {x} != {deserialized}"
            except Exception as e:
                raise AssertionError(f"Serialize/deserialize raised: {e}")

        return self._run_test(prop)

    def test_commutative_property(
        self,
        operation: Callable[[Any, Any], Any],
        a_generator: Any,
        b_generator: Any,
    ) -> PropertyTestResult:
        """Test that operation(a, b) == operation(b, a) for commutative operations."""
        @given(a=a_generator, b=b_generator)
        @settings(
            max_examples=self.config.max_examples,
            deadline=self.config.deadline,
            max_shrinks=self.config.max_shrinks,
        )
        def prop(a: Any, b: Any) -> None:
            result1 = operation(a, b)
            result2 = operation(b, a)
            assert result1 == result2, f"Commutative property violated: {result1} != {result2}"

        return self._run_test(prop)

    def test_identity_property(
        self,
        operation: Callable[[Any, Any], Any],
        identity: Any,
        generator: Any,
    ) -> PropertyTestResult:
        """Test that operation(x, identity) == x."""
        @given(x=generator)
        @settings(
            max_examples=self.config.max_examples,
            deadline=self.config.deadline,
            max_shrinks=self.config.max_shrinks,
        )
        def prop(x: Any) -> None:
            result = operation(x, identity)
            assert result == x, f"Identity property violated: {result} != {x}"

        return self._run_test(prop)

    def test_associative_property(
        self,
        operation: Callable[[Any, Any], Any],
        a_generator: Any,
        b_generator: Any,
        c_generator: Any,
    ) -> PropertyTestResult:
        """Test that operation(operation(a, b), c) == operation(a, operation(b, c))."""
        @given(a=a_generator, b=b_generator, c=c_generator)
        @settings(
            max_examples=self.config.max_examples,
            deadline=self.config.deadline,
            max_shrinks=self.config.max_shrinks,
        )
        def prop(a: Any, b: Any, c: Any) -> None:
            result1 = operation(operation(a, b), c)
            result2 = operation(a, operation(b, c))
            assert result1 == result2, f"Associative property violated"

        return self._run_test(prop)

    def test_monotonic_property(
        self,
        function: Callable[[Any], Any],
        x_generator: Any,
        y_generator: Any,
    ) -> PropertyTestResult:
        """Test that if x < y then f(x) <= f(y)."""
        @given(x=x_generator, y=y_generator)
        @settings(
            max_examples=self.config.max_examples,
            deadline=self.config.deadline,
            max_shrinks=self.config.max_shrinks,
        )
        def prop(x: Any, y: Any) -> None:
            assume(x < y)
            fx = function(x)
            fy = function(y)
            assert fx <= fy, f"Monotonicity violated: f({x})={fx} > f({y})={fy}"

        return self._run_test(prop)

    def _run_test(self, test_func: Callable) -> PropertyTestResult:
        result = PropertyTestResult()
        try:
            test_func()
            result.num_examples = self.config.max_examples
            result.is_passed = True
        except Exception as e:
            result.is_passed = False
            result.error_message = str(e)
            result.num_failed = 1
        return result


class StatefulModel:
    """Base class for stateful property-based testing."""

    def __init__(self) -> None:
        self._state: dict[str, Any] = {}

    def checks_invariants(self) -> None:
        """Override to define invariants that must hold."""
        pass


class StatefulTestMachine(StateMachine):
    """Hypothesis stateful testing machine base."""

    def __init__(self) -> None:
        super().__init__()
        self._model = StatefulModel()
