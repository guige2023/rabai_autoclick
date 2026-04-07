"""
Builder Pattern Implementation

Provides fluent builder interfaces for constructing complex objects
step-by-step, with validation and post-processing hooks.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TProduct = TypeVar("TProduct")


class BuilderError(Exception):
    """Base exception for builder errors."""
    pass


class ValidationError(BuilderError):
    """Raised when builder validation fails."""
    pass


class BuildError(BuilderError):
    """Raised when the build process fails."""
    pass


@dataclass
class ValidationResult:
    """Result of a validation check."""
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.valid

    def add_error(self, error: str) -> None:
        self.errors.append(error)
        self.valid = False

    def add_warning(self, warning: str) -> None:
        self.warnings.append(warning)

    def merge(self, other: ValidationResult) -> ValidationResult:
        """Merge another validation result into this one."""
        if other.valid:
            return self
        result = ValidationResult(valid=self.valid and other.valid)
        result.errors = self.errors + other.errors
        result.warnings = self.warnings + other.warnings
        return result


class Validator(ABC):
    """Abstract base class for validators."""

    @abstractmethod
    def validate(self, value: Any) -> ValidationResult:
        """Validate a value and return the result."""
        pass


class RequiredValidator(Validator):
    """Validates that a value is not None or empty."""

    def __init__(self, field_name: str = "value"):
        self.field_name = field_name

    def validate(self, value: Any) -> ValidationResult:
        result = ValidationResult(valid=True)
        if value is None:
            result.add_error(f"{self.field_name} is required but was None")
        elif isinstance(value, (str, list, dict, tuple, set)) and len(value) == 0:
            result.add_error(f"{self.field_name} cannot be empty")
        return result


class RangeValidator(Validator):
    """Validates that a value is within a range."""

    def __init__(
        self,
        min_value: float | None = None,
        max_value: float | None = None,
        field_name: str = "value",
    ):
        self.min_value = min_value
        self.max_value = max_value
        self.field_name = field_name

    def validate(self, value: Any) -> ValidationResult:
        result = ValidationResult(valid=True)
        if not isinstance(value, (int, float)):
            result.add_error(f"{self.field_name} must be a number")
            return result

        if self.min_value is not None and value < self.min_value:
            result.add_error(f"{self.field_name} must be >= {self.min_value}, got {value}")
        if self.max_value is not None and value > self.max_value:
            result.add_error(f"{self.field_name} must be <= {self.max_value}, got {value}")
        return result


class TypeValidator(Validator):
    """Validates that a value is of the expected type."""

    def __init__(self, expected_type: type | tuple[type, ...], field_name: str = "value"):
        self.expected_type = expected_type
        self.field_name = field_name

    def validate(self, value: Any) -> ValidationResult:
        result = ValidationResult(valid=True)
        if not isinstance(value, self.expected_type):
            expected = (
                self.expected_type.__name__
                if isinstance(self.expected_type, type)
                else ", ".join(t.__name__ for t in self.expected_type)
            )
            result.add_error(
                f"{self.field_name} must be of type {expected}, got {type(value).__name__}"
            )
        return result


class Builder(ABC, Generic[T]):
    """
    Abstract base class for builders.

    Type Parameters:
        T: The type of object this builder creates.
    """

    @abstractmethod
    def build(self) -> T:
        """Build and return the final object."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the builder to initial state."""
        pass

    @abstractmethod
    def validate(self) -> ValidationResult:
        """Validate the current state before building."""
        pass

    def build_or_raise(self) -> T:
        """Build the object or raise an exception if validation fails."""
        result = self.validate()
        if not result.is_valid:
            raise BuildError(f"Build validation failed: {'; '.join(result.errors)}")
        return self.build()


class StepBuilder(ABC, Generic[T]):
    """
    Builder with explicit step methods for fluent interface.
    """

    @abstractmethod
    def step_1(self, value: Any) -> StepBuilder:
        """Step 1 of the build process."""
        pass

    @abstractmethod
    def step_2(self, value: Any) -> StepBuilder:
        """Step 2 of the build process."""
        pass

    @abstractmethod
    def step_3(self, value: Any) -> StepBuilder:
        """Step 3 of the build process."""
        pass


class FluentBuilder(ABC, Generic[T]):
    """
    Base class for fluent builders that return self from each method.
    """

    def build(self) -> T:
        """Build and return the final object."""
        raise NotImplementedError

    def reset(self) -> FluentBuilder:
        """Reset the builder to initial state."""
        raise NotImplementedError


@dataclass
class BuildContext:
    """Context passed through the build process."""
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def has(self, key: str) -> bool:
        return key in self.data


class BuilderWithHooks(Generic[T]):
    """
    Builder that supports pre-build and post-build hooks.
    """

    def __init__(self):
        self._pre_build_hooks: list[Callable[[BuildContext], None]] = []
        self._post_build_hooks: list[Callable[[T, BuildContext], None]] = []
        self._context = BuildContext()

    def add_pre_hook(self, hook: Callable[[BuildContext], None]) -> BuilderWithHooks:
        """Add a pre-build hook."""
        self._pre_build_hooks.append(hook)
        return self

    def add_post_hook(self, hook: Callable[[T, BuildContext], None]) -> BuilderWithHooks:
        """Add a post-build hook."""
        self._post_build_hooks.append(hook)
        return self

    def _run_pre_hooks(self) -> None:
        for hook in self._pre_build_hooks:
            hook(self._context)

    def _run_post_hooks(self, product: T) -> None:
        for hook in self._post_build_hooks:
            hook(product, self._context)


class ConfigurableBuilder(Generic[T]):
    """
    Builder that can be configured with different build strategies.
    """

    def __init__(self):
        self._builders: dict[str, Callable[[], T]] = {}
        self._default_builder: str = ""

    def register_builder(self, name: str, builder: Callable[[], T]) -> None:
        """Register a named build strategy."""
        self._builders[name] = builder
        if not self._default_builder:
            self._default_builder = name

    def set_default(self, name: str) -> None:
        """Set the default build strategy."""
        if name not in self._builders:
            raise ValueError(f"Unknown builder: {name}")
        self._default_builder = name

    def build(self, strategy: str | None = None) -> T:
        """Build using the specified or default strategy."""
        name = strategy or self._default_builder
        if name not in self._builders:
            raise ValueError(f"Unknown builder strategy: {name}")
        return self._builders[name]()

    def list_strategies(self) -> list[str]:
        """List available build strategies."""
        return list(self._builders.keys())


class LazyBuilder(Generic[T]):
    """
    Builder that defers construction until build() is called.
    """

    def __init__(self, factory: Callable[[], T]):
        self._factory = factory
        self._built: T | None = None
        self._built = None

    def build(self) -> T:
        """Build the object, caching the result."""
        if self._built is None:
            self._built = self._factory()
        return copy.deepcopy(self._built)

    def is_built(self) -> bool:
        """Check if the object has been built."""
        return self._built is not None

    def reset(self) -> None:
        """Reset the built object."""
        self._built = None


@dataclass
class BuildPlan:
    """A plan for building an object with steps."""
    name: str
    steps: list[Callable[[], Any]] = field(default_factory=list)
    validators: list[Validator] = field(default_factory=list)

    def add_step(self, step: Callable[[], Any]) -> BuildPlan:
        """Add a step to the plan."""
        self.steps.append(step)
        return self

    def add_validator(self, validator: Validator) -> BuildPlan:
        """Add a validator to the plan."""
        self.validators.append(validator)
        return self


class Director(ABC, Generic[T]):
    """
    Director class for directing the build process.
    """

    def __init__(self, builder: Builder[T]):
        self._builder = builder

    @abstractmethod
    def construct(self) -> T:
        """Direct the construction process."""
        pass

    def get_builder(self) -> Builder[T]:
        """Get the builder."""
        return self._builder
