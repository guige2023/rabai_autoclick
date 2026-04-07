"""Builder pattern utilities for RabAI AutoClick.

Provides:
- Generic builder class
- Fluent interface helpers
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar


T = TypeVar("T")


class Builder:
    """Generic builder for creating objects with fluent interface.

    Usage:
        user = (
            Builder(User)
            .set("name", "Alice")
            .set("age", 30)
            .build()
        )
    """

    def __init__(self, cls: Type = None) -> None:
        """Initialize builder.

        Args:
            cls: Class to build instance of.
        """
        self._cls = cls
        self._attrs: Dict[str, Any] = {}

    def set(self, name: str, value: Any) -> 'Builder':
        """Set attribute value.

        Args:
            name: Attribute name.
            value: Value to set.

        Returns:
            Self for chaining.
        """
        self._attrs[name] = value
        return self

    def build(self) -> Any:
        """Build instance.

        Returns:
            Instance of target class.
        """
        if self._cls is None:
            raise ValueError("No class specified")

        return self._cls(**self._attrs)

    def reset(self) -> 'Builder':
        """Reset builder state.

        Returns:
            Self for chaining.
        """
        self._attrs.clear()
        return self


class FluentDict:
    """Dictionary with fluent interface.

    Usage:
        data = (
            FluentDict()
            .set("name", "Alice")
            .set("age", 30)
            .get()
        )
    """

    def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
        """Initialize fluent dict.

        Args:
            initial: Optional initial dictionary.
        """
        self._data = initial.copy() if initial else {}

    def set(self, key: str, value: Any) -> 'FluentDict':
        """Set value.

        Args:
            key: Dictionary key.
            value: Value to set.

        Returns:
            Self for chaining.
        """
        self._data[key] = value
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """Get value.

        Args:
            key: Dictionary key.
            default: Default if not found.

        Returns:
            Value or default.
        """
        return self._data.get(key, default)

    def update(self, data: Dict[str, Any]) -> 'FluentDict':
        """Update with dictionary.

        Args:
            data: Dictionary to update with.

        Returns:
            Self for chaining.
        """
        self._data.update(data)
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Get underlying dictionary.

        Returns:
            Dictionary copy.
        """
        return self._data.copy()


class ChainBuilder:
    """Chain builder for building objects with complex initialization.

    Supports nested builders and validation.
    """

    def __init__(self) -> None:
        self._builders: Dict[str, Any] = {}
        self._result: Any = None

    def add_builder(self, name: str, builder: Builder) -> 'ChainBuilder':
        """Add a sub-builder.

        Args:
            name: Builder name.
            builder: Builder instance.

        Returns:
            Self for chaining.
        """
        self._builders[name] = builder
        return self

    def build_with(self, name: str, attrs: Dict[str, Any]) -> 'ChainBuilder':
        """Build sub-component.

        Args:
            name: Builder name.
            attrs: Dictionary of attributes for builder.

        Returns:
            Self for chaining.
        """
        if name in self._builders:
            builder = self._builders[name]
            for key, value in attrs.items():
                builder.set(key, value)
        return self

    def finalize(self) -> Dict[str, Any]:
        """Finalize all builders.

        Returns:
            Dictionary of built objects.
        """
        result = {}
        for name, builder in self._builders.items():
            result[name] = builder.build()
        self._result = result
        return result

    def get(self, key: str) -> Any:
        """Get built component.

        Args:
            key: Component name.

        Returns:
            Built component.
        """
        if self._result is None:
            self.finalize()
        return self._result.get(key)


class ValidationBuilder(Builder):
    """Builder with validation support.

    Adds validation before building.
    """

    def __init__(self, cls: Type = None) -> None:
        super().__init__(cls)
        self._validators: List[Callable[[Dict[str, Any]], bool]] = []
        self._errors: List[str] = []

    def add_validator(self, validator: Callable[[Dict[str, Any]], bool], message: str) -> 'ValidationBuilder':
        """Add a validator.

        Args:
            validator: Function that returns True if valid.
            message: Error message if invalid.

        Returns:
            Self for chaining.
        """
        self._validators.append((validator, message))
        return self

    def build(self) -> Any:
        """Build with validation.

        Returns:
            Built instance.

        Raises:
            ValueError: If validation fails.
        """
        self._errors = []
        for validator, message in self._validators:
            if not validator(self._attrs):
                self._errors.append(message)

        if self._errors:
            raise ValueError(f"Validation failed: {', '.join(self._errors)}")

        return super().build()


class BuilderRegistry:
    """Registry for builder factories.

    Allows registering and looking up builders by name.
    """

    _builders: Dict[str, type] = {}

    @classmethod
    def register(cls, name: str, builder_cls: type) -> None:
        """Register a builder class.

        Args:
            name: Builder name.
            builder_cls: Builder class.
        """
        cls._builders[name] = builder_cls

    @classmethod
    def create(cls, name: str) -> Builder:
        """Create a builder by name.

        Args:
            name: Builder name.

        Returns:
            Builder instance.
        """
        if name not in cls._builders:
            raise KeyError(f"No builder registered for: {name}")
        return cls._builders[name]()

    @classmethod
    def list_builders(cls) -> List[str]:
        """List registered builder names."""
        return list(cls._builders.keys())