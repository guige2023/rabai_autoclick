"""Tests for builder utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataclasses import dataclass
from utils.builder import (
    Builder,
    FluentDict,
    ChainBuilder,
    ValidationBuilder,
    BuilderRegistry,
)


@dataclass
class Person:
    name: str
    age: int = 0


class TestBuilder:
    """Tests for Builder."""

    def test_create(self) -> None:
        """Test creating builder."""
        builder = Builder(Person)
        assert builder._cls is Person

    def test_set_attribute(self) -> None:
        """Test setting attribute."""
        builder = Builder(Person)
        builder.set("name", "Alice")
        assert builder._attrs == {"name": "Alice"}

    def test_build(self) -> None:
        """Test building instance."""
        builder = Builder(Person)
        builder.set("name", "Alice").set("age", 30)
        person = builder.build()
        assert isinstance(person, Person)
        assert person.name == "Alice"
        assert person.age == 30

    def test_build_without_class(self) -> None:
        """Test building without class raises."""
        builder = Builder()
        with pytest.raises(ValueError, match="No class specified"):
            builder.build()

    def test_reset(self) -> None:
        """Test resetting builder."""
        builder = Builder(Person)
        builder.set("name", "Alice")
        builder.reset()
        assert builder._attrs == {}

    def test_fluent_interface(self) -> None:
        """Test fluent interface."""
        person = Builder(Person).set("name", "Bob").set("age", 25).build()
        assert person.name == "Bob"
        assert person.age == 25


class TestFluentDict:
    """Tests for FluentDict."""

    def test_create_empty(self) -> None:
        """Test creating empty FluentDict."""
        fd = FluentDict()
        assert fd.get("nonexistent") is None

    def test_create_with_initial(self) -> None:
        """Test creating with initial data."""
        fd = FluentDict({"name": "Alice"})
        assert fd.get("name") == "Alice"

    def test_set(self) -> None:
        """Test setting value."""
        fd = FluentDict()
        fd.set("name", "Bob")
        assert fd.get("name") == "Bob"

    def test_set_returns_self(self) -> None:
        """Test set returns self for chaining."""
        fd = FluentDict()
        result = fd.set("key", "value")
        assert result is fd

    def test_get_with_default(self) -> None:
        """Test get with default."""
        fd = FluentDict()
        assert fd.get("missing", "default") == "default"

    def test_update(self) -> None:
        """Test updating with dict."""
        fd = FluentDict({"name": "Alice"})
        fd.update({"age": 30})
        assert fd.get("name") == "Alice"
        assert fd.get("age") == 30

    def test_to_dict(self) -> None:
        """Test getting dict copy."""
        fd = FluentDict({"name": "Alice"})
        d = fd.to_dict()
        d["name"] = "Modified"
        assert fd.get("name") == "Alice"  # Original unchanged


class TestChainBuilder:
    """Tests for ChainBuilder."""

    def test_create(self) -> None:
        """Test creating chain builder."""
        cb = ChainBuilder()
        assert cb._builders == {}

    def test_add_builder(self) -> None:
        """Test adding builder."""
        cb = ChainBuilder()
        builder = Builder(Person)
        cb.add_builder("person", builder)
        assert "person" in cb._builders

    def test_build_with(self) -> None:
        """Test building with arguments."""
        cb = ChainBuilder()
        cb.add_builder("person", Builder(Person))
        cb.build_with("person", name="Alice", age=30)
        result = cb.finalize()
        assert result["person"].name == "Alice"

    def test_get(self) -> None:
        """Test getting built component."""
        cb = ChainBuilder()
        cb.add_builder("person", Builder(Person))
        cb.build_with("person", name="Bob")
        cb.finalize()
        person = cb.get("person")
        assert person.name == "Bob"


class TestValidationBuilder:
    """Tests for ValidationBuilder."""

    def test_create(self) -> None:
        """Test creating validation builder."""
        vb = ValidationBuilder(Person)
        assert vb._validators == []

    def test_add_validator(self) -> None:
        """Test adding validator."""
        vb = ValidationBuilder(Person)
        vb.add_validator(lambda attrs: attrs.get("age", 0) > 0, "Age must be positive")
        assert len(vb._validators) == 1

    def test_build_passes(self) -> None:
        """Test building when validation passes."""
        vb = ValidationBuilder(Person)
        vb.add_validator(lambda attrs: len(attrs.get("name", "")) > 0, "Name required")
        vb.set("name", "Alice").set("age", 25)
        person = vb.build()
        assert person.name == "Alice"

    def test_build_fails(self) -> None:
        """Test building when validation fails."""
        vb = ValidationBuilder(Person)
        vb.add_validator(lambda attrs: len(attrs.get("name", "")) > 0, "Name required")
        vb.set("age", 25)
        with pytest.raises(ValueError, match="Validation failed"):
            vb.build()


class TestBuilderRegistry:
    """Tests for BuilderRegistry."""

    def test_register(self) -> None:
        """Test registering builder."""
        BuilderRegistry.register("person", Builder)
        assert "person" in BuilderRegistry.list_builders()

    def test_create(self) -> None:
        """Test creating registered builder."""
        BuilderRegistry.register("test_create", Builder)
        builder = BuilderRegistry.create("test_create")
        assert isinstance(builder, Builder)

    def test_create_unknown_raises(self) -> None:
        """Test creating unknown builder raises."""
        with pytest.raises(KeyError, match="No builder registered"):
            BuilderRegistry.create("nonexistent_builder")

    def test_list_builders(self) -> None:
        """Test listing builders."""
        builders = BuilderRegistry.list_builders()
        assert isinstance(builders, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])