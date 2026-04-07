"""Tests for visitor utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.visitor import (
    ConcreteElement,
    CompositeElement,
    TreeBuilder,
    Visitor,
    Traverser,
)


class TestConcreteElement:
    """Tests for ConcreteElement."""

    def test_creation(self) -> None:
        """Test element creation."""
        element = ConcreteElement("test", {"key": "value"})
        assert element.name == "test"
        assert element.data["key"] == "value"


class TestCompositeElement:
    """Tests for CompositeElement."""

    def test_add_child(self) -> None:
        """Test adding children."""
        parent = CompositeElement("parent")
        child = ConcreteElement("child")
        parent.add(child)

        assert len(parent.children) == 1

    def test_remove_child(self) -> None:
        """Test removing children."""
        parent = CompositeElement("parent")
        child = ConcreteElement("child")
        parent.add(child)
        result = parent.remove(child)

        assert result is True
        assert len(parent.children) == 0


class TestTreeBuilder:
    """Tests for TreeBuilder."""

    def test_build_tree(self) -> None:
        """Test building tree."""
        builder = TreeBuilder()
        root = (
            builder
            .root("root")
            .node("child1")
            .node("child2")
            .build()
        )

        assert root is not None
        assert root.name == "root"
        assert len(root.children) == 2


class TestTraverser:
    """Tests for Traverser."""

    def test_flatten(self) -> None:
        """Test flattening tree."""
        root = CompositeElement("root")
        root.add(ConcreteElement("child1"))
        root.add(ConcreteElement("child2"))

        flat = Traverser.flatten(root)
        assert len(flat) == 3

    def test_depth(self) -> None:
        """Test getting depth."""
        root = CompositeElement("root")
        child = CompositeElement("child")
        root.add(child)
        child.add(ConcreteElement("grandchild"))

        assert Traverser.depth(root) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])