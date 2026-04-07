"""Tests for dependency injection utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.dependency import Container, ServiceLocator


class DummyService:
    """Dummy service for testing."""
    def __init__(self):
        self.value = "test"


class TestContainer:
    """Tests for Container."""

    def test_register_and_resolve(self) -> None:
        """Test registering and resolving a service."""
        container = Container()

        container.register(DummyService, lambda c: DummyService())
        service = container.resolve(DummyService)

        assert isinstance(service, DummyService)

    def test_register_instance(self) -> None:
        """Test registering an instance."""
        container = Container()
        service = DummyService()

        container.register_instance(DummyService, service)
        resolved = container.resolve(DummyService)

        assert resolved is service

    def test_singleton(self) -> None:
        """Test singleton behavior."""
        container = Container()

        container.register(DummyService, lambda c: DummyService(), singleton=True)

        s1 = container.resolve(DummyService)
        s2 = container.resolve(DummyService)

        assert s1 is s2

    def test_has(self) -> None:
        """Test has method."""
        container = Container()
        container.register(DummyService, lambda c: DummyService())

        assert container.has(DummyService)
        assert not container.has(str)

    def test_create_child(self) -> None:
        """Test creating child container."""
        parent = Container()
        child = parent.create_child()

        assert child._parent is parent


class TestServiceLocator:
    """Tests for ServiceLocator."""

    def test_register_and_resolve(self) -> None:
        """Test registering and resolving globally."""
        ServiceLocator.reset()
        ServiceLocator.register(DummyService, lambda c: DummyService())

        service = ServiceLocator.resolve(DummyService)

        assert isinstance(service, DummyService)

    def test_reset(self) -> None:
        """Test resetting global container."""
        ServiceLocator.register(DummyService, lambda c: DummyService())
        ServiceLocator.reset()

        assert not ServiceLocator.get_container().has(DummyService)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])