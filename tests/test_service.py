"""Tests for service management utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.service import (
    ServiceStatus,
    ServiceInfo,
    Service,
    FunctionService,
    ServiceManager,
    ServiceRegistry,
)


class DummyService(Service):
    """Dummy service for testing."""

    def __init__(self, name: str = "test"):
        super().__init__(name)
        self._started = False
        self._stopped = False

    def start(self) -> None:
        self._status = ServiceStatus.STARTING
        self._started = True
        self._status = ServiceStatus.RUNNING

    def stop(self) -> None:
        self._status = ServiceStatus.STOPPING
        self._stopped = True
        self._status = ServiceStatus.STOPPED


class TestServiceStatus:
    """Tests for ServiceStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert ServiceStatus.STOPPED.value == "stopped"
        assert ServiceStatus.RUNNING.value == "running"


class TestServiceInfo:
    """Tests for ServiceInfo."""

    def test_create(self) -> None:
        """Test creating info."""
        info = ServiceInfo(name="test", status=ServiceStatus.RUNNING)
        assert info.name == "test"
        assert info.status == ServiceStatus.RUNNING


class TestService:
    """Tests for Service."""

    def test_create(self) -> None:
        """Test creating service."""
        service = DummyService("test")
        assert service.name == "test"
        assert service.status == ServiceStatus.STOPPED

    def test_is_running(self) -> None:
        """Test is_running."""
        service = DummyService()
        assert service.is_running is False
        service.start()
        assert service.is_running is True


class TestFunctionService:
    """Tests for FunctionService."""

    def test_create(self) -> None:
        """Test creating service."""
        started = []
        stopped = []

        service = FunctionService(
            name="test",
            start_func=lambda: started.append(1),
            stop_func=lambda: stopped.append(1),
        )

        assert service.name == "test"

    def test_start_stop(self) -> None:
        """Test starting and stopping."""
        started = []
        stopped = []

        service = FunctionService(
            name="test",
            start_func=lambda: started.append(1),
            stop_func=lambda: stopped.append(1),
        )

        service.start()
        assert service.is_running
        assert len(started) == 1

        service.stop()
        assert not service.is_running
        assert len(stopped) == 1


class TestServiceManager:
    """Tests for ServiceManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = ServiceManager()
        assert len(manager._services) == 0

    def test_register(self) -> None:
        """Test registering service."""
        manager = ServiceManager()
        service = DummyService("test")
        manager.register(service)
        assert manager.get_service("test") is service

    def test_unregister(self) -> None:
        """Test unregistering service."""
        manager = ServiceManager()
        service = DummyService("test")
        manager.register(service)
        manager.unregister("test")
        assert manager.get_service("test") is None

    def test_start_stop(self) -> None:
        """Test starting and stopping service."""
        manager = ServiceManager()
        service = DummyService("test")
        manager.register(service)
        manager.start("test")
        assert service.is_running
        manager.stop("test")
        assert not service.is_running

    def test_add_dependency(self) -> None:
        """Test adding dependency."""
        manager = ServiceManager()
        manager.add_dependency("a", "b")
        assert "b" in manager._dependencies["a"]

    def test_get_status(self) -> None:
        """Test getting status."""
        manager = ServiceManager()
        service = DummyService("test")
        manager.register(service)
        status = manager.get_status()
        assert "test" in status


class TestServiceRegistry:
    """Tests for ServiceRegistry."""

    def test_singleton(self) -> None:
        """Test singleton behavior."""
        reg1 = ServiceRegistry()
        reg2 = ServiceRegistry()
        assert reg1 is reg2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])