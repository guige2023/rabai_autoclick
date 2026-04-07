"""Service management utilities for RabAI AutoClick.

Provides:
- Service lifecycle management
- Service dependencies
- Service health checks
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class ServiceStatus(Enum):
    """Service status."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


@dataclass
class ServiceInfo:
    """Service information."""
    name: str
    status: ServiceStatus
    health: bool = True
    error: Optional[str] = None


class Service(ABC):
    """Base service class."""

    def __init__(self, name: str) -> None:
        """Initialize service.

        Args:
            name: Service name.
        """
        self._name = name
        self._status = ServiceStatus.STOPPED
        self._health = True

    @property
    def name(self) -> str:
        """Get service name."""
        return self._name

    @property
    def status(self) -> ServiceStatus:
        """Get service status."""
        return self._status

    @property
    def is_running(self) -> bool:
        """Check if running."""
        return self._status == ServiceStatus.RUNNING

    @abstractmethod
    def start(self) -> None:
        """Start service."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop service."""
        pass

    def health_check(self) -> bool:
        """Check service health.

        Returns:
            True if healthy.
        """
        return self._health

    def get_info(self) -> ServiceInfo:
        """Get service info.

        Returns:
            Service information.
        """
        return ServiceInfo(
            name=self._name,
            status=self._status,
            health=self._health,
        )


class FunctionService(Service):
    """Service wrapping start/stop functions."""

    def __init__(
        self,
        name: str,
        start_func: Callable[[], None],
        stop_func: Callable[[], None],
        health_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Initialize function service.

        Args:
            name: Service name.
            start_func: Function to call on start.
            stop_func: Function to call on stop.
            health_check: Optional health check function.
        """
        super().__init__(name)
        self._start_func = start_func
        self._stop_func = stop_func
        self._health_check = health_check

    def start(self) -> None:
        """Start service."""
        self._status = ServiceStatus.STARTING
        try:
            self._start_func()
            self._status = ServiceStatus.RUNNING
        except Exception as e:
            self._status = ServiceStatus.FAILED
            raise

    def stop(self) -> None:
        """Stop service."""
        self._status = ServiceStatus.STOPPING
        try:
            self._stop_func()
        finally:
            self._status = ServiceStatus.STOPPED

    def health_check(self) -> bool:
        """Check service health."""
        if self._health_check:
            return self._health_check()
        return True


class ServiceManager:
    """Manage multiple services."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._services: Dict[str, Service] = {}
        self._dependencies: Dict[str, List[str]] = {}

    def register(self, service: Service) -> None:
        """Register a service.

        Args:
            service: Service to register.
        """
        self._services[service.name] = service

    def unregister(self, name: str) -> bool:
        """Unregister a service.

        Args:
            name: Service name.

        Returns:
            True if removed.
        """
        if name in self._services:
            del self._services[name]
            return True
        return False

    def get_service(self, name: str) -> Optional[Service]:
        """Get service by name.

        Args:
            name: Service name.

        Returns:
            Service or None.
        """
        return self._services.get(name)

    def start(self, name: str) -> bool:
        """Start a service.

        Args:
            name: Service name.

        Returns:
            True if started successfully.
        """
        service = self._services.get(name)
        if not service:
            return False

        # Check dependencies
        deps = self._dependencies.get(name, [])
        for dep in deps:
            dep_service = self._services.get(dep)
            if dep_service and not dep_service.is_running:
                dep_service.start()

        try:
            service.start()
            return True
        except Exception:
            return False

    def stop(self, name: str) -> bool:
        """Stop a service.

        Args:
            name: Service name.

        Returns:
            True if stopped successfully.
        """
        service = self._services.get(name)
        if not service:
            return False

        try:
            service.stop()
            return True
        except Exception:
            return False

    def start_all(self) -> int:
        """Start all services.

        Returns:
            Number of services started.
        """
        started = 0
        for name in self._services:
            if self.start(name):
                started += 1
        return started

    def stop_all(self) -> int:
        """Stop all services.

        Returns:
            Number of services stopped.
        """
        stopped = 0
        for name in self._services:
            if self.stop(name):
                stopped += 1
        return stopped

    def add_dependency(self, service: str, depends_on: str) -> None:
        """Add service dependency.

        Args:
            service: Service name.
            depends_on: Dependency name.
        """
        if service not in self._dependencies:
            self._dependencies[service] = []
        if depends_on not in self._dependencies[service]:
            self._dependencies[service].append(depends_on)

    def get_status(self) -> Dict[str, ServiceInfo]:
        """Get status of all services.

        Returns:
            Dict of service info.
        """
        return {name: s.get_info() for name, s in self._services.items()}

    def check_health(self) -> Dict[str, bool]:
        """Check health of all services.

        Returns:
            Dict of service name to health status.
        """
        return {name: s.health_check() for name, s in self._services.items()}


class ServiceRegistry:
    """Global service registry (singleton)."""

    _instance: Optional["ServiceRegistry"] = None

    def __new__(cls) -> "ServiceRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._manager = ServiceManager()
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, "_manager"):
            self._manager = ServiceManager()

    def register(self, service: Service) -> None:
        """Register service."""
        self._manager.register(service)

    def get(self, name: str) -> Optional[Service]:
        """Get service."""
        return self._manager.get_service(name)

    def start(self, name: str) -> bool:
        """Start service."""
        return self._manager.start(name)

    def stop(self, name: str) -> bool:
        """Stop service."""
        return self._manager.stop(name)

    def start_all(self) -> int:
        """Start all services."""
        return self._manager.start_all()

    def stop_all(self) -> int:
        """Stop all services."""
        return self._manager.stop_all()

    def get_status(self) -> Dict[str, ServiceInfo]:
        """Get status."""
        return self._manager.get_status()
