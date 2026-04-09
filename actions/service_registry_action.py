"""
Service registry action for microservice discovery.

Provides registration, health checking, and service lookup.
"""

from typing import Any, Dict, Optional
import time
import threading
import hashlib


class ServiceRegistryAction:
    """Service registry for microservice discovery and health tracking."""

    def __init__(
        self,
        health_check_interval: float = 30.0,
        deregister_timeout: float = 60.0,
        max_services: int = 1000,
    ) -> None:
        """
        Initialize service registry.

        Args:
            health_check_interval: Seconds between health checks
            deregister_timeout: Seconds before auto-deregistration
            max_services: Maximum registered services
        """
        self.health_check_interval = health_check_interval
        self.deregister_timeout = deregister_timeout
        self.max_services = max_services

        self._services: Dict[str, Dict[str, Any]] = {}
        self._service_instances: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute registry operation.

        Args:
            params: Dictionary containing:
                - operation: 'register', 'deregister', 'discover', 'heartbeat', 'status'
                - service_name: Service identifier
                - instance_id: Service instance identifier
                - endpoint: Service endpoint URL
                - metadata: Service metadata
                - health_check_url: URL for health checking

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "register")

        if operation == "register":
            return self._register_service(params)
        elif operation == "deregister":
            return self._deregister_service(params)
        elif operation == "discover":
            return self._discover_service(params)
        elif operation == "heartbeat":
            return self._process_heartbeat(params)
        elif operation == "status":
            return self._get_status(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _register_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register service in registry."""
        service_name = params.get("service_name", "")
        instance_id = params.get("instance_id", "")
        endpoint = params.get("endpoint", "")
        metadata = params.get("metadata", {})
        health_check_url = params.get("health_check_url")

        if not service_name or not endpoint:
            return {"success": False, "error": "service_name and endpoint are required"}

        if len(self._service_instances) >= self.max_services:
            return {"success": False, "error": "Registry is full"}

        instance_id = instance_id or self._generate_instance_id(service_name, endpoint)

        with self._lock:
            self._service_instances[instance_id] = {
                "service_name": service_name,
                "endpoint": endpoint,
                "metadata": metadata,
                "health_check_url": health_check_url,
                "registered_at": time.time(),
                "last_heartbeat": time.time(),
                "status": "healthy",
            }

            if service_name not in self._services:
                self._services[service_name] = []
            self._services[service_name].append(instance_id)

        return {
            "success": True,
            "service_name": service_name,
            "instance_id": instance_id,
        }

    def _deregister_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Deregister service from registry."""
        instance_id = params.get("instance_id", "")

        if not instance_id:
            return {"success": False, "error": "instance_id is required"}

        with self._lock:
            if instance_id not in self._service_instances:
                return {"success": False, "error": "Instance not found"}

            service_name = self._service_instances[instance_id]["service_name"]

            del self._service_instances[instance_id]

            if service_name in self._services:
                self._services[service_name] = [
                    i for i in self._services[service_name] if i != instance_id
                ]
                if not self._services[service_name]:
                    del self._services[service_name]

        return {"success": True, "instance_id": instance_id}

    def _discover_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Discover service instances."""
        service_name = params.get("service_name", "")
        filter_tags = params.get("tags", [])

        if not service_name:
            return {"success": False, "error": "service_name is required"}

        with self._lock:
            if service_name not in self._services:
                return {"success": True, "instances": []}

            instances = []
            for instance_id in self._services[service_name]:
                instance = self._service_instances.get(instance_id)
                if instance and instance["status"] == "healthy":
                    if not filter_tags or self._matches_tags(instance, filter_tags):
                        instances.append(
                            {
                                "instance_id": instance_id,
                                "endpoint": instance["endpoint"],
                                "metadata": instance["metadata"],
                            }
                        )

        return {"success": True, "instances": instances}

    def _matches_tags(self, instance: dict[str, Any], tags: list[str]) -> bool:
        """Check if instance matches required tags."""
        instance_tags = instance.get("metadata", {}).get("tags", [])
        return all(tag in instance_tags for tag in tags)

    def _process_heartbeat(self, params: dict[str, Any]) -> dict[str, Any]:
        """Process service heartbeat."""
        instance_id = params.get("instance_id", "")

        with self._lock:
            if instance_id not in self._service_instances:
                return {"success": False, "error": "Instance not found"}

            self._service_instances[instance_id]["last_heartbeat"] = time.time()
            self._service_instances[instance_id]["status"] = "healthy"

        return {"success": True, "instance_id": instance_id}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get registry status."""
        with self._lock:
            total_instances = len(self._service_instances)
            healthy_instances = sum(
                1 for i in self._service_instances.values() if i["status"] == "healthy"
            )

            return {
                "success": True,
                "total_services": len(self._services),
                "total_instances": total_instances,
                "healthy_instances": healthy_instances,
                "services": {
                    name: len(instances) for name, instances in self._services.items()
                },
            }

    def _generate_instance_id(self, service_name: str, endpoint: str) -> str:
        """Generate unique instance ID."""
        raw = f"{service_name}:{endpoint}:{time.time()}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def cleanup_stale_services(self) -> int:
        """Remove services that haven't sent heartbeat."""
        now = time.time()
        stale_instances = []

        with self._lock:
            for instance_id, instance in self._service_instances.items():
                if now - instance["last_heartbeat"] > self.deregister_timeout:
                    stale_instances.append(instance_id)

            for instance_id in stale_instances:
                service_name = self._service_instances[instance_id]["service_name"]
                del self._service_instances[instance_id]
                if service_name in self._services:
                    self._services[service_name].remove(instance_id)

        return len(stale_instances)
