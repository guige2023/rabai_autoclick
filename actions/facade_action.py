"""Facade action module for RabAI AutoClick.

Provides facade pattern implementation:
- Facade: Unified interface to subsystems
- SubsystemClasses: Individual subsystem components
- Factory: Facade factory
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class SubsystemResult:
    """Result from a subsystem operation."""
    subsystem: str
    success: bool
    data: Any = None
    error: str = ""


class Subsystem:
    """Base subsystem class."""

    def __init__(self, name: str):
        self.name = name

    def process(self, operation: str, data: Any) -> SubsystemResult:
        """Process an operation."""
        try:
            result = getattr(self, operation, lambda d: None)(data)
            return SubsystemResult(subsystem=self.name, success=True, data=result)
        except Exception as e:
            return SubsystemResult(subsystem=self.name, success=False, error=str(e))

    def get_name(self) -> str:
        """Get subsystem name."""
        return self.name


class DatabaseSubsystem(Subsystem):
    """Database subsystem."""

    def __init__(self):
        super().__init__("database")
        self._tables: Dict[str, List[Dict]] = {}

    def query(self, data: Dict) -> Any:
        """Execute a query."""
        table = data.get("table", "")
        if table not in self._tables:
            self._tables[table] = []
        return {"table": table, "count": len(self._tables[table])}

    def insert(self, data: Dict) -> Any:
        """Insert a record."""
        table = data.get("table", "")
        record = data.get("record", {})
        if table not in self._tables:
            self._tables[table] = []
        self._tables[table].append(record)
        return {"inserted": 1}

    def update(self, data: Dict) -> Any:
        """Update records."""
        return {"updated": 0}

    def delete(self, data: Dict) -> Any:
        """Delete records."""
        return {"deleted": 0}


class CacheSubsystem(Subsystem):
    """Cache subsystem."""

    def __init__(self):
        super().__init__("cache")
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}

    def get(self, data: Dict) -> Any:
        """Get from cache."""
        key = data.get("key", "")
        return {"key": key, "value": self._cache.get(key)}

    def set(self, data: Dict) -> Any:
        """Set cache value."""
        key = data.get("key", "")
        value = data.get("value")
        self._cache[key] = value
        return {"key": key, "set": True}

    def delete(self, data: Dict) -> Any:
        """Delete from cache."""
        key = data.get("key", "")
        if key in self._cache:
            del self._cache[key]
            return {"deleted": 1}
        return {"deleted": 0}

    def clear(self, data: Dict) -> Any:
        """Clear cache."""
        count = len(self._cache)
        self._cache.clear()
        return {"cleared": count}


class NetworkSubsystem(Subsystem):
    """Network subsystem."""

    def __init__(self):
        super().__init__("network")
        self._requests: List[Dict] = []

    def request(self, data: Dict) -> Any:
        """Make a network request."""
        url = data.get("url", "")
        method = data.get("method", "GET")
        self._requests.append({"url": url, "method": method})
        return {"url": url, "status": "sent"}

    def fetch(self, data: Dict) -> Any:
        """Fetch data."""
        url = data.get("url", "")
        return {"url": url, "data": None, "status": "fetched"}


class LoggingSubsystem(Subsystem):
    """Logging subsystem."""

    def __init__(self):
        super().__init__("logging")
        self._logs: List[Dict] = []

    def log(self, data: Dict) -> Any:
        """Log a message."""
        level = data.get("level", "info")
        message = data.get("message", "")
        self._logs.append({"level": level, "message": message})
        return {"logged": 1}

    def get_logs(self, data: Dict) -> Any:
        """Get logs."""
        level = data.get("level")
        if level:
            return {"logs": [l for l in self._logs if l["level"] == level]}
        return {"logs": self._logs.copy()}


class Facade:
    """Facade providing unified interface to subsystems."""

    def __init__(self):
        self._subsystems: Dict[str, Subsystem] = {}
        self._register_default_subsystems()

    def _register_default_subsystems(self) -> None:
        """Register default subsystems."""
        self.register_subsystem(DatabaseSubsystem())
        self.register_subsystem(CacheSubsystem())
        self.register_subsystem(NetworkSubsystem())
        self.register_subsystem(LoggingSubsystem())

    def register_subsystem(self, subsystem: Subsystem) -> None:
        """Register a subsystem."""
        self._subsystems[subsystem.get_name()] = subsystem

    def unregister_subsystem(self, name: str) -> bool:
        """Unregister a subsystem."""
        if name in self._subsystems:
            del self._subsystems[name]
            return True
        return False

    def execute(self, subsystem_name: str, operation: str, data: Any = None) -> SubsystemResult:
        """Execute operation on subsystem."""
        subsystem = self._subsystems.get(subsystem_name)
        if not subsystem:
            return SubsystemResult(
                subsystem=subsystem_name,
                success=False,
                error=f"Subsystem not found: {subsystem_name}",
            )
        return subsystem.process(operation, data or {})

    def execute_all(self, operations: List[Dict]) -> List[SubsystemResult]:
        """Execute multiple operations."""
        results = []
        for op in operations:
            subsystem = op.get("subsystem")
            operation = op.get("operation")
            data = op.get("data")
            if subsystem and operation:
                results.append(self.execute(subsystem, operation, data))
        return results

    def batch(self, operations: List[Dict]) -> Dict[str, SubsystemResult]:
        """Execute batch operations."""
        results = {}
        for op in operations:
            op_id = op.get("id", str(uuid.uuid4()))
            subsystem = op.get("subsystem")
            operation = op.get("operation")
            data = op.get("data")
            if subsystem and operation:
                results[op_id] = self.execute(subsystem, operation, data)
        return results

    def list_subsystems(self) -> List[str]:
        """List all subsystems."""
        return list(self._subsystems.keys())


class FacadeAction(BaseAction):
    """Facade pattern action."""
    action_type = "facade"
    display_name = "门面模式"
    description = "统一接口访问子系统"

    def __init__(self):
        super().__init__()
        self._facade = Facade()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "execute":
                return self._execute(params)
            elif operation == "batch":
                return self._batch(params)
            elif operation == "register":
                return self._register_subsystem(params)
            elif operation == "unregister":
                return self._unregister_subsystem(params)
            elif operation == "list":
                return self._list_subsystems()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Facade error: {str(e)}")

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a subsystem operation."""
        subsystem = params.get("subsystem")
        op = params.get("operation")
        data = params.get("data", {})

        if not subsystem or not op:
            return ActionResult(success=False, message="subsystem and operation are required")

        result = self._facade.execute(subsystem, op, data)

        return ActionResult(
            success=result.success,
            message=f"{subsystem}.{op}: {'OK' if result.success else result.error}",
            data={"result": result.data, "error": result.error},
        )

    def _batch(self, params: Dict[str, Any]) -> ActionResult:
        """Execute batch operations."""
        operations = params.get("operations", [])

        if not operations:
            return ActionResult(success=False, message="operations is required")

        results = self._facade.batch(operations)
        successful = sum(1 for r in results.values() if r.success)

        return ActionResult(
            success=successful == len(results),
            message=f"Batch: {successful}/{len(results)} successful",
            data={
                "total": len(results),
                "successful": successful,
                "results": {k: {"success": v.success, "data": v.data, "error": v.error} for k, v in results.items()},
            },
        )

    def _register_subsystem(self, params: Dict[str, Any]) -> ActionResult:
        """Register a subsystem."""
        subsystem_type = params.get("type", "database")

        subsystem_map = {
            "database": DatabaseSubsystem,
            "cache": CacheSubsystem,
            "network": NetworkSubsystem,
            "logging": LoggingSubsystem,
        }

        if subsystem_type not in subsystem_map:
            return ActionResult(success=False, message=f"Unknown subsystem type: {subsystem_type}")

        subsystem = subsystem_map[subsystem_type]()
        self._facade.register_subsystem(subsystem)

        return ActionResult(success=True, message=f"Subsystem registered: {subsystem_type}")

    def _unregister_subsystem(self, params: Dict[str, Any]) -> ActionResult:
        """Unregister a subsystem."""
        name = params.get("name")
        if not name:
            return ActionResult(success=False, message="name is required")

        success = self._facade.unregister_subsystem(name)
        return ActionResult(success=success, message="Subsystem unregistered" if success else "Subsystem not found")

    def _list_subsystems(self) -> ActionResult:
        """List all subsystems."""
        subsystems = self._facade.list_subsystems()
        return ActionResult(success=True, message=f"{len(subsystems)} subsystems", data={"subsystems": subsystems})
