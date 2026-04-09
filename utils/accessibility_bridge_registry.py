"""Accessibility bridge registry for managing bridge instances."""
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import threading


class BridgeStatus(Enum):
    """Status of an accessibility bridge."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()
    RECONNECTING = auto()


@dataclass
class BridgeEntry:
    """Registry entry for a bridge."""
    name: str
    bridge_type: str
    status: BridgeStatus = BridgeStatus.DISCONNECTED
    priority: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class AccessibilityBridgeRegistry:
    """Central registry for managing accessibility bridge instances.
    
    Provides registration, lookup, health monitoring, and failover
    coordination for multiple accessibility bridges.
    
    Example:
        registry = AccessibilityBridgeRegistry()
        registry.register("primary", "uia", priority=10)
        registry.register("fallback", "ax", priority=5)
        bridge = registry.get_active()
    """

    def __init__(self) -> None:
        self._bridges: Dict[str, BridgeEntry] = {}
        self._lock = threading.RLock()
        self._listeners: List[Callable] = []

    def register(
        self,
        name: str,
        bridge_type: str,
        priority: int = 0,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a new bridge."""
        with self._lock:
            self._bridges[name] = BridgeEntry(
                name=name,
                bridge_type=bridge_type,
                priority=priority,
                enabled=enabled,
                metadata=metadata or {},
            )

    def unregister(self, name: str) -> bool:
        """Unregister a bridge."""
        with self._lock:
            return bool(self._bridges.pop(name, None))

    def get(self, name: str) -> Optional[BridgeEntry]:
        """Get bridge entry by name."""
        return self._bridges.get(name)

    def get_active(self) -> Optional[BridgeEntry]:
        """Get the highest priority active bridge."""
        with self._lock:
            active = [b for b in self._bridges.values() if b.enabled and b.status == BridgeStatus.CONNECTED]
            if not active:
                return None
            return max(active, key=lambda b: b.priority)

    def update_status(self, name: str, status: BridgeStatus) -> bool:
        """Update bridge status and notify listeners."""
        with self._lock:
            if name not in self._bridges:
                return False
            self._bridges[name].status = status
            for listener in self._listeners:
                try:
                    listener(name, status)
                except Exception:
                    pass
            return True

    def list_all(self) -> List[BridgeEntry]:
        """List all registered bridges."""
        return list(self._bridges.values())

    def list_by_status(self, status: BridgeStatus) -> List[BridgeEntry]:
        """List bridges with a specific status."""
        return [b for b in self._bridges.values() if b.status == status]

    def add_status_listener(self, listener: Callable) -> None:
        """Add a status change listener."""
        self._listeners.append(listener)

    def remove_status_listener(self, listener: Callable) -> None:
        """Remove a status change listener."""
        self._listeners.remove(listener)
