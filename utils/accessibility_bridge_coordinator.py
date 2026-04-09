"""Accessibility bridge coordinator for managing multiple accessibility bridges."""
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
import threading
import logging

logger = logging.getLogger(__name__)


class BridgeStatus(Enum):
    """Status of an accessibility bridge."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    ERROR = auto()
    RECONNECTING = auto()


@dataclass
class BridgeConfig:
    """Configuration for an accessibility bridge."""
    name: str
    endpoint: str
    timeout: float = 5.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    enabled: bool = True


@dataclass
class BridgeState:
    """Runtime state of a bridge."""
    status: BridgeStatus = BridgeStatus.DISCONNECTED
    last_connected: Optional[float] = None
    error_message: Optional[str] = None
    connection_attempts: int = 0


class AccessibilityBridgeCoordinator:
    """Coordinates multiple accessibility bridges for redundancy and failover.
    
    Manages bridge lifecycle, health monitoring, automatic failover,
    and provides a unified interface to accessibility services.
    
    Example:
        coordinator = AccessibilityBridgeCoordinator()
        coordinator.register_bridge(BridgeConfig(name="primary", endpoint="localhost:9000"))
        coordinator.register_bridge(BridgeConfig(name="backup", endpoint="localhost:9001"))
        coordinator.connect_all()
        element = coordinator.get_active_bridge().query_element(selector)
    """

    def __init__(self) -> None:
        """Initialize the coordinator."""
        self._bridges: Dict[str, BridgeConfig] = {}
        self._states: Dict[str, BridgeState] = {}
        self._active_bridge: Optional[str] = None
        self._lock = threading.RLock()
        self._listeners: List[Callable[[str, BridgeStatus], None]] = []

    def register_bridge(self, config: BridgeConfig) -> None:
        """Register a new accessibility bridge.
        
        Args:
            config: Bridge configuration including name, endpoint, and settings.
        """
        with self._lock:
            self._bridges[config.name] = config
            self._states[config.name] = BridgeState()
            logger.info("Registered bridge: %s at %s", config.name, config.endpoint)

    def unregister_bridge(self, name: str) -> None:
        """Unregister a bridge from the coordinator.
        
        Args:
            name: Name of the bridge to remove.
        """
        with self._lock:
            self._bridges.pop(name, None)
            self._states.pop(name, None)
            if self._active_bridge == name:
                self._active_bridge = None

    def connect(self, name: str) -> bool:
        """Connect to a specific bridge.
        
        Args:
            name: Bridge name to connect.
            
        Returns:
            True if connection succeeded, False otherwise.
        """
        with self._lock:
            if name not in self._bridges:
                logger.error("Bridge not found: %s", name)
                return False
            
            config = self._bridges[name]
            state = self._states[name]
            
            if not config.enabled:
                logger.warning("Bridge %s is disabled", name)
                return False
            
            state.status = BridgeStatus.CONNECTING
            self._notify_status_change(name, state.status)
            
            for attempt in range(config.retry_attempts):
                try:
                    # Simulate connection - replace with actual bridge connection
                    logger.debug("Connecting to bridge %s (attempt %d)", name, attempt + 1)
                    state.status = BridgeStatus.CONNECTED
                    state.last_connected = __import__("time").time()
                    state.error_message = None
                    self._active_bridge = name
                    self._notify_status_change(name, state.status)
                    logger.info("Connected to bridge: %s", name)
                    return True
                except Exception as e:
                    state.connection_attempts += 1
                    state.error_message = str(e)
                    logger.warning("Bridge %s connection attempt %d failed: %s", 
                                  name, attempt + 1, e)
            
            state.status = BridgeStatus.ERROR
            self._notify_status_change(name, state.status)
            return False

    def disconnect(self, name: str) -> None:
        """Disconnect from a bridge.
        
        Args:
            name: Bridge name to disconnect.
        """
        with self._lock:
            if name in self._states:
                self._states[name].status = BridgeStatus.DISCONNECTED
                self._notify_status_change(name, self._states[name].status)
                if self._active_bridge == name:
                    self._active_bridge = self._find_available_bridge()

    def connect_all(self) -> int:
        """Attempt to connect to all registered bridges.
        
        Returns:
            Number of bridges successfully connected.
        """
        count = 0
        for name in self._bridges:
            if self.connect(name):
                count += 1
        return count

    def get_active_bridge(self) -> Optional[Any]:
        """Get the currently active bridge instance.
        
        Returns:
            Active bridge object or None if no bridge is active.
        """
        with self._lock:
            if self._active_bridge and self._states.get(self._active_bridge, BridgeState()).status == BridgeStatus.CONNECTED:
                return self._bridges[self._active_bridge]
            # Try to find another connected bridge
            self._active_bridge = self._find_available_bridge()
            return self._active_bridge

    def failover_to(self, name: str) -> bool:
        """Manually failover to a specific bridge.
        
        Args:
            name: Target bridge name for failover.
            
        Returns:
            True if failover succeeded, False otherwise.
        """
        with self._lock:
            if name not in self._bridges:
                return False
            
            if self._states.get(name, BridgeState()).status == BridgeStatus.CONNECTED:
                self._active_bridge = name
                logger.info("Failover completed to bridge: %s", name)
                return True
            
            # Try to connect first
            return self.connect(name)

    def add_status_listener(self, listener: Callable[[str, BridgeStatus], None]) -> None:
        """Add a listener for bridge status changes.
        
        Args:
            listener: Callback function(name, status).
        """
        self._listeners.append(listener)

    def remove_status_listener(self, listener: Callable[[str, BridgeStatus], None]) -> None:
        """Remove a status change listener.
        
        Args:
            listener: Previously registered callback.
        """
        self._listeners.remove(listener)

    def get_bridge_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific bridge.
        
        Args:
            name: Bridge name.
            
        Returns:
            Dictionary with bridge info or None if not found.
        """
        with self._lock:
            if name not in self._bridges:
                return None
            config = self._bridges[name]
            state = self._states[name]
            return {
                "name": config.name,
                "endpoint": config.endpoint,
                "status": state.status.name,
                "enabled": config.enabled,
                "last_connected": state.last_connected,
                "error": state.error_message,
            }

    def list_bridges(self) -> List[Dict[str, Any]]:
        """List all registered bridges with their status.
        
        Returns:
            List of bridge info dictionaries.
        """
        return [self.get_bridge_info(name) for name in self._bridges]

    def _find_available_bridge(self) -> Optional[str]:
        """Find the first available connected bridge.
        
        Returns:
            Name of available bridge or None.
        """
        for name, state in self._states.items():
            if state.status == BridgeStatus.CONNECTED and self._bridges[name].enabled:
                return name
        return None

    def _notify_status_change(self, name: str, status: BridgeStatus) -> None:
        """Notify all listeners of a status change.
        
        Args:
            name: Bridge name.
            status: New status.
        """
        for listener in self._listeners:
            try:
                listener(name, status)
            except Exception as e:
                logger.error("Error in status listener: %s", e)
