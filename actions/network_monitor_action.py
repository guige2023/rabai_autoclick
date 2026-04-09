"""Network monitor action for tracking network connections and bandwidth.

Monitors network interfaces, connection states, and provides
bandwidth usage statistics.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    ESTABLISHED = "established"
    LISTENING = "listening"
    TIME_WAIT = "time_wait"
    CLOSE_WAIT = "close_wait"
    SYN_SENT = "syn_sent"
    SYN_RECV = "syn_recv"


@dataclass
class NetworkConnection:
    local_addr: str
    local_port: int
    remote_addr: str
    remote_port: int
    state: ConnectionState
    pid: Optional[int] = None
    protocol: str = "tcp"


@dataclass
class BandwidthSample:
    bytes_sent: int
    bytes_recv: int
    packets_sent: int
    packets_recv: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class NetworkStats:
    total_connections: int = 0
    established_connections: int = 0
    bandwidth_samples: list[BandwidthSample] = field(default_factory=list)
    top_talkers: list[tuple[str, int]] = field(default_factory=list)


class NetworkMonitorAction:
    """Monitor network connections and bandwidth usage.

    Args:
        interface: Network interface to monitor (None for all).
        sample_interval: Bandwidth sampling interval in seconds.
    """

    def __init__(
        self,
        interface: Optional[str] = None,
        sample_interval: float = 1.0,
    ) -> None:
        self._interface = interface
        self._sample_interval = sample_interval
        self._connections: dict[str, NetworkConnection] = {}
        self._bandwidth_history: list[BandwidthSample] = []
        self._max_history = 3600
        self._last_sample: Optional[BandwidthSample] = None
        self._monitoring = False

    def start_monitoring(self) -> None:
        """Start network monitoring."""
        self._monitoring = True
        logger.info(f"Network monitor started (interface: {self._interface or 'all'})")

    def stop_monitoring(self) -> None:
        """Stop network monitoring."""
        self._monitoring = False
        logger.info("Network monitor stopped")

    def is_monitoring(self) -> bool:
        """Check if monitoring is active.

        Returns:
            True if monitoring is active.
        """
        return self._monitoring

    def get_connections(
        self,
        state_filter: Optional[ConnectionState] = None,
        protocol: Optional[str] = None,
    ) -> list[NetworkConnection]:
        """Get current network connections.

        Args:
            state_filter: Filter by connection state.
            protocol: Filter by protocol ('tcp' or 'udp').

        Returns:
            List of network connections.
        """
        connections = list(self._connections.values())
        if state_filter:
            connections = [c for c in connections if c.state == state_filter]
        if protocol:
            connections = [c for c in connections if c.protocol == protocol]
        return connections

    def get_established_count(self) -> int:
        """Get count of established connections.

        Returns:
            Number of established connections.
        """
        return sum(1 for c in self._connections.values() if c.state == ConnectionState.ESTABLISHED)

    def add_connection(self, connection: NetworkConnection) -> str:
        """Add a network connection to tracking.

        Args:
            connection: Connection to track.

        Returns:
            Connection key.
        """
        key = self._make_key(connection)
        self._connections[key] = connection
        return key

    def remove_connection(self, key: str) -> bool:
        """Remove a connection from tracking.

        Args:
            key: Connection key to remove.

        Returns:
            True if connection was found and removed.
        """
        if key in self._connections:
            del self._connections[key]
            return True
        return False

    def _make_key(self, conn: NetworkConnection) -> str:
        """Generate a unique key for a connection.

        Args:
            conn: Network connection.

        Returns:
            Unique connection key.
        """
        return f"{conn.protocol}:{conn.local_addr}:{conn.local_port}->{conn.remote_addr}:{conn.remote_port}"

    def record_bandwidth(self, sample: BandwidthSample) -> None:
        """Record a bandwidth sample.

        Args:
            sample: Bandwidth sample to record.
        """
        self._bandwidth_history.append(sample)
        if len(self._bandwidth_history) > self._max_history:
            self._bandwidth_history.pop(0)
        self._last_sample = sample

    def get_bandwidth_stats(
        self,
        window_seconds: Optional[float] = None,
    ) -> dict[str, Any]:
        """Get bandwidth statistics.

        Args:
            window_seconds: Time window for stats (None for all history).

        Returns:
            Dictionary with bandwidth statistics.
        """
        samples = self._bandwidth_history
        if window_seconds:
            cutoff = time.time() - window_seconds
            samples = [s for s in samples if s.timestamp >= cutoff]

        if not samples:
            return {
                "bytes_sent": 0,
                "bytes_recv": 0,
                "packets_sent": 0,
                "packets_recv": 0,
                "sample_count": 0,
            }

        first = samples[0]
        last = samples[-1]
        time_delta = last.timestamp - first.timestamp or 1.0

        bytes_sent_delta = last.bytes_sent - first.bytes_sent
        bytes_recv_delta = last.bytes_recv - first.bytes_recv
        packets_sent_delta = last.packets_sent - first.packets_sent
        packets_recv_delta = last.packets_recv - first.packets_recv

        return {
            "bytes_sent": bytes_sent_delta,
            "bytes_recv": bytes_recv_delta,
            "packets_sent": packets_sent_delta,
            "packets_recv": packets_recv_delta,
            "send_rate_bps": bytes_sent_delta * 8 / time_delta,
            "recv_rate_bps": bytes_recv_delta * 8 / time_delta,
            "send_rate_mbps": bytes_sent_delta * 8 / time_delta / 1_000_000,
            "recv_rate_mbps": bytes_recv_delta * 8 / time_delta / 1_000_000,
            "sample_count": len(samples),
            "time_window": time_delta,
        }

    def get_connection_stats(self) -> dict[str, Any]:
        """Get connection statistics.

        Returns:
            Dictionary with connection stats.
        """
        by_state: dict[str, int] = {}
        by_protocol: dict[str, int] = {}

        for conn in self._connections.values():
            state_name = conn.state.value
            by_state[state_name] = by_state.get(state_name, 0) + 1
            by_protocol[conn.protocol] = by_protocol.get(conn.protocol, 0) + 1

        return {
            "total": len(self._connections),
            "by_state": by_state,
            "by_protocol": by_protocol,
        }

    def clear_connections(self) -> int:
        """Clear all tracked connections.

        Returns:
            Number of connections cleared.
        """
        count = len(self._connections)
        self._connections.clear()
        return count

    def clear_bandwidth_history(self) -> int:
        """Clear bandwidth history.

        Returns:
            Number of samples cleared.
        """
        count = len(self._bandwidth_history)
        self._bandwidth_history.clear()
        self._last_sample = None
        return count
