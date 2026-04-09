"""
Network latency simulation utilities for testing automation resilience.

Provides latency injection, packet simulation, and network condition
modeling for testing automation under various network conditions.

Example:
    >>> from network_latency_utils import LatencySimulator, PacketDelay
    >>> sim = LatencySimulator(base_delay_ms=100, jitter_ms=20)
    >>> delayed = sim.apply(latency_func)
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# =============================================================================
# Types
# =============================================================================


class LatencyDistribution(Enum):
    """Latency distribution models."""
    CONSTANT = "constant"
    UNIFORM = "uniform"
    NORMAL = "normal"
    EXPONENTIAL = "exponential"
    BIMODAL = "bimodal"
    PACKET_LOSS = "packet_loss"


@dataclass
class LatencyProfile:
    """Profile of network latency characteristics."""
    name: str
    base_delay_ms: float
    jitter_ms: float
    distribution: LatencyDistribution
    packet_loss_rate: float = 0.0
    corruption_rate: float = 0.0


# =============================================================================
# Latency Simulator
# =============================================================================


class LatencySimulator:
    """
    Simulates network latency for testing.

    Example:
        >>> sim = LatencySimulator(base_delay_ms=50, jitter_ms=10)
        >>> sim.apply(my_network_function)
    """

    def __init__(
        self,
        base_delay_ms: float = 0,
        jitter_ms: float = 0,
        distribution: LatencyDistribution = LatencyDistribution.UNIFORM,
    ):
        self.base_delay_ms = base_delay_ms
        self.jitter_ms = jitter_ms
        self.distribution = distribution
        self._enabled = True

    def enable(self) -> None:
        """Enable latency simulation."""
        self._enabled = True

    def disable(self) -> None:
        """Disable latency simulation."""
        self._enabled = False

    def get_delay(self) -> float:
        """
        Get a random delay based on configured distribution.

        Returns:
            Delay in milliseconds.
        """
        if not self._enabled:
            return 0.0

        if self.distribution == LatencyDistribution.CONSTANT:
            return self.base_delay_ms

        elif self.distribution == LatencyDistribution.UNIFORM:
            return self.base_delay_ms + random.uniform(
                -self.jitter_ms, self.jitter_ms
            )

        elif self.distribution == LatencyDistribution.NORMAL:
            return max(0, random.gauss(self.base_delay_ms, self.jitter_ms / 2))

        elif self.distribution == LatencyDistribution.EXPONENTIAL:
            return random.expovariate(1.0 / max(self.base_delay_ms, 0.001))

        elif self.distribution == LatencyDistribution.BIMODAL:
            # Mix of low and high latency
            if random.random() < 0.7:
                return self.base_delay_ms * 0.5
            else:
                return self.base_delay_ms * 2 + random.uniform(0, self.jitter_ms)

        return self.base_delay_ms

    def apply(self, func: Callable) -> Callable:
        """
        Decorator to apply latency simulation to a function.

        Example:
            >>> @sim.apply
            >>> def fetch_data():
            ...     return requests.get("https://api.example.com/data")
        """
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay_s = self.get_delay() / 1000.0
            time.sleep(delay_s)
            return func(*args, **kwargs)

        return wrapper

    async def apply_async(self, func: Callable) -> Callable:
        """
        Decorator for async functions.
        """
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay_s = self.get_delay() / 1000.0
            await asyncio.sleep(delay_s)
            return await func(*args, **kwargs)

        return wrapper


# =============================================================================
# Packet Delay
# =============================================================================


class PacketDelay:
    """
    Simulates variable packet delays.

    Tracks packets and applies individual delays based on network model.

    Example:
        >>> delay = PacketDelay(bandwidth_kbps=1000)
        >>> delayed_packet = delay.send(packet_data)
    """

    def __init__(
        self,
        bandwidth_kbps: float = 1000,
        base_latency_ms: float = 20,
        jitter_ms: float = 10,
    ):
        self.bandwidth_kbps = bandwidth_kbps
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self._queue_delay_ms: float = 0.0

    def calculate_transmission_time(self, size_bytes: int) -> float:
        """
        Calculate transmission time for a packet.

        Args:
            size_bytes: Packet size in bytes.

        Returns:
            Transmission time in milliseconds.
        """
        bits = size_bytes * 8
        kbits = bits / 1000.0
        return (kbits / self.bandwidth_kbps) * 1000

    def get_delay(self, packet_size: int) -> float:
        """
        Get total delay for a packet.

        Args:
            packet_size: Size of packet in bytes.

        Returns:
            Total delay in milliseconds.
        """
        transmission = self.calculate_transmission_time(packet_size)
        jitter = random.uniform(-self.jitter_ms, self.jitter_ms)
        return max(0, self.base_latency_ms + transmission + jitter + self._queue_delay_ms)

    def send(self, data: bytes) -> Tuple[bytes, float]:
        """
        Simulate sending a packet with delay.

        Args:
            data: Packet data.

        Returns:
            Tuple of (data, delay_ms).
        """
        delay = self.get_delay(len(data))
        return data, delay


# =============================================================================
# Network Condition Model
# =============================================================================


@dataclass
class NetworkConditions:
    """Snapshot of network conditions."""
    latency_ms: float
    bandwidth_kbps: float
    packet_loss_rate: float
    corruption_rate: float
    timestamp: float


class NetworkConditionModel:
    """
    Models changing network conditions over time.

    Useful for testing automation under varying conditions.

    Example:
        >>> model = NetworkConditionModel()
        >>> model.add_phase(conditions={"latency_ms": 50, "packet_loss": 0.01})
        >>> model.add_phase(conditions={"latency_ms": 500, "packet_loss": 0.1})
        >>> model.start()
    """

    def __init__(self):
        self._phases: List[Dict[str, float]] = []
        self._current_phase: int = 0
        self._running = False
        self._start_time: float = 0.0

    def add_phase(
        self,
        duration_s: float,
        latency_ms: float = 0,
        bandwidth_kbps: float = 10000,
        packet_loss_rate: float = 0.0,
        corruption_rate: float = 0.0,
    ) -> None:
        """Add a network condition phase."""
        self._phases.append({
            "duration_s": duration_s,
            "latency_ms": latency_ms,
            "bandwidth_kbps": bandwidth_kbps,
            "packet_loss_rate": packet_loss_rate,
            "corruption_rate": corruption_rate,
        })

    def start(self) -> None:
        """Start the network model."""
        self._running = True
        self._start_time = time.monotonic()
        self._current_phase = 0

    def stop(self) -> None:
        """Stop the network model."""
        self._running = False

    def get_conditions(self) -> Optional[NetworkConditions]:
        """Get current network conditions."""
        if not self._phases:
            return None

        elapsed = time.monotonic() - self._start_time
        total_time = 0.0

        for i, phase in enumerate(self._phases):
            total_time += phase["duration_s"]
            if elapsed < total_time:
                return NetworkConditions(
                    latency_ms=phase["latency_ms"],
                    bandwidth_kbps=phase["bandwidth_kbps"],
                    packet_loss_rate=phase["packet_loss_rate"],
                    corruption_rate=phase["corruption_rate"],
                    timestamp=time.monotonic(),
                )

        # Loop back to start
        self._start_time = time.monotonic()
        self._current_phase = 0

        if self._phases:
            phase = self._phases[0]
            return NetworkConditions(
                latency_ms=phase["latency_ms"],
                bandwidth_kbps=phase["bandwidth_kbps"],
                packet_loss_rate=phase["packet_loss_rate"],
                corruption_rate=phase["corruption_rate"],
                timestamp=time.monotonic(),
            )

        return None


# =============================================================================
# Preset Profiles
# =============================================================================


class LatencyProfiles:
    """Predefined latency profiles for common scenarios."""

    PERFECT = LatencyProfile(
        name="perfect",
        base_delay_ms=0,
        jitter_ms=0,
        distribution=LatencyDistribution.CONSTANT,
    )

    WIFI = LatencyProfile(
        name="wifi",
        base_delay_ms=5,
        jitter_ms=3,
        distribution=LatencyDistribution.NORMAL,
    )

    MOBILE_4G = LatencyProfile(
        name="4g",
        base_delay_ms=50,
        jitter_ms=20,
        distribution=LatencyDistribution.UNIFORM,
    )

    MOBILE_3G = LatencyProfile(
        name="3g",
        base_delay_ms=150,
        jitter_ms=50,
        distribution=LatencyDistribution.UNIFORM,
    )

    SATELLITE = LatencyProfile(
        name="satellite",
        base_delay_ms=600,
        jitter_ms=100,
        distribution=LatencyDistribution.BIMODAL,
    )

    CONGESTED = LatencyProfile(
        name="congested",
        base_delay_ms=200,
        jitter_ms=150,
        distribution=LatencyDistribution.EXPONENTIAL,
        packet_loss_rate=0.05,
    )

    @classmethod
    def get_profile(cls, name: str) -> LatencyProfile:
        """Get a profile by name."""
        profiles = {
            "perfect": cls.PERFECT,
            "wifi": cls.WIFI,
            "4g": cls.MOBILE_4G,
            "3g": cls.MOBILE_3G,
            "satellite": cls.SATELLITE,
            "congested": cls.CONGESTED,
        }
        return profiles.get(name.lower(), cls.PERFECT)


# =============================================================================
# Decorators
# =============================================================================


def with_latency(
    base_delay_ms: float = 0,
    jitter_ms: float = 0,
    distribution: LatencyDistribution = LatencyDistribution.UNIFORM,
) -> Callable:
    """
    Decorator to add latency to a function.

    Example:
        >>> @with_latency(base_delay_ms=50, jitter_ms=10)
        >>> def api_call():
        ...     pass
    """
    sim = LatencySimulator(base_delay_ms, jitter_ms, distribution)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay_s = sim.get_delay() / 1000.0
            time.sleep(delay_s)
            return func(*args, **kwargs)

        return wrapper

    return decorator
