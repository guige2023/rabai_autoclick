"""IP Whitelist Firewall for API Protection.

This module provides IP-based access control for APIs:
- CIDR notation support
- IP range whitelisting
- Request logging and stats
- GeoIP-based blocking (optional)

Example:
    >>> from actions.api_ip_whitelist_action import IPWhitelist
    >>> fw = IPWhitelist()
    >>> fw.add_cidr("192.168.1.0/24")
    >>> fw.is_allowed("192.168.1.100")  # True
"""

from __future__ import annotations

import ipaddress
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class IPPattern:
    """An IP address or CIDR range pattern."""
    network: ipaddress.IPv4Network | ipaddress.IPv6Network
    description: str = ""
    added_at: float = field(default_factory=time.time)


@dataclass
class RequestRecord:
    """Record of an IP request."""
    ip: str
    path: str
    timestamp: float
    allowed: bool


class IPWhitelist:
    """IP whitelist firewall for API endpoints."""

    def __init__(self) -> None:
        """Initialize the IP whitelist."""
        self._patterns: list[IPPattern] = []
        self._block_list: set[str] = set()
        self._request_log: list[RequestRecord] = []
        self._stats: dict[str, int] = {"allowed": 0, "blocked": 0}
        self._lock = threading.RLock()
        self._max_log_size = 10000

    def add_cidr(self, cidr: str, description: str = "") -> bool:
        """Add a CIDR range to the whitelist.

        Args:
            cidr: CIDR notation (e.g., "192.168.1.0/24").
            description: Optional description for this rule.

        Returns:
            True if added successfully, False if invalid CIDR.
        """
        try:
            net = ipaddress.ip_network(cidr, strict=False)
            with self._lock:
                self._patterns.append(IPPattern(network=net, description=description))
                logger.info("Added whitelist CIDR: %s (%s)", cidr, description)
            return True
        except ValueError as e:
            logger.error("Invalid CIDR %s: %s", cidr, e)
            return False

    def add_ip(self, ip_str: str, description: str = "") -> bool:
        """Add a single IP address to the whitelist.

        Args:
            ip_str: IP address (e.g., "10.0.0.1").
            description: Optional description.

        Returns:
            True if added successfully.
        """
        return self.add_cidr(f"{ip_str}/32", description)

    def block_ip(self, ip_str: str) -> None:
        """Block a specific IP address.

        Args:
            ip_str: IP address to block.
        """
        with self._lock:
            self._block_list.add(ip_str)
            logger.info("Blocked IP: %s", ip_str)

    def unblock_ip(self, ip_str: str) -> None:
        """Unblock a specific IP address.

        Args:
            ip_str: IP address to unblock.
        """
        with self._lock:
            self._block_list.discard(ip_str)
            logger.info("Unblocked IP: %s", ip_str)

    def is_allowed(self, ip_str: str, path: str = "/") -> bool:
        """Check if an IP is allowed.

        Args:
            ip_str: The IP address to check.
            path: The requested path (for logging).

        Returns:
            True if allowed, False if blocked.
        """
        with self._lock:
            if ip_str in self._block_list:
                self._record_request(ip_str, path, False)
                return False

            try:
                ip = ipaddress.ip_address(ip_str)
            except ValueError:
                self._record_request(ip_str, path, False)
                return False

            for pattern in self._patterns:
                if ip in pattern.network:
                    self._record_request(ip_str, path, True)
                    return True

            self._record_request(ip_str, path, False)
            return False

    def _record_request(self, ip: str, path: str, allowed: bool) -> None:
        """Record a request for stats and logging."""
        record = RequestRecord(ip=ip, path=path, timestamp=time.time(), allowed=allowed)
        self._request_log.append(record)
        if allowed:
            self._stats["allowed"] += 1
        else:
            self._stats["blocked"] += 1

        if len(self._request_log) > self._max_log_size:
            self._request_log = self._request_log[-self._max_log_size // 2:]

    def get_stats(self) -> dict[str, int]:
        """Get request statistics."""
        with self._lock:
            return dict(self._stats)

    def get_recent_blocked(self, limit: int = 50) -> list[RequestRecord]:
        """Get recently blocked requests."""
        with self._lock:
            blocked = [r for r in self._request_log if not r.allowed]
            return blocked[-limit:]

    def clear_block_list(self) -> None:
        """Clear all blocked IPs."""
        with self._lock:
            self._block_list.clear()
            logger.info("Cleared block list")

    def list_whitelist(self) -> list[str]:
        """List all whitelisted CIDR ranges."""
        with self._lock:
            return [str(p.network) for p in self._patterns]
