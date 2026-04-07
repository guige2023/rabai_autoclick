"""ipaddress action extensions for rabai_autoclick.

Provides utilities for IP address manipulation, validation,
network calculations, and conversion utilities.
"""

from __future__ import annotations

import ipaddress
import socket
import struct
from typing import Any

__all__ = [
    "ip_address",
    "IPv4Address",
    "IPv6Address",
    "IPv4Network",
    "IPv6Network",
    "is_ipv4",
    "is_ipv6",
    "is_private_ip",
    "is_public_ip",
    "is_loopback_ip",
    "is_multicast_ip",
    "is_reserved_ip",
    "is_valid_ip",
    "parse_ip",
    "parse_network",
    "ip_to_int",
    "int_to_ip",
    "ip_to_bytes",
    "bytes_to_ip",
    "ip_to_hex",
    "hex_to_ip",
    "get_broadcast",
    "get_network_address",
    "get_hostmask",
    "get_netmask",
    "get_subnet_mask",
    "subnet_count",
    "hosts_in_network",
    "networks_overlap",
    "networks_equal",
    "summarize_address_range",
    "collapse_addresses",
    "get_my_ip",
    "resolve_hostname",
    "reverse_dns",
    "IPInfo",
    "NetworkInfo",
]


def ip_address(address: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """Parse an IP address string.

    Args:
        address: IP address string.

    Returns:
        IPv4 or IPv6 address object.

    Raises:
        ValueError: If address is invalid.
    """
    return ipaddress.ip_address(address)


def is_ipv4(address: str | int | bytes) -> bool:
    """Check if address is IPv4.

    Args:
        address: Address to check.

    Returns:
        True if IPv4.
    """
    try:
        return isinstance(ipaddress.ip_address(address), ipaddress.IPv4Address)
    except ValueError:
        return False


def is_ipv6(address: str | int | bytes) -> bool:
    """Check if address is IPv6.

    Args:
        address: Address to check.

    Returns:
        True if IPv6.
    """
    try:
        return isinstance(ipaddress.ip_address(address), ipaddress.IPv6Address)
    except ValueError:
        return False


def is_private_ip(address: str) -> bool:
    """Check if address is private.

    Args:
        address: IP address string.

    Returns:
        True if private.
    """
    try:
        ip = ipaddress.ip_address(address)
        return ip.is_private
    except ValueError:
        return False


def is_public_ip(address: str) -> bool:
    """Check if address is public.

    Args:
        address: IP address string.

    Returns:
        True if public.
    """
    try:
        ip = ipaddress.ip_address(address)
        return ip.is_global
    except ValueError:
        return False


def is_loopback_ip(address: str) -> bool:
    """Check if address is loopback.

    Args:
        address: IP address string.

    Returns:
        True if loopback.
    """
    try:
        ip = ipaddress.ip_address(address)
        return ip.is_loopback
    except ValueError:
        return False


def is_multicast_ip(address: str) -> bool:
    """Check if address is multicast.

    Args:
        address: IP address string.

    Returns:
        True if multicast.
    """
    try:
        ip = ipaddress.ip_address(address)
        return ip.is_multicast
    except ValueError:
        return False


def is_reserved_ip(address: str) -> bool:
    """Check if address is reserved.

    Args:
        address: IP address string.

    Returns:
        True if reserved.
    """
    try:
        ip = ipaddress.ip_address(address)
        return ip.is_reserved
    except ValueError:
        return False


def is_valid_ip(address: str) -> bool:
    """Check if address is valid.

    Args:
        address: Address string to validate.

    Returns:
        True if valid IP address.
    """
    try:
        ipaddress.ip_address(address)
        return True
    except ValueError:
        return False


def parse_ip(address: str) -> dict[str, Any]:
    """Parse IP address into components.

    Args:
        address: IP address string.

    Returns:
        Dict with address info.
    """
    ip = ipaddress.ip_address(address)
    return {
        "address": str(ip),
        "version": ip.version,
        "is_private": ip.is_private,
        "is_public": ip.is_global,
        "is_loopback": ip.is_loopback,
        "is_multicast": ip.is_multicast,
        "is_reserved": ip.is_reserved,
        "packed": ip.packed,
        "hex": ip.packed.hex(),
        "int": int(ip),
    }


def parse_network(network: str) -> dict[str, Any]:
    """Parse network string into components.

    Args:
        network: Network string (e.g., "192.168.0.0/24").

    Returns:
        Dict with network info.
    """
    net = ipaddress.ip_network(network, strict=False)
    return {
        "network": str(net),
        "network_address": str(net.network_address),
        "broadcast_address": str(net.broadcast_address),
        "prefix_length": net.prefixlen,
        "netmask": str(net.netmask),
        "hostmask": str(net.hostmask),
        "num_addresses": net.num_addresses,
        "num_hosts": net.num_addresses - 2 if net.num_addresses > 2 else 0,
        "version": net.version,
        "is_private": net.is_private,
    }


def ip_to_int(address: str) -> int:
    """Convert IP address to integer.

    Args:
        address: IP address string.

    Returns:
        Integer representation.
    """
    return int(ipaddress.ip_address(address))


def int_to_ip(value: int, version: int = 4) -> str:
    """Convert integer to IP address.

    Args:
        value: Integer value.
        version: IP version (4 or 6).

    Returns:
        IP address string.
    """
    if version == 4:
        return str(ipaddress.IPv4Address(value))
    return str(ipaddress.IPv6Address(value))


def ip_to_bytes(address: str) -> bytes:
    """Convert IP address to bytes.

    Args:
        address: IP address string.

    Returns:
        Packed bytes.
    """
    return ipaddress.ip_address(address).packed


def bytes_to_ip(data: bytes) -> str:
    """Convert bytes to IP address.

    Args:
        data: Packed bytes.

    Returns:
        IP address string.
    """
    return str(ipaddress.ip_address(data))


def ip_to_hex(address: str) -> str:
    """Convert IP address to hex string.

    Args:
        address: IP address string.

    Returns:
        Hex string.
    """
    return ipaddress.ip_address(address).packed.hex()


def hex_to_ip(hex_str: str) -> str:
    """Convert hex string to IP address.

    Args:
        hex_str: Hex string.

    Returns:
        IP address string.
    """
    data = bytes.fromhex(hex_str)
    return bytes_to_ip(data)


def get_broadcast(network: str) -> str:
    """Get broadcast address of network.

    Args:
        network: Network string.

    Returns:
        Broadcast address.
    """
    net = ipaddress.ip_network(network, strict=False)
    return str(net.broadcast_address)


def get_network_address(network: str) -> str:
    """Get network address.

    Args:
        network: Network string.

    Returns:
        Network address.
    """
    net = ipaddress.ip_network(network, strict=False)
    return str(net.network_address)


def get_hostmask(network: str) -> str:
    """Get host mask of network.

    Args:
        network: Network string.

    Returns:
        Host mask.
    """
    net = ipaddress.ip_network(network, strict=False)
    return str(net.hostmask)


def get_netmask(network: str) -> str:
    """Get network mask of network.

    Args:
        network: Network string.

    Returns:
        Netmask.
    """
    net = ipaddress.ip_network(network, strict=False)
    return str(net.netmask)


def get_subnet_mask(prefix_length: int) -> str:
    """Get subnet mask from prefix length.

    Args:
        prefix_length: Prefix length (0-32 for IPv4).

    Returns:
        Subnet mask string.
    """
    return str(ipaddress.IPv4Network(f"0.0.0.0/{prefix_length}", strict=False).netmask)


def subnet_count(network: str) -> int:
    """Get number of possible subnets.

    Args:
        network: Network string.

    Returns:
        Subnet count.
    """
    net = ipaddress.ip_network(network, strict=False)
    return 2 ** (32 - net.prefixlen) if net.version == 4 else 0


def hosts_in_network(network: str) -> list[str]:
    """Get list of host addresses in network.

    Args:
        network: Network string.

    Returns:
        List of host address strings.

    Note:
        Returns empty for very large networks.
    """
    net = ipaddress.ip_network(network, strict=False)
    if net.num_addresses > 256:
        return []
    return [str(ip) for ip in net.hosts()]


def networks_overlap(net1: str, net2: str) -> bool:
    """Check if two networks overlap.

    Args:
        net1: First network.
        net2: Second network.

    Returns:
        True if networks overlap.
    """
    network1 = ipaddress.ip_network(net1, strict=False)
    network2 = ipaddress.ip_network(net2, strict=False)
    return network1.overlaps(network2)


def networks_equal(net1: str, net2: str) -> bool:
    """Check if two networks are equal.

    Args:
        net1: First network.
        net2: Second network.

    Returns:
        True if equal.
    """
    return ipaddress.ip_network(net1, strict=False) == ipaddress.ip_network(
        net2, strict=False
    )


def summarize_address_range(start: str, end: str) -> list[str]:
    """Summarize address range as network list.

    Args:
        start: Start address.
        end: End address.

    Returns:
        List of networks covering the range.
    """
    start_ip = ipaddress.ip_address(start)
    end_ip = ipaddress.ip_address(end)
    return [str(net) for net in ipaddress.summarize_address_range(start_ip, end_ip)]


def collapse_addresses(addresses: list[str]) -> list[str]:
    """Collapse adjacent addresses into networks.

    Args:
        addresses: List of address strings.

    Returns:
        List of summarized networks.
    """
    ips = [ipaddress.ip_address(addr) for addr in addresses]
    return [str(net) for net in ipaddress.collapse_addresses(ips)]


def get_my_ip() -> str:
    """Get the local machine's IP address.

    Returns:
        Local IP address.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def resolve_hostname(hostname: str) -> str | None:
    """Resolve hostname to IP address.

    Args:
        hostname: Hostname to resolve.

    Returns:
        IP address or None if resolution fails.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def reverse_dns(ip: str) -> str:
    """Get reverse DNS for IP address.

    Args:
        ip: IP address.

    Returns:
        Hostname or empty string.
    """
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return ""


class IPInfo:
    """Container for IP address information."""

    def __init__(self, address: str) -> None:
        self._ip = ipaddress.ip_address(address)

    @property
    def address(self) -> str:
        return str(self._ip)

    @property
    def version(self) -> int:
        return self._ip.version

    @property
    def is_private(self) -> bool:
        return self._ip.is_private

    @property
    def is_public(self) -> bool:
        return self._ip.is_global

    @property
    def is_loopback(self) -> bool:
        return self._ip.is_loopback

    @property
    def is_multicast(self) -> bool:
        return self._ip.is_multicast

    @property
    def packed(self) -> bytes:
        return self._ip.packed

    @property
    def int(self) -> int:
        return int(self._ip)

    def __repr__(self) -> str:
        return f"IPInfo({self.address})"


class NetworkInfo:
    """Container for network information."""

    def __init__(self, network: str) -> None:
        self._network = ipaddress.ip_network(network, strict=False)

    @property
    def network_address(self) -> str:
        return str(self._network.network_address)

    @property
    def broadcast_address(self) -> str:
        return str(self._network.broadcast_address)

    @property
    def prefix_length(self) -> int:
        return self._network.prefixlen

    @property
    def netmask(self) -> str:
        return str(self._network.netmask)

    @property
    def hostmask(self) -> str:
        return str(self._network.hostmask)

    @property
    def num_addresses(self) -> int:
        return self._network.num_addresses

    @property
    def num_hosts(self) -> int:
        return self._network.num_addresses - 2 if self._network.num_addresses > 2 else 0

    @property
    def is_private(self) -> bool:
        return self._network.is_private

    def __repr__(self) -> str:
        return f"NetworkInfo({str(self._network)})"
