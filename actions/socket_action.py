"""
TCP/UDP socket operations actions.
"""
from __future__ import annotations

import socket
import select
from typing import Dict, Any, Optional, List


def create_tcp_client(
    host: str,
    port: int,
    timeout: float = 10.0
) -> socket.socket:
    """
    Create a TCP client socket.

    Args:
        host: Server hostname.
        port: Server port.
        timeout: Socket timeout.

    Returns:
        Connected socket.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    return sock


def create_udp_client(
    host: str,
    port: int
) -> socket.socket:
    """
    Create a UDP socket.

    Args:
        host: Server hostname.
        port: Server port.

    Returns:
        UDP socket.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect((host, port))
    return sock


def send_tcp_message(
    host: str,
    port: int,
    message: str,
    timeout: float = 10.0
) -> Dict[str, Any]:
    """
    Send a message via TCP and receive response.

    Args:
        host: Server hostname.
        port: Server port.
        message: Message to send.
        timeout: Socket timeout.

    Returns:
        Response data.
    """
    try:
        sock = create_tcp_client(host, port, timeout)

        sock.sendall(message.encode('utf-8'))

        data = sock.recv(4096)

        sock.close()

        return {
            'success': True,
            'response': data.decode('utf-8', errors='replace'),
            'host': host,
            'port': port,
        }
    except socket.timeout:
        return {
            'success': False,
            'error': 'Connection timed out',
        }
    except socket.error as e:
        return {
            'success': False,
            'error': str(e),
        }


def send_udp_message(
    host: str,
    port: int,
    message: str
) -> Dict[str, Any]:
    """
    Send a UDP datagram.

    Args:
        host: Server hostname.
        port: Server port.
        message: Message to send.

    Returns:
        Send result.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode('utf-8'), (host, port))
        sock.close()

        return {
            'success': True,
            'host': host,
            'port': port,
        }
    except socket.error as e:
        return {
            'success': False,
            'error': str(e),
        }


def start_tcp_server(
    host: str,
    port: int,
    handler: callable,
    timeout: float = 30.0
) -> None:
    """
    Start a TCP server.

    Args:
        host: Bind address.
        port: Bind port.
        handler: Callback function(client_socket, address).
        timeout: Connection timeout.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    server.settimeout(timeout)

    try:
        while True:
            client, address = server.accept()
            handler(client, address)
            client.close()
    except socket.timeout:
        pass
    finally:
        server.close()


def port_is_open(
    host: str,
    port: int,
    timeout: float = 3.0
) -> bool:
    """
    Check if a port is open.

    Args:
        host: Hostname.
        port: Port number.
        timeout: Connection timeout.

    Returns:
        True if port is open.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        sock.connect((host, port))
        sock.close()
        return True
    except socket.error:
        return False


def scan_ports(
    host: str,
    ports: List[int],
    timeout: float = 1.0
) -> List[int]:
    """
    Scan multiple ports on a host.

    Args:
        host: Hostname.
        ports: List of ports to scan.
        timeout: Connection timeout per port.

    Returns:
        List of open ports.
    """
    open_ports = []

    for port in ports:
        if port_is_open(host, port, timeout):
            open_ports.append(port)

    return open_ports


def resolve_hostname(hostname: str) -> Optional[str]:
    """
    Resolve hostname to IP address.

    Args:
        hostname: Hostname to resolve.

    Returns:
        IP address or None.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def get_local_ip() -> str:
    """
    Get the local machine's IP address.

    Returns:
        Local IP address.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(('8.8.8.8', 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except socket.error:
        return '127.0.0.1'


def reverse_dns_lookup(ip: str) -> Optional[str]:
    """
    Perform reverse DNS lookup.

    Args:
        ip: IP address.

    Returns:
        Hostname or None.
    """
    try:
        return socket.gethostbyaddr(ip)[0]
    except socket.herror:
        return None


def create_tcp_server_simple(
    host: str,
    port: int,
    max_connections: int = 5
) -> socket.socket:
    """
    Create a TCP server socket (without listening).

    Args:
        host: Bind address.
        port: Bind port.
        max_connections: Maximum backlog connections.

    Returns:
        Server socket.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(max_connections)
    return server


def accept_connection(server: socket.socket, timeout: float = 30.0) -> tuple:
    """
    Accept a connection on server socket.

    Args:
        server: Server socket.
        timeout: Accept timeout.

    Returns:
        Tuple of (client_socket, address).
    """
    server.settimeout(timeout)

    try:
        return server.accept()
    except socket.timeout:
        raise TimeoutError("Accept timed out")


def send_data(sock: socket.socket, data: bytes) -> bool:
    """
    Send data through socket.

    Args:
        sock: Socket object.
        data: Data to send.

    Returns:
        True if sent successfully.
    """
    try:
        sock.sendall(data)
        return True
    except socket.error:
        return False


def receive_data(
    sock: socket.socket,
    buffer_size: int = 4096,
    timeout: float = 10.0
) -> Optional[bytes]:
    """
    Receive data from socket.

    Args:
        sock: Socket object.
        buffer_size: Receive buffer size.
        timeout: Receive timeout.

    Returns:
        Received data or None.
    """
    sock.settimeout(timeout)

    try:
        data = sock.recv(buffer_size)
        return data if data else None
    except socket.timeout:
        return None


def close_socket(sock: socket.socket) -> None:
    """
    Safely close a socket.

    Args:
        sock: Socket to close.
    """
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except socket.error:
        pass

    try:
        sock.close()
    except socket.error:
        pass


def socket_pair() -> tuple:
    """
    Create a pair of connected sockets.

    Returns:
        Tuple of (sock1, sock2).
    """
    return socket.socketpair()


def get_service_name(port: int, protocol: str = 'tcp') -> str:
    """
    Get service name for port.

    Args:
        port: Port number.
        protocol: Protocol ('tcp' or 'udp').

    Returns:
        Service name or empty string.
    """
    try:
        return socket.getservbyport(port, protocol)
    except OSError:
        return ''


def is_ipv4(address: str) -> bool:
    """
    Check if address is IPv4.

    Args:
        address: IP address string.

    Returns:
        True if IPv4.
    """
    try:
        socket.inet_pton(socket.AF_INET, address)
        return True
    except socket.error:
        return False


def is_ipv6(address: str) -> bool:
    """
    Check if address is IPv6.

    Args:
        address: IP address string.

    Returns:
        True if IPv6.
    """
    try:
        socket.inet_pton(socket.AF_INET6, address)
        return True
    except socket.error:
        return False


def create_udp_server(
    host: str,
    port: int,
    max_packet_size: int = 65535
) -> socket.socket:
    """
    Create a UDP server socket.

    Args:
        host: Bind address.
        port: Bind port.
        max_packet_size: Maximum receive packet size.

    Returns:
        Server socket.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    return sock


def recvfrom_udp(
    sock: socket.socket,
    buffer_size: int = 65535
) -> tuple:
    """
    Receive datagram from UDP socket.

    Args:
        sock: UDP socket.
        buffer_size: Buffer size.

    Returns:
        Tuple of (data, address).
    """
    return sock.recvfrom(buffer_size)


def sendto_udp(
    sock: socket.socket,
    data: bytes,
    address: tuple
) -> int:
    """
    Send datagram to UDP socket.

    Args:
        sock: UDP socket.
        data: Data to send.
        address: (host, port) tuple.

    Returns:
        Number of bytes sent.
    """
    return sock.sendto(data, address)


def set_socket_timeout(sock: socket.socket, timeout: float) -> None:
    """
    Set socket timeout.

    Args:
        sock: Socket object.
        timeout: Timeout in seconds.
    """
    sock.settimeout(timeout)


def set_socketBlocking(sock: socket.socket, blocking: bool) -> None:
    """
    Set socket blocking mode.

    Args:
        sock: Socket object.
        blocking: True for blocking, False for non-blocking.
    """
    sock.setblocking(blocking)


def get_socket_info(sock: socket.socket) -> Dict[str, Any]:
    """
    Get socket connection information.

    Args:
        sock: Socket object.

    Returns:
        Socket information.
    """
    try:
        local = sock.getsockname()
        remote = sock.getpeername()

        return {
            'local_address': local[0],
            'local_port': local[1],
            'remote_address': remote[0],
            'remote_port': remote[1],
            'family': sock.family,
            'type': sock.type,
            'proto': sock.proto,
        }
    except socket.error:
        return {'error': 'Socket not connected'}


def check_connection_alive(sock: socket.socket) -> bool:
    """
    Check if socket connection is still alive.

    Args:
        sock: Socket object.

    Returns:
        True if connection is alive.
    """
    try:
        sock.setblocking(False)
        ready = select.select([sock], [], [], 0)

        if ready[0]:
            data = sock.recv(1, socket.MSG_PEEK)
            if data == b'':
                return False

        sock.setblocking(True)
        return True
    except:
        return False
