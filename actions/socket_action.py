"""Socket action module for RabAI AutoClick.

Provides TCP/UDP socket operations for network communication.
"""

import socket
import sys
import os
import time
import struct
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SocketAction(BaseAction):
    """TCP/UDP socket operations.
    
    Supports connecting, sending, receiving, and listening
    via TCP and UDP sockets with various protocols.
    """
    action_type = "socket"
    display_name = "Socket通信"
    description = "TCP/UDP网络套接字通信"
    
    def __init__(self) -> None:
        super().__init__()
        self._socket: Optional[socket.socket] = None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute socket operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'connect', 'listen', 'send', 'receive', 'close'
                - host: Target host
                - port: Port number
                - protocol: 'tcp' or 'udp' (default 'tcp')
                - message: Message to send
                - timeout: Socket timeout in seconds
                - size: Receive buffer size (default 4096)
                - bind_host: Bind address for listen (default '0.0.0.0')
        
        Returns:
            ActionResult with operation result.
        """
        command = params.get('command', 'connect')
        host = params.get('host')
        port = params.get('port')
        protocol = params.get('protocol', 'tcp').lower()
        message = params.get('message')
        timeout = params.get('timeout', 10)
        size = params.get('size', 4096)
        bind_host = params.get('bind_host', '0.0.0.0')
        
        if command == 'connect':
            if not host or port is None:
                return ActionResult(success=False, message="host and port required for connect")
            return self._socket_connect(host, port, protocol, timeout)
        
        if command == 'listen':
            if port is None:
                return ActionResult(success=False, message="port required for listen")
            return self._socket_listen(host or bind_host, port, protocol, timeout)
        
        if command == 'send':
            if not message:
                return ActionResult(success=False, message="message required for send")
            return self._socket_send(message)
        
        if command == 'receive':
            return self._socket_receive(size)
        
        if command == 'close':
            return self._socket_close()
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _socket_connect(self, host: str, port: int, protocol: str, timeout: int) -> ActionResult:
        """Connect to remote socket."""
        try:
            if protocol == 'udp':
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                sock.connect((host, port))
            
            self._socket = sock
            return ActionResult(
                success=True,
                message=f"Connected to {host}:{port} ({protocol.upper()})",
                data={'host': host, 'port': port, 'protocol': protocol}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to connect: {e}")
    
    def _socket_listen(self, host: str, port: int, protocol: str, timeout: int) -> ActionResult:
        """Listen for incoming connections."""
        try:
            if protocol == 'udp':
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, port))
                sock.settimeout(timeout)
                self._socket = sock
                return ActionResult(
                    success=True,
                    message=f"UDP server listening on {host}:{port}",
                    data={'listening': True, 'host': host, 'port': port}
                )
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, port))
                sock.listen(1)
                sock.settimeout(timeout)
                self._socket = sock
                return ActionResult(
                    success=True,
                    message=f"TCP server listening on {host}:{port}",
                    data={'listening': True, 'host': host, 'port': port}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to listen: {e}")
    
    def _socket_send(self, message: Any) -> ActionResult:
        """Send data through socket."""
        if not self._socket:
            return ActionResult(success=False, message="Socket not connected. Run 'connect' or 'listen' first.")
        
        try:
            if isinstance(message, str):
                data = message.encode('utf-8')
            elif isinstance(message, int):
                data = struct.pack('!I', message)
            elif isinstance(message, float):
                data = struct.pack('!d', message)
            else:
                data = str(message).encode('utf-8')
            
            self._socket.sendall(data)
            return ActionResult(
                success=True,
                message=f"Sent {len(data)} bytes",
                data={'bytes_sent': len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to send: {e}")
    
    def _socket_receive(self, size: int) -> ActionResult:
        """Receive data from socket."""
        if not self._socket:
            return ActionResult(success=False, message="Socket not connected. Run 'connect' or 'listen' first.")
        
        try:
            data, addr = self._socket.recvfrom(size)
            decoded = data.decode('utf-8', errors='replace')
            return ActionResult(
                success=True,
                message=f"Received {len(data)} bytes from {addr[0]}:{addr[1]}",
                data={'data': decoded, 'bytes': len(data), 'from': f'{addr[0]}:{addr[1]}'}
            )
        except socket.timeout:
            return ActionResult(success=True, message="Receive timeout", data={'timed_out': True})
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to receive: {e}")
    
    def _socket_close(self) -> ActionResult:
        """Close socket connection."""
        if self._socket:
            try:
                self._socket.close()
                self._socket = None
                return ActionResult(success=True, message="Socket closed")
            except Exception as e:
                return ActionResult(success=False, message=f"Error closing socket: {e}")
        return ActionResult(success=True, message="No socket to close")
