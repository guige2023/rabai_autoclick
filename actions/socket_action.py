"""Socket action module for RabAI AutoClick.

Provides socket/network operations:
- SocketConnectAction: Connect to TCP socket
- SocketSendAction: Send data via socket
- SocketReceiveAction: Receive data from socket
- SocketCloseAction: Close socket connection
- SocketListenAction: Start TCP server
- SocketUdpSendAction: Send UDP datagram
- SocketUdpReceiveAction: Receive UDP datagram
"""

import socket
import ssl
import json
import time
from typing import Any, Dict, List, Optional
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SocketConnectAction(BaseAction):
    """Connect to TCP socket."""
    action_type = "socket_connect"
    display_name = "Socket连接"
    description = "建立TCP socket连接"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute connect.

        Args:
            context: Execution context.
            params: Dict with host, port, use_ssl, timeout, output_var.

        Returns:
            ActionResult with socket fd.
        """
        host = params.get('host', '')
        port = params.get('port', 80)
        use_ssl = params.get('use_ssl', False)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'socket_fd')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_ssl = context.resolve_value(use_ssl)
            resolved_timeout = context.resolve_value(timeout)

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(int(resolved_timeout))

            if resolved_ssl:
                context_s = ssl.create_default_context()
                sock = context_s.wrap_socket(sock, server_hostname=resolved_host)

            sock.connect((resolved_host, int(resolved_port)))

            # Store socket in context for later use
            context.set(output_var, id(sock))
            context._socket_registry = getattr(context, '_socket_registry', {})
            context._socket_registry[str(id(sock))] = sock

            return ActionResult(
                success=True,
                message=f"已连接到 {resolved_host}:{resolved_port}",
                data={'host': resolved_host, 'port': resolved_port, 'socket_id': id(sock), 'output_var': output_var}
            )
        except socket.timeout:
            return ActionResult(
                success=False,
                message=f"连接超时: {resolved_host}:{resolved_port}"
            )
        except socket.error as e:
            return ActionResult(
                success=False,
                message=f"Socket连接失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Socket连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'use_ssl': False, 'timeout': 30, 'output_var': 'socket_fd'}


class SocketSendAction(BaseAction):
    """Send data via socket."""
    action_type = "socket_send"
    display_name = "Socket发送"
    description = "通过socket发送数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute send.

        Args:
            context: Execution context.
            params: Dict with socket_id, data, encoding.

        Returns:
            ActionResult indicating success.
        """
        socket_id = params.get('socket_id', '')
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            registry = getattr(context, '_socket_registry', {})
            sock = registry.get(str(socket_id))

            if sock is None:
                return ActionResult(
                    success=False,
                    message=f"Socket不存在: {socket_id}"
                )

            resolved_data = context.resolve_value(data)
            resolved_encoding = context.resolve_value(encoding)

            if isinstance(resolved_data, dict):
                encoded = json.dumps(resolved_data).encode(resolved_encoding)
            else:
                encoded = str(resolved_data).encode(resolved_encoding)

            sock.sendall(encoded)

            return ActionResult(
                success=True,
                message=f"已发送 {len(encoded)} 字节",
                data={'bytes_sent': len(encoded)}
            )
        except socket.error as e:
            return ActionResult(
                success=False,
                message=f"Socket发送失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Socket发送失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['socket_id', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8'}


class SocketReceiveAction(BaseAction):
    """Receive data from socket."""
    action_type = "socket_receive"
    display_name = "Socket接收"
    description = "从socket接收数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute receive.

        Args:
            context: Execution context.
            params: Dict with socket_id, size, output_var.

        Returns:
            ActionResult with received data.
        """
        socket_id = params.get('socket_id', '')
        size = params.get('size', 4096)
        output_var = params.get('output_var', 'socket_data')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            registry = getattr(context, '_socket_registry', {})
            sock = registry.get(str(socket_id))

            if sock is None:
                return ActionResult(
                    success=False,
                    message=f"Socket不存在: {socket_id}"
                )

            resolved_size = context.resolve_value(size)

            sock.settimeout(10)
            data = sock.recv(int(resolved_size))

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"接收到 {len(data)} 字节",
                data={'data': data, 'size': len(data), 'output_var': output_var}
            )
        except socket.timeout:
            return ActionResult(
                success=False,
                message="Socket接收超时"
            )
        except socket.error as e:
            return ActionResult(
                success=False,
                message=f"Socket接收失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Socket接收失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['socket_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'size': 4096, 'output_var': 'socket_data'}


class SocketCloseAction(BaseAction):
    """Close socket connection."""
    action_type = "socket_close"
    display_name = "Socket关闭"
    description = "关闭socket连接"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute close.

        Args:
            context: Execution context.
            params: Dict with socket_id.

        Returns:
            ActionResult indicating success.
        """
        socket_id = params.get('socket_id', '')

        try:
            registry = getattr(context, '_socket_registry', {})
            sock = registry.pop(str(socket_id), None)

            if sock is None:
                return ActionResult(
                    success=False,
                    message=f"Socket不存在: {socket_id}"
                )

            sock.close()

            return ActionResult(
                success=True,
                message=f"Socket已关闭: {socket_id}",
                data={'socket_id': socket_id}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Socket关闭失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['socket_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SocketUdpSendAction(BaseAction):
    """Send UDP datagram."""
    action_type = "socket_udp_send"
    display_name = "UDP发送"
    description = "发送UDP数据报"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UDP send.

        Args:
            context: Execution context.
            params: Dict with host, port, data, output_var.

        Returns:
            ActionResult indicating success.
        """
        host = params.get('host', '')
        port = params.get('port', 80)
        data = params.get('data', '')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_data = context.resolve_value(data)

            if isinstance(resolved_data, dict):
                encoded = json.dumps(resolved_data).encode('utf-8')
            else:
                encoded = str(resolved_data).encode('utf-8')

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(encoded, (resolved_host, int(resolved_port)))
            sock.close()

            return ActionResult(
                success=True,
                message=f"UDP已发送 {len(encoded)} 字节到 {resolved_host}:{resolved_port}",
                data={'bytes_sent': len(encoded), 'host': resolved_host, 'port': resolved_port}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UDP发送失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'port', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SocketUdpReceiveAction(BaseAction):
    """Receive UDP datagram."""
    action_type = "socket_udp_receive"
    display_name = "UDP接收"
    description = "接收UDP数据报"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UDP receive.

        Args:
            context: Execution context.
            params: Dict with port, size, timeout, output_var.

        Returns:
            ActionResult with received data.
        """
        port = params.get('port', 8080)
        size = params.get('size', 4096)
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'udp_data')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_port = context.resolve_value(port)
            resolved_size = context.resolve_value(size)
            resolved_timeout = context.resolve_value(timeout)

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(int(resolved_timeout))
            sock.bind(('0.0.0.0', int(resolved_port)))

            data, addr = sock.recvfrom(int(resolved_size))
            sock.close()

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"UDP接收 {len(data)} 字节 from {addr[0]}:{addr[1]}",
                data={'data': data, 'from': f'{addr[0]}:{addr[1]}', 'output_var': output_var}
            )
        except socket.timeout:
            return ActionResult(
                success=False,
                message="UDP接收超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UDP接收失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'size': 4096, 'timeout': 5, 'output_var': 'udp_data'}


class SocketPingAction(BaseAction):
    """Ping a host via socket (TCP handshake check)."""
    action_type = "socket_ping"
    display_name = "Socket Ping"
    description = "通过socket检查主机可达性"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ping.

        Args:
            context: Execution context.
            params: Dict with host, port, timeout, output_var.

        Returns:
            ActionResult with latency.
        """
        host = params.get('host', '')
        port = params.get('port', 80)
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'ping_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_timeout = context.resolve_value(timeout)

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(int(resolved_timeout))

            start = time.time()
            sock.connect((resolved_host, int(resolved_port)))
            latency_ms = (time.time() - start) * 1000
            sock.close()

            result = {
                'host': resolved_host,
                'port': resolved_port,
                'latency_ms': round(latency_ms, 2),
                'reachable': True
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Ping {resolved_host}:{resolved_port} - {latency_ms:.2f}ms",
                data=result
            )
        except socket.timeout:
            result = {
                'host': resolved_host,
                'port': resolved_port,
                'reachable': False,
                'error': 'timeout'
            }
            context.set(output_var, result)
            return ActionResult(
                success=False,
                message=f"Ping超时: {resolved_host}:{resolved_port}",
                data=result
            )
        except Exception as e:
            result = {
                'host': resolved_host,
                'port': resolved_port,
                'reachable': False,
                'error': str(e)
            }
            context.set(output_var, result)
            return ActionResult(
                success=False,
                message=f"Ping失败: {str(e)}",
                data=result
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 5, 'output_var': 'ping_result'}


class SocketScanAction(BaseAction):
    """Scan ports on a host."""
    action_type = "socket_scan"
    display_name = "端口扫描"
    description = "扫描主机端口"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute port scan.

        Args:
            context: Execution context.
            params: Dict with host, ports, timeout, output_var.

        Returns:
            ActionResult with open ports.
        """
        host = params.get('host', '')
        ports = params.get('ports', [21, 22, 23, 25, 80, 443, 3306, 5432, 6379, 8080])
        timeout = params.get('timeout', 2)
        output_var = params.get('output_var', 'scan_results')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_ports = context.resolve_value(ports)
            resolved_timeout = context.resolve_value(timeout)

            if isinstance(resolved_ports, str):
                resolved_ports = [int(p.strip()) for p in resolved_ports.split(',')]

            open_ports = []

            for port in resolved_ports:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(int(resolved_timeout))
                try:
                    result = sock.connect_ex((resolved_host, int(port)))
                    if result == 0:
                        open_ports.append(port)
                except:
                    pass
                finally:
                    sock.close()

            context.set(output_var, open_ports)

            return ActionResult(
                success=True,
                message=f"扫描完成: {len(open_ports)}/{len(resolved_ports)} 端口开放",
                data={'open_ports': open_ports, 'total': len(resolved_ports), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"端口扫描失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ports': [21, 22, 23, 25, 80, 443, 3306, 5432, 6379, 8080], 'timeout': 2, 'output_var': 'scan_results'}
