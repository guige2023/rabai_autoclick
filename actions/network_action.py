"""Network action module for RabAI AutoClick.

Provides network operations:
- NetworkPingAction: Ping a host
- NetworkDnsLookupAction: DNS lookup
- NetworkGetLocalIpAction: Get local IP
- NetworkGetPublicIpAction: Get public IP
- NetworkPortCheckAction: Check if port is open
"""

import socket
import urllib.request
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkPingAction(BaseAction):
    """Ping a host."""
    action_type = "network_ping"
    display_name = "Ping主机"
    description = "Ping指定主机"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ping.

        Args:
            context: Execution context.
            params: Dict with host, count, output_var.

        Returns:
            ActionResult with ping result.
        """
        host = params.get('host', '')
        count = params.get('count', 4)
        output_var = params.get('output_var', 'network_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_count = context.resolve_value(count)

            # Use socket to check if host is reachable
            import subprocess
            result = subprocess.run(
                ['ping', '-c', str(resolved_count), resolved_host],
                capture_output=True,
                text=True,
                timeout=10
            )

            success = result.returncode == 0
            context.set(output_var, success)

            return ActionResult(
                success=True,
                message=f"Ping {'成功' if success else '失败'}: {resolved_host}",
                data={
                    'success': success,
                    'host': resolved_host,
                    'output_var': output_var
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"Ping超时: {resolved_host}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Ping失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 4, 'output_var': 'network_result'}


class NetworkDnsLookupAction(BaseAction):
    """DNS lookup."""
    action_type = "network_dns_lookup"
    display_name = "DNS查询"
    description = "查询主机名的IP地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DNS lookup.

        Args:
            context: Execution context.
            params: Dict with hostname, output_var.

        Returns:
            ActionResult with IP addresses.
        """
        hostname = params.get('hostname', '')
        output_var = params.get('output_var', 'network_result')

        valid, msg = self.validate_type(hostname, str, 'hostname')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_hostname = context.resolve_value(hostname)
            result = socket.gethostbyname_ex(resolved_hostname)
            ips = result[2] if len(result) > 2 else []
            context.set(output_var, ips)

            return ActionResult(
                success=True,
                message=f"DNS查询成功: {len(ips)} 个IP",
                data={
                    'hostname': resolved_hostname,
                    'ips': ips,
                    'output_var': output_var
                }
            )
        except socket.gaierror as e:
            return ActionResult(
                success=False,
                message=f"DNS查询失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DNS查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['hostname']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'network_result'}


class NetworkGetLocalIpAction(BaseAction):
    """Get local IP."""
    action_type = "network_get_local_ip"
    display_name = "获取本地IP"
    description = "获取本机IP地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get local IP.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with local IP.
        """
        output_var = params.get('output_var', 'network_result')

        try:
            # Create a socket to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(("8.8.8.8", 80))
                result = s.getsockname()[0]
            finally:
                s.close()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"本地IP: {result}",
                data={
                    'ip': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取本地IP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'network_result'}


class NetworkGetPublicIpAction(BaseAction):
    """Get public IP."""
    action_type = "network_get_public_ip"
    display_name = "获取公网IP"
    description = "获取公网IP地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get public IP.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with public IP.
        """
        output_var = params.get('output_var', 'network_result')

        try:
            # Use ipify API to get public IP
            response = urllib.request.urlopen('https://api.ipify.org', timeout=5)
            result = response.read().decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"公网IP: {result}",
                data={
                    'ip': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取公网IP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'network_result'}


class NetworkPortCheckAction(BaseAction):
    """Check if port is open."""
    action_type = "network_port_check"
    display_name = "检查端口"
    description = "检查主机端口是否开放"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute port check.

        Args:
            context: Execution context.
            params: Dict with host, port, output_var.

        Returns:
            ActionResult with open status.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        output_var = params.get('output_var', 'network_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(port, int, 'port')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((resolved_host, int(resolved_port)))
            sock.close()

            is_open = result == 0
            context.set(output_var, is_open)

            return ActionResult(
                success=True,
                message=f"端口 {'开放' if is_open else '关闭'}: {resolved_host}:{resolved_port}",
                data={
                    'open': is_open,
                    'host': resolved_host,
                    'port': resolved_port,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"端口检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'network_result'}