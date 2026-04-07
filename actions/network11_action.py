"""Network11 action module for RabAI AutoClick.

Provides additional network operations:
- NetworkPingAction: Ping host
- NetworkHostNameAction: Get hostname
- NetworkIPAddressAction: Get IP address
- NetworkPortOpenAction: Check if port is open
- NetworkDNSLookupAction: DNS lookup
- NetworkMACAddressAction: Get MAC address
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkPingAction(BaseAction):
    """Ping host."""
    action_type = "network11_ping"
    display_name = "Ping主机"
    description = "Ping主机"
    version = "11.0"

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
        output_var = params.get('output_var', 'ping_result')

        try:
            import subprocess

            resolved_host = context.resolve_value(host)
            resolved_count = int(context.resolve_value(count)) if count else 4

            result = subprocess.run(
                ['ping', '-c', str(resolved_count), resolved_host],
                capture_output=True,
                text=True
            )

            success = result.returncode == 0

            context.set(output_var, success)

            return ActionResult(
                success=True,
                message=f"Ping {resolved_host}: {'成功' if success else '失败'}",
                data={
                    'host': resolved_host,
                    'count': resolved_count,
                    'success': success,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Ping失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 4, 'output_var': 'ping_result'}


class NetworkHostNameAction(BaseAction):
    """Get hostname."""
    action_type = "network11_hostname"
    display_name = "获取主机名"
    description = "获取主机名"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hostname.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with hostname.
        """
        output_var = params.get('output_var', 'hostname')

        try:
            import socket

            result = socket.gethostname()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"主机名: {result}",
                data={
                    'hostname': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取主机名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hostname'}


class NetworkIPAddressAction(BaseAction):
    """Get IP address."""
    action_type = "network11_ipaddress"
    display_name = "获取IP地址"
    description = "获取IP地址"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute IP address.

        Args:
            context: Execution context.
            params: Dict with hostname, output_var.

        Returns:
            ActionResult with IP address.
        """
        hostname = params.get('hostname', None)
        output_var = params.get('output_var', 'ip_address')

        try:
            import socket

            resolved_hostname = context.resolve_value(hostname) if hostname else None

            if resolved_hostname:
                result = socket.gethostbyname(resolved_hostname)
            else:
                result = socket.gethostbyname(socket.gethostname())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"IP地址: {result}",
                data={
                    'hostname': resolved_hostname,
                    'ip_address': result,
                    'output_var': output_var
                }
            )
        except socket.gaierror as e:
            return ActionResult(
                success=False,
                message=f"获取IP地址失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取IP地址失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'hostname': None, 'output_var': 'ip_address'}


class NetworkPortOpenAction(BaseAction):
    """Check if port is open."""
    action_type = "network11_port_open"
    display_name = "检查端口"
    description = "检查端口是否开放"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute port open check.

        Args:
            context: Execution context.
            params: Dict with host, port, output_var.

        Returns:
            ActionResult with port check result.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        output_var = params.get('output_var', 'port_open_result')

        try:
            import socket

            resolved_host = context.resolve_value(host) if host else 'localhost'
            resolved_port = int(context.resolve_value(port)) if port else 80

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((resolved_host, resolved_port))
            sock.close()

            is_open = result == 0
            context.set(output_var, is_open)

            return ActionResult(
                success=True,
                message=f"端口 {resolved_port}: {'开放' if is_open else '关闭'}",
                data={
                    'host': resolved_host,
                    'port': resolved_port,
                    'is_open': is_open,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查端口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'port_open_result'}


class NetworkDNSLookupAction(BaseAction):
    """DNS lookup."""
    action_type = "network11_dns_lookup"
    display_name = "DNS查询"
    description = "DNS查询"
    version = "11.0"

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
            ActionResult with DNS lookup result.
        """
        hostname = params.get('hostname', '')
        output_var = params.get('output_var', 'dns_result')

        try:
            import socket

            resolved_hostname = context.resolve_value(hostname)

            result = socket.gethostbyname_ex(resolved_hostname)

            data = {
                'hostname': result[0],
                'aliases': result[1],
                'addresses': result[2]
            }

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"DNS查询: {resolved_hostname}",
                data={
                    'hostname': resolved_hostname,
                    'result': data,
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
        return {'output_var': 'dns_result'}


class NetworkMACAddressAction(BaseAction):
    """Get MAC address."""
    action_type = "network11_mac_address"
    display_name: "获取MAC地址"
    description = "获取MAC地址"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MAC address.

        Args:
            context: Execution context.
            params: Dict with interface, output_var.

        Returns:
            ActionResult with MAC address.
        """
        interface = params.get('interface', 'eth0')
        output_var = params.get('output_var', 'mac_address')

        try:
            import uuid

            resolved_interface = context.resolve_value(interface) if interface else 'eth0'

            # Get MAC address from network interface
            mac = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff) for i in range(0, 48, 8)])

            context.set(output_var, mac)

            return ActionResult(
                success=True,
                message=f"MAC地址: {mac}",
                data={
                    'interface': resolved_interface,
                    'mac_address': mac,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取MAC地址失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'interface': 'eth0', 'output_var': 'mac_address'}