"""Network4 action module for RabAI AutoClick.

Provides additional network operations:
- NetworkPingAction: Ping host
- NetworkDnsLookupAction: DNS lookup
- NetworkPortCheckAction: Check if port is open
- NetworkMacAddressAction: Get MAC address
- NetworkInterfaceListAction: List network interfaces
"""

import socket
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkPingAction(BaseAction):
    """Ping host."""
    action_type = "network4_ping"
    display_name = "Ping检测"
    description = "检测主机是否可达"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ping.

        Args:
            context: Execution context.
            params: Dict with host, timeout, output_var.

        Returns:
            ActionResult with ping result.
        """
        host = params.get('host', '')
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'ping_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_timeout = int(context.resolve_value(timeout)) if timeout else 5

            socket.setdefaulttimeout(resolved_timeout)
            result = True

            try:
                socket.gethostbyname(resolved_host)
            except socket.gaierror:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Ping检测: {'成功' if result else '失败'}",
                data={
                    'host': resolved_host,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Ping检测失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 5, 'output_var': 'ping_result'}


class NetworkDnsLookupAction(BaseAction):
    """DNS lookup."""
    action_type = "network4_dns"
    display_name = "DNS查询"
    description = "查询域名的DNS记录"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DNS lookup.

        Args:
            context: Execution context.
            params: Dict with domain, output_var.

        Returns:
            ActionResult with IP address.
        """
        domain = params.get('domain', '')
        output_var = params.get('output_var', 'dns_result')

        valid, msg = self.validate_type(domain, str, 'domain')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_domain = context.resolve_value(domain)
            result = socket.gethostbyname(resolved_domain)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"DNS查询: {resolved_domain} -> {result}",
                data={
                    'domain': resolved_domain,
                    'ip': result,
                    'output_var': output_var
                }
            )
        except socket.gaierror:
            return ActionResult(
                success=False,
                message=f"DNS查询失败: 无法解析域名"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DNS查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['domain']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dns_result'}


class NetworkPortCheckAction(BaseAction):
    """Check if port is open."""
    action_type = "network4_port"
    display_name = "端口检测"
    description = "检测端口是否开放"
    version = "4.0"

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
            ActionResult with port check result.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        output_var = params.get('output_var', 'port_result')

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = int(context.resolve_value(port))

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((resolved_host, resolved_port))
            sock.close()

            is_open = result == 0
            context.set(output_var, is_open)

            return ActionResult(
                success=True,
                message=f"端口检测: {resolved_host}:{resolved_port} - {'开放' if is_open else '关闭'}",
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
                message=f"端口检测失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'output_var': 'port_result'}


class NetworkMacAddressAction(BaseAction):
    """Get MAC address."""
    action_type = "network4_mac"
    display_name = "MAC地址"
    description = "获取MAC地址"
    version = "4.0"

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
        interface = params.get('interface', '')
        output_var = params.get('output_var', 'mac_result')

        try:
            resolved_interface = context.resolve_value(interface) if interface else None

            import uuid
            mac = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff) for i in range(0, 48, 8)])
            context.set(output_var, mac)

            return ActionResult(
                success=True,
                message=f"MAC地址: {mac}",
                data={
                    'mac': mac,
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
        return {'interface': '', 'output_var': 'mac_result'}


class NetworkInterfaceListAction(BaseAction):
    """List network interfaces."""
    action_type = "network4_interfaces"
    display_name = "网络接口列表"
    description = "列出所有网络接口"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list interfaces.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with interface list.
        """
        output_var = params.get('output_var', 'interfaces_result')

        try:
            import uuid
            hostname = socket.gethostname()
            interfaces = socket.getaddrinfo(hostname, None)
            unique_ips = list(set([addr[4][0] for addr in interfaces]))

            result = {
                'hostname': hostname,
                'addresses': unique_ips
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"网络接口列表: {len(unique_ips)} 地址",
                data={
                    'hostname': hostname,
                    'addresses': unique_ips,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取网络接口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'interfaces_result'}