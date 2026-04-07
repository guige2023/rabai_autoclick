"""Network5 action module for RabAI AutoClick.

Provides additional network operations:
- NetworkPingAction: Ping host
- NetworkTracerouteAction: Traceroute to host
- NetworkDNSLookupAction: DNS lookup
- NetworkReverseDNSAction: Reverse DNS lookup
- NetworkGetMacAddressAction: Get MAC address
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkPingAction(BaseAction):
    """Ping host."""
    action_type = "network5_ping"
    display_name = "Ping主机"
    description = "Ping主机检测连通性"
    version = "5.0"

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

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import subprocess

            resolved_host = context.resolve_value(host)
            resolved_count = int(context.resolve_value(count)) if count else 4

            result = subprocess.run(
                ['ping', '-c', str(resolved_count), resolved_host],
                capture_output=True,
                text=True,
                timeout=30
            )

            success = result.returncode == 0

            context.set(output_var, {
                'success': success,
                'output': result.stdout,
                'host': resolved_host
            })

            return ActionResult(
                success=True,
                message=f"Ping {resolved_host}: {'成功' if success else '失败'}",
                data={
                    'host': resolved_host,
                    'success': success,
                    'output': result.stdout,
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


class NetworkTracerouteAction(BaseAction):
    """Traceroute to host."""
    action_type = "network5_traceroute"
    display_name = "路由跟踪"
    description = "跟踪到主机的路由"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute traceroute.

        Args:
            context: Execution context.
            params: Dict with host, output_var.

        Returns:
            ActionResult with traceroute result.
        """
        host = params.get('host', '')
        output_var = params.get('output_var', 'traceroute_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import subprocess

            resolved_host = context.resolve_value(host)

            result = subprocess.run(
                ['traceroute', resolved_host],
                capture_output=True,
                text=True,
                timeout=60
            )

            success = result.returncode == 0

            context.set(output_var, {
                'success': success,
                'output': result.stdout,
                'host': resolved_host
            })

            return ActionResult(
                success=True,
                message=f"路由跟踪 {resolved_host}: {'完成' if success else '失败'}",
                data={
                    'host': resolved_host,
                    'success': success,
                    'output': result.stdout,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路由跟踪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'traceroute_result'}


class NetworkDNSLookupAction(BaseAction):
    """DNS lookup."""
    action_type = "network5_dns"
    display_name = "DNS查询"
    description = "DNS域名查询"
    version = "5.0"

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
            ActionResult with DNS result.
        """
        domain = params.get('domain', '')
        output_var = params.get('output_var', 'dns_result')

        valid, msg = self.validate_type(domain, str, 'domain')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import socket

            resolved_domain = context.resolve_value(domain)

            ip_address = socket.gethostbyname(resolved_domain)

            context.set(output_var, ip_address)

            return ActionResult(
                success=True,
                message=f"DNS查询: {resolved_domain} -> {ip_address}",
                data={
                    'domain': resolved_domain,
                    'ip_address': ip_address,
                    'output_var': output_var
                }
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


class NetworkReverseDNSAction(BaseAction):
    """Reverse DNS lookup."""
    action_type = "network5_reverse_dns"
    display_name = "反向DNS查询"
    description = "反向DNS查询"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse DNS lookup.

        Args:
            context: Execution context.
            params: Dict with ip_address, output_var.

        Returns:
            ActionResult with reverse DNS result.
        """
        ip_address = params.get('ip_address', '')
        output_var = params.get('output_var', 'reverse_dns_result')

        valid, msg = self.validate_type(ip_address, str, 'ip_address')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import socket

            resolved_ip = context.resolve_value(ip_address)

            hostname = socket.gethostbyaddr(resolved_ip)[0]

            context.set(output_var, hostname)

            return ActionResult(
                success=True,
                message=f"反向DNS: {resolved_ip} -> {hostname}",
                data={
                    'ip_address': resolved_ip,
                    'hostname': hostname,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反向DNS查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['ip_address']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reverse_dns_result'}


class NetworkGetMacAddressAction(BaseAction):
    """Get MAC address."""
    action_type = "network5_mac"
    display_name = "获取MAC地址"
    description = "获取网络接口MAC地址"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get MAC address.

        Args:
            context: Execution context.
            params: Dict with interface, output_var.

        Returns:
            ActionResult with MAC address.
        """
        interface = params.get('interface', 'en0')
        output_var = params.get('output_var', 'mac_address')

        try:
            import subprocess

            resolved_interface = context.resolve_value(interface) if interface else 'en0'

            result = subprocess.run(
                ['ifconfig', resolved_interface],
                capture_output=True,
                text=True
            )

            mac = None
            for line in result.stdout.split('\n'):
                if 'ether' in line:
                    mac = line.split('ether')[1].strip().split()[0]
                    break

            if mac is None:
                return ActionResult(
                    success=False,
                    message=f"获取MAC地址失败: 接口 {resolved_interface} 不存在"
                )

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
        return ['interface']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mac_address'}