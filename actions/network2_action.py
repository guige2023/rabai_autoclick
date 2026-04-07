"""Network2 action module for RabAI AutoClick.

Provides additional network operations:
- NetworkPingAction: Ping host
- NetworkDnsLookupAction: DNS lookup
- NetworkGetLocalIpAction: Get local IP
- NetworkGetPublicIpAction: Get public IP
- NetworkPortCheckAction: Check if port is open
"""

import socket
import urllib.request
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkPingAction(BaseAction):
    """Ping host."""
    action_type = "network2_ping"
    display_name = "Ping"
    description = "Ping主机检测连通性"

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
            resolved_host = context.resolve_value(host)
            resolved_count = int(context.resolve_value(count)) if count else 4

            if sys.platform == 'win32':
                cmd = ['ping', '-n', str(resolved_count), resolved_host]
            else:
                cmd = ['ping', '-c', str(resolved_count), resolved_host]

            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            success = result.returncode == 0
            context.set(output_var, success)

            return ActionResult(
                success=True,
                message=f"Ping {'成功' if success else '失败'}: {resolved_host}",
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


class NetworkDnsLookupAction(BaseAction):
    """DNS lookup."""
    action_type = "network2_dns"
    display_name = "DNS查询"
    description = "查询域名的DNS记录"

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
            resolved_domain = context.resolve_value(domain)

            ip = socket.gethostbyname(resolved_domain)
            context.set(output_var, ip)

            return ActionResult(
                success=True,
                message=f"DNS查询: {resolved_domain} -> {ip}",
                data={
                    'domain': resolved_domain,
                    'ip': ip,
                    'output_var': output_var
                }
            )
        except socket.gaierror:
            return ActionResult(
                success=False,
                message=f"DNS查询失败: 域名不存在"
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


class NetworkGetLocalIpAction(BaseAction):
    """Get local IP."""
    action_type = "network2_local_ip"
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
        output_var = params.get('output_var', 'local_ip')

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            finally:
                s.close()

            context.set(output_var, ip)

            return ActionResult(
                success=True,
                message=f"本地IP: {ip}",
                data={
                    'ip': ip,
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
        return {'output_var': 'local_ip'}


class NetworkGetPublicIpAction(BaseAction):
    """Get public IP."""
    action_type = "network2_public_ip"
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
        output_var = params.get('output_var', 'public_ip')

        try:
            response = urllib.request.urlopen('https://api.ipify.org', timeout=10)
            ip = response.read().decode()

            context.set(output_var, ip)

            return ActionResult(
                success=True,
                message=f"公网IP: {ip}",
                data={
                    'ip': ip,
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
        return {'output_var': 'public_ip'}


class NetworkPortCheckAction(BaseAction):
    """Check if port is open."""
    action_type = "network2_port_check"
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
            params: Dict with host, port, timeout, output_var.

        Returns:
            ActionResult with port check result.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'port_result')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = int(context.resolve_value(port))
            resolved_timeout = int(context.resolve_value(timeout)) if timeout else 5

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(resolved_timeout)
            result = sock.connect_ex((resolved_host, resolved_port))
            sock.close()

            is_open = result == 0
            context.set(output_var, is_open)

            return ActionResult(
                success=True,
                message=f"端口{'开放' if is_open else '关闭'}: {resolved_host}:{resolved_port}",
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
        return {'timeout': 5, 'output_var': 'port_result'}