"""Network3 action module for RabAI AutoClick.

Provides additional network operations:
- NetworkIsReachableAction: Check if host is reachable
- NetworkGetMacAction: Get MAC address
- NetworkInterfaceListAction: List network interfaces
- NetworkDownloadFileAction: Download file from URL
- NetworkExtractDomainAction: Extract domain from URL
"""

import socket
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NetworkIsReachableAction(BaseAction):
    """Check if host is reachable."""
    action_type = "network3_is_reachable"
    display_name = "检查网络可达"
    description = "检查主机是否可達"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is reachable.

        Args:
            context: Execution context.
            params: Dict with host, port, timeout, output_var.

        Returns:
            ActionResult with reachability.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        timeout = params.get('timeout', 5)
        output_var = params.get('output_var', 'is_reachable')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = int(context.resolve_value(port))
            resolved_timeout = int(context.resolve_value(timeout))

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(resolved_timeout)
            result = sock.connect_ex((resolved_host, resolved_port)) == 0
            sock.close()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"网络可达: {'是' if result else '否'}",
                data={
                    'host': resolved_host,
                    'port': resolved_port,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查网络可达失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'port': 80, 'timeout': 5, 'output_var': 'is_reachable'}


class NetworkGetMacAction(BaseAction):
    """Get MAC address."""
    action_type = "network3_get_mac"
    display_name = "获取MAC地址"
    description = "获取主机的MAC地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get MAC.

        Args:
            context: Execution context.
            params: Dict with host, output_var.

        Returns:
            ActionResult with MAC address.
        """
        host = params.get('host', '')
        output_var = params.get('output_var', 'mac_address')

        try:
            resolved_host = context.resolve_value(host) if host else None

            if resolved_host:
                result = socket.gethostbyaddr(resolved_host)
            else:
                import uuid
                result = ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff) for i in range(0, 48, 8)][::-1])

            context.set(output_var, str(result))

            return ActionResult(
                success=True,
                message=f"MAC地址: {result}",
                data={
                    'host': resolved_host,
                    'result': str(result),
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
        return {'host': '', 'output_var': 'mac_address'}


class NetworkInterfaceListAction(BaseAction):
    """List network interfaces."""
    action_type = "network3_interface_list"
    display_name = "列出网络接口"
    description = "列出所有网络接口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute interface list.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with interface list.
        """
        output_var = params.get('output_var', 'interface_list')

        try:
            import subprocess
            result = subprocess.check_output(['ifconfig'], text=True).strip()
            interfaces = [line.split(':')[0] for line in result.split('\n') if ':' in line]

            context.set(output_var, interfaces)

            return ActionResult(
                success=True,
                message=f"网络接口: {len(interfaces)} 个",
                data={
                    'interfaces': interfaces,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出网络接口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'interface_list'}


class NetworkDownloadFileAction(BaseAction):
    """Download file from URL."""
    action_type = "network3_download_file"
    display_name = "下载文件"
    description = "从URL下载文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute download file.

        Args:
            context: Execution context.
            params: Dict with url, path, output_var.

        Returns:
            ActionResult with download result.
        """
        url = params.get('url', '')
        path = params.get('path', './download')
        output_var = params.get('output_var', 'download_result')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request
            resolved_url = context.resolve_value(url)
            resolved_path = context.resolve_value(path)

            urllib.request.urlretrieve(resolved_url, resolved_path)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"文件下载完成: {resolved_path}",
                data={
                    'url': resolved_url,
                    'path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"下载文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'download_result'}


class NetworkExtractDomainAction(BaseAction):
    """Extract domain from URL."""
    action_type = "network3_extract_domain"
    display_name = "提取域名"
    description = "从URL提取域名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract domain.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with domain.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'domain')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from urllib.parse import urlparse
            resolved_url = context.resolve_value(url)

            parsed = urlparse(resolved_url)
            result = parsed.netloc or parsed.path.split('/')[0]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"域名: {result}",
                data={
                    'url': resolved_url,
                    'domain': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取域名失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'domain'}
