"""Http3 action module for RabAI AutoClick.

Provides additional HTTP operations:
- HttpStatusAction: Get HTTP status code
- HttpIsSuccessAction: Check if status is success
- HttpIsRedirectAction: Check if status is redirect
- HttpIsClientErrorAction: Check if status is client error
- HttpIsServerErrorAction: Check if status is server error
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpStatusAction(BaseAction):
    """Get HTTP status code."""
    action_type = "http3_status"
    display_name = "HTTP状态码"
    description = "获取HTTP状态码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP status.

        Args:
            context: Execution context.
            params: Dict with status_code, output_var.

        Returns:
            ActionResult with status info.
        """
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'http_status')

        try:
            resolved = int(context.resolve_value(status_code))
            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"HTTP状态码: {resolved}",
                data={
                    'status_code': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取HTTP状态码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['status_code']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'http_status'}


class HttpIsSuccessAction(BaseAction):
    """Check if status is success."""
    action_type = "http3_is_success"
    display_name = "HTTP成功判断"
    description = "判断HTTP状态码是否表示成功"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is success.

        Args:
            context: Execution context.
            params: Dict with status_code, output_var.

        Returns:
            ActionResult with success check.
        """
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'is_success')

        try:
            resolved = int(context.resolve_value(status_code))
            result = 200 <= resolved < 300
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP成功: {'是' if result else '否'}",
                data={
                    'status_code': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP成功判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['status_code']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_success'}


class HttpIsRedirectAction(BaseAction):
    """Check if status is redirect."""
    action_type = "http3_is_redirect"
    display_name = "HTTP重定向判断"
    description = "判断HTTP状态码是否表示重定向"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is redirect.

        Args:
            context: Execution context.
            params: Dict with status_code, output_var.

        Returns:
            ActionResult with redirect check.
        """
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'is_redirect')

        try:
            resolved = int(context.resolve_value(status_code))
            result = 300 <= resolved < 400
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP重定向: {'是' if result else '否'}",
                data={
                    'status_code': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP重定向判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['status_code']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_redirect'}


class HttpIsClientErrorAction(BaseAction):
    """Check if status is client error."""
    action_type = "http3_is_client_error"
    display_name = "HTTP客户端错误判断"
    description = "判断HTTP状态码是否表示客户端错误"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is client error.

        Args:
            context: Execution context.
            params: Dict with status_code, output_var.

        Returns:
            ActionResult with client error check.
        """
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'is_client_error')

        try:
            resolved = int(context.resolve_value(status_code))
            result = 400 <= resolved < 500
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP客户端错误: {'是' if result else '否'}",
                data={
                    'status_code': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP客户端错误判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['status_code']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_client_error'}


class HttpIsServerErrorAction(BaseAction):
    """Check if status is server error."""
    action_type = "http3_is_server_error"
    display_name = "HTTP服务端错误判断"
    description = "判断HTTP状态码是否表示服务端错误"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is server error.

        Args:
            context: Execution context.
            params: Dict with status_code, output_var.

        Returns:
            ActionResult with server error check.
        """
        status_code = params.get('status_code', 200)
        output_var = params.get('output_var', 'is_server_error')

        try:
            resolved = int(context.resolve_value(status_code))
            result = 500 <= resolved < 600
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTTP服务端错误: {'是' if result else '否'}",
                data={
                    'status_code': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTTP服务端错误判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['status_code']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_server_error'}
