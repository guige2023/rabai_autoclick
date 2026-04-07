"""Environment4 action module for RabAI AutoClick.

Provides additional environment operations:
- EnvironmentGetAllAction: Get all environment variables
- EnvironmentGetUserAction: Get current user
- EnvironmentGetHomeAction: Get home directory
- EnvironmentGetCwdAction: Get current working directory
- EnvironmentGetHostnameAction: Get hostname
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnvironmentGetAllAction(BaseAction):
    """Get all environment variables."""
    action_type = "environment4_get_all"
    display_name = "获取所有环境变量"
    description = "获取所有环境变量"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get all env.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with all environment variables.
        """
        output_var = params.get('output_var', 'all_env')

        try:
            result = dict(os.environ)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取所有环境变量: {len(result)}个",
                data={
                    'count': len(result),
                    'environment': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取所有环境变量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'all_env'}


class EnvironmentGetUserAction(BaseAction):
    """Get current user."""
    action_type = "environment4_user"
    display_name = "获取当前用户"
    description = "获取当前用户名"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get user.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current user.
        """
        output_var = params.get('output_var', 'current_user')

        try:
            import getpass

            result = getpass.getuser()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前用户: {result}",
                data={
                    'user': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前用户失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'current_user'}


class EnvironmentGetHomeAction(BaseAction):
    """Get home directory."""
    action_type = "environment4_home"
    display_name = "获取主目录"
    description = "获取用户主目录"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get home.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with home directory.
        """
        output_var = params.get('output_var', 'home_dir')

        try:
            result = os.path.expanduser('~')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"主目录: {result}",
                data={
                    'home': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取主目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'home_dir'}


class EnvironmentGetCwdAction(BaseAction):
    """Get current working directory."""
    action_type = "environment4_cwd"
    display_name = "获取当前目录"
    description = "获取当前工作目录"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get cwd.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current working directory.
        """
        output_var = params.get('output_var', 'cwd')

        try:
            result = os.getcwd()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前目录: {result}",
                data={
                    'cwd': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cwd'}


class EnvironmentGetHostnameAction(BaseAction):
    """Get hostname."""
    action_type = "environment4_hostname"
    display_name = "获取主机名"
    description = "获取主机名"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get hostname.

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