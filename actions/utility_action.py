"""Utility action module for RabAI AutoClick.

Provides utility operations:
- UtilityNowAction: Get current timestamp
- UtilityUuidAction: Generate UUID
- UtilityHashAction: Hash value
- UtilityIdentityAction: Return input unchanged
- UtilityConstAction: Return constant value
"""

import uuid
import hashlib
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UtilityNowAction(BaseAction):
    """Get current timestamp."""
    action_type = "utility_now"
    display_name = "获取当前时间戳"
    description = "获取当前Unix时间戳"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get now.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with timestamp.
        """
        output_var = params.get('output_var', 'current_timestamp')

        try:
            import time
            result = time.time()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前时间戳: {result}",
                data={
                    'timestamp': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时间戳失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'current_timestamp'}


class UtilityUuidAction(BaseAction):
    """Generate UUID."""
    action_type = "utility_uuid"
    display_name = "生成UUID"
    description = "生成通用唯一标识符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate uuid.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UUID.
        """
        output_var = params.get('output_var', 'generated_uuid')

        try:
            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'generated_uuid'}


class UtilityHashAction(BaseAction):
    """Hash value."""
    action_type = "utility_hash"
    display_name = "哈希值"
    description = "计算值的哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hash.

        Args:
            context: Execution context.
            params: Dict with value, algorithm, output_var.

        Returns:
            ActionResult with hash.
        """
        value = params.get('value', '')
        algorithm = params.get('algorithm', 'md5')
        output_var = params.get('output_var', 'hash_result')

        valid, msg = self.validate_type(algorithm, str, 'algorithm')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_algo = context.resolve_value(algorithm)

            if resolved_algo == 'md5':
                result = hashlib.md5(str(resolved_value).encode()).hexdigest()
            elif resolved_algo == 'sha1':
                result = hashlib.sha1(str(resolved_value).encode()).hexdigest()
            elif resolved_algo == 'sha256':
                result = hashlib.sha256(str(resolved_value).encode()).hexdigest()
            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的哈希算法: {resolved_algo}"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"哈希值: {result[:16]}...",
                data={
                    'algorithm': resolved_algo,
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'md5', 'output_var': 'hash_result'}


class UtilityIdentityAction(BaseAction):
    """Return input unchanged."""
    action_type = "utility_identity"
    display_name = "返回输入"
    description = "返回输入值，不做任何修改"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute identity.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with input value.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'identity_result')

        try:
            resolved = context.resolve_value(value) if value is not None else None
            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"返回: {resolved}",
                data={
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"返回输入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'identity_result'}


class UtilityConstAction(BaseAction):
    """Return constant value."""
    action_type = "utility_const"
    display_name = "返回常量"
    description = "返回常量值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute const.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with constant value.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'const_result')

        try:
            resolved = context.resolve_value(value) if value is not None else None
            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"常量: {resolved}",
                data={
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"返回常量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': None, 'output_var': 'const_result'}
