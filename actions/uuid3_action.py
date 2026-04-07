"""Uuid3 action module for RabAI AutoClick.

Provides additional UUID operations:
- UuidUuid1Action: Generate UUID1
- UuidUuid4Action: Generate UUID4
- UuidFromStringAction: Convert string to UUID
- UuidToStringAction: Convert UUID to string
- UuidValidateAction: Validate UUID format
"""

import uuid
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UuidUuid1Action(BaseAction):
    """Generate UUID1."""
    action_type = "uuid3_uuid1"
    display_name = "UUID1生成"
    description = "基于时间和主机生成UUID"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID1 generation.

        Args:
            context: Execution context.
            params: Dict with node, clock_seq, output_var.

        Returns:
            ActionResult with generated UUID.
        """
        node = params.get('node', None)
        clock_seq = params.get('clock_seq', None)
        output_var = params.get('output_var', 'uuid_result')

        try:
            resolved_node = int(context.resolve_value(node)) if node else None
            resolved_clock_seq = int(context.resolve_value(clock_seq)) if clock_seq else None

            result = str(uuid.uuid1(node=resolved_node, clock_seq=resolved_clock_seq))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID1生成: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID1生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'node': None, 'clock_seq': None, 'output_var': 'uuid_result'}


class UuidUuid4Action(BaseAction):
    """Generate UUID4."""
    action_type = "uuid3_uuid4"
    display_name = "UUID4生成"
    description = "生成随机UUID"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID4 generation.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with generated UUID.
        """
        output_var = params.get('output_var', 'uuid_result')

        try:
            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID4生成: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID4生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_result'}


class UuidFromStringAction(BaseAction):
    """Convert string to UUID."""
    action_type = "uuid3_from_string"
    display_name = "字符串转UUID"
    description = "将字符串转换为UUID对象"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string to UUID.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with UUID string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'uuid_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            u = uuid.UUID(resolved)
            result = str(u)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串转UUID: {result}",
                data={
                    'original': resolved,
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串转UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_result'}


class UuidToStringAction(BaseAction):
    """Convert UUID to string."""
    action_type = "uuid3_to_string"
    display_name = "UUID转字符串"
    description = "将UUID转换为字符串"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID to string.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with UUID string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'string_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = resolved
            else:
                result = str(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID转字符串: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class UuidValidateAction(BaseAction):
    """Validate UUID format."""
    action_type = "uuid3_validate"
    display_name = "UUID验证"
    description = "验证字符串是否为有效UUID格式"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID validation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'validate_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            uuid.UUID(resolved)
            result = True

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID验证: {'有效' if result else '无效'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            context.set(output_var, False)

            return ActionResult(
                success=True,
                message=f"UUID验证: 无效",
                data={
                    'value': resolved,
                    'result': False,
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validate_result'}