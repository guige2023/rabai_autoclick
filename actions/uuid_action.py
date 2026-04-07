"""UUID action module for RabAI AutoClick.

Provides UUID operations:
- UuidGenerateAction: Generate UUID
- UuidUuid1Action: Generate UUID from host and time
- UuidUuid4Action: Generate random UUID
"""

import uuid
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UuidGenerateAction(BaseAction):
    """Generate UUID."""
    action_type = "uuid_generate"
    display_name = "生成UUID"
    description = "生成通用唯一标识符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID generation.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UUID.
        """
        output_var = params.get('output_var', 'uuid_value')

        try:
            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID已生成: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_value'}


class UuidUuid1Action(BaseAction):
    """Generate UUID from host and time."""
    action_type = "uuid_uuid1"
    display_name = "生成UUID1"
    description = "基于主机和时间生成UUID"

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
            ActionResult with UUID.
        """
        node = params.get('node', None)
        clock_seq = params.get('clock_seq', None)
        output_var = params.get('output_var', 'uuid_value')

        try:
            if node is not None:
                resolved_node = context.resolve_value(node)
                node = int(resolved_node)

            if clock_seq is not None:
                resolved_clock = context.resolve_value(clock_seq)
                clock_seq = int(resolved_clock)

            result = str(uuid.uuid1(node=node, clock_seq=clock_seq))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID1已生成: {result}",
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
        return {'node': None, 'clock_seq': None, 'output_var': 'uuid_value'}


class UuidUuid4Action(BaseAction):
    """Generate random UUID."""
    action_type = "uuid_uuid4"
    display_name = "生成UUID4"
    description = "生成随机UUID"

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
            ActionResult with UUID.
        """
        output_var = params.get('output_var', 'uuid_value')

        try:
            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID4已生成: {result}",
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
        return {'output_var': 'uuid_value'}


class UuidValidateAction(BaseAction):
    """Validate UUID string."""
    action_type = "uuid_validate"
    display_name = "验证UUID"
    description = "验证UUID字符串格式"

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
        output_var = params.get('output_var', 'uuid_valid')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            try:
                uuid.UUID(resolved)
                result = True
            except ValueError:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'uuid': resolved if result else None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_valid'}