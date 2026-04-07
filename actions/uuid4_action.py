"""Uuid4 action module for RabAI AutoClick.

Provides additional UUID operations:
- UuidUUID1Action: Generate UUID1
- UuidUUID4Action: Generate UUID4
- UuidUUID5Action: Generate UUID5
- UuidValidateAction: Validate UUID
- UuidToStringAction: UUID to string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UuidUUID1Action(BaseAction):
    """Generate UUID1."""
    action_type = "uuid4_uuid1"
    display_name = "生成UUID1"
    description = "生成基于时间的UUID"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID1.

        Args:
            context: Execution context.
            params: Dict with node, clock_seq, output_var.

        Returns:
            ActionResult with UUID.
        """
        node = params.get('node', None)
        clock_seq = params.get('clock_seq', None)
        output_var = params.get('output_var', 'uuid1')

        try:
            import uuid

            resolved_node = int(context.resolve_value(node)) if node else None
            resolved_clock = int(context.resolve_value(clock_seq)) if clock_seq else None

            if resolved_node and resolved_clock:
                result = str(uuid.uuid1(node=resolved_node, clock_seq=resolved_clock))
            elif resolved_node:
                result = str(uuid.uuid1(node=resolved_node))
            else:
                result = str(uuid.uuid1())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID1生成: {result}",
                data={
                    'uuid': result,
                    'version': 1,
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
        return {'node': None, 'clock_seq': None, 'output_var': 'uuid1'}


class UuidUUID4Action(BaseAction):
    """Generate UUID4."""
    action_type = "uuid4_uuid4"
    display_name = "生成UUID4"
    description = "生成随机UUID"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID4.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UUID.
        """
        output_var = params.get('output_var', 'uuid4')

        try:
            import uuid

            result = str(uuid.uuid4())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID4生成: {result}",
                data={
                    'uuid': result,
                    'version': 4,
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
        return {'output_var': 'uuid4'}


class UuidUUID5Action(BaseAction):
    """Generate UUID5."""
    action_type = "uuid4_uuid5"
    display_name = "生成UUID5"
    description = "生成基于命名空间的UUID5"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID5.

        Args:
            context: Execution context.
            params: Dict with namespace, name, output_var.

        Returns:
            ActionResult with UUID.
        """
        namespace = params.get('namespace', 'DNS')
        name = params.get('name', '')
        output_var = params.get('output_var', 'uuid5')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import uuid

            resolved_ns = context.resolve_value(namespace)
            resolved_name = context.resolve_value(name)

            if resolved_ns == 'DNS':
                ns = uuid.NAMESPACE_DNS
            elif resolved_ns == 'URL':
                ns = uuid.NAMESPACE_URL
            elif resolved_ns == 'OID':
                ns = uuid.NAMESPACE_OID
            elif resolved_ns == 'X500':
                ns = uuid.NAMESPACE_X500
            else:
                ns = uuid.NAMESPACE_DNS

            result = str(uuid.uuid5(ns, resolved_name))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID5生成: {result}",
                data={
                    'uuid': result,
                    'namespace': resolved_ns,
                    'name': resolved_name,
                    'version': 5,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID5生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'namespace': 'DNS', 'output_var': 'uuid5'}


class UuidValidateAction(BaseAction):
    """Validate UUID."""
    action_type = "uuid4_validate"
    display_name = "验证UUID"
    description = "验证UUID格式"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate UUID.

        Args:
            context: Execution context.
            params: Dict with uuid, output_var.

        Returns:
            ActionResult with validation result.
        """
        uuid_str = params.get('uuid', '')
        output_var = params.get('output_var', 'uuid_valid')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

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
                    'uuid': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_valid'}


class UuidToStringAction(BaseAction):
    """UUID to string."""
    action_type = "uuid4_to_string"
    display_name = "UUID转字符串"
    description = "将UUID转换为字符串"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID to string.

        Args:
            context: Execution context.
            params: Dict with uuid, output_var.

        Returns:
            ActionResult with UUID string.
        """
        uuid_str = params.get('uuid', '')
        output_var = params.get('output_var', 'uuid_string')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

            try:
                uid = uuid.UUID(resolved)
                result = str(uid)
            except ValueError:
                result = str(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID字符串: {result}",
                data={
                    'original': resolved,
                    'uuid_string': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_string'}