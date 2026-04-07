"""UUID2 action module for RabAI AutoClick.

Provides additional UUID operations:
- UuidGenerateV1Action: Generate UUID v1
- UuidGenerateV4Action: Generate UUID v4
- UuidGenerateV5Action: Generate UUID v5
- UuidFromStringAction: Parse UUID from string
- UuidToStringAction: Convert UUID to string
"""

import uuid
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UuidGenerateV1Action(BaseAction):
    """Generate UUID v1 (time-based)."""
    action_type = "uuid_v1"
    display_name = "生成UUIDv1"
    description = "生成基于时间的UUID v1"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID v1 generation.

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
            resolved_node = int(context.resolve_value(node), 16) if node else None
            resolved_clock = int(context.resolve_value(clock_seq)) if clock_seq else None

            if resolved_node:
                result = str(uuid.uuid1(node=resolved_node, clock_seq=resolved_clock))
            else:
                result = str(uuid.uuid1())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID v1生成: {result}",
                data={
                    'uuid': result,
                    'version': 1,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID v1生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'node': None, 'clock_seq': None, 'output_var': 'uuid_value'}


class UuidGenerateV4Action(BaseAction):
    """Generate UUID v4 (random)."""
    action_type = "uuid_v4"
    display_name = "生成UUIDv4"
    description = "生成随机UUID v4"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID v4 generation.

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
                message=f"UUID v4生成: {result}",
                data={
                    'uuid': result,
                    'version': 4,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID v4生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_value'}


class UuidGenerateV5Action(BaseAction):
    """Generate UUID v5 (name-based with SHA-1)."""
    action_type = "uuid_v5"
    display_name = "生成UUIDv5"
    description = "生成基于名字的UUID v5"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID v5 generation.

        Args:
            context: Execution context.
            params: Dict with namespace, name, output_var.

        Returns:
            ActionResult with UUID.
        """
        namespace = params.get('namespace', 'dns')
        name = params.get('name', '')
        output_var = params.get('output_var', 'uuid_value')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_namespace = context.resolve_value(namespace)
            resolved_name = context.resolve_value(name)

            if resolved_namespace == 'dns':
                ns = uuid.NAMESPACE_DNS
            elif resolved_namespace == 'url':
                ns = uuid.NAMESPACE_URL
            elif resolved_namespace == 'oid':
                ns = uuid.NAMESPACE_OID
            elif resolved_namespace == 'x500':
                ns = uuid.NAMESPACE_X500
            else:
                ns = uuid.UUID(resolved_namespace)

            result = str(uuid.uuid5(ns, resolved_name))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID v5生成: {result}",
                data={
                    'uuid': result,
                    'version': 5,
                    'namespace': str(ns),
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID v5生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'namespace': 'dns', 'output_var': 'uuid_value'}


class UuidFromStringAction(BaseAction):
    """Parse UUID from string."""
    action_type = "uuid_from_string"
    display_name = "字符串转UUID"
    description = "从字符串解析UUID"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID parsing.

        Args:
            context: Execution context.
            params: Dict with uuid_string, output_var.

        Returns:
            ActionResult with UUID object.
        """
        uuid_string = params.get('uuid_string', '')
        output_var = params.get('output_var', 'uuid_value')

        valid, msg = self.validate_type(uuid_string, str, 'uuid_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(uuid_string)
            u = uuid.UUID(resolved)

            context.set(output_var, str(u))

            return ActionResult(
                success=True,
                message=f"UUID解析成功: {u.version}",
                data={
                    'uuid': str(u),
                    'version': u.version,
                    'variant': u.variant,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"UUID格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_value'}


class UuidToStringAction(BaseAction):
    """Convert UUID to string."""
    action_type = "uuid_to_string"
    display_name = "UUID转字符串"
    description = "将UUID转换为标准化字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID to string.

        Args:
            context: Execution context.
            params: Dict with uuid_value, output_var.

        Returns:
            ActionResult with string.
        """
        uuid_value = params.get('uuid_value', '')
        output_var = params.get('output_var', 'uuid_string')

        try:
            resolved = context.resolve_value(uuid_value)

            if isinstance(resolved, uuid.UUID):
                result = str(resolved)
            else:
                u = uuid.UUID(str(resolved))
                result = str(u)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID转字符串: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"UUID格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_string'}