"""UUID9 action module for RabAI AutoClick.

Provides additional UUID operations:
- UUIDGenerateAction: Generate UUID
- UUIDParseAction: Parse UUID string
- UUIDValidateAction: Validate UUID
- UUIDVersionAction: Get UUID version
- UUIDFieldsAction: Get UUID fields
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UUIDGenerateAction(BaseAction):
    """Generate UUID."""
    action_type = "uuid9_generate"
    display_name = "生成UUID"
    description = "生成UUID"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID generate.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with generated UUID.
        """
        version = params.get('version', 4)
        output_var = params.get('output_var', 'generated_uuid')

        try:
            import uuid

            resolved_version = int(context.resolve_value(version)) if version else 4

            if resolved_version == 1:
                result = str(uuid.uuid1())
            elif resolved_version == 4:
                result = str(uuid.uuid4())
            else:
                result = str(uuid.uuid4())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成UUID: {result}",
                data={
                    'uuid': result,
                    'version': resolved_version,
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
        return {'version': 4, 'output_var': 'generated_uuid'}


class UUIDParseAction(BaseAction):
    """Parse UUID string."""
    action_type = "uuid9_parse"
    display_name = "解析UUID"
    description = "解析UUID字符串"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID parse.

        Args:
            context: Execution context.
            params: Dict with uuid_str, output_var.

        Returns:
            ActionResult with parsed UUID.
        """
        uuid_str = params.get('uuid_str', '')
        output_var = params.get('output_var', 'parsed_uuid')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

            result = uuid.UUID(resolved)
            context.set(output_var, str(result))

            return ActionResult(
                success=True,
                message=f"解析UUID: {str(result)}",
                data={
                    'uuid': str(result),
                    'version': result.version,
                    'variant': result.variant,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"无效的UUID格式: {uuid_str}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_uuid'}


class UUIDValidateAction(BaseAction):
    """Validate UUID."""
    action_type = "uuid9_validate"
    display_name = "验证UUID"
    description = "验证UUID格式"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID validate.

        Args:
            context: Execution context.
            params: Dict with uuid_str, output_var.

        Returns:
            ActionResult with validation result.
        """
        uuid_str = params.get('uuid_str', '')
        output_var = params.get('output_var', 'validate_result')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

            result = True
            try:
                uuid.UUID(resolved)
            except ValueError:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"验证UUID: {'有效' if result else '无效'}",
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
        return ['uuid_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validate_result'}


class UUIDVersionAction(BaseAction):
    """Get UUID version."""
    action_type = "uuid9_version"
    display_name = "UUID版本"
    description = "获取UUID版本"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID version.

        Args:
            context: Execution context.
            params: Dict with uuid_str, output_var.

        Returns:
            ActionResult with UUID version.
        """
        uuid_str = params.get('uuid_str', '')
        output_var = params.get('output_var', 'uuid_version')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

            result = uuid.UUID(resolved)
            version = result.version

            context.set(output_var, version)

            return ActionResult(
                success=True,
                message=f"UUID版本: {version}",
                data={
                    'uuid': str(result),
                    'version': version,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"无效的UUID格式: {uuid_str}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取UUID版本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_version'}


class UUIDFieldsAction(BaseAction):
    """Get UUID fields."""
    action_type = "uuid9_fields"
    display_name = "UUID字段"
    description = "获取UUID字段"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID fields.

        Args:
            context: Execution context.
            params: Dict with uuid_str, output_var.

        Returns:
            ActionResult with UUID fields.
        """
        uuid_str = params.get('uuid_str', '')
        output_var = params.get('output_var', 'uuid_fields')

        try:
            import uuid

            resolved = context.resolve_value(uuid_str)

            result = uuid.UUID(resolved)
            fields = result.fields

            data = {
                'time_low': fields[0],
                'time_mid': fields[1],
                'time_hi_version': fields[2],
                'clock_seq_hi_variant': fields[3],
                'clock_seq_low': fields[4],
                'node': fields[5]
            }

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"UUID字段: {len(data)}个",
                data={
                    'uuid': str(result),
                    'fields': data,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"无效的UUID格式: {uuid_str}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取UUID字段失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_fields'}