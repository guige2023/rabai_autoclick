"""UUID action module for RabAI AutoClick.

Provides UUID operations:
- UuidGenerateAction: Generate UUID
- UuidValidateAction: Validate UUID
- UuidInfoAction: Get UUID info
"""

import uuid
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UuidGenerateAction(BaseAction):
    """Generate UUID."""
    action_type = "uuid_generate"
    display_name = "生成UUID"
    description = "生成UUID"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with UUID.
        """
        version = params.get('version', 4)
        output_var = params.get('output_var', 'uuid')

        valid, msg = self.validate_type(version, int, 'version')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_version = context.resolve_value(version)

            if resolved_version == 1:
                uid = str(uuid.uuid1())
            elif resolved_version == 4:
                uid = str(uuid.uuid4())
            else:
                uid = str(uuid.uuid4())

            context.set(output_var, uid)

            return ActionResult(
                success=True,
                message=f"UUID: {uid}",
                data={'uuid': uid, 'version': resolved_version, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"UUID生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 4, 'output_var': 'uuid'}


class UuidBatchGenerateAction(BaseAction):
    """Generate batch UUIDs."""
    action_type = "uuid_batch_generate"
    display_name = "批量生成UUID"
    description = "批量生成UUID"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch generate.

        Args:
            context: Execution context.
            params: Dict with count, version, output_var.

        Returns:
            ActionResult with UUIDs.
        """
        count = params.get('count', 10)
        version = params.get('version', 4)
        output_var = params.get('output_var', 'uuids')

        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_count = context.resolve_value(count)
            resolved_version = context.resolve_value(version)

            uuids = []
            for _ in range(min(resolved_count, 1000)):
                if resolved_version == 1:
                    uuids.append(str(uuid.uuid1()))
                elif resolved_version == 4:
                    uuids.append(str(uuid.uuid4()))
                else:
                    uuids.append(str(uuid.uuid4()))

            context.set(output_var, uuids)

            return ActionResult(
                success=True,
                message=f"已生成 {len(uuids)} 个UUID",
                data={'count': len(uuids), 'uuids': uuids, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"UUID批量生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 4, 'output_var': 'uuids'}


class UuidValidateAction(BaseAction):
    """Validate UUID."""
    action_type = "uuid_validate"
    display_name = "验证UUID"
    description = "验证UUID格式"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with uuid, output_var.

        Returns:
            ActionResult with validation result.
        """
        uuid_str = params.get('uuid', '')
        output_var = params.get('output_var', 'uuid_valid')

        valid, msg = self.validate_type(uuid_str, str, 'uuid')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_uuid = context.resolve_value(uuid_str)

            is_valid = self._validate_uuid(resolved_uuid)
            context.set(output_var, is_valid)

            return ActionResult(
                success=True,
                message=f"UUID {'有效' if is_valid else '无效'}",
                data={'uuid': resolved_uuid, 'valid': is_valid, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"UUID验证失败: {str(e)}")

    def _validate_uuid(self, uuid_str: str) -> bool:
        try:
            uuid.UUID(uuid_str)
            return True
        except ValueError:
            return False

    def get_required_params(self) -> List[str]:
        return ['uuid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_valid'}


class UuidInfoAction(BaseAction):
    """Get UUID info."""
    action_type = "uuid_info"
    display_name = "UUID信息"
    description = "获取UUID信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info.

        Args:
            context: Execution context.
            params: Dict with uuid, output_var.

        Returns:
            ActionResult with UUID info.
        """
        uuid_str = params.get('uuid', '')
        output_var = params.get('output_var', 'uuid_info')

        valid, msg = self.validate_type(uuid_str, str, 'uuid')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_uuid = context.resolve_value(uuid_str)

            try:
                uid = uuid.UUID(resolved_uuid)
                info = {
                    'uuid': str(uid),
                    'version': uid.version,
                    'variant': str(uid.variant),
                    'node': str(uid.node) if hasattr(uid, 'node') and uid.node else None,
                    'time': str(uid.time) if hasattr(uid, 'time') and uid.time else None,
                }
                context.set(output_var, info)

                return ActionResult(
                    success=True,
                    message=f"UUID版本: {uid.version}",
                    data={'info': info, 'output_var': output_var}
                )
            except ValueError:
                return ActionResult(success=False, message="无效的UUID")
        except Exception as e:
            return ActionResult(success=False, message=f"UUID信息获取失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['uuid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_info'}
