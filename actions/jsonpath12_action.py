"""JSONPath12 action module for RabAI AutoClick.

Provides additional JSONPath operations:
- JSONPathQueryAction: Query JSON with JSONPath
- JSONPathSetAction: Set value with JSONPath
- JSONPathDeleteAction: Delete value with JSONPath
- JSONPathKeysAction: Get all keys with JSONPath
- JSONPathValuesAction: Get all values with JSONPath
- JSONPathExistsAction: Check if path exists
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JSONPathQueryAction(BaseAction):
    """Query JSON with JSONPath."""
    action_type = "jsonpath12_query"
    display_name = "JSONPath查询"
    description = "使用JSONPath查询JSON"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath query.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, output_var.

        Returns:
            ActionResult with query results.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '$')
        output_var = params.get('output_var', 'query_result')

        try:
            import json

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            resolved_path = context.resolve_value(path) if path else '$'

            # Simple JSONPath implementation
            result = self._jsonpath_query(resolved, resolved_path)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath查询: {len(result) if isinstance(result, list) else 1}结果",
                data={
                    'path': resolved_path,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath查询失败: {str(e)}"
            )

    def _jsonpath_query(self, obj, path):
        """Simple JSONPath query implementation."""
        if not path or path == '$':
            return obj

        if path.startswith('$.'):
            path = path[2:]

        parts = path.split('.')
        current = obj

        for part in parts:
            if part == '*':
                if isinstance(current, list):
                    return current
                elif isinstance(current, dict):
                    return list(current.values())
            elif isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    current = None
            else:
                return None

        return current if current is not None else []

    def get_required_params(self) -> List[str]:
        return ['json_obj', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'query_result'}


class JSONPathSetAction(BaseAction):
    """Set value with JSONPath."""
    action_type = "jsonpath12_set"
    display_name = "JSONPath设置"
    description = "使用JSONPath设置值"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath set.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, value, output_var.

        Returns:
            ActionResult with modified JSON.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'modified_json')

        try:
            import json
            import copy

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            resolved = copy.deepcopy(resolved)
            resolved_path = context.resolve_value(path) if path else ''
            resolved_value = context.resolve_value(value) if value is not None else None

            # Simple JSONPath set implementation
            result = self._jsonpath_set(resolved, resolved_path, resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath设置: {resolved_path}",
                data={
                    'path': resolved_path,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath设置失败: {str(e)}"
            )

    def _jsonpath_set(self, obj, path, value):
        """Simple JSONPath set implementation."""
        if not path or path == '$':
            return value

        if path.startswith('$.'):
            path = path[2:]

        parts = path.split('.')
        current = obj

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value
        return obj

    def get_required_params(self) -> List[str]:
        return ['json_obj', 'path', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'modified_json'}


class JSONPathDeleteAction(BaseAction):
    """Delete value with JSONPath."""
    action_type = "jsonpath12_delete"
    display_name = "JSONPath删除"
    description = "使用JSONPath删除值"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath delete.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, output_var.

        Returns:
            ActionResult with modified JSON.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '')
        output_var = params.get('output_var', 'modified_json')

        try:
            import json
            import copy

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            resolved = copy.deepcopy(resolved)
            resolved_path = context.resolve_value(path) if path else ''

            # Simple JSONPath delete implementation
            result = self._jsonpath_delete(resolved, resolved_path)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath删除: {resolved_path}",
                data={
                    'path': resolved_path,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath删除失败: {str(e)}"
            )

    def _jsonpath_delete(self, obj, path):
        """Simple JSONPath delete implementation."""
        if not path or path == '$':
            return None

        if path.startswith('$.'):
            path = path[2:]

        parts = path.split('.')
        current = obj

        for part in parts[:-1]:
            if part not in current:
                return obj
            current = current[part]

        if parts[-1] in current:
            del current[parts[-1]]

        return obj

    def get_required_params(self) -> List[str]:
        return ['json_obj', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'modified_json'}


class JSONPathKeysAction(BaseAction):
    """Get all keys with JSONPath."""
    action_type = "jsonpath12_keys"
    display_name = "JSONPath键"
    description = "使用JSONPath获取所有键"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath keys.

        Args:
            context: Execution context.
            params: Dict with json_obj, output_var.

        Returns:
            ActionResult with keys.
        """
        json_obj = params.get('json_obj', {})
        output_var = params.get('output_var', 'keys_result')

        try:
            import json

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            if isinstance(resolved, dict):
                result = list(resolved.keys())
            elif isinstance(resolved, list):
                result = list(range(len(resolved)))
            else:
                result = []

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath键: {len(result)}个",
                data={
                    'keys': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'keys_result'}


class JSONPathValuesAction(BaseAction):
    """Get all values with JSONPath."""
    action_type = "jsonpath12_values"
    display_name = "JSONPath值"
    description = "使用JSONPath获取所有值"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath values.

        Args:
            context: Execution context.
            params: Dict with json_obj, output_var.

        Returns:
            ActionResult with values.
        """
        json_obj = params.get('json_obj', {})
        output_var = params.get('output_var', 'values_result')

        try:
            import json

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            if isinstance(resolved, dict):
                result = list(resolved.values())
            elif isinstance(resolved, list):
                result = list(resolved)
            else:
                result = [resolved]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath值: {len(result)}个",
                data={
                    'values': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'values_result'}


class JSONPathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "jsonpath12_exists"
    display_name = "JSONPath存在"
    description = "检查JSONPath是否存在"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSONPath exists.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, output_var.

        Returns:
            ActionResult with exists result.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        try:
            import json

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                resolved = json.loads(resolved)

            resolved_path = context.resolve_value(path) if path else ''

            # Simple JSONPath exists implementation
            result = self._jsonpath_exists(resolved, resolved_path)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSONPath存在: {'是' if result else '否'}",
                data={
                    'path': resolved_path,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSONPath存在检查失败: {str(e)}"
            )

    def _jsonpath_exists(self, obj, path):
        """Simple JSONPath exists implementation."""
        if not path or path == '$':
            return True

        if path.startswith('$.'):
            path = path[2:]

        parts = path.split('.')
        current = obj

        for part in parts:
            if part == '*':
                continue
            elif isinstance(current, dict):
                if part not in current:
                    return False
                current = current[part]
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    if idx < 0 or idx >= len(current):
                        return False
                    current = current[idx]
                except ValueError:
                    return False
            else:
                return False

        return True

    def get_required_params(self) -> List[str]:
        return ['json_obj', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}