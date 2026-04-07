"""YAML10 action module for RabAI AutoClick.

Provides additional YAML operations:
- YAMLParseAction: Parse YAML string
- YAMLToStringAction: Convert to YAML string
- YAMLGetAction: Get value from YAML
- YAMLSetAction: Set value in YAML
- YAMLLoadAction: Load YAML from file
- YAML DumpAction: Dump YAML to file
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class YAMLParseAction(BaseAction):
    """Parse YAML string."""
    action_type = "yaml10_parse"
    display_name = "解析YAML"
    description = "解析YAML字符串"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML parse.

        Args:
            context: Execution context.
            params: Dict with yaml_str, output_var.

        Returns:
            ActionResult with parsed YAML.
        """
        yaml_str = params.get('yaml_str', '')
        output_var = params.get('output_var', 'parsed_yaml')

        try:
            import yaml

            resolved = context.resolve_value(yaml_str)

            if isinstance(resolved, str):
                result = yaml.safe_load(resolved)
            else:
                result = resolved

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析YAML成功",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except yaml.YAMLError as e:
            return ActionResult(
                success=False,
                message=f"YAML解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析YAML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_yaml'}


class YAMLToStringAction(BaseAction):
    """Convert to YAML string."""
    action_type = "yaml10_tostring"
    display_name = "转换为YAML"
    description = "将对象转换为YAML字符串"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML to string.

        Args:
            context: Execution context.
            params: Dict with obj, output_var.

        Returns:
            ActionResult with YAML string.
        """
        obj = params.get('obj', None)
        output_var = params.get('output_var', 'yaml_string')

        try:
            import yaml

            resolved = context.resolve_value(obj) if obj is not None else None

            result = yaml.dump(resolved, allow_unicode=True, default_flow_style=False)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为YAML: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为YAML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_string'}


class YAMLGetAction(BaseAction):
    """Get value from YAML."""
    action_type = "yaml10_get"
    display_name = "YAML取值"
    description = "从YAML获取值"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML get.

        Args:
            context: Execution context.
            params: Dict with yaml_obj, path, output_var.

        Returns:
            ActionResult with value.
        """
        yaml_obj = params.get('yaml_obj', {})
        path = params.get('path', '')
        output_var = params.get('output_var', 'yaml_value')

        try:
            import yaml

            resolved = context.resolve_value(yaml_obj)

            if isinstance(resolved, str):
                resolved = yaml.safe_load(resolved)

            resolved_path = context.resolve_value(path) if path else ''

            if resolved_path:
                parts = resolved_path.split('.')
                result = resolved
                for part in parts:
                    if isinstance(result, dict):
                        result = result.get(part)
                    elif isinstance(result, list):
                        try:
                            result = result[int(part)]
                        except (ValueError, IndexError):
                            result = None
                    else:
                        result = None
            else:
                result = resolved

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML取值: {resolved_path}",
                data={
                    'yaml_obj': resolved,
                    'path': resolved_path,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'output_var': 'yaml_value'}


class YAMLSetAction(BaseAction):
    """Set value in YAML."""
    action_type = "yaml10_set"
    display_name = "YAML设值"
    description = "在YAML中设置值"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML set.

        Args:
            context: Execution context.
            params: Dict with yaml_obj, path, value, output_var.

        Returns:
            ActionResult with modified YAML.
        """
        yaml_obj = params.get('yaml_obj', {})
        path = params.get('path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'modified_yaml')

        try:
            import yaml
            import copy

            resolved = context.resolve_value(yaml_obj)

            if isinstance(resolved, str):
                resolved = yaml.safe_load(resolved) or {}

            resolved = copy.deepcopy(resolved)
            resolved_path = context.resolve_value(path) if path else ''
            resolved_value = context.resolve_value(value) if value is not None else None

            if resolved_path:
                parts = resolved_path.split('.')
                current = resolved
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = resolved_value
            else:
                resolved = resolved_value

            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"YAML设值: {resolved_path}",
                data={
                    'path': resolved_path,
                    'value': resolved_value,
                    'result': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML设值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_obj', 'path', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'modified_yaml'}


class YAMLLoadAction(BaseAction):
    """Load YAML from file."""
    action_type = "yaml10_load"
    display_name = "加载YAML文件"
    description = "从文件加载YAML"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML load from file.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with loaded YAML.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'loaded_yaml')

        try:
            import yaml

            resolved_path = context.resolve_value(path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                result = yaml.safe_load(f)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"加载YAML文件: {resolved_path}",
                data={
                    'path': resolved_path,
                    'result': result,
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"文件未找到: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"加载YAML文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'loaded_yaml'}


class YAMLDumpAction(BaseAction):
    """Dump YAML to file."""
    action_type = "yaml10_dump"
    display_name = "保存YAML文件"
    description = "保存YAML到文件"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML dump to file.

        Args:
            context: Execution context.
            params: Dict with path, obj, output_var.

        Returns:
            ActionResult with dump status.
        """
        path = params.get('path', '')
        obj = params.get('obj', None)
        output_var = params.get('output_var', 'dump_status')

        try:
            import yaml

            resolved_path = context.resolve_value(path)
            resolved_obj = context.resolve_value(obj) if obj is not None else None

            with open(resolved_path, 'w', encoding='utf-8') as f:
                yaml.dump(resolved_obj, f, allow_unicode=True, default_flow_style=False)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"保存YAML文件: {resolved_path}",
                data={
                    'path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"保存YAML文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dump_status'}