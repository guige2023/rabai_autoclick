"""YAML action module for RabAI AutoClick.

Provides YAML operations:
- YamlParseAction: Parse YAML string
- YamlStringifyAction: Convert to YAML string
- YamlReadAction: Read YAML file
- YamlWriteAction: Write YAML file
"""

import yaml
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class YamlParseAction(BaseAction):
    """Parse YAML string."""
    action_type = "yaml_parse"
    display_name = "解析YAML"
    description = "解析YAML字符串为对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML parse.

        Args:
            context: Execution context.
            params: Dict with yaml_string, output_var.

        Returns:
            ActionResult with parsed object.
        """
        yaml_string = params.get('yaml_string', '')
        output_var = params.get('output_var', 'parsed_yaml')

        valid, msg = self.validate_type(yaml_string, str, 'yaml_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(yaml_string)
            parsed = yaml.safe_load(resolved)
            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"YAML解析成功",
                data={
                    'result': parsed,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_yaml'}


class YamlStringifyAction(BaseAction):
    """Convert to YAML string."""
    action_type = "yaml_stringify"
    display_name = "转换为YAML"
    description = "将对象转换为YAML字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML stringify.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with YAML string.
        """
        data = params.get('data', None)
        output_var = params.get('output_var', 'yaml_string')

        try:
            resolved = context.resolve_value(data)
            result = yaml.safe_dump(resolved, allow_unicode=True, default_flow_style=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML转换成功",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_string'}


class YamlReadAction(BaseAction):
    """Read YAML file."""
    action_type = "yaml_read"
    display_name = "读取YAML"
    description = "读取YAML文件内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML read.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with YAML data.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"YAML读取成功",
                data={
                    'result': data,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_data'}


class YamlWriteAction(BaseAction):
    """Write YAML file."""
    action_type = "yaml_write"
    display_name = "写入YAML"
    description = "将数据写入YAML文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, output_var.

        Returns:
            ActionResult with write result.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', None)
        output_var = params.get('output_var', 'yaml_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(resolved_data, f, allow_unicode=True, default_flow_style=False)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"YAML写入成功",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_result'}