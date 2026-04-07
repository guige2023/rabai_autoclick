"""Yaml2 action module for RabAI AutoClick.

Provides additional YAML operations:
- YamlParseAction: Parse YAML string
- YamlDumpAction: Dump object to YAML
- YamlReadAction: Read YAML file
- YamlWriteAction: Write YAML file
- YamlValidateAction: Validate YAML string
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
    action_type = "yaml2_parse"
    display_name = "YAML解析"
    description = "解析YAML字符串"
    version = "2.0"

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
            ActionResult with parsed data.
        """
        yaml_str = params.get('yaml_str', '')
        output_var = params.get('output_var', 'yaml_parsed')

        valid, msg = self.validate_type(yaml_str, str, 'yaml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(yaml_str)
            result = yaml.safe_load(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML解析完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_parsed'}


class YamlDumpAction(BaseAction):
    """Dump object to YAML."""
    action_type = "yaml2_dump"
    display_name = "YAML转储"
    description = "将对象转换为YAML"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML dump.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with YAML string.
        """
        data = params.get('data', None)
        output_var = params.get('output_var', 'yaml_result')

        try:
            resolved = context.resolve_value(data)

            result = yaml.dump(resolved, allow_unicode=True, default_flow_style=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML转储完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML转储失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_result'}


class YamlReadAction(BaseAction):
    """Read YAML file."""
    action_type = "yaml2_read"
    display_name = "YAML读取"
    description = "读取YAML文件"
    version = "2.0"

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
                result = yaml.safe_load(f)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML读取完成",
                data={
                    'file_path': resolved_path,
                    'result': result,
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
    action_type = "yaml2_write"
    display_name = "YAML写入"
    description = "写入YAML文件"
    version = "2.0"

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
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', None)
        output_var = params.get('output_var', 'write_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                yaml.dump(resolved_data, f, allow_unicode=True, default_flow_style=False)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"YAML写入完成",
                data={
                    'file_path': resolved_path,
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
        return {'output_var': 'write_status'}


class YamlValidateAction(BaseAction):
    """Validate YAML string."""
    action_type = "yaml2_validate"
    display_name = "YAML验证"
    description = "验证YAML字符串格式"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute YAML validate.

        Args:
            context: Execution context.
            params: Dict with yaml_str, output_var.

        Returns:
            ActionResult with validation result.
        """
        yaml_str = params.get('yaml_str', '')
        output_var = params.get('output_var', 'validate_result')

        valid, msg = self.validate_type(yaml_str, str, 'yaml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(yaml_str)
            yaml.safe_load(resolved)
            result = True

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"YAML验证: {'有效' if result else '无效'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            context.set(output_var, False)

            return ActionResult(
                success=True,
                message=f"YAML验证: 无效 - {str(e)}",
                data={
                    'result': False,
                    'error': str(e),
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['yaml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validate_result'}