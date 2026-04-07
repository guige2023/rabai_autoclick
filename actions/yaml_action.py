"""YAML action module for RabAI AutoClick.

Provides YAML processing operations:
- YamlParseAction: Parse YAML string/file
- YamlDumpAction: Dump dictionary to YAML
- YamlValidateAction: Validate YAML syntax
- YamlMergeAction: Merge multiple YAML files
- YamlFlattenAction: Flatten nested YAML
- YamlCompactAction: Compact YAML (remove comments)
"""

import yaml
import os
from typing import Any, Dict, List, Optional, Union

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class YamlParseAction(BaseAction):
    """Parse YAML string/file."""
    action_type = "yaml_parse"
    display_name = "解析YAML"
    description = "解析YAML字符串或文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with content, file_path, output_var.

        Returns:
            ActionResult with parsed YAML.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_parsed')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            parsed = yaml.safe_load(resolved_content)
            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"YAML已解析: {type(parsed).__name__}",
                data={'parsed': parsed, 'output_var': output_var}
            )
        except yaml.YAMLError as e:
            return ActionResult(
                success=False,
                message=f"YAML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'yaml_parsed'}


class YamlDumpAction(BaseAction):
    """Dump dictionary to YAML."""
    action_type = "yaml_dump"
    display_name = "导出YAML"
    description = "将字典导出为YAML"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dump.

        Args:
            context: Execution context.
            params: Dict with data, output_var, output_file, default_flow_style.

        Returns:
            ActionResult with YAML string.
        """
        data = params.get('data', {})
        output_var = params.get('output_var', 'yaml_output')
        output_file = params.get('output_file', '')
        default_flow_style = params.get('default_flow_style', False)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_flow = context.resolve_value(default_flow_style)

            yaml_str = yaml.dump(
                resolved_data,
                default_flow_style=resolved_flow,
                allow_unicode=True,
                sort_keys=False
            )

            if output_file:
                resolved_out = context.resolve_value(output_file)
                with open(resolved_out, 'w', encoding='utf-8') as f:
                    f.write(yaml_str)

            context.set(output_var, yaml_str)

            return ActionResult(
                success=True,
                message=f"YAML已导出 ({len(yaml_str)} 字符)",
                data={'yaml': yaml_str, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML导出失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_output', 'output_file': '', 'default_flow_style': False}


class YamlValidateAction(BaseAction):
    """Validate YAML syntax."""
    action_type = "yaml_validate"
    display_name = "验证YAML"
    description = "验证YAML语法"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with content, file_path, output_var.

        Returns:
            ActionResult with validation result.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_valid')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            yaml.safe_load(resolved_content)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="YAML语法有效",
                data={'valid': True, 'output_var': output_var}
            )
        except yaml.YAMLError as e:
            context.set(output_var, False)
            return ActionResult(
                success=False,
                message=f"YAML无效: {str(e)}",
                data={'valid': False, 'error': str(e), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'yaml_valid'}


class YamlMergeAction(BaseAction):
    """Merge multiple YAML files."""
    action_type = "yaml_merge"
    display_name = "合并YAML"
    description = "合并多个YAML文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with files, output_var.

        Returns:
            ActionResult with merged YAML.
        """
        files = params.get('files', [])
        output_var = params.get('output_var', 'yaml_merged')

        valid, msg = self.validate_type(files, list, 'files')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_files = context.resolve_value(files)

            merged = {}
            for filepath in resolved_files:
                if not os.path.exists(filepath):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {filepath}"
                    )
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    if data:
                        merged.update(data)

            yaml_str = yaml.dump(merged, default_flow_style=False, allow_unicode=True, sort_keys=False)
            context.set(output_var, merged)

            return ActionResult(
                success=True,
                message=f"YAML已合并: {len(resolved_files)} 个文件",
                data={'merged': merged, 'yaml': yaml_str, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['files']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_merged'}


class YamlFlattenAction(BaseAction):
    """Flatten nested YAML."""
    action_type = "yaml_flatten"
    display_name = "扁平化YAML"
    description = "将嵌套YAML展平为扁平键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with content, file_path, separator, output_var.

        Returns:
            ActionResult with flattened dict.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'yaml_flat')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            resolved_sep = context.resolve_value(separator)

            data = yaml.safe_load(resolved_content)

            def flatten(d, parent_key=''):
                items = []
                if isinstance(d, dict):
                    for k, v in d.items():
                        new_key = f"{parent_key}{resolved_sep}{k}" if parent_key else k
                        if isinstance(v, (dict, list)):
                            items.extend(flatten(v, new_key).items())
                        else:
                            items.append((new_key, v))
                elif isinstance(d, list):
                    for i, v in enumerate(d):
                        new_key = f"{parent_key}{resolved_sep}{i}"
                        if isinstance(v, (dict, list)):
                            items.extend(flatten(v, new_key).items())
                        else:
                            items.append((new_key, v))
                else:
                    items.append((parent_key, d))
                return dict(items)

            flattened = flatten(data)
            context.set(output_var, flattened)

            return ActionResult(
                success=True,
                message=f"YAML已扁平化: {len(flattened)} 个键",
                data={'flat': flattened, 'count': len(flattened), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"YAML扁平化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'separator': '.', 'output_var': 'yaml_flat'}
