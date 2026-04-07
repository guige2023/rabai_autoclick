"""YAML action module for RabAI AutoClick.

Provides YAML operations:
- YAMLParseAction: Parse YAML string or file
- YAMLDumpAction: Convert dict to YAML string
- YAMLReadAction: Read YAML file
- YAMLWriteAction: Write dict to YAML file
- YAMLValidateAction: Validate YAML syntax
- YAMLGetAction: Get value by key path
- YAMLSetAction: Set value by key path
"""

from typing import Any, Dict, List, Optional, Union
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class YAMLParseAction(BaseAction):
    """Parse YAML string into dict."""
    action_type = "yaml_parse"
    display_name = "YAML解析"
    description = "解析YAML字符串为字典"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML parse operation.

        Args:
            context: Execution context.
            params: Dict with yaml_content, output_var.

        Returns:
            ActionResult with parsed YAML data.
        """
        yaml_content = params.get('yaml_content', '')
        output_var = params.get('output_var', 'yaml_data')

        if not yaml_content:
            return ActionResult(success=False, message="yaml_content is required")

        try:
            import yaml as yaml_lib
        except ImportError:
            return ActionResult(success=False, message="PyYAML required: pip install pyyaml")

        try:
            resolved_content = context.resolve_value(yaml_content)

            data = yaml_lib.safe_load(resolved_content)
            if data is None:
                data = {}

            context.set(output_var, data)
            return ActionResult(success=True, data=data,
                               message=f"Parsed YAML with {len(data) if isinstance(data, dict) else 'scalar'} top-level keys")

        except yaml_lib.YAMLError as e:
            return ActionResult(success=False, message=f"YAML parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML error: {str(e)}")


class YAMLDumpAction(BaseAction):
    """Convert dict to YAML string."""
    action_type = "yaml_dump"
    display_name = "YAML输出"
    description = "将字典转换为YAML字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML dump operation.

        Args:
            context: Execution context.
            params: Dict with data, default_flow_style, output_var.

        Returns:
            ActionResult with YAML string.
        """
        data = params.get('data', {})
        default_flow_style = params.get('default_flow_style', False)
        output_var = params.get('output_var', 'yaml_string')

        try:
            import yaml as yaml_lib
        except ImportError:
            return ActionResult(success=False, message="PyYAML required: pip install pyyaml")

        try:
            resolved_data = context.resolve_value(data)
            resolved_flow = context.resolve_value(default_flow_style)

            yaml_str = yaml_lib.dump(resolved_data, default_flow_style=resolved_flow, allow_unicode=True, sort_keys=False)

            context.set(output_var, yaml_str)
            return ActionResult(success=True, data=yaml_str,
                               message=f"Dumped YAML: {len(yaml_str)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"YAML dump error: {str(e)}")


class YAMLReadAction(BaseAction):
    """Read YAML file."""
    action_type = "yaml_read"
    display_name = "YAML读取"
    description = "读取YAML文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML read operation.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with YAML file data.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import yaml as yaml_lib
        except ImportError:
            return ActionResult(success=False, message="PyYAML required: pip install pyyaml")

        try:
            resolved_path = context.resolve_value(file_path)

            with open(resolved_path, 'r', encoding='utf-8') as f:
                data = yaml_lib.safe_load(f)

            if data is None:
                data = {}

            context.set(output_var, data)
            return ActionResult(success=True, data=data,
                               message=f"Read YAML from {resolved_path}")

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except yaml_lib.YAMLError as e:
            return ActionResult(success=False, message=f"YAML read error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML error: {str(e)}")


class YAMLWriteAction(BaseAction):
    """Write dict to YAML file."""
    action_type = "yaml_write"
    display_name = "YAML写入"
    description = "将字典写入YAML文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML write operation.

        Args:
            context: Execution context.
            params: Dict with file_path, data, default_flow_style, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        default_flow_style = params.get('default_flow_style', False)
        output_var = params.get('output_var', 'yaml_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import yaml as yaml_lib
        except ImportError:
            return ActionResult(success=False, message="PyYAML required: pip install pyyaml")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)
            resolved_flow = context.resolve_value(default_flow_style)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                yaml_lib.dump(resolved_data, f, default_flow_style=resolved_flow, allow_unicode=True, sort_keys=False)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Wrote YAML to {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"YAML write error: {str(e)}")


class YAMLValidateAction(BaseAction):
    """Validate YAML syntax."""
    action_type = "yaml_validate"
    display_name = "YAML验证"
    description = "验证YAML语法是否正确"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML validate operation.

        Args:
            context: Execution context.
            params: Dict with yaml_content or file_path, output_var.

        Returns:
            ActionResult with validation status.
        """
        yaml_content = params.get('yaml_content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_valid')

        if not yaml_content and not file_path:
            return ActionResult(success=False, message="yaml_content or file_path required")

        try:
            import yaml as yaml_lib
        except ImportError:
            return ActionResult(success=False, message="PyYAML required: pip install pyyaml")

        try:
            if file_path:
                resolved_path = context.resolve_value(file_path)
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    yaml_lib.safe_load(f)
            else:
                resolved_content = context.resolve_value(yaml_content)
                yaml_lib.safe_load(resolved_content)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="YAML is valid")

        except yaml_lib.YAMLError as e:
            context.set(output_var, False)
            return ActionResult(success=False, data=False,
                               message=f"YAML invalid: {str(e)}")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML validate error: {str(e)}")


class YAMLGetAction(BaseAction):
    """Get value from YAML by key path."""
    action_type = "yaml_get"
    display_name = "YAML取值"
    description = "按路径获取YAML中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML get operation.

        Args:
            context: Execution context.
            params: Dict with data, key_path, default, output_var.

        Returns:
            ActionResult with value.
        """
        data = params.get('data', {})
        key_path = params.get('key_path', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'yaml_value')

        if not key_path:
            return ActionResult(success=False, message="key_path is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_path = context.resolve_value(key_path)

            keys = resolved_path.replace('/', '.').split('.')
            value = resolved_data
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    value = default
                    break

            context.set(output_var, value)
            return ActionResult(success=True, data=value,
                               message=f"Got value for key path: {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"YAML get error: {str(e)}")


class YAMLSetAction(BaseAction):
    """Set value in YAML dict by key path."""
    action_type = "yaml_set"
    display_name = "YAML设值"
    description = "按路径设置YAML字典中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML set operation.

        Args:
            context: Execution context.
            params: Dict with data, key_path, value, output_var.

        Returns:
            ActionResult with updated data.
        """
        data = params.get('data', {})
        key_path = params.get('key_path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'yaml_data')

        if not key_path:
            return ActionResult(success=False, message="key_path is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_path = context.resolve_value(key_path)
            resolved_value = context.resolve_value(value)

            if not isinstance(resolved_data, dict):
                resolved_data = {}

            keys = resolved_path.replace('/', '.').split('.')
            current = resolved_data
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            current[keys[-1]] = resolved_value

            context.set(output_var, resolved_data)
            return ActionResult(success=True, data=resolved_data,
                               message=f"Set {resolved_path} = {resolved_value}")

        except Exception as e:
            return ActionResult(success=False, message=f"YAML set error: {str(e)}")
