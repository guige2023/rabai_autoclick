"""TOML action module for RabAI AutoClick.

Provides TOML operations:
- TOMLParseAction: Parse TOML string or file
- TOMLDumpAction: Convert dict to TOML string
- TOMLReadAction: Read TOML file
- TOMLWriteAction: Write dict to TOML file
- TOMLGetAction: Get value by key path
- TOMLSetAction: Set value by key path
"""

from typing import Any, Dict, List, Optional, Union
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TOMLParseAction(BaseAction):
    """Parse TOML string into dict."""
    action_type = "toml_parse"
    display_name = "TOML解析"
    description = "解析TOML字符串为字典"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML parse operation.

        Args:
            context: Execution context.
            params: Dict with toml_content, output_var.

        Returns:
            ActionResult with parsed TOML data.
        """
        toml_content = params.get('toml_content', '')
        output_var = params.get('output_var', 'toml_data')

        if not toml_content:
            return ActionResult(success=False, message="toml_content is required")

        try:
            import tomlkit
        except ImportError:
            try:
                import tomllib
            except ImportError:
                return ActionResult(success=False,
                                  message="tomllib (Python 3.11+) or tomlkit required")

        try:
            resolved_content = context.resolve_value(toml_content)

            if 'tomllib' in dir():
                data = tomllib.loads(resolved_content)
            else:
                data = tomlkit.loads(resolved_content)

            context.set(output_var, data)
            return ActionResult(success=True, data=data,
                               message=f"Parsed TOML with {len(data)} top-level keys")

        except Exception as e:
            return ActionResult(success=False, message=f"TOML parse error: {str(e)}")


class TOMLDumpAction(BaseAction):
    """Convert dict to TOML string."""
    action_type = "toml_dump"
    display_name = "TOML输出"
    description = "将字典转换为TOML字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML dump operation.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with TOML string.
        """
        data = params.get('data', {})
        output_var = params.get('output_var', 'toml_string')

        try:
            import tomlkit
        except ImportError:
            return ActionResult(success=False, message="tomlkit required: pip install tomlkit")

        try:
            resolved_data = context.resolve_value(data)

            toml_str = tomlkit.dumps(resolved_data)

            context.set(output_var, toml_str)
            return ActionResult(success=True, data=toml_str,
                               message=f"Dumped TOML: {len(toml_str)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"TOML dump error: {str(e)}")


class TOMLReadAction(BaseAction):
    """Read TOML file."""
    action_type = "toml_read"
    display_name = "TOML读取"
    description = "读取TOML文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML read operation.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with TOML file data.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'toml_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import tomlkit
        except ImportError:
            try:
                import tomllib
            except ImportError:
                return ActionResult(success=False,
                                  message="tomllib or tomlkit required")

        try:
            resolved_path = context.resolve_value(file_path)

            if 'tomllib' in dir():
                with open(resolved_path, 'rb') as f:
                    data = tomllib.load(f)
            else:
                with open(resolved_path, encoding='utf-8') as f:
                    data = tomlkit.load(f)

            context.set(output_var, data)
            return ActionResult(success=True, data=data,
                               message=f"Read TOML from {resolved_path}")

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"TOML read error: {str(e)}")


class TOMLWriteAction(BaseAction):
    """Write dict to TOML file."""
    action_type = "toml_write"
    display_name = "TOML写入"
    description = "将字典写入TOML文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML write operation.

        Args:
            context: Execution context.
            params: Dict with file_path, data, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        output_var = params.get('output_var', 'toml_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import tomlkit
        except ImportError:
            return ActionResult(success=False, message="tomlkit required: pip install tomlkit")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            toml_str = tomlkit.dumps(resolved_data)
            with open(resolved_path, 'w', encoding='utf-8') as f:
                f.write(toml_str)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Wrote TOML to {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"TOML write error: {str(e)}")


class TOMLGetAction(BaseAction):
    """Get value from TOML by key path."""
    action_type = "toml_get"
    display_name = "TOML取值"
    description = "按路径获取TOML中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML get operation.

        Args:
            context: Execution context.
            params: Dict with data, key_path, default, output_var.

        Returns:
            ActionResult with value.
        """
        data = params.get('data', {})
        key_path = params.get('key_path', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'toml_value')

        if not key_path:
            return ActionResult(success=False, message="key_path is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_path = context.resolve_value(key_path)

            keys = resolved_path.split('.')
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
            return ActionResult(success=False, message=f"TOML get error: {str(e)}")


class TOMLSetAction(BaseAction):
    """Set value in TOML dict by key path."""
    action_type = "toml_set"
    display_name = "TOML设值"
    description = "按路径设置TOML字典中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute TOML set operation.

        Args:
            context: Execution context.
            params: Dict with data, key_path, value, output_var.

        Returns:
            ActionResult with updated data.
        """
        data = params.get('data', {})
        key_path = params.get('key_path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'toml_data')

        if not key_path:
            return ActionResult(success=False, message="key_path is required")

        try:
            resolved_data = context.resolve_value(data)
            resolved_path = context.resolve_value(key_path)
            resolved_value = context.resolve_value(value)

            if not isinstance(resolved_data, dict):
                resolved_data = {}

            keys = resolved_path.split('.')
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
            return ActionResult(success=False, message=f"TOML set error: {str(e)}")
