"""INI action module for RabAI AutoClick.

Provides INI/CFG file operations:
- INIParseAction: Parse INI file/string
- INIDumpAction: Convert dict to INI string
- INIReadAction: Read INI file
- INIWriteAction: Write dict to INI file
- INIGetAction: Get value from section/key
- INISetAction: Set value in section/key
- INIGetSectionAction: Get entire section
"""

from typing import Any, Dict, List, Optional, Union
import configparser
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class INIParseAction(BaseAction):
    """Parse INI string into ConfigParser."""
    action_type = "ini_parse"
    display_name = "INI解析"
    description = "解析INI格式字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI parse operation.

        Args:
            context: Execution context.
            params: Dict with ini_content, output_var.

        Returns:
            ActionResult with parsed INI data.
        """
        ini_content = params.get('ini_content', '')
        output_var = params.get('output_var', 'ini_data')

        if not ini_content:
            return ActionResult(success=False, message="ini_content is required")

        try:
            resolved_content = context.resolve_value(ini_content)

            parser = configparser.ConfigParser()
            parser.read_string(resolved_content)

            data = {section: dict(parser.items(section)) for section in parser.sections()}

            context.set(output_var, data)
            context.set(f'{output_var}_parser', parser)
            return ActionResult(success=True, data=data,
                               message=f"Parsed INI with {len(data)} sections")

        except configparser.Error as e:
            return ActionResult(success=False, message=f"INI parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"INI error: {str(e)}")


class INIDumpAction(BaseAction):
    """Convert dict to INI string."""
    action_type = "ini_dump"
    display_name = "INI输出"
    description = "将字典转换为INI格式字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI dump operation.

        Args:
            context: Execution context.
            params: Dict with data, default_section, output_var.

        Returns:
            ActionResult with INI string.
        """
        data = params.get('data', {})
        default_section = params.get('default_section', 'DEFAULT')
        output_var = params.get('output_var', 'ini_string')

        try:
            resolved_data = context.resolve_value(data)
            resolved_default = context.resolve_value(default_section)

            parser = configparser.ConfigParser()

            if isinstance(resolved_data, dict):
                for section, values in resolved_data.items():
                    if isinstance(values, dict):
                        parser.add_section(section)
                        for key, val in values.items():
                            parser.set(section, key, str(val))
                    elif section != resolved_default:
                        parser.add_section(section)
                        parser.set(section, 'value', str(values))
            else:
                parser.add_section(resolved_default)
                parser.set(resolved_default, 'data', str(resolved_data))

            from io import StringIO
            output = StringIO()
            parser.write(output)
            ini_str = output.getvalue()

            context.set(output_var, ini_str)
            return ActionResult(success=True, data=ini_str,
                               message=f"Dumped INI: {len(ini_str)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"INI dump error: {str(e)}")


class INIReadAction(BaseAction):
    """Read INI file."""
    action_type = "ini_read"
    display_name = "INI读取"
    description = "读取INI配置文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI read operation.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with INI file data.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path)

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            data = {section: dict(parser.items(section)) for section in parser.sections()}

            context.set(output_var, data)
            context.set(f'{output_var}_parser', parser)
            return ActionResult(success=True, data=data,
                               message=f"Read INI: {len(data)} sections from {resolved_path}")

        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"INI read error: {str(e)}")


class INIWriteAction(BaseAction):
    """Write dict to INI file."""
    action_type = "ini_write"
    display_name = "INI写入"
    description = "将字典写入INI文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI write operation.

        Args:
            context: Execution context.
            params: Dict with file_path, data, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        output_var = params.get('output_var', 'ini_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            parser = configparser.ConfigParser()

            if isinstance(resolved_data, dict):
                for section, values in resolved_data.items():
                    if isinstance(values, dict):
                        parser.add_section(section)
                        for key, val in values.items():
                            parser.set(section, key, str(val))
                    else:
                        parser.add_section(section)
                        parser.set(section, 'value', str(values))

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                parser.write(f)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Wrote INI to {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"INI write error: {str(e)}")


class INIGetAction(BaseAction):
    """Get value from INI section and key."""
    action_type = "ini_get"
    display_name = "INI取值"
    description = "从INI获取section/key的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI get operation.

        Args:
            context: Execution context.
            params: Dict with data, section, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        data = params.get('data', None)
        parser = params.get('parser', None)
        section = params.get('section', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'ini_value')

        if not section or not key:
            return ActionResult(success=False, message="section and key are required")

        try:
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)

            if parser is not None:
                resolved_parser = context.resolve_value(parser)
                if isinstance(resolved_parser, configparser.ConfigParser):
                    value = resolved_parser.get(resolved_section, resolved_key, fallback=default)
                    context.set(output_var, value)
                    return ActionResult(success=True, data=value,
                                       message=f"Got {resolved_section}.{resolved_key} = {value}")

            if data is not None:
                resolved_data = context.resolve_value(data)
                if isinstance(resolved_data, dict) and resolved_section in resolved_data:
                    section_data = resolved_data[resolved_section]
                    if isinstance(section_data, dict):
                        value = section_data.get(resolved_key, default)
                    else:
                        value = default
                else:
                    value = default

                context.set(output_var, value)
                return ActionResult(success=True, data=value,
                                   message=f"Got {resolved_section}.{resolved_key} = {value}")

            return ActionResult(success=False, message="data or parser required")

        except Exception as e:
            return ActionResult(success=False, message=f"INI get error: {str(e)}")


class INISetAction(BaseAction):
    """Set value in INI section and key."""
    action_type = "ini_set"
    display_name = "INI设值"
    description = "设置INI section/key的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI set operation.

        Args:
            context: Execution context.
            params: Dict with parser, section, key, value, output_var.

        Returns:
            ActionResult with update status.
        """
        parser = params.get('parser', None)
        section = params.get('section', '')
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'ini_set_result')

        if not section or not key:
            return ActionResult(success=False, message="section and key are required")

        try:
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)

            if parser is not None:
                resolved_parser = context.resolve_value(parser)
                if isinstance(resolved_parser, configparser.ConfigParser):
                    if not resolved_parser.has_section(resolved_section):
                        resolved_parser.add_section(resolved_section)
                    resolved_parser.set(resolved_section, resolved_key, str(context.resolve_value(value)))
                    context.set(output_var, True)
                    return ActionResult(success=True, data=True,
                                       message=f"Set {resolved_section}.{resolved_key}")

            return ActionResult(success=False, message="parser required for set operation")

        except Exception as e:
            return ActionResult(success=False, message=f"INI set error: {str(e)}")


class INIGetSectionAction(BaseAction):
    """Get entire INI section."""
    action_type = "ini_get_section"
    display_name = "INI取Section"
    description = "获取整个INI section的数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI get section operation.

        Args:
            context: Execution context.
            params: Dict with data or parser, section, output_var.

        Returns:
            ActionResult with section data.
        """
        data = params.get('data', None)
        parser = params.get('parser', None)
        section = params.get('section', '')
        output_var = params.get('output_var', 'ini_section')

        if not section:
            return ActionResult(success=False, message="section is required")

        try:
            resolved_section = context.resolve_value(section)

            if parser is not None:
                resolved_parser = context.resolve_value(parser)
                if isinstance(resolved_parser, configparser.ConfigParser):
                    if resolved_parser.has_section(resolved_section):
                        section_data = dict(resolved_parser.items(resolved_section))
                    else:
                        section_data = {}
                    context.set(output_var, section_data)
                    return ActionResult(success=True, data=section_data,
                                       message=f"Got section '{resolved_section}'")

            if data is not None:
                resolved_data = context.resolve_value(data)
                if isinstance(resolved_data, dict) and resolved_section in resolved_data:
                    section_data = resolved_data[resolved_section]
                else:
                    section_data = {}
                context.set(output_var, section_data)
                return ActionResult(success=True, data=section_data,
                                   message=f"Got section '{resolved_section}'")

            return ActionResult(success=False, message="data or parser required")

        except Exception as e:
            return ActionResult(success=False, message=f"INI get section error: {str(e)}")
