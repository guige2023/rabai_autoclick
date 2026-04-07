"""INI action module for RabAI AutoClick.

Provides INI file operations:
- IniParseAction: Parse INI file
- IniDumpAction: Write INI file
- IniGetAction: Get INI value
- IniSetAction: Set INI value
- IniSectionsAction: List all sections
- IniDeleteAction: Delete section or key
"""

import configparser
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IniParseAction(BaseAction):
    """Parse INI file."""
    action_type = "ini_parse"
    display_name = "解析INI"
    description = "解析INI配置文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with parsed INI.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_parsed')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            # Convert to dict
            result = {s: dict(parser.items(s)) for s in parser.sections()}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"INI已解析: {len(result)} 个section",
                data={'sections': len(result), 'parsed': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_parsed'}


class IniDumpAction(BaseAction):
    """Write INI file."""
    action_type = "ini_dump"
    display_name = "写入INI"
    description = "将字典写入INI文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dump.

        Args:
            context: Execution context.
            params: Dict with data, output_file.

        Returns:
            ActionResult indicating success.
        """
        data = params.get('data', {})
        output_file = params.get('output_file', '')

        valid, msg = self.validate_type(data, dict, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_file, str, 'output_file')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_output = context.resolve_value(output_file)

            parser = configparser.ConfigParser()

            for section, items in resolved_data.items():
                if not isinstance(items, dict):
                    items = {'value': items}
                parser.add_section(str(section))
                for k, v in items.items():
                    parser.set(str(section), str(k), str(v))

            with open(resolved_output, 'w', encoding='utf-8') as f:
                parser.write(f)

            return ActionResult(
                success=True,
                message=f"INI已写入: {resolved_output}",
                data={'path': resolved_output, 'sections': len(resolved_data)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'output_file']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class IniGetAction(BaseAction):
    """Get INI value."""
    action_type = "ini_get"
    display_name = "INI读取"
    description = "读取INI配置值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        default = params.get('default', '')
        output_var = params.get('output_var', 'ini_value')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(section, str, 'section')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default else None

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            if resolved_section not in parser:
                context.set(output_var, resolved_default)
                return ActionResult(
                    success=True,
                    message=f"Section不存在: {resolved_section}",
                    data={'value': resolved_default, 'output_var': output_var}
                )

            value = parser.get(resolved_section, resolved_key, fallback=resolved_default)

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"{resolved_section}.{resolved_key} = {value}",
                data={'value': value, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': '', 'output_var': 'ini_value'}


class IniSetAction(BaseAction):
    """Set INI value."""
    action_type = "ini_set"
    display_name = "INI写入"
    description = "写入INI配置值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, value.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        value = params.get('value', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(section, str, 'section')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            parser = configparser.ConfigParser()

            if os.path.exists(resolved_path):
                parser.read(resolved_path, encoding='utf-8')

            if resolved_section not in parser:
                parser.add_section(resolved_section)

            parser.set(resolved_section, resolved_key, resolved_value)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                parser.write(f)

            return ActionResult(
                success=True,
                message=f"已设置: [{resolved_section}] {resolved_key} = {resolved_value}",
                data={'section': resolved_section, 'key': resolved_key, 'value': resolved_value}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class IniSectionsAction(BaseAction):
    """List all sections."""
    action_type = "ini_sections"
    display_name = "INI列出Sections"
    description = "列出INI所有Section"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sections.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with section list.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_sections')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            sections = parser.sections()
            context.set(output_var, sections)

            return ActionResult(
                success=True,
                message=f"Sections: {len(sections)} 个",
                data={'count': len(sections), 'sections': sections, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI列出Sections失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_sections'}
