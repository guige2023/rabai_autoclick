"""Ini2 action module for RabAI AutoClick.

Provides additional INI file operations:
- IniReadAction: Read INI file
- IniWriteAction: Write INI file
- IniGetValueAction: Get INI value
- IniSetValueAction: Set INI value
- IniGetSectionAction: Get INI section
"""

import configparser
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IniReadAction(BaseAction):
    """Read INI file."""
    action_type = "ini2_read"
    display_name = "INI读取"
    description = "读取INI文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI read.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with INI data.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            data = {section: dict(parser.items(section)) for section in parser.sections()}
            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"INI读取完成: {len(data)} 节",
                data={
                    'file_path': resolved_path,
                    'data': data,
                    'sections': list(data.keys()),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_data'}


class IniWriteAction(BaseAction):
    """Write INI file."""
    action_type = "ini2_write"
    display_name = "INI写入"
    description = "写入INI文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, output_var.

        Returns:
            ActionResult with write status.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        output_var = params.get('output_var', 'write_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            parser = configparser.ConfigParser()

            for section, values in resolved_data.items():
                parser.add_section(section)
                if isinstance(values, dict):
                    for key, value in values.items():
                        parser.set(section, key, str(value))

            with open(resolved_path, 'w') as f:
                parser.write(f)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"INI写入完成",
                data={
                    'file_path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'write_status'}


class IniGetValueAction(BaseAction):
    """Get INI value."""
    action_type = "ini2_get_value"
    display_name = "INI获取值"
    description = "获取INI中的值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI get value.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, output_var.

        Returns:
            ActionResult with value.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'ini_value')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(section, str, 'section')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)

            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            result = parser.get(resolved_section, resolved_key)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"INI获取值: {resolved_key}={result}",
                data={
                    'section': resolved_section,
                    'key': resolved_key,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI获取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_value'}


class IniSetValueAction(BaseAction):
    """Set INI value."""
    action_type = "ini2_set_value"
    display_name = "INI设置值"
    description = "设置INI中的值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI set value.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, value, output_var.

        Returns:
            ActionResult with set status.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'set_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_section = context.resolve_value(section)
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            if not parser.has_section(resolved_section):
                parser.add_section(resolved_section)

            parser.set(resolved_section, resolved_key, str(resolved_value))

            with open(resolved_path, 'w') as f:
                parser.write(f)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"INI设置值完成",
                data={
                    'section': resolved_section,
                    'key': resolved_key,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI设置值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_status'}


class IniGetSectionAction(BaseAction):
    """Get INI section."""
    action_type = "ini2_get_section"
    display_name = "INI获取节"
    description = "获取INI中的整个节"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI get section.

        Args:
            context: Execution context.
            params: Dict with file_path, section, output_var.

        Returns:
            ActionResult with section data.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        output_var = params.get('output_var', 'section_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(section, str, 'section')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_section = context.resolve_value(section)

            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            if not parser.has_section(resolved_section):
                return ActionResult(
                    success=False,
                    message=f"INI节不存在: {resolved_section}"
                )

            result = dict(parser.items(resolved_section))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"INI获取节: {resolved_section}",
                data={
                    'section': resolved_section,
                    'data': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INI获取节失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'section_data'}