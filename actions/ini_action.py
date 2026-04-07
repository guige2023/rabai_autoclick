"""INI action module for RabAI AutoClick.

Provides INI file operations:
- IniReadAction: Read INI file
- IniWriteAction: Write INI file
- IniGetAction: Get value from INI
- IniSetAction: Set value in INI
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
    action_type = "ini_read"
    display_name = "读取INI"
    description = "读取INI配置文件"

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
            parser.read(resolved_path, encoding='utf-8')

            result = {section: dict(parser.items(section)) for section in parser.sections()}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"INI读取成功: {len(result)} 节",
                data={
                    'result': result,
                    'sections': list(result.keys()),
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
    action_type = "ini_write"
    display_name = "写入INI"
    description = "写入INI配置文件"

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
            ActionResult with write result.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        output_var = params.get('output_var', 'ini_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)

            parser = configparser.ConfigParser()

            if isinstance(resolved_data, dict):
                for section, values in resolved_data.items():
                    parser.add_section(section)
                    if isinstance(values, dict):
                        for key, value in values.items():
                            parser.set(section, key, str(value))

            with open(resolved_path, 'w', encoding='utf-8') as f:
                parser.write(f)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"INI写入成功",
                data={
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
        return {'output_var': 'ini_result'}


class IniGetAction(BaseAction):
    """Get value from INI."""
    action_type = "ini_get"
    display_name = "获取INI值"
    description = "从INI文件中获取指定节和键的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI get.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        default = params.get('default', None)
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
            resolved_default = context.resolve_value(default) if default is not None else None

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            if parser.has_option(resolved_section, resolved_key):
                result = parser.get(resolved_section, resolved_key)
            else:
                result = resolved_default

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取INI值: [{resolved_section}] {resolved_key}",
                data={
                    'section': resolved_section,
                    'key': resolved_key,
                    'value': result,
                    'found': result is not None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取INI值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'ini_value'}


class IniSetAction(BaseAction):
    """Set value in INI."""
    action_type = "ini_set"
    display_name = "设置INI值"
    description = "在INI文件中设置指定节和键的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INI set.

        Args:
            context: Execution context.
            params: Dict with file_path, section, key, value, output_var.

        Returns:
            ActionResult with set result.
        """
        file_path = params.get('file_path', '')
        section = params.get('section', '')
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'ini_result')

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
            resolved_value = context.resolve_value(value)

            parser = configparser.ConfigParser()
            parser.read(resolved_path, encoding='utf-8')

            if not parser.has_section(resolved_section):
                parser.add_section(resolved_section)

            parser.set(resolved_section, resolved_key, str(resolved_value))

            with open(resolved_path, 'w', encoding='utf-8') as f:
                parser.write(f)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"设置INI值: [{resolved_section}] {resolved_key} = {resolved_value}",
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
                message=f"设置INI值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'section', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_result'}