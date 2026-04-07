"""INI file action module for RabAI AutoClick.

Provides INI file operations:
- IniReadAction: Read INI file
- IniWriteAction: Write INI file
- IniGetAction: Get INI value
- IniSetAction: Set INI value
- IniSectionsAction: List INI sections
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IniReadAction(BaseAction):
    """Read INI file."""
    action_type = "ini_read"
    display_name = "INI读取"
    description = "读取INI文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI read."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import configparser

            resolved_path = context.resolve_value(file_path) if context else file_path
            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            data = {s: dict(parser[s]) for s in parser.sections()}
            result = {'sections': list(parser.sections()), 'data': data}

            if context:
                context.set(output_var, data)
            return ActionResult(success=True, message=f"Read INI with {len(parser.sections())} sections", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"INI read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_data'}


class IniWriteAction(BaseAction):
    """Write INI file."""
    action_type = "ini_write"
    display_name = "INI写入"
    description = "写入INI文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI write."""
        file_path = params.get('file_path', '')
        data = params.get('data', {})  # {section: {key: value}}
        output_var = params.get('output_var', 'ini_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import configparser

            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)
            parser = configparser.ConfigParser()

            for section, values in resolved_data.items():
                parser.add_section(section)
                if isinstance(values, dict):
                    for k, v in values.items():
                        parser.set(section, k, str(v))

            with open(resolved_path, 'w') as f:
                parser.write(f)

            result = {'written': True, 'sections': len(resolved_data), 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Written INI to {resolved_path}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"INI write error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_write_result'}


class IniGetAction(BaseAction):
    """Get INI value."""
    action_type = "ini_get"
    display_name = "INI获取"
    description = "获取INI值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI get."""
        ini_var = params.get('ini_var', 'ini_data')
        section = params.get('section', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'ini_value')

        if not section or not key:
            return ActionResult(success=False, message="section and key are required")

        try:
            resolved_section = context.resolve_value(section) if context else section
            resolved_key = context.resolve_value(key) if context else key

            ini_data = context.resolve_value(ini_var) if context else None
            if ini_data is None:
                ini_data = context.resolve_value(ini_var)

            if isinstance(ini_data, dict) and resolved_section in ini_data:
                value = ini_data[resolved_section].get(resolved_key, default)
            else:
                value = default

            result = {'section': resolved_section, 'key': resolved_key, 'value': value}
            if context:
                context.set(output_var, value)
            return ActionResult(success=True, message=f"{resolved_section}.{resolved_key} = {value}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"INI get error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['section', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ini_var': 'ini_data', 'default': None, 'output_var': 'ini_value'}


class IniSetAction(BaseAction):
    """Set INI value."""
    action_type = "ini_set"
    display_name = "INI设置"
    description = "设置INI值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI set."""
        ini_var = params.get('ini_var', 'ini_data')
        section = params.get('section', '')
        key = params.get('key', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'ini_set_result')

        if not section or not key:
            return ActionResult(success=False, message="section and key are required")

        try:
            import copy

            resolved_section = context.resolve_value(section) if context else section
            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value

            ini_data = context.resolve_value(ini_var) if context else None
            if ini_data is None or not isinstance(ini_data, dict):
                ini_data = {}

            if resolved_section not in ini_data:
                ini_data[resolved_section] = {}
            ini_data[resolved_section][resolved_key] = str(resolved_value)

            result = {'section': resolved_section, 'key': resolved_key, 'value': str(resolved_value)}
            if context:
                context.set(ini_var, ini_data)
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Set {resolved_section}.{resolved_key} = {resolved_value}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"INI set error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['section', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ini_var': 'ini_data', 'output_var': 'ini_set_result'}


class IniSectionsAction(BaseAction):
    """List INI sections."""
    action_type = "ini_sections"
    display_name = "INI节列表"
    description = "列出INI节"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute INI sections."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'ini_sections')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import configparser

            resolved_path = context.resolve_value(file_path) if context else file_path
            parser = configparser.ConfigParser()
            parser.read(resolved_path)

            result = {'sections': parser.sections(), 'count': len(parser.sections())}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"INI has {len(parser.sections())} sections", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"INI sections error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ini_sections'}
