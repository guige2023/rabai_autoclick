"""Configuration management action module for RabAI AutoClick.

Provides configuration operations:
- ConfigLoadAction: Load configuration file
- ConfigSaveAction: Save configuration
- ConfigGetAction: Get config value
- ConfigSetAction: Set config value
- ConfigMergeAction: Merge configurations
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConfigLoadAction(BaseAction):
    """Load configuration file."""
    action_type = "config_load"
    display_name = "配置加载"
    description = "加载配置文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute config load."""
        file_path = params.get('file_path', '')
        format_type = params.get('format', 'auto')  # auto, json, yaml, ini, env
        output_var = params.get('output_var', 'config_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import json

            resolved_path = context.resolve_value(file_path) if context else file_path

            if format_type == 'auto':
                ext = resolved_path.lower().split('.')[-1]
                if ext in ('json',):
                    format_type = 'json'
                elif ext in ('yaml', 'yml'):
                    format_type = 'yaml'
                elif ext in ('ini',):
                    format_type = 'ini'
                else:
                    format_type = 'json'

            if format_type == 'json':
                with open(resolved_path, 'r') as f:
                    data = json.load(f)
            elif format_type == 'yaml':
                try:
                    import yaml
                    with open(resolved_path, 'r') as f:
                        data = yaml.safe_load(f)
                except ImportError:
                    return ActionResult(success=False, message="PyYAML not installed")
            elif format_type == 'ini':
                import configparser
                parser = configparser.ConfigParser()
                parser.read(resolved_path)
                data = {s: dict(parser[s]) for s in parser.sections()}
            elif format_type == 'env':
                data = {}
                with open(resolved_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            k, v = line.split('=', 1)
                            data[k.strip()] = v.strip().strip('"').strip("'")
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            if context:
                context.set(output_var, data)
            return ActionResult(success=True, message=f"Config loaded: {len(str(data))} chars", data={'keys': list(data.keys()) if isinstance(data, dict) else 'N/A'})
        except FileNotFoundError:
            return ActionResult(success=False, message=f"Config file not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Config load error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'auto', 'output_var': 'config_data'}


class ConfigSaveAction(BaseAction):
    """Save configuration file."""
    action_type = "config_save"
    display_name = "配置保存"
    description = "保存配置文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute config save."""
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        format_type = params.get('format', 'json')  # json, yaml
        output_var = params.get('output_var', 'config_save_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import json

            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            if format_type == 'json':
                with open(resolved_path, 'w') as f:
                    json.dump(resolved_data, f, indent=2, ensure_ascii=False)
            elif format_type == 'yaml':
                try:
                    import yaml
                    with open(resolved_path, 'w') as f:
                        yaml.dump(resolved_data, f, default_flow_style=False, allow_unicode=True)
                except ImportError:
                    return ActionResult(success=False, message="PyYAML not installed")
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            result = {'saved': True, 'path': resolved_path, 'format': format_type}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Config saved to {resolved_path}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Config save error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'json', 'output_var': 'config_save_result'}


class ConfigGetAction(BaseAction):
    """Get config value by key."""
    action_type = "config_get"
    display_name = "配置获取"
    description = "获取配置值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute config get."""
        config_var = params.get('config_var', 'config_data')
        key = params.get('key', '')
        default = params.get('default', None)
        separator = params.get('separator', '.')  # for nested keys like "db.host"
        output_var = params.get('output_var', 'config_value')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_sep = context.resolve_value(separator) if context else separator

            config = context.resolve_value(config_var) if context else None
            if config is None:
                config = context.resolve_value(config_var)

            if isinstance(config, str):
                config = json.loads(config)

            keys = resolved_key.split(resolved_sep)
            value = config
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    value = default
                    break

            result = {'value': value, 'key': resolved_key, 'found': value != default}
            if context:
                context.set(output_var, value)
            return ActionResult(success=True, message=f"{resolved_key} = {value}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Config get error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'config_var': 'config_data', 'default': None, 'separator': '.', 'output_var': 'config_value'}


class ConfigSetAction(BaseAction):
    """Set config value by key."""
    action_type = "config_set"
    display_name = "配置设置"
    description = "设置配置值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute config set."""
        config_var = params.get('config_var', 'config_data')
        key = params.get('key', '')
        value = params.get('value', None)
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'config_set_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            import copy

            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value
            resolved_sep = context.resolve_value(separator) if context else separator

            config = context.resolve_value(config_var) if context else None
            if config is None or not isinstance(config, dict):
                config = {}

            keys = resolved_key.split(resolved_sep)
            current = config
            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]
            current[keys[-1]] = resolved_value

            result = {'key': resolved_key, 'value': resolved_value}
            if context:
                context.set(config_var, config)
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Set {resolved_key} = {resolved_value}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Config set error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'config_var': 'config_data', 'separator': '.', 'output_var': 'config_set_result'}


class ConfigMergeAction(BaseAction):
    """Merge configurations."""
    action_type = "config_merge"
    display_name = "配置合并"
    description = "合并配置"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute config merge."""
        configs = params.get('configs', [])  # list of config dicts
        strategy = params.get('strategy', 'override')  # override, keep, deep
        output_var = params.get('output_var', 'merged_config')

        if not configs:
            return ActionResult(success=False, message="configs is required")

        try:
            import copy

            resolved_configs = context.resolve_value(configs) if context else configs
            resolved_strategy = context.resolve_value(strategy) if context else strategy

            if resolved_strategy == 'deep':
                def deep_merge(base, overlay):
                    result = copy.deepcopy(base)
                    for k, v in overlay.items():
                        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                            result[k] = deep_merge(result[k], v)
                        else:
                            result[k] = copy.deepcopy(v)
                    return result

                merged = {}
                for cfg in resolved_configs:
                    merged = deep_merge(merged, cfg)
            elif resolved_strategy == 'keep':
                merged = {}
                for cfg in resolved_configs:
                    for k, v in cfg.items():
                        if k not in merged:
                            merged[k] = copy.deepcopy(v)
            else:  # override
                merged = {}
                for cfg in resolved_configs:
                    merged.update(copy.deepcopy(cfg))

            if context:
                context.set(output_var, merged)
            return ActionResult(success=True, message=f"Merged {len(resolved_configs)} configs", data={'keys': list(merged.keys())})
        except Exception as e:
            return ActionResult(success=False, message=f"Config merge error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['configs']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'strategy': 'override', 'output_var': 'merged_config'}
