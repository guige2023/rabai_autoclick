"""Config manager action module for RabAI AutoClick.

Provides configuration file operations including
reading, writing, and hot-reloading of config files.
"""

import os
import sys
import json
import time
from typing import Any, Dict, List, Optional
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ConfigLoadAction(BaseAction):
    """Load configuration from file.
    
    Supports JSON, YAML, and INI formats.
    Supports default values and auto-creation.
    """
    action_type = "config_load"
    display_name = "加载配置"
    description = "从文件加载配置文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Load config file.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, format, defaults,
                   auto_create, save_to_var.
        
        Returns:
            ActionResult with loaded config.
        """
        path = params.get('path', '')
        config_format = params.get('format', 'auto')
        defaults = params.get('defaults', {})
        auto_create = params.get('auto_create', False)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Config path is required")

        # Auto-detect format
        if config_format == 'auto':
            ext = os.path.splitext(path)[1].lower()
            if ext in ('.json',):
                config_format = 'json'
            elif ext in ('.yaml', '.yml'):
                config_format = 'yaml'
            elif ext in ('.ini', '.conf'):
                config_format = 'ini'
            else:
                config_format = 'json'

        # Check if file exists
        if not os.path.exists(path):
            if auto_create:
                # Create with defaults
                try:
                    config = dict(defaults)
                    self._write_config(path, config, config_format)
                    result_data = {
                        'config': config,
                        'path': path,
                        'created': True,
                        'format': config_format
                    }
                    if save_to_var:
                        context.variables[save_to_var] = result_data
                    return ActionResult(
                        success=True,
                        message=f"配置文件已创建: {path}",
                        data=result_data
                    )
                except Exception as e:
                    return ActionResult(
                        success=False,
                        message=f"创建配置文件失败: {e}"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"配置文件不存在: {path}"
                )

        # Load config
        try:
            config = self._read_config(path, config_format)
            result_data = {
                'config': config,
                'path': path,
                'created': False,
                'format': config_format,
                'size': os.path.getsize(path)
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"配置加载成功: {path}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"加载配置失败: {str(e)}"
            )

    def _read_config(self, path: str, fmt: str) -> Dict:
        with open(path, 'r', encoding='utf-8') as f:
            if fmt == 'json':
                return json.load(f)
            elif fmt == 'yaml':
                import yaml
                return yaml.safe_load(f) or {}
            elif fmt == 'ini':
                import configparser
                parser = configparser.ConfigParser()
                parser.read(path)
                return {s: dict(parser[s]) for s in parser.sections()}
            else:
                return json.load(f)

    def _write_config(self, path: str, config: Dict, fmt: str) -> None:
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            if fmt == 'json':
                json.dump(config, f, indent=2, ensure_ascii=False)
            elif fmt == 'yaml':
                import yaml
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
            elif fmt == 'ini':
                import configparser
                parser = configparser.ConfigParser()
                for section, values in config.items():
                    parser[section] = values
                parser.write(f)
            else:
                json.dump(config, f, indent=2)

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format': 'auto',
            'defaults': {},
            'auto_create': False,
            'save_to_var': None
        }


class ConfigSaveAction(BaseAction):
    """Save configuration to file.
    
    Supports atomic write and backup creation.
    """
    action_type = "config_save"
    display_name = "保存配置"
    description = "保存配置到文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Save config file.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, config, format,
                   backup, atomic, save_to_var.
        
        Returns:
            ActionResult with save result.
        """
        path = params.get('path', '')
        config = params.get('config', {})
        config_format = params.get('format', 'auto')
        backup = params.get('backup', False)
        atomic = params.get('atomic', True)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Config path is required")

        if not isinstance(config, dict):
            return ActionResult(
                success=False,
                message=f"Config must be dict, got {type(config).__name__}"
            )

        # Auto-detect format
        if config_format == 'auto':
            ext = os.path.splitext(path)[1].lower()
            if ext in ('.json',):
                config_format = 'json'
            elif ext in ('.yaml', '.yml'):
                config_format = 'yaml'
            elif ext in ('.ini', '.conf'):
                config_format = 'ini'
            else:
                config_format = 'json'

        # Create backup
        backup_path = None
        if backup and os.path.exists(path):
            backup_path = f"{path}.backup.{int(time.time())}"
            try:
                with open(path, 'rb') as src, open(backup_path, 'wb') as dst:
                    dst.write(src.read())
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Backup failed: {e}"
                )

        # Atomic write
        try:
            if atomic:
                temp_path = f"{path}.tmp.{os.getpid()}"
                self._write_config(temp_path, config, config_format)
                os.replace(temp_path, path)
            else:
                self._write_config(path, config, config_format)

            result_data = {
                'saved': True,
                'path': path,
                'format': config_format,
                'backup_path': backup_path
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"配置保存成功: {path}",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"保存配置失败: {str(e)}"
            )

    def _write_config(self, path: str, config: Dict, fmt: str) -> None:
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            if fmt == 'json':
                json.dump(config, f, indent=2, ensure_ascii=False)
            elif fmt == 'yaml':
                import yaml
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
            elif fmt == 'ini':
                import configparser
                parser = configparser.ConfigParser()
                for section, values in config.items():
                    parser[section] = values
                parser.write(f)
            else:
                json.dump(config, f, indent=2)

    def get_required_params(self) -> List[str]:
        return ['path', 'config']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format': 'auto',
            'backup': False,
            'atomic': True,
            'save_to_var': None
        }


class ConfigWatchAction(BaseAction):
    """Watch config file for changes.
    
    Monitors file modification and reloads on change.
    """
    action_type = "config_watch"
    display_name = "监控配置"
    description = "监控配置文件变化并自动重载"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Watch config file.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, timeout, save_to_var.
        
        Returns:
            ActionResult with change result.
        """
        path = params.get('path', '')
        timeout = params.get('timeout', 60)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Config path is required")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"Config file not found: {path}")

        try:
            initial_mtime = os.path.getmtime(path)
            start_time = time.time()

            while time.time() - start_time < timeout:
                time.sleep(1)
                current_mtime = os.path.getmtime(path)
                if current_mtime > initial_mtime:
                    result_data = {
                        'changed': True,
                        'path': path,
                        'old_mtime': initial_mtime,
                        'new_mtime': current_mtime
                    }
                    if save_to_var:
                        context.variables[save_to_var] = result_data
                    return ActionResult(
                        success=True,
                        message=f"配置文件已变化",
                        data=result_data
                    )

            result_data = {
                'changed': False,
                'path': path,
                'timeout': timeout
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=True,
                message=f"监控超时({timeout}s)未检测到变化",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"监控失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'timeout': 60,
            'save_to_var': None
        }
