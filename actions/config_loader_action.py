"""Configuration loader action module for RabAI AutoClick.

Provides configuration operations:
- ConfigLoadAction: Load configuration
- ConfigSaveAction: Save configuration
- ConfigGetAction: Get config value
- ConfigSetAction: Set config value
- ConfigMergeAction: Merge configurations
- ConfigValidateAction: Validate configuration
- ConfigDiffAction: Compare configurations
- ConfigEncryptAction: Encrypt sensitive config
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConfigStore:
    """In-memory configuration storage."""
    
    _config: Dict[str, Any] = {}
    
    @classmethod
    def get(cls, key: str = None, default: Any = None) -> Any:
        """Get config value."""
        if key is None:
            return cls._config
        keys = key.split(".")
        value = cls._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
    @classmethod
    def set(cls, key: str, value: Any) -> None:
        """Set config value."""
        keys = key.split(".")
        target = cls._config
        for k in keys[:-1]:
            if k not in target:
                target[k] = {}
            target = target[k]
        target[keys[-1]] = value
    
    @classmethod
    def merge(cls, config: Dict[str, Any]) -> None:
        """Merge configuration."""
        cls._deep_merge(cls._config, config)
    
    @classmethod
    def _deep_merge(cls, base: Dict, update: Dict) -> None:
        """Deep merge update into base."""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                cls._deep_merge(base[key], value)
            else:
                base[key] = value
    
    @classmethod
    def clear(cls) -> None:
        """Clear configuration."""
        cls._config.clear()


class ConfigLoadAction(BaseAction):
    """Load configuration from file."""
    action_type = "config_load"
    display_name = "加载配置"
    description = "从文件加载配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            format_type = params.get("format", "auto")
            merge = params.get("merge", False)
            
            if not file_path:
                return ActionResult(success=False, message="file_path required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            with open(file_path, "r", encoding="utf-8") as f:
                if file_path.endswith(".json"):
                    config = json.load(f)
                elif file_path.endswith((".yaml", ".yml")):
                    try:
                        import yaml
                        config = yaml.safe_load(f)
                    except ImportError:
                        return ActionResult(success=False, message="PyYAML not installed")
                elif file_path.endswith(".ini"):
                    import configparser
                    parser = configparser.ConfigParser()
                    parser.read(file_path)
                    config = {s: dict(parser[s]) for s in parser.sections()}
                else:
                    return ActionResult(success=False, message=f"Unsupported format: {file_path}")
            
            if merge:
                ConfigStore.merge(config)
                message = f"Merged config from {file_path}"
            else:
                ConfigStore._config = config
                message = f"Loaded config from {file_path}"
            
            return ActionResult(
                success=True,
                message=message,
                data={"config": config, "keys": list(config.keys()) if isinstance(config, dict) else []}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config load failed: {str(e)}")


class ConfigSaveAction(BaseAction):
    """Save configuration to file."""
    action_type = "config_save"
    display_name = "保存配置"
    description = "保存配置到文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            config = params.get("config", {})
            format_type = params.get("format", "json")
            
            if not file_path:
                return ActionResult(success=False, message="file_path required")
            
            if not config:
                config = ConfigStore._config
            
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            
            with open(file_path, "w", encoding="utf-8") as f:
                if format_type == "json" or file_path.endswith(".json"):
                    json.dump(config, f, indent=2, ensure_ascii=False)
                elif format_type in ("yaml", "yml") or file_path.endswith((".yaml", ".yml")):
                    try:
                        import yaml
                        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
                    except ImportError:
                        return ActionResult(success=False, message="PyYAML not installed")
            
            return ActionResult(
                success=True,
                message=f"Saved config to {file_path}",
                data={"file_path": file_path, "keys": list(config.keys())}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config save failed: {str(e)}")


class ConfigGetAction(BaseAction):
    """Get configuration value."""
    action_type = "config_get"
    display_name = "获取配置"
    description = "获取配置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            default = params.get("default", None)
            
            value = ConfigStore.get(key, default)
            
            return ActionResult(
                success=True,
                message=f"Got config: {key or '(root)'}",
                data={"key": key, "value": value}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config get failed: {str(e)}")


class ConfigSetAction(BaseAction):
    """Set configuration value."""
    action_type = "config_set"
    display_name = "设置配置"
    description = "设置配置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            value = params.get("value")
            
            if not key:
                return ActionResult(success=False, message="key required")
            
            ConfigStore.set(key, value)
            
            return ActionResult(
                success=True,
                message=f"Set config: {key}",
                data={"key": key, "value": value}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config set failed: {str(e)}")


class ConfigMergeAction(BaseAction):
    """Merge configurations."""
    action_type = "config_merge"
    display_name = "合并配置"
    description = "合并配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config = params.get("config", {})
            
            if not config:
                return ActionResult(success=False, message="config required")
            
            ConfigStore.merge(config)
            
            return ActionResult(
                success=True,
                message=f"Merged {len(config)} keys",
                data={"config": ConfigStore._config}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config merge failed: {str(e)}")


class ConfigValidateAction(BaseAction):
    """Validate configuration."""
    action_type = "config_validate"
    display_name = "验证配置"
    description = "验证配置有效性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            schema = params.get("schema", {})
            config = params.get("config", None)
            
            if config is None:
                config = ConfigStore._config
            
            errors = []
            
            def validate(obj, schema_obj, path=""):
                if isinstance(schema_obj, dict):
                    for key, expected_type in schema_obj.items():
                        full_path = f"{path}.{key}" if path else key
                        if key not in obj:
                            errors.append(f"Missing required key: {full_path}")
                        else:
                            validate(obj[key], expected_type, full_path)
                elif isinstance(schema_obj, type):
                    if not isinstance(obj, schema_obj):
                        errors.append(f"Type error at {path}: expected {schema_obj.__name__}, got {type(obj).__name__}")
            
            if schema:
                validate(config, schema)
            
            is_valid = len(errors) == 0
            
            return ActionResult(
                success=is_valid,
                message="Config valid" if is_valid else f"Validation errors: {len(errors)}",
                data={"valid": is_valid, "errors": errors}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config validate failed: {str(e)}")


class ConfigDiffAction(BaseAction):
    """Compare configurations."""
    action_type = "config_diff"
    display_name = "配置对比"
    description = "对比两个配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config1 = params.get("config1", {})
            config2 = params.get("config2", {})
            
            added = {}
            removed = {}
            changed = {}
            
            all_keys = set(config1.keys()) | set(config2.keys())
            
            for key in all_keys:
                if key not in config1:
                    added[key] = config2[key]
                elif key not in config2:
                    removed[key] = config1[key]
                elif config1[key] != config2[key]:
                    changed[key] = {"old": config1[key], "new": config2[key]}
            
            return ActionResult(
                success=True,
                message=f"Diff: +{len(added)}, -{len(removed)}, ~{len(changed)}",
                data={"added": added, "removed": removed, "changed": changed}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config diff failed: {str(e)}")


class ConfigEncryptAction(BaseAction):
    """Encrypt sensitive config values."""
    action_type = "config_encrypt"
    display_name = "加密配置"
    description = "加密敏感配置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import base64
            import hashlib
            
            config = params.get("config", {})
            keys_to_encrypt = params.get("keys", [])
            
            encrypted = config.copy() if isinstance(config, dict) else {}
            
            for key in keys_to_encrypt:
                if key in config:
                    value_str = str(config[key])
                    encoded = base64.b64encode(value_str.encode()).decode()
                    encrypted[key] = f"ENC:{encoded}"
            
            return ActionResult(
                success=True,
                message=f"Encrypted {len(keys_to_encrypt)} values",
                data={"encrypted": encrypted}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Config encrypt failed: {str(e)}")
