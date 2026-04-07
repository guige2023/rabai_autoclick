"""Configuration management action module for RabAI AutoClick.

Provides configuration operations:
- ConfigLoadAction: Load configuration
- ConfigSaveAction: Save configuration
- ConfigMergeAction: Merge configurations
- ConfigValidateAction: Validate configuration
- ConfigGetAction: Get configuration value
- ConfigSetAction: Set configuration value
"""

import json
import os
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConfigLoadAction(BaseAction):
    """Load configuration."""
    action_type = "config_load"
    display_name = "加载配置"
    description = "加载配置文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            path = params.get("path", "")
            format = params.get("format", "auto")
            default = params.get("default", {})

            if not path:
                return ActionResult(success=False, message="path is required")

            if not os.path.exists(path):
                return ActionResult(
                    success=True,
                    message=f"Config file not found, using default",
                    data={"config": default, "source": "default"}
                )

            if format == "auto":
                ext = os.path.splitext(path)[-1].lower()
                if ext in (".json",):
                    format = "json"
                elif ext in (".yaml", ".yml"):
                    format = "yaml"
                elif ext in (".ini", ".conf"):
                    format = "ini"
                elif ext in (".env",):
                    format = "env"
                else:
                    format = "json"

            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            config = {}

            if format == "json":
                config = json.loads(content)
            elif format == "yaml":
                try:
                    import yaml
                    config = yaml.safe_load(content) or {}
                except ImportError:
                    return ActionResult(success=False, message="yaml module not available")
            elif format == "env":
                for line in content.split("\n"):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "=" in line:
                            key, value = line.split("=", 1)
                            config[key.strip()] = value.strip().strip('"').strip("'")
            elif format == "ini":
                current_section = "default"
                config[current_section] = {}
                for line in content.split("\n"):
                    line = line.strip()
                    if line.startswith("[") and line.endswith("]"):
                        current_section = line[1:-1]
                        config[current_section] = {}
                    elif line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        config[current_section][key.strip()] = value.strip()

            return ActionResult(
                success=True,
                message=f"Loaded config from {path}",
                data={"config": config, "path": path, "format": format}
            )

        except json.JSONDecodeError as e:
            return ActionResult(success=False, message=f"JSON parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Load config error: {str(e)}")


class ConfigSaveAction(BaseAction):
    """Save configuration."""
    action_type = "config_save"
    display_name = "保存配置"
    description = "保存配置文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            path = params.get("path", "")
            config = params.get("config", {})
            format = params.get("format", "auto")
            indent = params.get("indent", 2)

            if not path:
                return ActionResult(success=False, message="path is required")

            if not config:
                return ActionResult(success=False, message="config is required")

            if format == "auto":
                ext = os.path.splitext(path)[-1].lower()
                if ext in (".json",):
                    format = "json"
                elif ext in (".yaml", ".yml"):
                    format = "yaml"
                elif ext in (".env",):
                    format = "env"
                else:
                    format = "json"

            os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

            if format == "json":
                content = json.dumps(config, indent=indent, ensure_ascii=False)
            elif format == "yaml":
                try:
                    import yaml
                    content = yaml.dump(config, allow_unicode=True, default_flow_style=False)
                except ImportError:
                    return ActionResult(success=False, message="yaml module not available")
            elif format == "env":
                lines = []
                for key, value in config.items():
                    if isinstance(value, dict):
                        for subkey, subval in value.items():
                            lines.append(f"{key}_{subkey}={subval}")
                    else:
                        lines.append(f"{key}={value}")
                content = "\n".join(lines)
            else:
                content = json.dumps(config, indent=indent)

            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

            return ActionResult(
                success=True,
                message=f"Saved config to {path}",
                data={"path": path, "format": format, "size": len(content)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Save config error: {str(e)}")


class ConfigMergeAction(BaseAction):
    """Merge configurations."""
    action_type = "config_merge"
    display_name = "合并配置"
    description = "合并多个配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            configs = params.get("configs", [])
            strategy = params.get("strategy", "deep")

            if not configs:
                return ActionResult(success=False, message="configs list is required")

            if len(configs) == 1:
                return ActionResult(
                    success=True,
                    message="Single config returned",
                    data={"config": configs[0]}
                )

            if strategy == "shallow":
                merged = {}
                for cfg in configs:
                    if isinstance(cfg, dict):
                        merged.update(cfg)
            else:
                merged = {}
                for cfg in configs:
                    if isinstance(cfg, dict):
                        merged = self._deep_merge(merged, cfg)

            return ActionResult(
                success=True,
                message=f"Merged {len(configs)} configs",
                data={"config": merged, "config_count": len(configs)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Merge config error: {str(e)}")

    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        result = dict(base)
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result


class ConfigValidateAction(BaseAction):
    """Validate configuration."""
    action_type = "config_validate"
    display_name = "验证配置"
    description = "验证配置文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config = params.get("config", {})
            schema = params.get("schema", {})
            required_keys = params.get("required_keys", [])
            value_schema = params.get("value_schema", {})

            if not config:
                return ActionResult(success=False, message="config is required")

            errors = []

            for key in required_keys:
                if key not in config:
                    errors.append(f"Missing required key: {key}")

            for key, expected_type in value_schema.items():
                if key in config:
                    value = config[key]
                    type_map = {
                        "string": str, "str": str,
                        "number": (int, float), "num": (int, float),
                        "integer": int, "int": int,
                        "boolean": bool, "bool": bool,
                        "array": list, "list": list,
                        "dict": dict, "object": dict
                    }
                    expected = type_map.get(expected_type, str)
                    if not isinstance(value, expected):
                        errors.append(f"Key '{key}': expected {expected_type}, got {type(value).__name__}")

            if schema:
                schema_errors = self._validate_against_schema(config, schema)
                errors.extend(schema_errors)

            if errors:
                return ActionResult(
                    success=False,
                    message=f"Validation failed with {len(errors)} errors",
                    data={"errors": errors, "valid": False}
                )
            else:
                return ActionResult(
                    success=True,
                    message="Configuration is valid",
                    data={"valid": True}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Validate config error: {str(e)}")

    def _validate_against_schema(self, config: Dict, schema: Dict, path: str = "") -> List[str]:
        errors = []
        for key, field_schema in schema.items():
            current_path = f"{path}.{key}" if path else key
            if key not in config:
                if field_schema.get("required", False):
                    errors.append(f"Missing required field: {current_path}")
                continue
            value = config[key]
            if "type" in field_schema:
                type_map = {"string": str, "number": (int, float), "boolean": bool, "array": list, "object": dict}
                expected = type_map.get(field_schema["type"], str)
                if not isinstance(value, expected):
                    errors.append(f"Type mismatch at {current_path}: expected {field_schema['type']}")
        return errors


class ConfigGetAction(BaseAction):
    """Get configuration value."""
    action_type = "config_get"
    display_name = "获取配置"
    description = "获取配置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config = params.get("config", {})
            key = params.get("key", "")
            default = params.get("default", None)
            nested = params.get("nested", True)

            if not config:
                return ActionResult(success=False, message="config is required")

            if not key:
                return ActionResult(success=True, message="No key specified", data={"value": config})

            if nested and "." in key:
                parts = key.split(".")
                current = config
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return ActionResult(
                            success=True,
                            message=f"Key '{key}' not found, using default",
                            data={"value": default, "found": False}
                        )
                return ActionResult(
                    success=True,
                    message=f"Found value for '{key}'",
                    data={"value": current, "found": True}
                )
            else:
                if key in config:
                    return ActionResult(
                        success=True,
                        message=f"Found value for '{key}'",
                        data={"value": config[key], "found": True}
                    )
                else:
                    return ActionResult(
                        success=True,
                        message=f"Key '{key}' not found, using default",
                        data={"value": default, "found": False}
                    )

        except Exception as e:
            return ActionResult(success=False, message=f"Get config error: {str(e)}")


class ConfigSetAction(BaseAction):
    """Set configuration value."""
    action_type = "config_set"
    display_name = "设置配置"
    description = "设置配置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config = params.get("config", {})
            key = params.get("key", "")
            value = params.get("value")
            nested = params.get("nested", True)

            if not config:
                config = {}

            if not key:
                return ActionResult(success=False, message="key is required")

            if nested and "." in key:
                parts = key.split(".")
                current = config
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = value
            else:
                config[key] = value

            return ActionResult(
                success=True,
                message=f"Set '{key}' = {value}",
                data={"config": config, "key": key, "value": value}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Set config error: {str(e)}")
