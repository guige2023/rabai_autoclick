"""Configuration action module for RabAI AutoClick.

Provides configuration file loading, validation, and management
for YAML, JSON, INI, and environment variable configurations.
"""

import os
import sys
import json
import time
import traceback
from typing import Any, Dict, List, Optional, Union, Tuple
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ConfigLoader:
    """Configuration file loader with multi-format support.
    
    Supports loading configurations from:
    - JSON files (.json)
    - YAML files (.yaml, .yml)
    - INI files (.ini)
    - Environment variables
    - Python dict configurations
    """
    
    @staticmethod
    def load_json(path: str) -> Dict[str, Any]:
        """Load configuration from a JSON file.
        
        Args:
            path: Path to the JSON file.
            
        Returns:
            Dictionary containing configuration data.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file contains invalid JSON.
        """
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    @staticmethod
    def load_yaml(path: str) -> Dict[str, Any]:
        """Load configuration from a YAML file.
        
        Args:
            path: Path to the YAML file.
            
        Returns:
            Dictionary containing configuration data.
            
        Raises:
            ImportError: If PyYAML is not installed.
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If the file contains invalid YAML.
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML support. Install with: pip install pyyaml"
            )
        
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    
    @staticmethod
    def load_ini(path: str) -> Dict[str, Dict[str, str]]:
        """Load configuration from an INI file.
        
        Args:
            path: Path to the INI file.
            
        Returns:
            Dictionary mapping section names to their key-value pairs.
            
        Raises:
            ImportError: If configparser is not available.
            FileNotFoundError: If the file does not exist.
        """
        import configparser
        
        parser = configparser.ConfigParser()
        parser.read(path, encoding="utf-8")
        
        result: Dict[str, Dict[str, str]] = {}
        for section in parser.sections():
            result[section] = dict(parser.items(section))
        return result
    
    @staticmethod
    def load_env(prefix: str = "") -> Dict[str, str]:
        """Load configuration from environment variables.
        
        Args:
            prefix: Optional prefix to filter environment variables.
                   Only variables starting with this prefix are included.
                   The prefix is stripped from the key names.
            
        Returns:
            Dictionary of environment variable key-value pairs.
        """
        if prefix:
            prefix_upper = prefix.upper()
            return {
                k[len(prefix):].lstrip("_").lower(): v
                for k, v in os.environ.items()
                if k.upper().startswith(prefix_upper + "_")
            }
        return dict(os.environ)
    
    @classmethod
    def load(
        cls,
        path: Optional[str] = None,
        format_hint: Optional[str] = None,
        default: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Load configuration from a file or return default.
        
        Args:
            path: Path to the configuration file.
            format_hint: File format hint ('json', 'yaml', 'ini').
                        Auto-detected from extension if not provided.
            default: Default configuration to return if loading fails.
            
        Returns:
            Configuration dictionary.
        """
        if path is None or not os.path.exists(path):
            return default or {}
        
        if format_hint is None:
            ext = os.path.splitext(path)[1].lower()
            format_map = {
                ".json": "json",
                ".yaml": "yaml",
                ".yml": "yaml",
                ".ini": "ini",
            }
            format_hint = format_map.get(ext, "json")
        
        loader_map = {
            "json": cls.load_json,
            "yaml": cls.load_yaml,
            "ini": cls.load_ini,
        }
        
        loader = loader_map.get(format_hint, cls.load_json)
        return loader(path)


class ConfigValidator:
    """Configuration validator with schema support.
    
    Validates configuration against a schema definition,
    supporting required fields, type checking, and value constraints.
    """
    
    @staticmethod
    def validate(
        config: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Validate configuration against schema.
        
        Args:
            config: Configuration dictionary to validate.
            schema: Schema definition with field specifications.
                   Each field can have: type, required, default, choices, min, max.
            
        Returns:
            Tuple of (is_valid, list of error messages).
        """
        errors: List[str] = []
        
        for field_name, field_spec in schema.items():
            required = field_spec.get("required", False)
            
            if required and field_name not in config:
                errors.append(f"Missing required field: {field_name}")
                continue
            
            if field_name not in config:
                continue
            
            value = config[field_name]
            expected_type = field_spec.get("type")
            
            if expected_type and not isinstance(value, expected_type):
                errors.append(
                    f"Field '{field_name}' has invalid type: "
                    f"expected {expected_type.__name__}, got {type(value).__name__}"
                )
                continue
            
            if "choices" in field_spec:
                choices = field_spec["choices"]
                if value not in choices:
                    errors.append(
                        f"Field '{field_name}' value '{value}' not in allowed choices: {choices}"
                    )
            
            if "min" in field_spec and isinstance(value, (int, float)):
                if value < field_spec["min"]:
                    errors.append(
                        f"Field '{field_name}' value {value} is below minimum {field_spec['min']}"
                    )
            
            if "max" in field_spec and isinstance(value, (int, float)):
                if value > field_spec["max"]:
                    errors.append(
                        f"Field '{field_name}' value {value} exceeds maximum {field_spec['max']}"
                    )
        
        return len(errors) == 0, errors
    
    @staticmethod
    def apply_defaults(
        config: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply default values from schema to configuration.
        
        Args:
            config: Configuration dictionary.
            schema: Schema definition with default values.
            
        Returns:
            Configuration with defaults applied for missing fields.
        """
        result = dict(config)
        
        for field_name, field_spec in schema.items():
            if field_name not in result and "default" in field_spec:
                result[field_name] = field_spec["default"]
        
        return result


class ConfigAction(BaseAction):
    """Configuration action for loading and managing configuration files.
    
    Supports multiple file formats and environment variable integration.
    """
    action_type: str = "config"
    display_name: str = "配置动作"
    description: str = "加载和管理配置文件，支持JSON、YAML、INI和环境变量"
    
    def __init__(self) -> None:
        super().__init__()
        self._config_cache: Dict[str, Dict[str, Any]] = {}
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute configuration operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "load")
            
            if operation == "load":
                return self._execute_load(params, start_time)
            
            elif operation == "get":
                return self._execute_get(params, start_time)
            
            elif operation == "set":
                return self._execute_set(params, start_time)
            
            elif operation == "validate":
                return self._execute_validate(params, start_time)
            
            elif operation == "env":
                return self._execute_env(params, start_time)
            
            elif operation == "clear_cache":
                self._config_cache.clear()
                return ActionResult(
                    success=True,
                    message="Configuration cache cleared",
                    duration=time.time() - start_time
                )
            
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Config operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _execute_load(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Load configuration from file."""
        path = params.get("path", "")
        cache_key = params.get("cache_key", path)
        format_hint = params.get("format")
        
        if not path:
            return ActionResult(
                success=False,
                message="Path is required for load operation",
                duration=time.time() - start_time
            )
        
        use_cache = params.get("use_cache", False)
        
        if use_cache and cache_key in self._config_cache:
            return ActionResult(
                success=True,
                message=f"Loaded configuration from cache: {path}",
                data=self._config_cache[cache_key],
                duration=time.time() - start_time
            )
        
        if not os.path.exists(path):
            return ActionResult(
                success=False,
                message=f"Configuration file not found: {path}",
                duration=time.time() - start_time
            )
        
        try:
            config = ConfigLoader.load(path=path, format_hint=format_hint)
            
            if use_cache:
                self._config_cache[cache_key] = config
            
            return ActionResult(
                success=True,
                message=f"Loaded configuration from: {path}",
                data=config,
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to load configuration: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _execute_get(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a configuration value by key path."""
        cache_key = params.get("cache_key", "")
        key_path = params.get("key", "")
        default = params.get("default")
        
        if not cache_key or cache_key not in self._config_cache:
            return ActionResult(
                success=False,
                message="Configuration not loaded or cache_key not found",
                duration=time.time() - start_time
            )
        
        config = self._config_cache[cache_key]
        
        if not key_path:
            return ActionResult(
                success=True,
                message="Returning full configuration",
                data=config,
                duration=time.time() - start_time
            )
        
        keys = key_path.split(".")
        value = config
        
        try:
            for k in keys:
                value = value[k]
            
            return ActionResult(
                success=True,
                message=f"Retrieved value for key path: {key_path}",
                data={"key": key_path, "value": value},
                duration=time.time() - start_time
            )
        
        except (KeyError, TypeError):
            return ActionResult(
                success=False,
                message=f"Key path not found: {key_path}",
                data={"key": key_path, "value": default},
                duration=time.time() - start_time
            )
    
    def _execute_set(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set a configuration value."""
        cache_key = params.get("cache_key", "")
        key_path = params.get("key", "")
        value = params.get("value")
        
        if not cache_key:
            return ActionResult(
                success=False,
                message="cache_key is required for set operation",
                duration=time.time() - start_time
            )
        
        if cache_key not in self._config_cache:
            self._config_cache[cache_key] = {}
        
        if not key_path:
            if isinstance(value, dict):
                self._config_cache[cache_key] = value
            else:
                return ActionResult(
                    success=False,
                    message="key is required when value is not a dict",
                    duration=time.time() - start_time
                )
        else:
            keys = key_path.split(".")
            target = self._config_cache[cache_key]
            
            for k in keys[:-1]:
                if k not in target:
                    target[k] = {}
                target = target[k]
            
            target[keys[-1]] = value
        
        return ActionResult(
            success=True,
            message=f"Set value for key: {key_path or '(root)'}",
            duration=time.time() - start_time
        )
    
    def _execute_validate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Validate configuration against schema."""
        cache_key = params.get("cache_key", "")
        schema = params.get("schema", {})
        
        if not cache_key or cache_key not in self._config_cache:
            return ActionResult(
                success=False,
                message="Configuration not loaded or cache_key not found",
                duration=time.time() - start_time
            )
        
        config = self._config_cache[cache_key]
        is_valid, errors = ConfigValidator.validate(config, schema)
        
        return ActionResult(
            success=is_valid,
            message="Configuration is valid" if is_valid else f"Validation failed: {errors}",
            data={"valid": is_valid, "errors": errors},
            duration=time.time() - start_time
        )
    
    def _execute_env(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Load configuration from environment variables."""
        prefix = params.get("prefix", "")
        cache_key = params.get("cache_key", "env")
        
        env_config = ConfigLoader.load_env(prefix=prefix)
        self._config_cache[cache_key] = env_config
        
        return ActionResult(
            success=True,
            message=f"Loaded {len(env_config)} environment variables",
            data=env_config,
            duration=time.time() - start_time
        )
