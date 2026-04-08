"""
Configuration management utilities - loading, validation, environment parsing, secrets.
"""
from typing import Any, Dict, List, Optional, Union
import os
import re
import logging
import json
import yaml

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_env_file(text: str) -> Dict[str, str]:
    config: Dict[str, str] = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, val = line.split("=", 1)
            config[key.strip()] = val.strip().strip("\"'")
    return config


def _substitute_env_vars(text: str, config: Optional[Dict[str, str]] = None) -> str:
    if config is None:
        config = dict(os.environ)
    pattern = re.compile(r"\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)")

    def replacer(m: re.Match) -> str:
        var_name = m.group(1) or m.group(2)
        return config.get(var_name, m.group(0))

    return pattern.sub(replacer, text)


def _flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def _validate_schema(config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    errors = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})
    for field in required:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    for field, field_spec in properties.items():
        if field in config:
            expected_type = field_spec.get("type")
            value = config[field]
            if expected_type == "string" and not isinstance(value, str):
                errors.append(f"{field} must be string, got {type(value).__name__}")
            elif expected_type == "number" and not isinstance(value, (int, float)):
                errors.append(f"{field} must be number")
            elif expected_type == "boolean" and not isinstance(value, bool):
                errors.append(f"{field} must be boolean")
            elif expected_type == "array" and not isinstance(value, list):
                errors.append(f"{field} must be array")
            elif expected_type == "object" and not isinstance(value, dict):
                errors.append(f"{field} must be object")
            if "enum" in field_spec and value not in field_spec["enum"]:
                errors.append(f"{field} must be one of {field_spec['enum']}")
    return errors


class ConfigAction(BaseAction):
    """Configuration management operations.

    Provides env file parsing, variable substitution, schema validation, merging.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse_env")
        text = params.get("text", "")
        config = params.get("config", {})

        try:
            if operation == "parse_env":
                if not text:
                    return {"success": False, "error": "text required"}
                result = _parse_env_file(text)
                return {"success": True, "config": result, "keys": list(result.keys()), "count": len(result)}

            elif operation == "substitute":
                if not text:
                    return {"success": False, "error": "text required"}
                env_config = params.get("env", dict(os.environ))
                result = _substitute_env_vars(text, env_config)
                return {"success": True, "substituted": result}

            elif operation == "get":
                key = params.get("key", "")
                default = params.get("default")
                value = config.get(key, default)
                return {"success": True, "key": key, "value": value, "found": key in config}

            elif operation == "set":
                key = params.get("key", "")
                value = params.get("value")
                if not key:
                    return {"success": False, "error": "key required"}
                result = dict(config)
                result[key] = value
                return {"success": True, "config": result}

            elif operation == "flatten":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                sep = params.get("separator", ".")
                flat = _flatten_dict(config, sep=sep)
                return {"success": True, "config": flat, "keys": list(flat.keys())}

            elif operation == "validate":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                schema = params.get("schema", {})
                errors = _validate_schema(config, schema)
                return {"success": True, "valid": len(errors) == 0, "errors": errors}

            elif operation == "merge":
                configs = params.get("configs", [config])
                base = configs[0] if configs else {}
                merged = dict(base)
                for c in configs[1:]:
                    if isinstance(c, dict):
                        merged.update(c)
                return {"success": True, "config": merged}

            elif operation == "load_json":
                if not text:
                    return {"success": False, "error": "text required"}
                result = json.loads(text)
                return {"success": True, "config": result}

            elif operation == "load_yaml":
                if not text:
                    return {"success": False, "error": "text required"}
                result = yaml.safe_load(text)
                return {"success": True, "config": result}

            elif operation == "to_json":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                indent = int(params.get("indent", 2))
                result = json.dumps(config, indent=indent, ensure_ascii=False)
                return {"success": True, "json": result}

            elif operation == "to_yaml":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                result = yaml.dump(config, default_flow_style=False)
                return {"success": True, "yaml": result}

            elif operation == "diff":
                config1 = config
                config2 = params.get("other", {})
                if not isinstance(config1, dict) or not isinstance(config2, dict):
                    return {"success": False, "error": "Both configs must be dicts"}
                flat1 = _flatten_dict(config1)
                flat2 = _flatten_dict(config2)
                all_keys = set(flat1.keys()) | set(flat2.keys())
                diffs = []
                for key in sorted(all_keys):
                    if key not in flat1:
                        diffs.append({"key": key, "op": "add", "value": flat2[key]})
                    elif key not in flat2:
                        diffs.append({"key": key, "op": "remove", "value": flat1[key]})
                    elif flat1[key] != flat2[key]:
                        diffs.append({"key": key, "op": "change", "old": flat1[key], "new": flat2[key]})
                return {"success": True, "diffs": diffs, "count": len(diffs), "has_changes": len(diffs) > 0}

            elif operation == "extract_secrets":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                flat = _flatten_dict(config)
                secret_patterns = ["password", "secret", "token", "api_key", "apikey", "auth", "credential"]
                secrets = {k: v for k, v in flat.items() if any(p in k.lower() for p in secret_patterns)}
                redacted = {k: "***REDACTED***" for k in secrets}
                return {"success": True, "secrets": list(secrets.keys()), "redacted": redacted, "count": len(secrets)}

            elif operation == "mask_secrets":
                if not isinstance(config, dict):
                    return {"success": False, "error": "config must be a dict"}
                flat = _flatten_dict(config)
                secret_patterns = ["password", "secret", "token", "api_key", "apikey", "auth", "credential"]
                masked = {}
                for k, v in flat.items():
                    if any(p in k.lower() for p in secret_patterns) and isinstance(v, str):
                        masked[k] = "***"
                    else:
                        masked[k] = v
                return {"success": True, "config": masked, "count": len(masked)}

            elif operation == "defaults":
                defaults = params.get("defaults", {})
                result = dict(defaults)
                if isinstance(config, dict):
                    result.update(config)
                return {"success": True, "config": result}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"ConfigAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for config operations."""
    return ConfigAction().execute(context, params)
