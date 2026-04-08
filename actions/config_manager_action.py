"""Config manager action module for RabAI AutoClick.

Provides configuration management with environment overrides,
secrets management, and dynamic configuration updates.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from threading import Lock
from copy import deepcopy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ConfigEntry:
    """A configuration entry."""
    key: str
    value: Any
    source: str = "default"  # default, env, file, runtime
    encrypted: bool = False
    description: str = ""


class ConfigManagerAction(BaseAction):
    """Manage application configuration with layering.
    
    Supports environment-based overrides, secrets encryption,
    dynamic updates, and configuration validation.
    """
    action_type = "config_manager"
    display_name = "配置管理"
    description = "分层配置管理和环境覆盖"
    
    def __init__(self):
        super().__init__()
        self._config: Dict[str, ConfigEntry] = {}
        self._layers = ['default', 'env', 'file', 'runtime']
        self._lock = Lock()
        self._load_env()
    
    def _load_env(self) -> None:
        """Load configuration from environment variables."""
        # Common env var prefixes
        prefixes = ['APP_', 'API_', 'SERVICE_', 'CONFIG_']
        
        for key, value in os.environ.items():
            for prefix in prefixes:
                if key.startswith(prefix):
                    config_key = key[len(prefix):].lower()
                    self._config[key] = ConfigEntry(
                        key=config_key,
                        value=self._parse_value(value),
                        source='env'
                    )
                    break
    
    def _parse_value(self, value: str) -> Any:
        """Parse string value to appropriate type."""
        # Boolean
        if value.lower() in ('true', 'yes', '1'):
            return True
        if value.lower() in ('false', 'no', '0'):
            return False
        
        # Number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        # JSON
        if value.startswith('{') or value.startswith('['):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        
        return value
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute configuration operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'get', 'set', 'delete', 'list', 'validate'
                - key: Configuration key
                - value: Configuration value (for set)
                - source: Configuration source layer
                - encrypted: Encrypt the value
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'get').lower()
        
        if operation == 'get':
            return self._get(params)
        elif operation == 'set':
            return self._set(params)
        elif operation == 'delete':
            return self._delete(params)
        elif operation == 'list':
            return self._list(params)
        elif operation == 'validate':
            return self._validate(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get configuration value."""
        key = params.get('key')
        default = params.get('default')
        decrypt = params.get('decrypt', True)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        # Search layers from highest to lowest priority
        layers = ['runtime', 'env', 'file', 'default']
        
        for layer in layers:
            entry = self._config.get(f"{layer}:{key}")
            if entry:
                value = entry.value
                if entry.encrypted and decrypt:
                    # Would decrypt here
                    pass
                return ActionResult(
                    success=True,
                    message=f"Found in {layer} layer",
                    data={'key': key, 'value': value, 'source': layer}
                )
        
        # Return default if provided
        if default is not None:
            return ActionResult(
                success=True,
                message="Using default value",
                data={'key': key, 'value': default, 'source': 'default'}
            )
        
        return ActionResult(
            success=False,
            message=f"Configuration key '{key}' not found"
        )
    
    def _set(self, params: Dict[str, Any]) -> ActionResult:
        """Set configuration value."""
        key = params.get('key')
        value = params.get('value')
        source = params.get('source', 'runtime')
        encrypted = params.get('encrypted', False)
        description = params.get('description', '')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        # Encrypt if requested
        if encrypted:
            # Would encrypt value here
            pass
        
        entry = ConfigEntry(
            key=key,
            value=value,
            source=source,
            encrypted=encrypted,
            description=description
        )
        
        with self._lock:
            self._config[f"{source}:{key}"] = entry
        
        return ActionResult(
            success=True,
            message=f"Set '{key}' in {source} layer",
            data={'key': key, 'source': source}
        )
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete configuration value."""
        key = params.get('key')
        source = params.get('source')  # If None, delete from all layers
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        deleted = 0
        with self._lock:
            if source:
                entry_key = f"{source}:{key}"
                if entry_key in self._config:
                    del self._config[entry_key]
                    deleted = 1
            else:
                # Delete from all layers
                keys_to_delete = [
                    k for k in self._config.keys()
                    if k.endswith(f":{key}")
                ]
                for k in keys_to_delete:
                    del self._config[k]
                deleted = len(keys_to_delete)
        
        return ActionResult(
            success=True,
            message=f"Deleted {deleted} entries for '{key}'",
            data={'key': key, 'deleted': deleted}
        )
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List configuration entries."""
        source = params.get('source')
        pattern = params.get('pattern')
        
        entries = []
        with self._lock:
            for key, entry in self._config.items():
                if source and entry.source != source:
                    continue
                if pattern and pattern not in entry.key:
                    continue
                entries.append({
                    'key': entry.key,
                    'value': entry.value,
                    'source': entry.source,
                    'encrypted': entry.encrypted,
                    'description': entry.description
                })
        
        return ActionResult(
            success=True,
            message=f"Found {len(entries)} entries",
            data={'entries': entries, 'count': len(entries)}
        )
    
    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate configuration against schema."""
        schema = params.get('schema', {})
        required_keys = schema.get('required', [])
        type_map = schema.get('types', {})
        
        errors = []
        
        for key in required_keys:
            found = any(
                e.key == key for e in self._config.values()
            )
            if not found:
                errors.append(f"Required key '{key}' is missing")
        
        for key, expected_type in type_map.items():
            entry = next(
                (e for e in self._config.values() if e.key == key),
                None
            )
            if entry:
                if not isinstance(entry.value, expected_type):
                    errors.append(
                        f"Key '{key}' has wrong type: "
                        f"expected {expected_type}, got {type(entry.value)}"
                    )
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"{'Valid' if not errors else f'{len(errors)} validation errors'}",
            data={'valid': len(errors) == 0, 'errors': errors}
        )


class SecretsManagerAction(BaseAction):
    """Manage secrets with encryption and access control."""
    action_type = "secrets_manager"
    display_name = "密钥管理"
    description = "加密密钥和安全存储"
    
    def __init__(self):
        super().__init__()
        self._secrets: Dict[str, str] = {}
        self._encryption_key = os.environ.get('ENCRYPTION_KEY', '')[:32]
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute secrets operation."""
        operation = params.get('operation', 'get').lower()
        
        if operation == 'get':
            return self._get_secret(params)
        elif operation == 'set':
            return self._set_secret(params)
        elif operation == 'delete':
            return self._delete_secret(params)
        elif operation == 'list':
            return self._list_secrets(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _get_secret(self, params: Dict[str, Any]) -> ActionResult:
        """Get a secret value."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key not in self._secrets:
                return ActionResult(
                    success=False,
                    message=f"Secret '{key}' not found"
                )
            
            value = self._decrypt(self._secrets[key])
        
        return ActionResult(
            success=True,
            message=f"Retrieved secret '{key}'",
            data={'key': key, 'value': value}
        )
    
    def _set_secret(self, params: Dict[str, Any]) -> ActionResult:
        """Store a secret value."""
        key = params.get('key')
        value = params.get('value')
        
        if not key or value is None:
            return ActionResult(
                success=False,
                message="key and value are required"
            )
        
        encrypted = self._encrypt(str(value))
        
        with self._lock:
            self._secrets[key] = encrypted
        
        return ActionResult(
            success=True,
            message=f"Stored secret '{key}'"
        )
    
    def _delete_secret(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a secret."""
        key = params.get('key')
        
        with self._lock:
            if key in self._secrets:
                del self._secrets[key]
                return ActionResult(
                    success=True,
                    message=f"Deleted secret '{key}'"
                )
        
        return ActionResult(
            success=False,
            message=f"Secret '{key}' not found"
        )
    
    def _list_secrets(self, params: Dict[str, Any]) -> ActionResult:
        """List secret keys (not values)."""
        with self._lock:
            keys = list(self._secrets.keys())
        
        return ActionResult(
            success=True,
            message=f"{len(keys)} secrets",
            data={'keys': keys, 'count': len(keys)}
        )
    
    def _encrypt(self, value: str) -> str:
        """Encrypt a value (simplified)."""
        # In production, use proper encryption like Fernet or AES
        return value  # Simplified
    
    def _decrypt(self, value: str) -> str:
        """Decrypt a value (simplified)."""
        return value  # Simplified
