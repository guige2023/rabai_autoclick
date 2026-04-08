"""Parameter store action module for RabAI AutoClick.

Provides secure parameter storage and retrieval with encryption support,
versioning, and environment-based configuration management.
"""

import sys
import os
import json
import hashlib
import base64
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Parameter:
    """A stored parameter with metadata."""
    name: str
    value: str
    param_type: str = "String"  # String, SecureString, StringList
    version: int = 1
    created_at: str = ""
    updated_at: str = ""
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    encrypted: bool = False


class ParameterStore:
    """In-memory parameter store with versioning and encryption."""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self._params: Dict[str, Dict[int, Parameter]] = {}
        self._encryption_key = encryption_key or self._generate_key()
        self._lock = Lock()
    
    def _generate_key(self) -> bytes:
        """Generate a random encryption key."""
        import secrets
        return secrets.token_bytes(32)
    
    def _encrypt(self, value: str) -> str:
        """Encrypt a value using AES."""
        from cryptography.fernet import Fernet
        f = Fernet(base64.urlsafe_b64encode(self._encryption_key))
        return f.encrypt(value.encode()).decode()
    
    def _decrypt(self, encrypted: str) -> str:
        """Decrypt an encrypted value."""
        from cryptography.fernet import Fernet
        f = Fernet(base64.urlsafe_b64encode(self._encryption_key))
        return f.decrypt(encrypted.encode()).decode()
    
    def put(
        self,
        name: str,
        value: str,
        param_type: str = "String",
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        encrypt: bool = False
    ) -> Parameter:
        """Store a parameter."""
        with self._lock:
            now = datetime.utcnow().isoformat() + "Z"
            
            # Encrypt if requested
            stored_value = self._encrypt(value) if encrypt else value
            
            # Get next version
            if name in self._params:
                version = max(self._params[name].keys()) + 1
            else:
                version = 1
                self._params[name] = {}
            
            param = Parameter(
                name=name,
                value=stored_value,
                param_type=param_type,
                version=version,
                created_at=now,
                updated_at=now,
                description=description,
                tags=tags or {},
                encrypted=encrypt
            )
            
            self._params[name][version] = param
            return param
    
    def get(
        self,
        name: str,
        version: Optional[int] = None,
        decrypt: bool = True
    ) -> Optional[str]:
        """Get a parameter value."""
        with self._lock:
            if name not in self._params:
                return None
            
            versions = self._params[name]
            if version is None:
                version = max(versions.keys())
            
            if version not in versions:
                return None
            
            param = versions[version]
            value = param.value
            
            if param.encrypted and decrypt:
                value = self._decrypt(value)
            
            return value
    
    def get_metadata(self, name: str) -> Optional[List[Dict]]:
        """Get parameter metadata without values."""
        with self._lock:
            if name not in self._params:
                return None
            
            return [
                {
                    'name': p.name,
                    'version': p.version,
                    'type': p.param_type,
                    'created': p.created_at,
                    'updated': p.updated_at,
                    'description': p.description,
                    'tags': p.tags,
                    'encrypted': p.encrypted
                }
                for p in self._params[name].values()
            ]
    
    def delete(self, name: str, version: Optional[int] = None) -> bool:
        """Delete a parameter or specific version."""
        with self._lock:
            if name not in self._params:
                return False
            
            if version is None:
                del self._params[name]
            else:
                if version in self._params[name]:
                    del self._params[name][version]
                    if not self._params[name]:
                        del self._params[name]
                else:
                    return False
            
            return True
    
    def list_names(self, prefix: Optional[str] = None) -> List[str]:
        """List all parameter names, optionally filtered by prefix."""
        with self._lock:
            names = list(self._params.keys())
            if prefix:
                names = [n for n in names if n.startswith(prefix)]
            return sorted(names)


class ParameterStoreAction(BaseAction):
    """Manage parameters with encryption, versioning, and tagging.
    
    Provides secure storage for API keys, configuration values,
    and other sensitive data.
    """
    action_type = "parameter_store"
    display_name = "参数存储"
    description = "加密存储和版本化管理配置参数"
    
    def __init__(self):
        super().__init__()
        self._store = ParameterStore()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parameter store operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'put', 'get', 'delete', 'list', 'metadata'
                - name: Parameter name
                - value: Parameter value (for put)
                - type: Parameter type (default 'String')
                - version: Specific version (for get/delete)
                - description: Parameter description
                - tags: Parameter tags dict
                - encrypt: Encrypt the value (default False)
                - decrypt: Decrypt on get (default True)
                - prefix: Filter by name prefix (for list)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '').lower()
        
        if operation == 'put':
            return self._put(params)
        elif operation == 'get':
            return self._get(params)
        elif operation == 'delete':
            return self._delete(params)
        elif operation == 'list':
            return self._list(params)
        elif operation == 'metadata':
            return self._metadata(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _put(self, params: Dict[str, Any]) -> ActionResult:
        """Store a parameter."""
        name = params.get('name')
        value = params.get('value')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        param_type = params.get('type', 'String')
        description = params.get('description', '')
        tags = params.get('tags', {})
        encrypt = params.get('encrypt', False)
        
        param = self._store.put(
            name=name,
            value=str(value),
            param_type=param_type,
            description=description,
            tags=tags,
            encrypt=encrypt
        )
        
        return ActionResult(
            success=True,
            message=f"Stored {name} version {param.version}",
            data={
                'name': param.name,
                'version': param.version,
                'type': param.param_type,
                'encrypted': param.encrypted
            }
        )
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get a parameter."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        version = params.get('version')
        decrypt = params.get('decrypt', True)
        
        value = self._store.get(name, version, decrypt)
        
        if value is None:
            return ActionResult(
                success=False,
                message=f"Parameter {name} not found"
            )
        
        return ActionResult(
            success=True,
            message=f"Retrieved {name}",
            data={'name': name, 'value': value}
        )
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a parameter."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        version = params.get('version')
        deleted = self._store.delete(name, version)
        
        return ActionResult(
            success=deleted,
            message=f"{'Deleted' if deleted else 'Not found'}",
            data={'name': name, 'version': version}
        )
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List parameter names."""
        prefix = params.get('prefix')
        names = self._store.list_names(prefix)
        
        return ActionResult(
            success=True,
            message=f"Found {len(names)} parameters",
            data={'names': names, 'count': len(names)}
        )
    
    def _metadata(self, params: Dict[str, Any]) -> ActionResult:
        """Get parameter metadata."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        metadata = self._store.get_metadata(name)
        
        if metadata is None:
            return ActionResult(
                success=False,
                message=f"Parameter {name} not found"
            )
        
        return ActionResult(
            success=True,
            message=f"Retrieved metadata for {name}",
            data={'name': name, 'versions': metadata}
        )


class EnvironmentConfigAction(BaseAction):
    """Load configuration from environment variables with type casting."""
    action_type = "environment_config"
    display_name = "环境配置"
    description = "从环境变量加载配置并类型转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Load configuration from environment.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - mappings: Dict of env_var -> (key, type, default)
                  Example: {'API_KEY': ('api_key', 'str', None)}
                - prefix: Optional prefix to strip from env vars
        
        Returns:
            ActionResult with loaded configuration.
        """
        mappings = params.get('mappings', {})
        prefix = params.get('prefix', '')
        
        config = {}
        missing = []
        
        for env_var, spec in mappings.items():
            if isinstance(spec, tuple):
                key, value_type, default = spec
            else:
                key = spec
                value_type = 'str'
                default = None
            
            full_env_var = f"{prefix}{env_var}" if prefix else env_var
            value = os.environ.get(full_env_var)
            
            if value is None:
                if default is not None:
                    value = default
                else:
                    missing.append(full_env_var)
                    continue
            
            # Type casting
            try:
                if value_type == 'int':
                    value = int(value)
                elif value_type == 'float':
                    value = float(value)
                elif value_type == 'bool':
                    value = value.lower() in ('true', '1', 'yes')
                elif value_type == 'json':
                    value = json.loads(value)
            except (ValueError, TypeError) as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to cast {full_env_var}: {e}"
                )
            
            config[key] = value
        
        return ActionResult(
            success=len(missing) == 0,
            message=f"Loaded {len(config)} config values" +
                    (f", missing: {missing}" if missing else ""),
            data={'config': config, 'missing': missing}
        )
