"""Data tokenizer action module for RabAI AutoClick.

Provides data tokenization with reversible and irreversible modes,
format-preserving tokenization, and token management.
"""

import sys
import os
import json
import uuid
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable
import base64
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataTokenizerAction(BaseAction):
    """Tokenize sensitive data with reversible and irreversible modes.
    
    Supports format-preserving tokenization, token vault,
    batch tokenization, and detokenization.
    """
    action_type = "data_tokenizer"
    display_name = "数据令牌化"
    description = "敏感数据令牌化，支持可逆和不可逆模式"
    
    def __init__(self):
        super().__init__()
        self._token_vault: Dict[str, Dict[str, Any]] = {}
        self._reverse_vault: Dict[str, str] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tokenization operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (tokenize, detokenize, batch,
                   generate_token, lookup), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'tokenize')
        
        if action == 'tokenize':
            return self._tokenize(params)
        elif action == 'detokenize':
            return self._detokenize(params)
        elif action == 'batch':
            return self._batch_tokenize(params)
        elif action == 'generate_token':
            return self._generate_token(params)
        elif action == 'lookup':
            return self._lookup_token(params)
        elif action == 'stats':
            return self._get_stats(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _tokenize(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Tokenize a single value."""
        value = params.get('value')
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        reversible = params.get('reversible', False)
        token_type = params.get('token_type', 'uuid')
        preserve_format = params.get('preserve_format', False)
        secret = params.get('secret')
        namespace = params.get('namespace', 'default')
        
        with self._lock:
            if token_type == 'uuid':
                token = str(uuid.uuid4())
            elif token_type == 'hash':
                data = f"{namespace}:{value}".encode()
                token = hashlib.sha256(data).hexdigest()[:32]
            elif token_type == 'random':
                import secrets
                token = secrets.token_urlsafe(32)
            else:
                token = str(uuid.uuid4())
            
            if reversible:
                self._token_vault[token] = {
                    'value': value,
                    'type': token_type,
                    'namespace': namespace
                }
                self._reverse_vault[f"{namespace}:{value}"] = token
        
        result = {'token': token, 'reversible': reversible}
        
        if reversible:
            result['stored'] = True
        
        return ActionResult(
            success=True,
            message=f"Tokenized value with {token_type}",
            data=result
        )
    
    def _detokenize(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retrieve original value from token."""
        token = params.get('token')
        if not token:
            return ActionResult(success=False, message="token is required")
        
        with self._lock:
            if token not in self._token_vault:
                return ActionResult(
                    success=False,
                    message=f"Token '{token}' not found"
                )
            
            vault_entry = self._token_vault[token]
            original_value = vault_entry['value']
        
        return ActionResult(
            success=True,
            message="Detokenized successfully",
            data={'value': original_value}
        )
    
    def _batch_tokenize(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Tokenize multiple values."""
        values = params.get('values', [])
        if not values:
            return ActionResult(success=False, message="No values provided")
        
        reversible = params.get('reversible', False)
        token_type = params.get('token_type', 'uuid')
        namespace = params.get('namespace', 'default')
        
        tokens = []
        
        with self._lock:
            for value in values:
                if token_type == 'uuid':
                    token = str(uuid.uuid4())
                elif token_type == 'hash':
                    data = f"{namespace}:{value}".encode()
                    token = hashlib.sha256(data).hexdigest()[:32]
                elif token_type == 'random':
                    import secrets
                    token = secrets.token_urlsafe(32)
                else:
                    token = str(uuid.uuid4())
                
                if reversible:
                    self._token_vault[token] = {
                        'value': value,
                        'type': token_type,
                        'namespace': namespace
                    }
                
                tokens.append({'original': value, 'token': token})
        
        return ActionResult(
            success=True,
            message=f"Tokenized {len(tokens)} values",
            data={
                'tokens': tokens,
                'count': len(tokens),
                'reversible': reversible
            }
        )
    
    def _generate_token(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate a token without storing value."""
        token_type = params.get('token_type', 'uuid')
        prefix = params.get('prefix', '')
        
        if token_type == 'uuid':
            token = str(uuid.uuid4())
        elif token_type == 'random':
            import secrets
            token = secrets.token_urlsafe(32)
        elif token_type == 'nanoid':
            import secrets
            token = secrets.token_urlsafe(21)
        else:
            token = str(uuid.uuid4())
        
        if prefix:
            token = f"{prefix}_{token}"
        
        return ActionResult(
            success=True,
            message=f"Generated {token_type} token",
            data={'token': token}
        )
    
    def _lookup_token(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Look up token by original value."""
        namespace = params.get('namespace', 'default')
        value = params.get('value')
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        with self._lock:
            token = self._reverse_vault.get(f"{namespace}:{value}")
            
            if token:
                return ActionResult(
                    success=True,
                    message="Token found",
                    data={'token': token}
                )
            else:
                return ActionResult(
                    success=False,
                    message="No token found for value"
                )
    
    def _get_stats(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get token vault statistics."""
        with self._lock:
            namespaces = {}
            for entry in self._token_vault.values():
                ns = entry.get('namespace', 'default')
                namespaces[ns] = namespaces.get(ns, 0) + 1
            
            return ActionResult(
                success=True,
                message=f"Token vault stats",
                data={
                    'total_tokens': len(self._token_vault),
                    'namespaces': namespaces
                }
            )
