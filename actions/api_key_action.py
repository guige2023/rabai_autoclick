"""API Key Management action module for RabAI AutoClick.

Provides API key operations:
- APIKeyGenerateAction: Generate API keys
- APIKeyValidateAction: Validate API keys
- APIKeyRotateAction: Rotate API keys
- APIKeyScopeAction: Manage API key scopes
"""

from __future__ import annotations

import sys
import os
import secrets
import hashlib
import hmac
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIKeyGenerateAction(BaseAction):
    """Generate API keys."""
    action_type = "api_key_generate"
    display_name = "API密钥生成"
    description = "生成API密钥"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute key generation."""
        prefix = params.get('prefix', 'sk')
        length = params.get('length', 32)
        algorithm = params.get('algorithm', 'random')
        scope = params.get('scope', [])
        expires_in = params.get('expires_in', None)
        output_var = params.get('output_var', 'api_key_result')

        try:
            resolved_scope = context.resolve_value(scope) if context else scope

            if algorithm == 'random':
                key = f"{prefix}_{secrets.token_urlsafe(length)}"
            elif algorithm == 'uuid':
                import uuid
                key = f"{prefix}_{uuid.uuid4().hex}"
            elif algorithm == 'hmac':
                random_bytes = secrets.token_bytes(length)
                key = f"{prefix}_{random_bytes.hex()}"
            else:
                key = f"{prefix}_{secrets.token_urlsafe(length)}"

            secret_hash = hashlib.sha256(key.encode()).hexdigest()

            key_info = {
                'key': key,
                'prefix': prefix,
                'scope': resolved_scope,
                'algorithm': algorithm,
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(days=expires_in)).isoformat() if expires_in else None,
            }

            result = {
                'key': key_info,
                'key_hash': secret_hash,
                'key_prefix': key[:len(prefix) + 1] + '***',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Generated API key with prefix: {prefix}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Key generate error: {e}")


class APIKeyValidateAction(BaseAction):
    """Validate API keys."""
    action_type = "api_key_validate"
    display_name = "API密钥验证"
    description = "验证API密钥"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute key validation."""
        key = params.get('key', '')
        stored_hash = params.get('stored_hash', '')
        scope_required = params.get('scope_required', [])
        output_var = params.get('output_var', 'validation_result')

        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_scope_required = context.resolve_value(scope_required) if context else scope_required

            key_hash = hashlib.sha256(resolved_key.encode()).hexdigest()

            if stored_hash:
                hash_valid = hmac.compare_digest(key_hash, stored_hash)
            else:
                hash_valid = True

            scope_valid = True
            if resolved_scope_required:
                scope_valid = all(s in resolved_scope_required for s in resolved_scope_required)

            valid = hash_valid and scope_valid

            result = {
                'valid': valid,
                'key_hash': key_hash,
                'hash_valid': hash_valid,
                'scope_valid': scope_valid,
                'key_prefix': resolved_key[:10] + '***' if resolved_key else '',
            }

            return ActionResult(
                success=valid,
                data={output_var: result},
                message="API key valid" if valid else "API key invalid"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Key validate error: {e}")


class APIKeyRotateAction(BaseAction):
    """Rotate API keys."""
    action_type = "api_key_rotate"
    display_name = "API密钥轮换"
    description = "轮换API密钥"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute key rotation."""
        old_key = params.get('old_key', '')
        old_key_hash = params.get('old_key_hash', '')
        prefix = params.get('prefix', 'sk')
        grace_period = params.get('grace_period', 0)
        output_var = params.get('output_var', 'rotation_result')

        if not old_key:
            return ActionResult(success=False, message="old_key is required")

        try:
            import secrets

            new_key = f"{prefix}_{secrets.token_urlsafe(32)}"
            new_key_hash = hashlib.sha256(new_key.encode()).hexdigest()

            old_key_hash_computed = hashlib.sha256(old_key.encode()).hexdigest()
            old_valid = hmac.compare_digest(old_key_hash_computed, old_key_hash) if old_key_hash else True

            result = {
                'new_key': new_key,
                'new_key_hash': new_key_hash,
                'new_key_prefix': new_key[:len(prefix) + 1] + '***',
                'old_key_revoked': old_valid,
                'grace_period': grace_period,
                'rotated_at': datetime.now().isoformat(),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Key rotated, new key: {result['new_key_prefix']}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Key rotate error: {e}")


class APIKeyScopeAction(BaseAction):
    """Manage API key scopes."""
    action_type = "api_key_scope"
    display_name = "API密钥作用域"
    description = "管理API密钥作用域"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute scope management."""
        scopes = params.get('scopes', [])
        required_scope = params.get('required_scope', '')
        operation = params.get('operation', 'check')
        output_var = params.get('output_var', 'scope_result')

        if not scopes and operation in ['check', 'require']:
            return ActionResult(success=False, message="scopes are required")

        try:
            resolved_scopes = context.resolve_value(scopes) if context else scopes
            resolved_required = context.resolve_value(required_scope) if context else required_scope

            result = {}

            if operation == 'check':
                has_scope = resolved_required in resolved_scopes
                result = {
                    'has_scope': has_scope,
                    'scope': resolved_required,
                    'all_scopes': resolved_scopes,
                }
            elif operation == 'require':
                missing = [s for s in resolved_scopes if s not in resolved_scopes]
                result = {
                    'valid': len(missing) == 0,
                    'missing_scopes': missing,
                }
            elif operation == 'grant':
                new_scopes = list(set(resolved_scopes + resolved_scopes))
                result = {
                    'scopes': new_scopes,
                    'granted': resolved_scopes,
                }
            elif operation == 'revoke':
                remaining = [s for s in resolved_scopes if s not in resolved_scopes]
                result = {
                    'scopes': remaining,
                    'revoked': resolved_scopes,
                }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Scope operation '{operation}' completed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Scope error: {e}")
