"""API Key action module for RabAI AutoClick.

Provides API key management, validation, rotation, and format operations.
"""

import hashlib
import hmac
import secrets
import string
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIKeyAction(BaseAction):
    """API key generation, validation, and management.
    
    Supports generating secure API keys in various formats,
    validating existing keys, key prefix/suffix operations,
    and HMAC-based request signing.
    """
    action_type = "apikey"
    display_name = "API密钥管理"
    description = "API密钥生成、验证、轮换与签名"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API key operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'generate', 'validate', 'sign', 'verify', 'mask', 'parse'
                - key: API key to process
                - key_type: 'random', 'uuid', 'hmac', 'bearer', 'basic'
                - length: Key length in bytes (default 32)
                - prefix: Key prefix (e.g., 'sk_', 'api_')
                - secret: Secret for HMAC signing
                - algorithm: HMAC algorithm (sha256, sha512)
                - message: Message to sign
        
        Returns:
            ActionResult with operation result.
        """
        command = params.get('command', 'generate')
        key = params.get('key')
        key_type = params.get('key_type', 'random')
        length = params.get('length', 32)
        prefix = params.get('prefix', '')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'sha256')
        message = params.get('message')
        
        if command == 'generate':
            return self._generate_key(key_type, length, prefix)
        
        if command == 'validate':
            if not key:
                return ActionResult(success=False, message="key is required for validate")
            return self._validate_key(key)
        
        if command == 'sign':
            if not secret or not message:
                return ActionResult(success=False, message="secret and message required for sign")
            return self._sign_message(message, secret, algorithm)
        
        if command == 'verify':
            if not secret or not message or not key:
                return ActionResult(success=False, message="secret, message, and key required for verify")
            return self._verify_signature(message, key, secret, algorithm)
        
        if command == 'mask':
            if not key:
                return ActionResult(success=False, message="key is required for mask")
            return self._mask_key(key)
        
        if command == 'parse':
            if not key:
                return ActionResult(success=False, message="key is required for parse")
            return self._parse_key(key)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _generate_key(self, key_type: str, length: int, prefix: str) -> ActionResult:
        """Generate an API key."""
        if key_type == 'random':
            alphabet = string.ascii_letters + string.digits
            raw = ''.join(secrets.choice(alphabet) for _ in range(length))
            key = f"{prefix}{raw}"
        elif key_type == 'uuid':
            import uuid
            key = f"{prefix}{uuid.uuid4().hex}"
        elif key_type == 'bearer':
            raw = secrets.token_urlsafe(length)
            key = f"{prefix}{raw}"
        elif key_type == 'basic':
            raw = secrets.token_hex(length // 2)
            import base64
            encoded = base64.b64encode(f"user:{raw}".encode()).decode()
            key = f"{prefix}{encoded}"
        elif key_type == 'hmac':
            raw = secrets.token_hex(length)
            key = f"{prefix}{raw}"
        else:
            return ActionResult(success=False, message=f"Unknown key_type: {key_type}")
        
        return ActionResult(
            success=True,
            message=f"Generated {key_type} key",
            data={'key': key, 'key_type': key_type, 'prefix': prefix, 'length': len(key)}
        )
    
    def _validate_key(self, key: str) -> ActionResult:
        """Validate API key format."""
        checks = {
            'has_content': bool(key and len(key) > 0),
            'is_alphanumeric': key.replace('-', '').replace('_', '').replace(' ', '').isalnum(),
            'min_length': len(key) >= 8,
            'no_whitespace': ' ' not in key and '\t' not in key,
        }
        
        score = sum(checks.values()) / len(checks)
        valid = score >= 0.75
        
        return ActionResult(
            success=valid,
            message=f"Key validation: {score:.0%} score",
            data={'valid': valid, 'checks': checks, 'score': score}
        )
    
    def _sign_message(self, message: str, secret: str, algorithm: str) -> ActionResult:
        """Create HMAC signature for a message."""
        try:
            algo = algorithm.replace('-', '')
            if algo not in ('sha256', 'sha512', 'sha1', 'md5'):
                algo = 'sha256'
            signature = hmac.new(secret.encode(), message.encode(), hashlib.new(algo)).hexdigest()
            return ActionResult(
                success=True,
                message=f"Signed with {algorithm}",
                data={'signature': signature, 'algorithm': algorithm}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to sign: {e}")
    
    def _verify_signature(self, message: str, signature: str, secret: str, algorithm: str) -> ActionResult:
        """Verify HMAC signature."""
        try:
            algo = algorithm.replace('-', '')
            if algo not in ('sha256', 'sha512', 'sha1', 'md5'):
                algo = 'sha256'
            expected = hmac.new(secret.encode(), message.encode(), hashlib.new(algo)).hexdigest()
            valid = hmac.compare_digest(signature.lower(), expected.lower())
            return ActionResult(
                success=True,
                message=f"Signature {'valid' if valid else 'invalid'}",
                data={'valid': valid, 'algorithm': algorithm}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to verify: {e}")
    
    def _mask_key(self, key: str) -> ActionResult:
        """Mask an API key for safe display."""
        if len(key) <= 8:
            masked = '*' * len(key)
        elif len(key) <= 16:
            masked = key[:4] + '*' * (len(key) - 8) + key[-4:]
        else:
            masked = key[:6] + '*' * (len(key) - 12) + key[-6:]
        return ActionResult(
            success=True,
            message=f"Masked key: {masked}",
            data={'original_length': len(key), 'masked': masked}
        )
    
    def _parse_key(self, key: str) -> ActionResult:
        """Parse API key structure."""
        parts = key.split('-') if '-' in key else [key]
        prefix = parts[0] if len(parts) > 1 else None
        body = parts[1] if len(parts) > 1 else key
        suffix = parts[-1] if len(parts) > 2 else None
        
        return ActionResult(
            success=True,
            message=f"Parsed key with {len(parts)} parts",
            data={
                'original': key,
                'prefix': prefix,
                'body_length': len(body),
                'has_suffix': suffix is not None,
                'parts_count': len(parts)
            }
        )
