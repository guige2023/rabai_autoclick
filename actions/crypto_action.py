"""Crypto action module for RabAI AutoClick.

Provides cryptographic operations including hashing, encryption, and encoding.
"""

import hashlib
import hmac
import base64
import secrets
import sys
import os
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HashAction(BaseAction):
    """Compute cryptographic hash.
    
    Supports MD5, SHA1, SHA256, SHA512 algorithms.
    """
    action_type = "hash"
    display_name = "哈希计算"
    description = "计算数据的哈希值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute hash.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: data, algorithm, encoding.
        
        Returns:
            ActionResult with hash value.
        """
        data = params.get('data', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'hex')
        
        if not data:
            return ActionResult(success=False, message="data required")
        
        alg = algorithm.lower()
        if alg == 'md5':
            h = hashlib.md5()
        elif alg == 'sha1':
            h = hashlib.sha1()
        elif alg == 'sha256':
            h = hashlib.sha256()
        elif alg == 'sha512':
            h = hashlib.sha512()
        elif alg == 'sha384':
            h = hashlib.sha384()
        else:
            return ActionResult(
                success=False,
                message=f"Unknown algorithm: {algorithm}"
            )
        
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        h.update(data)
        
        if encoding == 'hex':
            result = h.hexdigest()
        elif encoding == 'base64':
            result = base64.b64encode(h.digest()).decode()
        else:
            result = h.hexdigest()
        
        return ActionResult(
            success=True,
            message=f"{algorithm.upper()} hash computed",
            data={'hash': result, 'algorithm': algorithm, 'encoding': encoding}
        )


class HmacAction(BaseAction):
    """Compute HMAC authentication code.
    
    Generates keyed-hash message authentication codes.
    """
    action_type = "hmac"
    display_name = "HMAC计算"
    description = "计算HMAC认证码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute HMAC.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: data, key, algorithm, encoding.
        
        Returns:
            ActionResult with HMAC value.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'hex')
        
        if not data:
            return ActionResult(success=False, message="data required")
        
        if not key:
            return ActionResult(success=False, message="key required")
        
        alg = algorithm.lower()
        if alg == 'sha256':
            h = hmac.new(key.encode(), data.encode(), hashlib.sha256)
        elif alg == 'sha512':
            h = hmac.new(key.encode(), data.encode(), hashlib.sha512)
        elif alg == 'md5':
            h = hmac.new(key.encode(), data.encode(), hashlib.md5)
        else:
            h = hmac.new(key.encode(), data.encode(), hashlib.sha256)
        
        if encoding == 'hex':
            result = h.hexdigest()
        elif encoding == 'base64':
            result = base64.b64encode(h.digest()).decode()
        else:
            result = h.hexdigest()
        
        return ActionResult(
            success=True,
            message="HMAC computed",
            data={'hmac': result, 'algorithm': algorithm}
        )


class Base64EncodeAction(BaseAction):
    """Base64 encode/decode data.
    
    Encodes or decodes data using Base64.
    """
    action_type = "base64"
    display_name = "Base64编码"
    description = "Base64编码和解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Base64 encode/decode.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: data, operation (encode/decode).
        
        Returns:
            ActionResult with result.
        """
        data = params.get('data', '')
        operation = params.get('operation', 'encode')
        
        if not data:
            return ActionResult(success=False, message="data required")
        
        try:
            if operation == 'encode':
                if isinstance(data, str):
                    data = data.encode('utf-8')
                result = base64.b64encode(data).decode('utf-8')
            else:
                if isinstance(data, str):
                    data = data.encode('utf-8')
                result = base64.b64decode(data).decode('utf-8')
            
            return ActionResult(
                success=True,
                message=f"Base64 {operation}d",
                data={'result': result, 'operation': operation}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64 error: {e}",
                data={'error': str(e)}
            )


class RandomStringAction(BaseAction):
    """Generate random strings.
    
    Creates random strings for passwords, tokens, etc.
    """
    action_type = "random_string"
    display_name = "随机字符串"
    description = "生成随机字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random string.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: length, charset, uppercase, lowercase,
                   digits, special, exclude_ambiguous.
        
        Returns:
            ActionResult with random string.
        """
        length = params.get('length', 16)
        charset = params.get('charset', None)
        uppercase = params.get('uppercase', True)
        lowercase = params.get('lowercase', True)
        digits = params.get('digits', True)
        special = params.get('special', False)
        exclude_ambiguous = params.get('exclude_ambiguous', False)
        
        if length < 1 or length > 1024:
            return ActionResult(success=False, message="length must be 1-1024")
        
        if charset:
            chars = charset
        else:
            chars = ''
            if uppercase:
                chars += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
            if lowercase:
                chars += 'abcdefghijklmnopqrstuvwxyz'
            if digits:
                chars += '0123456789'
            if special:
                chars += '!@#$%^&*()_+-=[]{}|;:,.<>?'
        
        if exclude_ambiguous:
            chars = chars.replace('0', '').replace('O', '').replace('l', '').replace('1', '').replace('I', '')
        
        if not chars:
            return ActionResult(success=False, message="No characters available")
        
        result = ''.join(secrets.choice(chars) for _ in range(length))
        
        return ActionResult(
            success=True,
            message=f"Generated {length} char random string",
            data={'result': result, 'length': length}
        )
