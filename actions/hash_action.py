"""Hashing and cryptographic utilities action module for RabAI AutoClick.

Provides hashing operations:
- HashMd5Action: Compute MD5 hash
- HashSha256Action: Compute SHA-256 hash
- HashSha512Action: Compute SHA-512 hash
- HashHmacAction: Compute HMAC
- HashPbkdf2Action: PBKDF2 key derivation
- HashbcryptAction: bcrypt hashing
- HashVerifyAction: Verify hash matches
- HashFileAction: Hash file contents
"""

from __future__ import annotations

import hashlib
import hmac
import os
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMd5Action(BaseAction):
    """Compute MD5 hash."""
    action_type = "hash_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute MD5 hash."""
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode(encoding)
            result = hashlib.md5(resolved_value).hexdigest()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"MD5: {result}", data={'algorithm': 'md5', 'hash': result})
        except Exception as e:
            return ActionResult(success=False, message=f"MD5 hash error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_result'}


class HashSha256Action(BaseAction):
    """Compute SHA-256 hash."""
    action_type = "hash_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA-256哈希值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SHA-256 hash."""
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode(encoding)
            result = hashlib.sha256(resolved_value).hexdigest()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"SHA256: {result}", data={'algorithm': 'sha256', 'hash': result})
        except Exception as e:
            return ActionResult(success=False, message=f"SHA256 hash error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_result'}


class HashSha512Action(BaseAction):
    """Compute SHA-512 hash."""
    action_type = "hash_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA-512哈希值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SHA-512 hash."""
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode(encoding)
            result = hashlib.sha512(resolved_value).hexdigest()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"SHA512: {result}", data={'algorithm': 'sha512', 'hash': result})
        except Exception as e:
            return ActionResult(success=False, message=f"SHA512 hash error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_result'}


class HashHmacAction(BaseAction):
    """Compute HMAC."""
    action_type = "hash_hmac"
    display_name = "HMAC哈希"
    description = "计算HMAC"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HMAC."""
        key = params.get('key', '')
        message = params.get('message', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hmac_result')

        if not key or not message:
            return ActionResult(success=False, message="key and message are required")

        try:
            resolved_key = context.resolve_value(key) if context else key
            resolved_message = context.resolve_value(message) if context else message

            if isinstance(resolved_key, str):
                resolved_key = resolved_key.encode(encoding)
            if isinstance(resolved_message, str):
                resolved_message = resolved_message.encode(encoding)

            alg_map = {'md5': hashlib.md5, 'sha1': hashlib.sha1, 'sha256': hashlib.sha256, 'sha512': hashlib.sha512}
            hash_func = alg_map.get(algorithm.lower(), hashlib.sha256)
            result = hmac.new(resolved_key, resolved_message, hash_func).hexdigest()

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HMAC-{algorithm.upper()}: {result}", data={'algorithm': algorithm, 'hash': result})
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'encoding': 'utf-8', 'output_var': 'hmac_result'}


class HashPbkdf2Action(BaseAction):
    """PBKDF2 key derivation."""
    action_type = "hash_pbkdf2"
    display_name = "PBKDF2推导"
    description = "PBKDF2密钥推导"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute PBKDF2."""
        password = params.get('password', '')
        salt = params.get('salt', '')
        iterations = params.get('iterations', 100000)
        key_length = params.get('key_length', 32)
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'pbkdf2_result')

        if not password or not salt:
            return ActionResult(success=False, message="password and salt are required")

        try:
            resolved_password = context.resolve_value(password) if context else password
            resolved_salt = context.resolve_value(salt) if context else salt
            resolved_iterations = context.resolve_value(iterations) if context else iterations
            resolved_key_length = context.resolve_value(key_length) if context else key_length

            if isinstance(resolved_password, str):
                resolved_password = resolved_password.encode(encoding)
            if isinstance(resolved_salt, str):
                resolved_salt = resolved_salt.encode(encoding)

            alg_map = {'sha1': hashlib.sha1, 'sha256': hashlib.sha256, 'sha512': hashlib.sha512}
            hash_func = alg_map.get(algorithm.lower(), hashlib.sha256)
            result = hashlib.pbkdf2_hmac(hash_func().name, resolved_password, resolved_salt, resolved_iterations, dklen=resolved_key_length)
            result_hex = result.hex()

            if context:
                context.set(output_var, result_hex)
            return ActionResult(success=True, message=f"PBKDF2: {result_hex[:32]}...", data={'derived_key': result_hex, 'iterations': resolved_iterations})
        except Exception as e:
            return ActionResult(success=False, message=f"PBKDF2 error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['password', 'salt']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'iterations': 100000, 'key_length': 32, 'algorithm': 'sha256', 'encoding': 'utf-8', 'output_var': 'pbkdf2_result'}


class HashBcryptAction(BaseAction):
    """bcrypt hashing."""
    action_type = "hash_bcrypt"
    display_name = "bcrypt哈希"
    description = "bcrypt密码哈希"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute bcrypt hash."""
        password = params.get('password', '')
        rounds = params.get('rounds', 12)
        output_var = params.get('output_var', 'bcrypt_result')

        if not password:
            return ActionResult(success=False, message="password is required")

        try:
            import bcrypt
            resolved_password = context.resolve_value(password) if context else password
            if isinstance(resolved_password, str):
                resolved_password = resolved_password.encode('utf-8')
            salt = bcrypt.gensalt(rounds=rounds)
            hashed = bcrypt.hashpw(resolved_password, salt).decode('utf-8')

            if context:
                context.set(output_var, hashed)
            return ActionResult(success=True, message=f"bcrypt: {hashed[:60]}...", data={'hash': hashed})
        except ImportError:
            return ActionResult(success=False, message="bcrypt module not installed. Install with: pip install bcrypt")
        except Exception as e:
            return ActionResult(success=False, message=f"bcrypt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['password']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'rounds': 12, 'output_var': 'bcrypt_result'}


class HashVerifyAction(BaseAction):
    """Verify hash matches."""
    action_type = "hash_verify"
    display_name = "验证哈希"
    description = "验证哈希值是否匹配"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute hash verify."""
        value = params.get('value', '')
        expected_hash = params.get('expected_hash', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'verify_result')

        if not value or not expected_hash:
            return ActionResult(success=False, message="value and expected_hash are required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            resolved_hash = context.resolve_value(expected_hash) if context else expected_hash

            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode(encoding)

            alg_map = {
                'md5': hashlib.md5,
                'sha1': hashlib.sha1,
                'sha256': hashlib.sha256,
                'sha512': hashlib.sha512,
            }
            hash_func = alg_map.get(algorithm.lower(), hashlib.sha256)
            computed = hash_func(resolved_value).hexdigest()
            matches = computed.lower() == resolved_hash.lower()

            result = {'matches': matches, 'computed': computed, 'expected': resolved_hash}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Hash match: {matches}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Verify error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'expected_hash']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'encoding': 'utf-8', 'output_var': 'verify_result'}


class HashFileAction(BaseAction):
    """Hash file contents."""
    action_type = "hash_file"
    display_name = "文件哈希"
    description = "计算文件哈希值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file hash."""
        file_path = params.get('file_path', '')
        algorithm = params.get('algorithm', 'sha256')
        chunk_size = params.get('chunk_size', 8192)
        output_var = params.get('output_var', 'file_hash')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            alg_map = {
                'md5': hashlib.md5,
                'sha1': hashlib.sha1,
                'sha256': hashlib.sha256,
                'sha512': hashlib.sha512,
            }
            hash_func = alg_map.get(algorithm.lower(), hashlib.sha256)

            h = hash_func()
            file_size = 0
            with open(resolved_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    h.update(chunk)
                    file_size += len(chunk)

            result = {'hash': h.hexdigest(), 'algorithm': algorithm, 'file_size': file_size, 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"File hash ({algorithm}): {result['hash']}", data=result)
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"File hash error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'chunk_size': 8192, 'output_var': 'file_hash'}
