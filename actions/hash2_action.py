"""Hash2 action module for RabAI AutoClick.

Provides additional hash operations:
- HashMd5Action: MD5 hash
- HashSha1Action: SHA-1 hash
- HashSha256Action: SHA-256 hash
- HashSha512Action: SHA-512 hash
- HashHmacAction: HMAC hash
"""

import hashlib
import hmac
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMd5Action(BaseAction):
    """MD5 hash."""
    action_type = "hash_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5 hash.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_value')

        try:
            resolved = context.resolve_value(value)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if isinstance(resolved, bytes):
                result = hashlib.md5(resolved).hexdigest()
            else:
                result = hashlib.md5(resolved.encode(resolved_encoding)).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': 'md5',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MD5哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_value'}


class HashSha1Action(BaseAction):
    """SHA-1 hash."""
    action_type = "hash_sha1"
    display_name = "SHA1哈希"
    description = "计算SHA-1哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA-1 hash.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_value')

        try:
            resolved = context.resolve_value(value)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if isinstance(resolved, bytes):
                result = hashlib.sha1(resolved).hexdigest()
            else:
                result = hashlib.sha1(resolved.encode(resolved_encoding)).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': 'sha1',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA1哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_value'}


class HashSha256Action(BaseAction):
    """SHA-256 hash."""
    action_type = "hash_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA-256哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA-256 hash.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_value')

        try:
            resolved = context.resolve_value(value)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if isinstance(resolved, bytes):
                result = hashlib.sha256(resolved).hexdigest()
            else:
                result = hashlib.sha256(resolved.encode(resolved_encoding)).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': 'sha256',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA256哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_value'}


class HashSha512Action(BaseAction):
    """SHA-512 hash."""
    action_type = "hash_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA-512哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA-512 hash.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with hash.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_value')

        try:
            resolved = context.resolve_value(value)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if isinstance(resolved, bytes):
                result = hashlib.sha512(resolved).hexdigest()
            else:
                result = hashlib.sha512(resolved.encode(resolved_encoding)).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA512哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': 'sha512',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA512哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'hash_value'}


class HashHmacAction(BaseAction):
    """HMAC hash."""
    action_type = "hash_hmac"
    display_name = "HMAC哈希"
    description = "计算HMAC哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC hash.

        Args:
            context: Execution context.
            params: Dict with value, key, algorithm, encoding, output_var.

        Returns:
            ActionResult with HMAC.
        """
        value = params.get('value', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hmac_value')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(algorithm, str, 'algorithm')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_key = context.resolve_value(key)
            resolved_algorithm = context.resolve_value(algorithm)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if isinstance(resolved_value, bytes):
                data = resolved_value
            else:
                data = resolved_value.encode(resolved_encoding)

            if isinstance(resolved_key, bytes):
                key_bytes = resolved_key
            else:
                key_bytes = resolved_key.encode(resolved_encoding)

            hash_func = getattr(hashlib, resolved_algorithm)
            result = hmac.new(key_bytes, data, hash_func).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': resolved_algorithm,
                    'output_var': output_var
                }
            )
        except AttributeError:
            return ActionResult(
                success=False,
                message=f"不支持的算法: {algorithm}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HMAC哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'encoding': 'utf-8', 'output_var': 'hmac_value'}