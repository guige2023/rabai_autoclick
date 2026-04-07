"""Hash6 action module for RabAI AutoClick.

Provides additional hash operations:
- HashMD5Action: MD5 hash
- HashSHA256Action: SHA256 hash
- HashSHA512Action: SHA512 hash
- HashSHA1Action: SHA1 hash
- HashHMACAction: HMAC hash
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMD5Action(BaseAction):
    """MD5 hash."""
    action_type = "hash6_md5"
    display_name = "MD5哈希"
    description = "MD5哈希计算"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5 hash.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with hash result.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            import hashlib

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = hashlib.md5(resolved).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5哈希: {result[:16]}...",
                data={
                    'algorithm': 'MD5',
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MD5哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashSHA256Action(BaseAction):
    """SHA256 hash."""
    action_type = "hash6_sha256"
    display_name = "SHA256哈希"
    description = "SHA256哈希计算"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA256 hash.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with hash result.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            import hashlib

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = hashlib.sha256(resolved).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256哈希: {result[:16]}...",
                data={
                    'algorithm': 'SHA256',
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA256哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashSHA512Action(BaseAction):
    """SHA512 hash."""
    action_type = "hash6_sha512"
    display_name = "SHA512哈希"
    description = "SHA512哈希计算"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA512 hash.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with hash result.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            import hashlib

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = hashlib.sha512(resolved).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA512哈希: {result[:16]}...",
                data={
                    'algorithm': 'SHA512',
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA512哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashSHA1Action(BaseAction):
    """SHA1 hash."""
    action_type = "hash6_sha1"
    display_name = "SHA1哈希"
    description = "SHA1哈希计算"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA1 hash.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with hash result.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            import hashlib

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = hashlib.sha1(resolved).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1哈希: {result[:16]}...",
                data={
                    'algorithm': 'SHA1',
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA1哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashHMACAction(BaseAction):
    """HMAC hash."""
    action_type = "hash6_hmac"
    display_name = "HMAC哈希"
    description = "HMAC哈希计算"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC hash.

        Args:
            context: Execution context.
            params: Dict with data, key, algorithm, output_var.

        Returns:
            ActionResult with hash result.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'hash_result')

        try:
            import hmac
            import hashlib

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')
            if isinstance(resolved_key, str):
                resolved_key = resolved_key.encode('utf-8')

            hash_func = getattr(hashlib, algorithm, hashlib.sha256)
            result = hmac.new(resolved_key, resolved_data, hash_func).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC哈希: {result[:16]}...",
                data={
                    'algorithm': algorithm.upper(),
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HMAC哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'hash_result'}