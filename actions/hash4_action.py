"""Hash4 action module for RabAI AutoClick.

Provides additional hash operations:
- HashMD5Action: MD5 hash
- HashSHA1Action: SHA1 hash
- HashSHA256Action: SHA256 hash
- HashSHA512Action: SHA512 hash
- HashBlake2Action: BLAKE2 hash
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMD5Action(BaseAction):
    """MD5 hash."""
    action_type = "hash4_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5 hash.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with MD5 hash.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'md5_hash')

        try:
            import hashlib

            resolved = context.resolve_value(text)
            result = hashlib.md5(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5哈希: {result}",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'md5_hash'}


class HashSHA1Action(BaseAction):
    """SHA1 hash."""
    action_type = "hash4_sha1"
    display_name = "SHA1哈希"
    description = "计算SHA1哈希值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA1 hash.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with SHA1 hash.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'sha1_hash')

        try:
            import hashlib

            resolved = context.resolve_value(text)
            result = hashlib.sha1(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1哈希: {result}",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha1_hash'}


class HashSHA256Action(BaseAction):
    """SHA256 hash."""
    action_type = "hash4_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA256哈希值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA256 hash.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with SHA256 hash.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'sha256_hash')

        try:
            import hashlib

            resolved = context.resolve_value(text)
            result = hashlib.sha256(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256哈希: {result[:32]}...",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha256_hash'}


class HashSHA512Action(BaseAction):
    """SHA512 hash."""
    action_type = "hash4_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA512哈希值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA512 hash.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with SHA512 hash.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'sha512_hash')

        try:
            import hashlib

            resolved = context.resolve_value(text)
            result = hashlib.sha512(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA512哈希: {result[:32]}...",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha512_hash'}


class HashBlake2Action(BaseAction):
    """BLAKE2 hash."""
    action_type = "hash4_blake2"
    display_name = "BLAKE2哈希"
    description = "计算BLAKE2哈希值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BLAKE2 hash.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with BLAKE2 hash.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'blake2_hash')

        try:
            import hashlib

            resolved = context.resolve_value(text)
            result = hashlib.blake2b(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"BLAKE2哈希: {result[:32]}...",
                data={
                    'original': resolved,
                    'hash': result,
                    'algorithm': 'blake2b',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BLAKE2哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'blake2_hash'}