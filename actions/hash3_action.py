"""Hash3 action module for RabAI AutoClick.

Provides additional hash operations:
- HashMd5Action: MD5 hash
- HashSha1Action: SHA1 hash
- HashSha256Action: SHA256 hash
- HashSha512Action: SHA512 hash
- HashBlake2bAction: BLAKE2b hash
"""

import hashlib
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMd5Action(BaseAction):
    """MD5 hash."""
    action_type = "hash3_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5 hash.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with MD5 hash.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'md5_result')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.md5(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5哈希: {result[:16]}...",
                data={
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'md5_result'}


class HashSha1Action(BaseAction):
    """SHA1 hash."""
    action_type = "hash3_sha1"
    display_name = "SHA1哈希"
    description = "计算SHA1哈希值"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA1 hash.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with SHA1 hash.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'sha1_result')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha1(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1哈希: {result[:16]}...",
                data={
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'sha1_result'}


class HashSha256Action(BaseAction):
    """SHA256 hash."""
    action_type = "hash3_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA256哈希值"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA256 hash.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with SHA256 hash.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'sha256_result')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha256(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256哈希: {result[:16]}...",
                data={
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'sha256_result'}


class HashSha512Action(BaseAction):
    """SHA512 hash."""
    action_type = "hash3_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA512哈希值"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA512 hash.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with SHA512 hash.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'sha512_result')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha512(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA512哈希: {result[:16]}...",
                data={
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'sha512_result'}


class HashBlake2bAction(BaseAction):
    """BLAKE2b hash."""
    action_type = "hash3_blake2b"
    display_name = "BLAKE2b哈希"
    description = "计算BLAKE2b哈希值"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BLAKE2b hash.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with BLAKE2b hash.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'blake2b_result')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.blake2b(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"BLAKE2b哈希: {result[:16]}...",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BLAKE2b哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'blake2b_result'}