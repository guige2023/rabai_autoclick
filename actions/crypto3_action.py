"""Crypto3 action module for RabAI AutoClick.

Provides additional crypto operations:
- CryptoMd5Action: Calculate MD5 hash
- CryptoSha1Action: Calculate SHA1 hash
- CryptoSha256Action: Calculate SHA256 hash
- CryptoSha512Action: Calculate SHA512 hash
- CryptoBcryptHashAction: Hash password with bcrypt
"""

import hashlib
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoMd5Action(BaseAction):
    """Calculate MD5 hash."""
    action_type = "crypto3_md5"
    display_name = "MD5哈希"
    description = "计算字符串的MD5哈希值"

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
        output_var = params.get('output_var', 'md5_hash')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.md5(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5哈希: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算MD5哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'md5_hash'}


class CryptoSha1Action(BaseAction):
    """Calculate SHA1 hash."""
    action_type = "crypto3_sha1"
    display_name = "SHA1哈希"
    description = "计算字符串的SHA1哈希值"

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
        output_var = params.get('output_var', 'sha1_hash')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha1(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1哈希: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算SHA1哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha1_hash'}


class CryptoSha256Action(BaseAction):
    """Calculate SHA256 hash."""
    action_type = "crypto3_sha256"
    display_name = "SHA256哈希"
    description = "计算字符串的SHA256哈希值"

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
        output_var = params.get('output_var', 'sha256_hash')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha256(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256哈希: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算SHA256哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha256_hash'}


class CryptoSha512Action(BaseAction):
    """Calculate SHA512 hash."""
    action_type = "crypto3_sha512"
    display_name = "SHA512哈希"
    description = "计算字符串的SHA512哈希值"

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
        output_var = params.get('output_var', 'sha512_hash')

        try:
            resolved = str(context.resolve_value(value))
            result = hashlib.sha512(resolved.encode('utf-8')).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA512哈希: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算SHA512哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sha512_hash'}


class CryptoBcryptHashAction(BaseAction):
    """Hash password with bcrypt."""
    action_type = "crypto3_bcrypt_hash"
    display_name = "Bcrypt哈希"
    description = "使用bcrypt哈希密码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bcrypt hash.

        Args:
            context: Execution context.
            params: Dict with password, rounds, output_var.

        Returns:
            ActionResult with bcrypt hash.
        """
        password = params.get('password', '')
        rounds = params.get('rounds', 10)
        output_var = params.get('output_var', 'bcrypt_hash')

        try:
            import bcrypt
            resolved = str(context.resolve_value(password))
            resolved_rounds = int(context.resolve_value(rounds))

            salt = bcrypt.gensalt(rounds=resolved_rounds)
            hashed = bcrypt.hashpw(resolved.encode('utf-8'), salt).decode('utf-8')
            context.set(output_var, hashed)

            return ActionResult(
                success=True,
                message=f"Bcrypt哈希完成",
                data={
                    'password': resolved,
                    'rounds': resolved_rounds,
                    'result': hashed,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="bcrypt库未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Bcrypt哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'rounds': 10, 'output_var': 'bcrypt_hash'}
