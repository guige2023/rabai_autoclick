"""Crypto2 action module for RabAI AutoClick.

Provides additional crypto operations:
- CryptoEncryptAction: Encrypt data
- CryptoDecryptAction: Decrypt data
- CryptoGenerateKeyAction: Generate encryption key
- CryptoHashPasswordAction: Hash password
- CryptoVerifyPasswordAction: Verify password hash
"""

import hashlib
import secrets
from cryptography.fernet import Fernet
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoEncryptAction(BaseAction):
    """Encrypt data."""
    action_type = "crypto2_encrypt"
    display_name = "加密数据"
    description = "使用密钥加密数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute encrypt.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with encrypted data.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'encrypted_data')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            f = Fernet(resolved_key.encode())
            encrypted = f.encrypt(resolved_data.encode())

            context.set(output_var, encrypted.decode())

            return ActionResult(
                success=True,
                message=f"数据加密成功",
                data={
                    'encrypted': encrypted.decode(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_data'}


class CryptoDecryptAction(BaseAction):
    """Decrypt data."""
    action_type = "crypto2_decrypt"
    display_name = "解密数据"
    description = "使用密钥解密数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decrypt.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with decrypted data.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'decrypted_data')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            f = Fernet(resolved_key.encode())
            decrypted = f.decrypt(resolved_data.encode())

            context.set(output_var, decrypted.decode())

            return ActionResult(
                success=True,
                message=f"数据解密成功",
                data={
                    'decrypted': decrypted.decode(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_data'}


class CryptoGenerateKeyAction(BaseAction):
    """Generate encryption key."""
    action_type = "crypto2_generate_key"
    display_name = "生成密钥"
    description = "生成加密密钥"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate key.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with generated key.
        """
        output_var = params.get('output_var', 'encryption_key')

        try:
            key = Fernet.generate_key()
            context.set(output_var, key.decode())

            return ActionResult(
                success=True,
                message=f"密钥生成成功",
                data={
                    'key': key.decode(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成密钥失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encryption_key'}


class CryptoHashPasswordAction(BaseAction):
    """Hash password."""
    action_type = "crypto2_hash_password"
    display_name = "哈希密码"
    description = "对密码进行哈希"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hash password.

        Args:
            context: Execution context.
            params: Dict with password, salt, output_var.

        Returns:
            ActionResult with hashed password.
        """
        password = params.get('password', '')
        salt = params.get('salt', None)
        output_var = params.get('output_var', 'hashed_password')

        valid, msg = self.validate_type(password, str, 'password')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_password = context.resolve_value(password)
            resolved_salt = context.resolve_value(salt) if salt else None

            if resolved_salt:
                hashed = hashlib.pbkdf2_hmac('sha256', resolved_password.encode(), resolved_salt.encode(), 100000)
            else:
                salt_bytes = secrets.token_bytes(16)
                hashed = hashlib.pbkdf2_hmac('sha256', resolved_password.encode(), salt_bytes, 100000)
                resolved_salt = salt_bytes.hex()

            result = resolved_salt + ':' + hashed.hex()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"密码哈希成功",
                data={
                    'hashed': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"哈希密码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'salt': None, 'output_var': 'hashed_password'}


class CryptoVerifyPasswordAction(BaseAction):
    """Verify password hash."""
    action_type = "crypto2_verify_password"
    display_name = "验证密码"
    description = "验证密码哈希"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute verify password.

        Args:
            context: Execution context.
            params: Dict with password, hashed, output_var.

        Returns:
            ActionResult with verify result.
        """
        password = params.get('password', '')
        hashed = params.get('hashed', '')
        output_var = params.get('output_var', 'verify_result')

        valid, msg = self.validate_type(password, str, 'password')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(hashed, str, 'hashed')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_password = context.resolve_value(password)
            resolved_hashed = context.resolve_value(hashed)

            parts = resolved_hashed.split(':')
            if len(parts) != 2:
                return ActionResult(
                    success=False,
                    message="无效的哈希格式"
                )

            salt, stored_hash = parts
            computed_hash = hashlib.pbkdf2_hmac('sha256', resolved_password.encode(), salt.encode(), 100000).hex()

            result = secrets.compare_digest(computed_hash, stored_hash)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"密码验证: {'成功' if result else '失败'}",
                data={
                    'verified': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证密码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password', 'hashed']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'verify_result'}