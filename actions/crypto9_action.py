"""Crypto9 action module for RabAI AutoClick.

Provides additional cryptographic operations:
- CryptoEncryptAction: Encrypt data
- CryptoDecryptAction: Decrypt data
- CryptoGenerateKeyAction: Generate encryption key
- CryptoHashPasswordAction: Hash password
- CryptoVerifyPasswordAction: Verify password
- CryptoGenerateSaltAction: Generate salt
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoEncryptAction(BaseAction):
    """Encrypt data."""
    action_type = "crypto9_encrypt"
    display_name = "加密数据"
    description = "加密数据"
    version = "9.0"

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

        try:
            from cryptography.fernet import Fernet

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            if len(resolved_key) < 32:
                resolved_key = resolved_key.ljust(32, '0').encode('utf-8')
            else:
                resolved_key = resolved_key[:32].encode('utf-8')

            f = Fernet(Fernet.generate_key())
            # Simple XOR encryption for demonstration
            encrypted = bytes(a ^ b for a, b in zip(resolved_data, resolved_key * (len(resolved_data) // len(resolved_key) + 1)))[:len(resolved_data)]

            context.set(output_var, encrypted.hex())

            return ActionResult(
                success=True,
                message=f"加密数据: {len(encrypted)}字节",
                data={
                    'original_size': len(resolved_data),
                    'encrypted_size': len(encrypted),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"加密数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_data'}


class CryptoDecryptAction(BaseAction):
    """Decrypt data."""
    action_type = "crypto9_decrypt"
    display_name = "解密数据"
    description = "解密数据"
    version = "9.0"

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

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if isinstance(resolved_data, str):
                resolved_data = bytes.fromhex(resolved_data)

            if isinstance(resolved_key, str):
                resolved_key = resolved_key.encode('utf-8')

            if len(resolved_key) < 32:
                resolved_key = resolved_key.ljust(32, b'0')
            else:
                resolved_key = resolved_key[:32]

            decrypted = bytes(a ^ b for a, b in zip(resolved_data, resolved_key * (len(resolved_data) // len(resolved_key) + 1)))[:len(resolved_data)]

            context.set(output_var, decrypted.decode('utf-8'))

            return ActionResult(
                success=True,
                message=f"解密数据: {len(decrypted)}字节",
                data={
                    'encrypted_size': len(resolved_data),
                    'decrypted_size': len(decrypted),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解密数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_data'}


class CryptoGenerateKeyAction(BaseAction):
    """Generate encryption key."""
    action_type = "crypto9_generate_key"
    display_name = "生成密钥"
    description = "生成加密密钥"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate key.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with generated key.
        """
        length = params.get('length', 32)
        output_var = params.get('output_var', 'generated_key')

        try:
            import secrets

            resolved_length = int(context.resolve_value(length)) if length else 32

            result = secrets.token_hex(resolved_length)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成密钥: {len(result)}字符",
                data={
                    'key': result,
                    'length': resolved_length,
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
        return {'length': 32, 'output_var': 'generated_key'}


class CryptoHashPasswordAction(BaseAction):
    """Hash password."""
    action_type = "crypto9_hash_password"
    display_name = "哈希密码"
    description = "哈希密码"
    version = "9.0"

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

        try:
            import hashlib

            resolved_password = context.resolve_value(password)
            resolved_salt = context.resolve_value(salt) if salt else secrets.token_hex(16)

            if isinstance(resolved_password, str):
                resolved_password = resolved_password.encode('utf-8')
            if isinstance(resolved_salt, str):
                resolved_salt = resolved_salt.encode('utf-8')

            result = hashlib.pbkdf2_hmac('sha256', resolved_password, resolved_salt, 100000).hex()

            context.set(output_var, {
                'hash': result,
                'salt': resolved_salt.decode('utf-8') if isinstance(resolved_salt, bytes) else resolved_salt
            })

            return ActionResult(
                success=True,
                message=f"哈希密码: 成功",
                data={
                    'hash': result,
                    'salt': resolved_salt.decode('utf-8') if isinstance(resolved_salt, bytes) else resolved_salt,
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
    """Verify password."""
    action_type = "crypto9_verify_password"
    display_name = "验证密码"
    description = "验证密码"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute verify password.

        Args:
            context: Execution context.
            params: Dict with password, hash, salt, output_var.

        Returns:
            ActionResult with verification result.
        """
        password = params.get('password', '')
        hash_val = params.get('hash', '')
        salt = params.get('salt', '')
        output_var = params.get('output_var', 'verify_result')

        try:
            import hashlib
            import secrets

            resolved_password = context.resolve_value(password)
            resolved_hash = context.resolve_value(hash_val)
            resolved_salt = context.resolve_value(salt) if salt else secrets.token_hex(16)

            if isinstance(resolved_password, str):
                resolved_password = resolved_password.encode('utf-8')
            if isinstance(resolved_salt, str):
                resolved_salt = resolved_salt.encode('utf-8')

            computed_hash = hashlib.pbkdf2_hmac('sha256', resolved_password, resolved_salt, 100000).hex()

            result = secrets.compare_digest(computed_hash, resolved_hash)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"验证密码: {'正确' if result else '错误'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证密码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password', 'hash', 'salt']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'verify_result'}


class CryptoGenerateSaltAction(BaseAction):
    """Generate salt."""
    action_type = "crypto9_generate_salt"
    display_name = "生成盐值"
    description = "生成盐值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate salt.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with generated salt.
        """
        length = params.get('length', 16)
        output_var = params.get('output_var', 'generated_salt')

        try:
            import secrets

            resolved_length = int(context.resolve_value(length)) if length else 16

            result = secrets.token_hex(resolved_length)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成盐值: {len(result)}字符",
                data={
                    'salt': result,
                    'length': resolved_length,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成盐值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 16, 'output_var': 'generated_salt'}