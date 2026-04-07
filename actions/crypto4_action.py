"""Crypto4 action module for RabAI AutoClick.

Provides additional crypto operations:
- CryptoBcryptVerifyAction: Verify bcrypt password
- CryptoHmacAction: Calculate HMAC
- CryptoUuid4Action: Generate UUID4
- CryptoSecureRandomAction: Generate secure random bytes
- CryptoAesEncryptAction: AES encrypt
"""

import hashlib
import secrets
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoBcryptVerifyAction(BaseAction):
    """Verify bcrypt password."""
    action_type = "crypto4_bcrypt_verify"
    display_name = "Bcrypt验证"
    description = "验证bcrypt哈希密码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bcrypt verify.

        Args:
            context: Execution context.
            params: Dict with password, hash, output_var.

        Returns:
            ActionResult with verification result.
        """
        password = params.get('password', '')
        hash_str = params.get('hash', '')
        output_var = params.get('output_var', 'bcrypt_verify_result')

        try:
            import bcrypt
            resolved_password = str(context.resolve_value(password))
            resolved_hash = str(context.resolve_value(hash_str))

            result = bcrypt.checkpw(resolved_password.encode('utf-8'), resolved_hash.encode('utf-8'))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Bcrypt验证: {'成功' if result else '失败'}",
                data={
                    'result': result,
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
                message=f"Bcrypt验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password', 'hash']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bcrypt_verify_result'}


class CryptoHmacAction(BaseAction):
    """Calculate HMAC."""
    action_type = "crypto4_hmac"
    display_name = "HMAC计算"
    description = "计算HMAC哈希"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC.

        Args:
            context: Execution context.
            params: Dict with message, key, algorithm, output_var.

        Returns:
            ActionResult with HMAC.
        """
        message = params.get('message', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'hmac_result')

        try:
            resolved_message = str(context.resolve_value(message))
            resolved_key = str(context.resolve_value(key))
            resolved_algo = str(context.resolve_value(algorithm))

            if resolved_algo == 'sha256':
                h = hashlib.sha256
            elif resolved_algo == 'sha512':
                h = hashlib.sha512
            elif resolved_algo == 'md5':
                h = hashlib.md5
            elif resolved_algo == 'sha1':
                h = hashlib.sha1
            else:
                return ActionResult(success=False, message=f"不支持的算法: {resolved_algo}")

            result = hmac.new(resolved_key.encode('utf-8'), resolved_message.encode('utf-8'), h).hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC计算完成",
                data={
                    'message': resolved_message,
                    'key': resolved_key,
                    'algorithm': resolved_algo,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HMAC计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'hmac_result'}


class CryptoUuid4Action(BaseAction):
    """Generate UUID4."""
    action_type = "crypto4_uuid4"
    display_name = "生成UUID4"
    description = "生成随机UUID"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID4.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UUID4.
        """
        output_var = params.get('output_var', 'uuid4_result')

        try:
            import uuid
            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID4: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成UUID4失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid4_result'}


class CryptoSecureRandomAction(BaseAction):
    """Generate secure random bytes."""
    action_type = "crypto4_secure_random"
    display_name = "安全随机字节"
    description = "生成加密安全的随机字节"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute secure random.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with random bytes.
        """
        length = params.get('length', 32)
        output_var = params.get('output_var', 'secure_random_result')

        try:
            resolved_length = int(context.resolve_value(length))
            result = secrets.token_bytes(resolved_length)
            context.set(output_var, result.hex())

            return ActionResult(
                success=True,
                message=f"安全随机字节: {len(result)} bytes",
                data={
                    'length': resolved_length,
                    'result': result.hex(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成安全随机字节失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 32, 'output_var': 'secure_random_result'}


class CryptoAesEncryptAction(BaseAction):
    """AES encrypt."""
    action_type = "crypto4_aes_encrypt"
    display_name = "AES加密"
    description = "使用AES加密数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute AES encrypt.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with encrypted data.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'aes_encrypt_result')

        try:
            from cryptography.fernet import Fernet
            resolved_data = str(context.resolve_value(data))
            resolved_key = str(context.resolve_value(key))

            if len(resolved_key) < 32:
                resolved_key = resolved_key.ljust(32, '0')[:32]

            f = Fernet(resolved_key.encode('utf-8'))
            encrypted = f.encrypt(resolved_data.encode('utf-8'))
            result = encrypted.decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"AES加密完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="cryptography库未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'aes_encrypt_result'}
