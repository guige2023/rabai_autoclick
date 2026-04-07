"""Crypto6 action module for RabAI AutoClick.

Provides additional crypto operations:
- CryptoAESEncryptAction: AES encryption
- CryptoAESDecryptAction: AES decryption
- CryptoRSAEncryptAction: RSA encryption
- CryptoRSADecryptAction: RSA decryption
- CryptoHashFileAction: Hash file content
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoAESEncryptAction(BaseAction):
    """AES encryption."""
    action_type = "crypto6_aes_encrypt"
    display_name = "AES加密"
    description = "AES加密"
    version = "6.0"

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
        output_var = params.get('output_var', 'encrypted_data')

        try:
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import pad
            import base64

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if len(resolved_key) < 16:
                resolved_key = resolved_key.zfill(16)
            elif len(resolved_key) > 16:
                resolved_key = resolved_key[:16]

            cipher = AES.new(resolved_key.encode(), AES.MODE_ECB)
            encrypted = cipher.encrypt(pad(resolved_data.encode(), AES.block_size))

            result = base64.b64encode(encrypted).decode()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"AES加密成功",
                data={
                    'encrypted': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="AES加密失败: 未安装pycryptodome库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_data'}


class CryptoAESDecryptAction(BaseAction):
    """AES decryption."""
    action_type = "crypto6_aes_decrypt"
    display_name = "AES解密"
    description = "AES解密"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute AES decrypt.

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
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import unpad
            import base64

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if len(resolved_key) < 16:
                resolved_key = resolved_key.zfill(16)
            elif len(resolved_key) > 16:
                resolved_key = resolved_key[:16]

            encrypted_bytes = base64.b64decode(resolved_data)
            cipher = AES.new(resolved_key.encode(), AES.MODE_ECB)
            decrypted = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)

            result = decrypted.decode()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"AES解密成功",
                data={
                    'decrypted': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="AES解密失败: 未安装pycryptodome库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_data'}


class CryptoRSAEncryptAction(BaseAction):
    """RSA encryption."""
    action_type = "crypto6_rsa_encrypt"
    display_name = "RSA加密"
    description = "RSA加密"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RSA encrypt.

        Args:
            context: Execution context.
            params: Dict with data, public_key, output_var.

        Returns:
            ActionResult with encrypted data.
        """
        data = params.get('data', '')
        public_key = params.get('public_key', '')
        output_var = params.get('output_var', 'encrypted_data')

        try:
            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_v1_5
            import base64

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(public_key)

            key = RSA.import_key(resolved_key)
            cipher = PKCS1_v1_5.new(key)
            encrypted = cipher.encrypt(resolved_data.encode())

            result = base64.b64encode(encrypted).decode()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RSA加密成功",
                data={
                    'encrypted': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="RSA加密失败: 未安装pycryptodome库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RSA加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'public_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_data'}


class CryptoRSADecryptAction(BaseAction):
    """RSA decryption."""
    action_type = "crypto6_rsa_decrypt"
    display_name = "RSA解密"
    description = "RSA解密"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RSA decrypt.

        Args:
            context: Execution context.
            params: Dict with data, private_key, output_var.

        Returns:
            ActionResult with decrypted data.
        """
        data = params.get('data', '')
        private_key = params.get('private_key', '')
        output_var = params.get('output_var', 'decrypted_data')

        try:
            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_v1_5
            import base64

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(private_key)

            key = RSA.import_key(resolved_key)
            cipher = PKCS1_v1_5.new(key)
            encrypted_bytes = base64.b64decode(resolved_data)
            decrypted = cipher.decrypt(encrypted_bytes, None)

            if decrypted is None:
                return ActionResult(
                    success=False,
                    message="RSA解密失败: 解密失败"
                )

            result = decrypted.decode()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RSA解密成功",
                data={
                    'decrypted': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="RSA解密失败: 未安装pycryptodome库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RSA解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'private_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_data'}


class CryptoHashFileAction(BaseAction):
    """Hash file content."""
    action_type = "crypto6_hash_file"
    display_name = "文件哈希"
    description = "计算文件哈希值"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hash file.

        Args:
            context: Execution context.
            params: Dict with file_path, algorithm, output_var.

        Returns:
            ActionResult with file hash.
        """
        file_path = params.get('file_path', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'file_hash')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import hashlib

            resolved_path = context.resolve_value(file_path)
            resolved_algo = context.resolve_value(algorithm) if algorithm else 'sha256'

            hash_obj = hashlib.new(resolved_algo)

            with open(resolved_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_obj.update(chunk)

            result = hash_obj.hexdigest()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件哈希: {result[:16]}...",
                data={
                    'file_path': resolved_path,
                    'algorithm': resolved_algo,
                    'hash': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件哈希失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'file_hash'}