"""Crypto5 action module for RabAI AutoClick.

Provides additional cryptographic operations:
- CryptoAesEncryptAction: AES encryption
- CryptoAesDecryptAction: AES decryption
- CryptoRsaGenerateAction: Generate RSA key pair
- CryptoRsaEncryptAction: RSA encryption
- CryptoRsaDecryptAction: RSA decryption
"""

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoAesEncryptAction(BaseAction):
    """AES encryption."""
    action_type = "crypto5_aes_encrypt"
    display_name = "AES加密"
    description = "使用AES算法加密数据"
    version = "5.0"

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
        output_var = params.get('output_var', 'encrypted_result')

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            if len(resolved_key) < 32:
                resolved_key = resolved_key.ljust(32, '0').encode('utf-8')
            else:
                resolved_key = resolved_key[:32].encode('utf-8')

            f = Fernet(resolved_key)
            encrypted = f.encrypt(resolved_data)
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
                message="AES加密失败: 未安装cryptography库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_result'}


class CryptoAesDecryptAction(BaseAction):
    """AES decryption."""
    action_type = "crypto5_aes_decrypt"
    display_name = "AES解密"
    description = "使用AES算法解密数据"
    version = "5.0"

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
        output_var = params.get('output_var', 'decrypted_result')

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            if len(resolved_key) < 32:
                resolved_key = resolved_key.ljust(32, '0').encode('utf-8')
            else:
                resolved_key = resolved_key[:32].encode('utf-8')

            f = Fernet(resolved_key)
            decrypted = f.decrypt(resolved_data.encode('utf-8'))
            result = decrypted.decode('utf-8')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"AES解密完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="AES解密失败: 未安装cryptography库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_result'}


class CryptoRsaGenerateAction(BaseAction):
    """Generate RSA key pair."""
    action_type = "crypto5_rsa_generate"
    display_name = "RSA密钥生成"
    description = "生成RSA密钥对"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RSA generate.

        Args:
            context: Execution context.
            params: Dict with key_size, output_var.

        Returns:
            ActionResult with key pair.
        """
        key_size = params.get('key_size', 2048)
        output_var = params.get('output_var', 'rsa_keys')

        try:
            resolved_size = int(context.resolve_value(key_size)) if key_size else 2048

            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=resolved_size,
                backend=default_backend()
            )
            public_key = private_key.public_key()

            private_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            result = {
                'private_key': private_pem.decode('utf-8'),
                'public_key': public_pem.decode('utf-8')
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RSA密钥生成完成: {resolved_size} 位",
                data={
                    'key_size': resolved_size,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="RSA密钥生成失败: 未安装cryptography库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RSA密钥生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_size': 2048, 'output_var': 'rsa_keys'}


class CryptoRsaEncryptAction(BaseAction):
    """RSA encryption."""
    action_type = "crypto5_rsa_encrypt"
    display_name = "RSA加密"
    description = "使用RSA算法加密数据"
    version = "5.0"

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
        output_var = params.get('output_var', 'encrypted_result')

        try:
            from cryptography.hazmat.primitives import serialization

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(public_key)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            pub_key = serialization.load_pem_public_key(resolved_key.encode('utf-8'), backend=default_backend())
            encrypted = pub_key.encrypt(
                resolved_data,
                padding.PKCS1v15()
            )
            result = encrypted.hex()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RSA加密完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="RSA加密失败: 未安装cryptography库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RSA加密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'public_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encrypted_result'}


class CryptoRsaDecryptAction(BaseAction):
    """RSA decryption."""
    action_type = "crypto5_rsa_decrypt"
    display_name = "RSA解密"
    description = "使用RSA算法解密数据"
    version = "5.0"

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
        output_var = params.get('output_var', 'decrypted_result')

        try:
            from cryptography.hazmat.primitives import serialization

            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(private_key)

            priv_key = serialization.load_pem_private_key(resolved_key.encode('utf-8'), password=None, backend=default_backend())
            decrypted = priv_key.decrypt(
                bytes.fromhex(resolved_data),
                padding.PKCS1v15()
            )
            result = decrypted.decode('utf-8')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RSA解密完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="RSA解密失败: 未安装cryptography库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RSA解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'private_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decrypted_result'}