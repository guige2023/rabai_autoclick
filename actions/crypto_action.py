"""Cryptographic operations action module for RabAI AutoClick.

Provides cryptographic operations:
- AesEncryptAction: AES encryption
- AesDecryptAction: AES decryption
- RsaGenerateKeyAction: Generate RSA key pair
- RsaEncryptAction: RSA encryption
- RsaDecryptAction: RSA decryption
- CryptoRandomAction: Generate random bytes
"""

from __future__ import annotations

import os
import sys
import base64
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AesEncryptAction(BaseAction):
    """AES encryption."""
    action_type = "aes_encrypt"
    display_name = "AES加密"
    description = "AES对称加密"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute AES encrypt."""
        data = params.get('data', '')
        key = params.get('key', '')
        mode = params.get('mode', 'CBC')  # CBC, GCM
        iv = params.get('iv', None)  # None = auto-generate
        output_var = params.get('output_var', 'aes_result')

        if not data or not key:
            return ActionResult(success=False, message="data and key are required")

        try:
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import pad

            resolved_data = context.resolve_value(data) if context else data
            resolved_key = context.resolve_value(key) if context else key
            resolved_iv = context.resolve_value(iv) if context else iv

            if isinstance(resolved_data, str):
                data_bytes = resolved_data.encode('utf-8')
            else:
                data_bytes = resolved_data

            key_bytes = resolved_key.encode('utf-8') if isinstance(resolved_key, str) else resolved_key
            key_bytes = key_bytes[:32].ljust(32, b'\0')  # Pad to 32 bytes

            if resolved_iv:
                iv_bytes = resolved_iv.encode('utf-8') if isinstance(resolved_iv, str) else resolved_iv
                iv_bytes = iv_bytes[:16].ljust(16, b'\0')
            else:
                iv_bytes = os.urandom(16)

            if mode.upper() == 'GCM':
                cipher = AES.new(key_bytes, AES.MODE_GCM, nonce=iv_bytes)
                ciphertext, tag = cipher.encrypt_and_digest(data_bytes)
                result_bytes = iv_bytes + ciphertext + tag
            else:
                cipher = AES.new(key_bytes, AES.MODE_CBC, iv=iv_bytes)
                padded = pad(data_bytes, AES.block_size)
                ciphertext = cipher.encrypt(padded)
                result_bytes = iv_bytes + ciphertext

            result_b64 = base64.b64encode(result_bytes).decode('ascii')
            result = {'ciphertext': result_b64, 'mode': mode, 'iv_provided': iv is not None}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"AES-{mode} encrypted", data=result)
        except ImportError:
            return ActionResult(success=False, message="pycryptodome not installed. Run: pip install pycryptodome")
        except Exception as e:
            return ActionResult(success=False, message=f"AES encrypt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'CBC', 'iv': None, 'output_var': 'aes_result'}


class AesDecryptAction(BaseAction):
    """AES decryption."""
    action_type = "aes_decrypt"
    display_name = "AES解密"
    description = "AES对称解密"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute AES decrypt."""
        ciphertext = params.get('ciphertext', '')
        key = params.get('key', '')
        mode = params.get('mode', 'CBC')
        output_var = params.get('output_var', 'aes_decrypted')

        if not ciphertext or not key:
            return ActionResult(success=False, message="ciphertext and key are required")

        try:
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import unpad

            resolved_ct = context.resolve_value(ciphertext) if context else ciphertext
            resolved_key = context.resolve_value(key) if context else key

            if isinstance(resolved_ct, str):
                ct_bytes = base64.b64decode(resolved_ct)
            else:
                ct_bytes = resolved_ct

            key_bytes = resolved_key.encode('utf-8') if isinstance(resolved_key, str) else resolved_key
            key_bytes = key_bytes[:32].ljust(32, b'\0')

            if mode.upper() == 'GCM':
                iv_bytes = ct_bytes[:16]
                tag = ct_bytes[-16:]
                ciphertext_only = ct_bytes[16:-16]
                cipher = AES.new(key_bytes, AES.MODE_GCM, nonce=iv_bytes)
                plaintext = cipher.decrypt_and_verify(ciphertext_only, tag)
            else:
                iv_bytes = ct_bytes[:16]
                ciphertext_only = ct_bytes[16:]
                cipher = AES.new(key_bytes, AES.MODE_CBC, iv=iv_bytes)
                plaintext = unpad(cipher.decrypt(ciphertext_only), AES.block_size)

            try:
                result = plaintext.decode('utf-8')
            except UnicodeDecodeError:
                result = plaintext.hex()

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="AES decrypted", data={'plaintext': result})
        except ImportError:
            return ActionResult(success=False, message="pycryptodome not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"AES decrypt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['ciphertext', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'CBC', 'output_var': 'aes_decrypted'}


class RsaGenerateKeyAction(BaseAction):
    """Generate RSA key pair."""
    action_type = "rsa_generate_key"
    display_name = "RSA密钥生成"
    description = "生成RSA密钥对"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute RSA key generation."""
        key_size = params.get('key_size', 2048)
        output_var = params.get('output_var', 'rsa_keys')

        try:
            from Crypto.PublicKey import RSA

            resolved_size = context.resolve_value(key_size) if context else key_size

            key = RSA.generate(resolved_size)
            private_pem = key.export_key().decode('utf-8')
            public_pem = key.publickey().export_key().decode('utf-8')

            result = {'private_key': private_pem, 'public_key': public_pem, 'key_size': resolved_size}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"RSA-{resolved_size} keys generated", data={'key_size': resolved_size})
        except ImportError:
            return ActionResult(success=False, message="pycryptodome not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"RSA key generation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_size': 2048, 'output_var': 'rsa_keys'}


class RsaEncryptAction(BaseAction):
    """RSA encryption."""
    action_type = "rsa_encrypt"
    display_name = "RSA加密"
    description = "RSA公钥加密"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute RSA encrypt."""
        data = params.get('data', '')
        public_key = params.get('public_key', '')
        output_var = params.get('output_var', 'rsa_encrypted')

        if not data or not public_key:
            return ActionResult(success=False, message="data and public_key are required")

        try:
            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_v1_5
            import binascii

            resolved_data = context.resolve_value(data) if context else data
            resolved_key = context.resolve_value(public_key) if context else public_key

            if isinstance(resolved_data, str):
                data_bytes = resolved_data.encode('utf-8')
            else:
                data_bytes = resolved_data

            key = RSA.import_key(resolved_key)
            cipher = PKCS1_v1_5.new(key)
            ciphertext = cipher.encrypt(data_bytes)
            result_b64 = base64.b64encode(ciphertext).decode('ascii')

            if context:
                context.set(output_var, result_b64)
            return ActionResult(success=True, message="RSA encrypted", data={'ciphertext': result_b64})
        except ImportError:
            return ActionResult(success=False, message="pycryptodome not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"RSA encrypt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'public_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rsa_encrypted'}


class RsaDecryptAction(BaseAction):
    """RSA decryption."""
    action_type = "rsa_decrypt"
    display_name = "RSA解密"
    description = "RSA私钥解密"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute RSA decrypt."""
        ciphertext = params.get('ciphertext', '')
        private_key = params.get('private_key', '')
        output_var = params.get('output_var', 'rsa_decrypted')

        if not ciphertext or not private_key:
            return ActionResult(success=False, message="ciphertext and private_key are required")

        try:
            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_v1_5

            resolved_ct = context.resolve_value(ciphertext) if context else ciphertext
            resolved_key = context.resolve_value(private_key) if context else private_key

            if isinstance(resolved_ct, str):
                ct_bytes = base64.b64decode(resolved_ct)
            else:
                ct_bytes = resolved_ct

            key = RSA.import_key(resolved_key)
            cipher = PKCS1_v1_5.new(key)
            sentinel = 'decryption error'
            plaintext = cipher.decrypt(ct_bytes, sentinel)

            if plaintext == sentinel:
                return ActionResult(success=False, message="RSA decryption failed")

            try:
                result = plaintext.decode('utf-8')
            except UnicodeDecodeError:
                result = plaintext.hex()

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="RSA decrypted", data={'plaintext': result})
        except ImportError:
            return ActionResult(success=False, message="pycryptodome not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"RSA decrypt error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['ciphertext', 'private_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rsa_decrypted'}


class CryptoRandomAction(BaseAction):
    """Generate random bytes."""
    action_type = "crypto_random"
    display_name = "随机字节生成"
    description = "生成随机字节"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random generation."""
        length = params.get('length', 32)
        format = params.get('format', 'hex')  # hex, base64, bytes
        output_var = params.get('output_var', 'random_bytes')

        try:
            resolved_length = context.resolve_value(length) if context else length
            resolved_format = context.resolve_value(format) if context else format

            data = os.urandom(int(resolved_length))

            if resolved_format == 'hex':
                result = data.hex()
            elif resolved_format == 'base64':
                result = base64.b64encode(data).decode('ascii')
            else:
                result = data

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Generated {resolved_length} random bytes", data={'data': result, 'length': resolved_length})
        except Exception as e:
            return ActionResult(success=False, message=f"Random generation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 32, 'format': 'hex', 'output_var': 'random_bytes'}
