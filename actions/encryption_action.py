"""Encryption action module for RabAI AutoClick.

Provides encryption operations:
- EncryptAESAction: AES encryption
- DecryptAESAction: AES decryption
- HashAction: Hash data
- HMACAction: Generate HMAC
- GenerateKeyAction: Generate encryption key
- EncryptPasswordAction: Encrypt password
- EncodeBase64Action: Base64 encode
- DecodeBase64Action: Base64 decode
"""

import base64
import hashlib
import hmac
import os
import random
import string
import sys
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncryptAESAction(BaseAction):
    """AES encryption."""
    action_type = "encrypt_aes"
    display_name = "AES加密"
    description = "AES加密数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            key = params.get("key", "")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            try:
                from Crypto.Cipher import AES
                from Crypto.Util.Padding import pad
            except ImportError:
                return ActionResult(success=False, message="PyCryptodome not installed")
            
            if not key:
                key = os.urandom(16)
            elif len(key) < 16:
                key = key.zfill(16)
            elif len(key) > 16:
                key = key[:16]
            
            iv = os.urandom(16)
            cipher = AES.new(key.encode() if isinstance(key, str) else key, AES.MODE_CBC, iv)
            padded_data = pad(data.encode() if isinstance(data, str) else data, AES.block_size)
            encrypted = iv + cipher.encrypt(padded_data)
            
            return ActionResult(
                success=True,
                message="AES encryption successful",
                data={"encrypted": base64.b64encode(encrypted).decode(), "key": base64.b64encode(key.encode() if isinstance(key, str) else key).decode() if len(key) <= 16 else None}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"AES encryption failed: {str(e)}")


class DecryptAESAction(BaseAction):
    """AES decryption."""
    action_type = "decrypt_aes"
    display_name = "AES解密"
    description = "AES解密数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            encrypted_data = params.get("data", "")
            key = params.get("key", "")
            
            if not encrypted_data or not key:
                return ActionResult(success=False, message="data and key required")
            
            try:
                from Crypto.Cipher import AES
                from Crypto.Util.Padding import unpad
            except ImportError:
                return ActionResult(success=False, message="PyCryptodome not installed")
            
            encrypted = base64.b64decode(encrypted_data)
            key_bytes = base64.b64decode(key) if len(key) > 20 else key.encode() if isinstance(key, str) else key
            if len(key_bytes) > 16:
                key_bytes = key_bytes[:16]
            elif len(key_bytes) < 16:
                key_bytes = key_bytes.zfill(16)
            
            iv = encrypted[:16]
            cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
            decrypted = unpad(cipher.decrypt(encrypted[16:]), AES.block_size)
            
            return ActionResult(
                success=True,
                message="AES decryption successful",
                data={"decrypted": decrypted.decode()}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"AES decryption failed: {str(e)}")


class HashAction(BaseAction):
    """Hash data."""
    action_type = "hash"
    display_name = "哈希"
    description = "计算数据哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "sha256")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            data_bytes = data.encode() if isinstance(data, str) else data
            
            if algorithm == "md5":
                result = hashlib.md5(data_bytes).hexdigest()
            elif algorithm == "sha1":
                result = hashlib.sha1(data_bytes).hexdigest()
            elif algorithm == "sha256":
                result = hashlib.sha256(data_bytes).hexdigest()
            elif algorithm == "sha512":
                result = hashlib.sha512(data_bytes).hexdigest()
            else:
                return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")
            
            return ActionResult(
                success=True,
                message=f"Hashed with {algorithm}",
                data={"hash": result, "algorithm": algorithm}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash failed: {str(e)}")


class HMACAction(BaseAction):
    """Generate HMAC."""
    action_type = "hmac_generate"
    display_name = "HMAC生成"
    description = "生成HMAC"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            key = params.get("key", "")
            algorithm = params.get("algorithm", "sha256")
            
            if not data or not key:
                return ActionResult(success=False, message="data and key required")
            
            key_bytes = key.encode() if isinstance(key, str) else key
            data_bytes = data.encode() if isinstance(data, str) else data
            
            if algorithm == "sha256":
                result = hmac.new(key_bytes, data_bytes, hashlib.sha256).hexdigest()
            elif algorithm == "sha512":
                result = hmac.new(key_bytes, data_bytes, hashlib.sha512).hexdigest()
            elif algorithm == "md5":
                result = hmac.new(key_bytes, data_bytes, hashlib.md5).hexdigest()
            else:
                result = hmac.new(key_bytes, data_bytes).hexdigest()
            
            return ActionResult(
                success=True,
                message=f"HMAC generated with {algorithm}",
                data={"hmac": result, "algorithm": algorithm}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC failed: {str(e)}")


class GenerateKeyAction(BaseAction):
    """Generate encryption key."""
    action_type = "generate_key"
    display_name = "生成密钥"
    description = "生成加密密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            length = params.get("length", 32)
            key_type = params.get("type", "random")
            
            if key_type == "random":
                key = os.urandom(length)
            elif key_type == "alphanumeric":
                chars = string.ascii_letters + string.digits
                key = "".join(random.choice(chars) for _ in range(length))
            elif key_type == "hex":
                key = os.urandom(length).hex()
            else:
                return ActionResult(success=False, message=f"Unknown key type: {key_type}")
            
            return ActionResult(
                success=True,
                message=f"Generated {key_type} key of length {length}",
                data={"key": base64.b64encode(key).decode() if isinstance(key, bytes) else key, "length": length}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Generate key failed: {str(e)}")


class EncryptPasswordAction(BaseAction):
    """Encrypt password."""
    action_type = "encrypt_password"
    display_name = "密码加密"
    description = "加密密码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            password = params.get("password", "")
            salt = params.get("salt", "")
            
            if not password:
                return ActionResult(success=False, message="password required")
            
            if not salt:
                salt = os.urandom(16).hex()
            
            password_hash = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
            
            return ActionResult(
                success=True,
                message="Password encrypted",
                data={"hash": password_hash, "salt": salt}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Password encryption failed: {str(e)}")


class EncodeBase64Action(BaseAction):
    """Base64 encode."""
    action_type = "encode_base64"
    display_name = "Base64编码"
    description = "Base64编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            data_bytes = data.encode() if isinstance(data, str) else data
            encoded = base64.b64encode(data_bytes).decode()
            
            return ActionResult(
                success=True,
                message="Base64 encoded",
                data={"encoded": encoded}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 encode failed: {str(e)}")


class DecodeBase64Action(BaseAction):
    """Base64 decode."""
    action_type = "decode_base64"
    display_name = "Base64解码"
    description = "Base64解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            
            if not data:
                return ActionResult(success=False, message="data required")
            
            try:
                decoded = base64.b64decode(data).decode()
            except Exception:
                return ActionResult(success=False, message="Invalid Base64 data")
            
            return ActionResult(
                success=True,
                message="Base64 decoded",
                data={"decoded": decoded}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 decode failed: {str(e)}")
