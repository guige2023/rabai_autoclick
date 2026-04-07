"""Hashing and encryption action module for RabAI AutoClick.

Provides cryptographic operations:
- HashMd5Action: MD5 hash
- HashSha1Action: SHA-1 hash
- HashSha256Action: SHA-256 hash
- HashSha512Action: SHA-512 hash
- HashHmacAction: HMAC authentication
- HashBcryptAction: Bcrypt password hash
- HashAesEncryptAction: AES encryption
- HashAesDecryptAction: AES decryption
- HashBase64EncodeAction: Base64 encoding
- HashBase64DecodeAction: Base64 decoding
- HashHexEncodeAction: Hex encoding
- HashHexDecodeAction: Hex decoding
- HashUuidAction: Generate UUID
- HashCRC32Action: CRC32 checksum
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import hashlib
    import hmac
    import base64
    import uuid as uuid_module
    import zlib
    HASH_AVAILABLE = True
except ImportError:
    HASH_AVAILABLE = False


class HashMd5Action(BaseAction):
    """MD5 hash."""
    action_type = "hash_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute MD5 hash.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            hash_value = hashlib.md5(data_bytes).hexdigest()

            context.set(output_var, hash_value)

            return ActionResult(
                success=True,
                message=f"MD5哈希成功: {hash_value}",
                data={'hash': hash_value, 'algorithm': 'md5'}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MD5哈希失败: {str(e)}"
            )


class HashSha1Action(BaseAction):
    """SHA-1 hash."""
    action_type = "hash_sha1"
    display_name = "SHA1哈希"
    description = "计算SHA-1哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SHA-1 hash.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            hash_value = hashlib.sha1(data_bytes).hexdigest()

            context.set(output_var, hash_value)

            return ActionResult(
                success=True,
                message=f"SHA1哈希成功: {hash_value}",
                data={'hash': hash_value, 'algorithm': 'sha1'}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA1哈希失败: {str(e)}"
            )


class HashSha256Action(BaseAction):
    """SHA-256 hash."""
    action_type = "hash_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA-256哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SHA-256 hash.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            hash_value = hashlib.sha256(data_bytes).hexdigest()

            context.set(output_var, hash_value)

            return ActionResult(
                success=True,
                message=f"SHA256哈希成功: {hash_value}",
                data={'hash': hash_value, 'algorithm': 'sha256'}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA256哈希失败: {str(e)}"
            )


class HashSha512Action(BaseAction):
    """SHA-512 hash."""
    action_type = "hash_sha512"
    display_name = "SHA512哈希"
    description = "计算SHA-512哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SHA-512 hash.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with hash value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hash_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            hash_value = hashlib.sha512(data_bytes).hexdigest()

            context.set(output_var, hash_value)

            return ActionResult(
                success=True,
                message=f"SHA512哈希成功: {hash_value}",
                data={'hash': hash_value, 'algorithm': 'sha512'}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA512哈希失败: {str(e)}"
            )


class HashHmacAction(BaseAction):
    """HMAC authentication."""
    action_type = "hash_hmac"
    display_name = "HMAC哈希"
    description = "计算HMAC认证码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HMAC hash.

        Args:
            context: Execution context.
            params: Dict with data, key, algorithm, encoding, output_var.

        Returns:
            ActionResult with HMAC value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hmac_result')

        if not data or not key:
            return ActionResult(success=False, message="数据和密钥都不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            if isinstance(key, str):
                key_bytes = key.encode(encoding)
            else:
                key_bytes = key

            hash_func = getattr(hashlib, algorithm, hashlib.sha256)
            hmac_value = hmac.new(key_bytes, data_bytes, hash_func).hexdigest()

            context.set(output_var, hmac_value)

            return ActionResult(
                success=True,
                message=f"HMAC哈希成功: {hmac_value}",
                data={'hmac': hmac_value, 'algorithm': algorithm}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HMAC哈希失败: {str(e)}"
            )


class HashBase64EncodeAction(BaseAction):
    """Base64 encoding."""
    action_type = "hash_base64_encode"
    display_name = "Base64编码"
    description = "Base64编码数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Base64 encoding.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with encoded string.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="Base64库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            encoded = base64.b64encode(data_bytes).decode(encoding)

            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base64编码成功: {encoded}",
                data={'encoded': encoded}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )


class HashBase64DecodeAction(BaseAction):
    """Base64 decoding."""
    action_type = "hash_base64_decode"
    display_name = "Base64解码"
    description = "Base64解码数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Base64 decoding.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with decoded string.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="Base64库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            decoded_bytes = base64.b64decode(data)
            decoded = decoded_bytes.decode(encoding, errors='replace')

            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message="Base64解码成功",
                data={'decoded': decoded}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )


class HashHexEncodeAction(BaseAction):
    """Hex encoding."""
    action_type = "hash_hex_encode"
    display_name = "Hex编码"
    description = "十六进制编码数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Hex encoding.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with hex string.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        output_var = params.get('output_var', 'hex_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = data

            hex_value = data_bytes.hex()

            context.set(output_var, hex_value)

            return ActionResult(
                success=True,
                message=f"Hex编码成功: {hex_value}",
                data={'hex': hex_value}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex编码失败: {str(e)}"
            )


class HashHexDecodeAction(BaseAction):
    """Hex decoding."""
    action_type = "hash_hex_decode"
    display_name = "Hex解码"
    description = "十六进制解码数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Hex decoding.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with decoded string.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="哈希库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'hex_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            decoded_bytes = bytes.fromhex(data)
            decoded = decoded_bytes.decode(encoding, errors='replace')

            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message="Hex解码成功",
                data={'decoded': decoded}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex解码失败: {str(e)}"
            )


class HashUuidAction(BaseAction):
    """Generate UUID."""
    action_type = "hash_uuid"
    display_name = "生成UUID"
    description = "生成通用唯一标识符"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute UUID generation.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with UUID.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="UUID库不可用")

        version = params.get('version', 4)
        output_var = params.get('output_var', 'uuid_result')

        try:
            if version == 1:
                uuid_value = uuid_module.uuid1()
            elif version == 4:
                uuid_value = uuid_module.uuid4()
            else:
                uuid_value = uuid_module.uuid4()

            uuid_str = str(uuid_value)

            context.set(output_var, uuid_str)

            return ActionResult(
                success=True,
                message=f"UUID生成成功: {uuid_str}",
                data={'uuid': uuid_str, 'version': version}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID生成失败: {str(e)}"
            )


class HashCRC32Action(BaseAction):
    """CRC32 checksum."""
    action_type = "hash_crc32"
    display_name = "CRC32校验"
    description = "计算CRC32校验和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CRC32 checksum.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with CRC32 value.
        """
        if not HASH_AVAILABLE:
            return ActionResult(success=False, message="zlib库不可用")

        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'crc32_result')

        if not data:
            return ActionResult(success=False, message="数据不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            crc = zlib.crc32(data_bytes) & 0xffffffff
            crc_hex = format(crc, '08x')

            context.set(output_var, crc)

            return ActionResult(
                success=True,
                message=f"CRC32校验成功: {crc} ({crc_hex})",
                data={'crc32': crc, 'crc32_hex': crc_hex}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"CRC32校验失败: {str(e)}"
            )


class HashBcryptAction(BaseAction):
    """Bcrypt password hash."""
    action_type = "hash_bcrypt"
    display_name = "Bcrypt哈希"
    description = "Bcrypt密码哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Bcrypt hash.

        Args:
            context: Execution context.
            params: Dict with password, salt_rounds, output_var.

        Returns:
            ActionResult with hashed password.
        """
        try:
            import bcrypt
        except ImportError:
            return ActionResult(success=False, message="bcrypt库不可用，请安装: pip install bcrypt")

        password = params.get('password', '')
        salt_rounds = params.get('salt_rounds', 12)
        output_var = params.get('output_var', 'bcrypt_result')

        if not password:
            return ActionResult(success=False, message="密码不能为空")

        try:
            password_bytes = password.encode('utf-8')
            salt = bcrypt.gensalt(rounds=salt_rounds)
            hashed = bcrypt.hashpw(password_bytes, salt).decode('utf-8')

            context.set(output_var, hashed)

            return ActionResult(
                success=True,
                message="Bcrypt哈希成功",
                data={'hashed': hashed}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Bcrypt哈希失败: {str(e)}"
            )


class HashBcryptVerifyAction(BaseAction):
    """Bcrypt password verify."""
    action_type = "hash_bcrypt_verify"
    display_name = "Bcrypt验证"
    description = "验证Bcrypt密码哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Bcrypt verify.

        Args:
            context: Execution context.
            params: Dict with password, hashed, output_var.

        Returns:
            ActionResult with verification result.
        """
        try:
            import bcrypt
        except ImportError:
            return ActionResult(success=False, message="bcrypt库不可用，请安装: pip install bcrypt")

        password = params.get('password', '')
        hashed = params.get('hashed', '')
        output_var = params.get('output_var', 'verify_result')

        if not password or not hashed:
            return ActionResult(success=False, message="密码和哈希值都不能为空")

        try:
            password_bytes = password.encode('utf-8')
            hashed_bytes = hashed.encode('utf-8')
            matches = bcrypt.checkpw(password_bytes, hashed_bytes)

            context.set(output_var, matches)

            return ActionResult(
                success=True,
                message="密码验证" + ("成功" if matches else "失败"),
                data={'matches': matches}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"密码验证失败: {str(e)}"
            )


class HashAesEncryptAction(BaseAction):
    """AES encryption."""
    action_type = "hash_aes_encrypt"
    display_name = "AES加密"
    description = "AES对称加密"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute AES encryption.

        Args:
            context: Execution context.
            params: Dict with data, key, iv, mode, encoding, output_var.

        Returns:
            ActionResult with encrypted data.
        """
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import padding
        except ImportError:
            return ActionResult(success=False, message="cryptography库不可用，请安装: pip install cryptography")

        data = params.get('data', '')
        key = params.get('key', '')
        iv = params.get('iv', None)
        mode = params.get('mode', 'cbc')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'aes_result')

        if not data or not key:
            return ActionResult(success=False, message="数据和密钥都不能为空")

        try:
            if isinstance(data, str):
                data_bytes = data.encode(encoding)
            else:
                data_bytes = data

            if isinstance(key, str):
                key_bytes = key.encode(encoding)
            else:
                key_bytes = key

            if len(key_bytes) not in [16, 24, 32]:
                return ActionResult(success=False, message="密钥长度必须是16/24/32字节")

            if iv is None:
                import os
                iv_bytes = os.urandom(16)
            elif isinstance(iv, str):
                iv_bytes = iv.encode(encoding)
            else:
                iv_bytes = iv

            padder = padding.PKCS7(128).padder()
            padded_data = padder.update(data_bytes) + padder.finalize()

            if mode == 'cbc':
                cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv_bytes), backend=default_backend())
            else:
                cipher = Cipher(algorithms.AES(key_bytes), modes.ECB(), backend=default_backend())
                iv_bytes = None

            encryptor = cipher.encryptor()
            encrypted = encryptor.update(padded_data) + encryptor.finalize()

            import base64
            encrypted_b64 = base64.b64encode(encrypted).decode(encoding)

            result = {
                'encrypted': encrypted_b64,
                'iv': base64.b64encode(iv_bytes).decode(encoding) if iv_bytes else None
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="AES加密成功",
                data=result
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES加密失败: {str(e)}"
            )


class HashAesDecryptAction(BaseAction):
    """AES decryption."""
    action_type = "hash_aes_decrypt"
    display_name = "AES解密"
    description = "AES对称解密"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute AES decryption.

        Args:
            context: Execution context.
            params: Dict with encrypted_b64, key, iv, mode, encoding, output_var.

        Returns:
            ActionResult with decrypted data.
        """
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import padding
        except ImportError:
            return ActionResult(success=False, message="cryptography库不可用，请安装: pip install cryptography")

        encrypted_b64 = params.get('encrypted_b64', '')
        key = params.get('key', '')
        iv = params.get('iv', None)
        mode = params.get('mode', 'cbc')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'aes_result')

        if not encrypted_b64 or not key:
            return ActionResult(success=False, message="加密数据和密钥都不能为空")

        try:
            import base64
            encrypted = base64.b64decode(encrypted_b64)

            if isinstance(key, str):
                key_bytes = key.encode(encoding)
            else:
                key_bytes = key

            if len(key_bytes) not in [16, 24, 32]:
                return ActionResult(success=False, message="密钥长度必须是16/24/32字节")

            if iv is not None:
                if isinstance(iv, str):
                    iv_bytes = base64.b64decode(iv)
                else:
                    iv_bytes = iv
            else:
                iv_bytes = None

            if mode == 'cbc' and iv_bytes:
                cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv_bytes), backend=default_backend())
            else:
                cipher = Cipher(algorithms.AES(key_bytes), modes.ECB(), backend=default_backend())

            decryptor = cipher.decryptor()
            decrypted_padded = decryptor.update(encrypted) + decryptor.finalize()

            unpadder = padding.PKCS7(128).unpadder()
            decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()
            decrypted_str = decrypted.decode(encoding, errors='replace')

            context.set(output_var, decrypted_str)

            return ActionResult(
                success=True,
                message="AES解密成功",
                data={'decrypted': decrypted_str}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"AES解密失败: {str(e)}"
            )
