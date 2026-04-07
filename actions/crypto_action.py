"""Crypto action module for RabAI AutoClick.

Provides cryptographic operations:
- CryptoEncryptAction: Encrypt data
- CryptoDecryptAction: Decrypt data
- CryptoGenerateKeyAction: Generate encryption key
"""

import base64
import hashlib
import os
import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CryptoEncryptAction(BaseAction):
    """Encrypt data."""
    action_type = "crypto_encrypt"
    display_name = "加密数据"
    description = "使用密钥加密数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute encryption.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with encrypted data.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'crypto_result')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            # Simple XOR encryption with key
            data_bytes = resolved_data.encode('utf-8')
            key_bytes = resolved_key.encode('utf-8')

            encrypted = bytearray()
            for i, byte in enumerate(data_bytes):
                encrypted.append(byte ^ key_bytes[i % len(key_bytes)])

            result = base64.b64encode(encrypted).decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="数据加密成功",
                data={
                    'result': result,
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
        return {'output_var': 'crypto_result'}


class CryptoDecryptAction(BaseAction):
    """Decrypt data."""
    action_type = "crypto_decrypt"
    display_name = "解密数据"
    description = "使用密钥解密数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decryption.

        Args:
            context: Execution context.
            params: Dict with encrypted_data, key, output_var.

        Returns:
            ActionResult with decrypted data.
        """
        encrypted_data = params.get('encrypted_data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'crypto_result')

        valid, msg = self.validate_type(encrypted_data, str, 'encrypted_data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(encrypted_data)
            resolved_key = context.resolve_value(key)

            # Base64 decode
            encrypted_bytes = base64.b64decode(resolved_data.encode('utf-8'))
            key_bytes = resolved_key.encode('utf-8')

            decrypted = bytearray()
            for i, byte in enumerate(encrypted_bytes):
                decrypted.append(byte ^ key_bytes[i % len(key_bytes)])

            result = decrypted.decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="数据解密成功",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解密失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['encrypted_data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'crypto_result'}


class CryptoGenerateKeyAction(BaseAction):
    """Generate encryption key."""
    action_type = "crypto_generate_key"
    display_name = "生成密钥"
    description = "生成随机加密密钥"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute key generation.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with generated key.
        """
        length = params.get('length', 32)
        output_var = params.get('output_var', 'crypto_result')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_length = context.resolve_value(length)
            result = base64.b64encode(os.urandom(int(resolved_length))).decode('utf-8')[:int(resolved_length)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"密钥已生成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
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
        return {'length': 32, 'output_var': 'crypto_result'}