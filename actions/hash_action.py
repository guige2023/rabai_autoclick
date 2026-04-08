"""Hash utilities action module for RabAI AutoClick.

Provides cryptographic hash functions for
data integrity verification and content identification.
"""

import hashlib
import hmac
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HashGenerateAction(BaseAction):
    """Generate hash from string or file.
    
    Supports MD5, SHA1, SHA256, SHA512 algorithms.
    """
    action_type = "hash_generate"
    display_name = "生成哈希"
    description = "生成字符串或文件的哈希值"

    ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'blake2b', 'blake2s']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate hash.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, file_path, algorithm,
                   encoding, save_to_var.
        
        Returns:
            ActionResult with hash value.
        """
        data = params.get('data', None)
        file_path = params.get('file_path', None)
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if algorithm not in self.ALGORITHMS:
            return ActionResult(
                success=False,
                message=f"Invalid algorithm: {algorithm}. Valid: {self.ALGORITHMS}"
            )

        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")

                hasher = hashlib.new(algorithm)
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        hasher.update(chunk)
                hash_value = hasher.hexdigest()
                input_type = 'file'
            elif data is not None:
                if isinstance(data, bytes):
                    hasher = hashlib.new(algorithm)
                    hasher.update(data)
                else:
                    hasher = hashlib.new(algorithm)
                    hasher.update(str(data).encode(encoding))
                hash_value = hasher.hexdigest()
                input_type = 'string'
            else:
                return ActionResult(
                    success=False,
                    message="Either data or file_path is required"
                )

            result_data = {
                'hash': hash_value,
                'algorithm': algorithm,
                'input_type': input_type,
                'length': len(hash_value)
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"{algorithm.upper()} = {hash_value[:20]}...",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"哈希生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['algorithm']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'data': None,
            'file_path': None,
            'encoding': 'utf-8',
            'save_to_var': None
        }


class HashVerifyAction(BaseAction):
    """Verify hash against expected value.
    
    Supports constant-time comparison to prevent timing attacks.
    """
    action_type = "hash_verify"
    display_name = "验证哈希"
    description = "验证数据哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Verify hash.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, file_path, expected_hash,
                   algorithm, encoding, save_to_var.
        
        Returns:
            ActionResult with verification result.
        """
        data = params.get('data', None)
        file_path = params.get('file_path', None)
        expected_hash = params.get('expected_hash', '').lower()
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not expected_hash:
            return ActionResult(success=False, message="expected_hash is required")

        # First generate actual hash
        actual_hash = ''
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                hasher = hashlib.new(algorithm)
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        hasher.update(chunk)
                actual_hash = hasher.hexdigest()
            elif data is not None:
                hasher = hashlib.new(algorithm)
                if isinstance(data, bytes):
                    hasher.update(data)
                else:
                    hasher.update(str(data).encode(encoding))
                actual_hash = hasher.hexdigest()
            else:
                return ActionResult(
                    success=False,
                    message="Either data or file_path is required"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"哈希计算失败: {str(e)}"
            )

        # Constant-time comparison
        is_valid = hmac.compare_digest(actual_hash.lower(), expected_hash.lower())

        result_data = {
            'valid': is_valid,
            'expected': expected_hash,
            'actual': actual_hash,
            'algorithm': algorithm
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"哈希验证通过",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"哈希验证失败: 期望 {expected_hash}, 实际 {actual_hash}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['expected_hash', 'algorithm']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'data': None,
            'file_path': None,
            'encoding': 'utf-8',
            'save_to_var': None
        }
