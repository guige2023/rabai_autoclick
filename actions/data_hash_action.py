"""Data Hash Action.

Computes cryptographic hashes of data with support for multiple algorithms,
hashing files, incremental hashing, and HMAC generation.
"""

import sys
import os
import hashlib
import hmac
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataHashAction(BaseAction):
    """Compute cryptographic hashes of data.
    
    Supports MD5, SHA-1, SHA-256, SHA-512, and custom HMAC
    with file and streaming hash computation.
    """
    action_type = "data_hash"
    display_name = "数据哈希"
    description = "计算数据哈希值，支持多种算法和HMAC"

    SUPPORTED_ALGORITHMS = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512', 'blake2b', 'blake2s']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compute hash of data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'hash', 'hmac', 'verify', 'batch_hash'.
                - data: Data to hash (string, bytes, or variable name).
                - algorithm: Hash algorithm (default: sha256).
                - source_file: File to hash.
                - key: Secret key for HMAC.
                - encoding: Encoding for string data (default: utf-8).
                - save_to_var: Variable name for result.
                - chunk_size: Chunk size for file hashing (default: 65536).
        
        Returns:
            ActionResult with hash result.
        """
        try:
            action = params.get('action', 'hash')
            save_to_var = params.get('save_to_var', 'hash_result')

            if action == 'hash':
                return self._compute_hash(context, params, save_to_var)
            elif action == 'hmac':
                return self._compute_hmac(context, params, save_to_var)
            elif action == 'verify':
                return self._verify_hash(context, params, save_to_var)
            elif action == 'batch_hash':
                return self._batch_hash(context, params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Hash error: {e}")

    def _compute_hash(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Compute hash of data."""
        data = params.get('data')
        algorithm = params.get('algorithm', 'sha256').lower()
        source_file = params.get('source_file')
        encoding = params.get('encoding', 'utf-8')
        chunk_size = params.get('chunk_size', 65536)

        if algorithm not in self.SUPPORTED_ALGORITHMS:
            return ActionResult(success=False, message=f"Unsupported algorithm: {algorithm}")

        if source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"File not found: {source_file}")
            
            hash_value = self._hash_file(source_file, algorithm, chunk_size)
            file_size = os.path.getsize(source_file)
            result = {
                'algorithm': algorithm,
                'hash': hash_value,
                'file': source_file,
                'size': file_size
            }
        elif data:
            if isinstance(data, str):
                data = data.encode(encoding)
            elif not isinstance(data, bytes):
                import json
                data = json.dumps(data).encode(encoding)
            
            hash_value = self._hash_bytes(data, algorithm)
            result = {
                'algorithm': algorithm,
                'hash': hash_value,
                'size': len(data)
            }
        else:
            data = context.get_variable(params.get('use_var', 'input_data'))
            if data:
                if isinstance(data, str):
                    data = data.encode(encoding)
                hash_value = self._hash_bytes(data, algorithm)
                result = {
                    'algorithm': algorithm,
                    'hash': hash_value,
                    'size': len(data)
                }
            else:
                return ActionResult(success=False, message="No data provided")

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, message=f"{algorithm}: {hash_value}")

    def _compute_hmac(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Compute HMAC of data."""
        data = params.get('data')
        key = params.get('key')
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')

        if not key:
            return ActionResult(success=False, message="key is required for HMAC")

        if not data:
            data = context.get_variable(params.get('use_var', 'input_data'))
            if not data:
                return ActionResult(success=False, message="No data provided")

        if isinstance(data, str):
            data = data.encode(encoding)
        if isinstance(key, str):
            key = key.encode(encoding)

        hmac_value = hmac.new(key, data, getattr(hashlib, algorithm)).hexdigest()

        result = {
            'algorithm': f'hmac-{algorithm}',
            'hash': hmac_value,
            'key_id': key.decode(encoding)[:8] + '...'
        }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, message=f"HMAC: {hmac_value}")

    def _verify_hash(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Verify hash of data."""
        data = params.get('data')
        source_file = params.get('source_file')
        expected_hash = params.get('expected_hash')
        algorithm = params.get('algorithm', 'sha256').lower()

        if not expected_hash:
            return ActionResult(success=False, message="expected_hash is required")

        # Compute actual hash
        if source_file:
            actual_hash = self._hash_file(source_file, algorithm)
        elif data:
            if isinstance(data, str):
                data = data.encode('utf-8')
            actual_hash = self._hash_bytes(data, algorithm)
        else:
            data = context.get_variable(params.get('use_var', 'input_data'))
            if not data:
                return ActionResult(success=False, message="No data provided")
            if isinstance(data, str):
                data = data.encode('utf-8')
            actual_hash = self._hash_bytes(data, algorithm)

        verified = hmac.compare_digest(actual_hash.lower(), expected_hash.lower())

        result = {
            'verified': verified,
            'expected': expected_hash,
            'actual': actual_hash,
            'algorithm': algorithm
        }

        context.set_variable(save_to_var, result)
        return ActionResult(success=verified, data=result,
                          message=f"Hash {'VERIFIED' if verified else 'MISMATCH'}")

    def _batch_hash(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Hash multiple items."""
        data_list = params.get('data_list') or context.get_variable(params.get('use_var', 'data_list'))
        algorithm = params.get('algorithm', 'sha256').lower()

        if not data_list:
            return ActionResult(success=False, message="data_list is required")

        results = []
        for i, item in enumerate(data_list):
            try:
                if isinstance(item, str):
                    item = item.encode('utf-8')
                elif not isinstance(item, bytes):
                    import json
                    item = json.dumps(item).encode('utf-8')
                
                hash_value = self._hash_bytes(item, algorithm)
                results.append({'index': i, 'hash': hash_value, 'success': True})
            except Exception as e:
                results.append({'index': i, 'error': str(e), 'success': False})

        summary = {
            'algorithm': algorithm,
            'total': len(data_list),
            'successful': sum(1 for r in results if r['success']),
            'results': results
        }

        context.set_variable(save_to_var, summary)
        return ActionResult(success=True, data=summary, 
                          message=f"Batch hash: {summary['successful']}/{len(data_list)}")

    def _hash_bytes(self, data: bytes, algorithm: str) -> str:
        """Hash bytes data."""
        if algorithm == 'md5':
            return hashlib.md5(data).hexdigest()
        elif algorithm == 'sha1':
            return hashlib.sha1(data).hexdigest()
        elif algorithm == 'sha224':
            return hashlib.sha224(data).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(data).hexdigest()
        elif algorithm == 'sha384':
            return hashlib.sha384(data).hexdigest()
        elif algorithm == 'sha512':
            return hashlib.sha512(data).hexdigest()
        elif algorithm == 'blake2b':
            return hashlib.blake2b(data).hexdigest()
        elif algorithm == 'blake2s':
            return hashlib.blake2s(data).hexdigest()
        else:
            return hashlib.sha256(data).hexdigest()

    def _hash_file(self, file_path: str, algorithm: str, chunk_size: int = 65536) -> str:
        """Hash file contents."""
        if algorithm == 'md5':
            hasher = hashlib.md5()
        elif algorithm == 'sha1':
            hasher = hashlib.sha1()
        elif algorithm == 'sha224':
            hasher = hashlib.sha224()
        elif algorithm == 'sha256':
            hasher = hashlib.sha256()
        elif algorithm == 'sha384':
            hasher = hashlib.sha384()
        elif algorithm == 'sha512':
            hasher = hashlib.sha512()
        elif algorithm == 'blake2b':
            hasher = hashlib.blake2b()
        elif algorithm == 'blake2s':
            hasher = hashlib.blake2s()
        else:
            hasher = hashlib.sha256()

        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                hasher.update(chunk)

        return hasher.hexdigest()
