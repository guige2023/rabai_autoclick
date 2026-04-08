"""Data hasher action module for RabAI AutoClick.

Provides data hashing with multiple algorithms,
HMAC support, salt generation, and hash verification.
"""

import hashlib
import hmac
import secrets
import sys
import os
from typing import Any, Dict, List, Optional, Union
import base64
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataHasherAction(BaseAction):
    """Hash data using various algorithms with HMAC support.
    
    Supports MD5, SHA-1, SHA-256, SHA-512, bcrypt-style hashing,
    HMAC generation/verification, and salt generation.
    """
    action_type = "data_hasher"
    display_name = "数据哈希"
    description = "数据哈希，支持多种算法和HMAC"
    SUPPORTED_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'sha3_512']
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hashing operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (hash, verify, generate_salt,
                   hmac, verify_hmac), data, algorithm, secret.
        
        Returns:
            ActionResult with hash or verification result.
        """
        action = params.get('action', 'hash')
        
        if action == 'hash':
            return self._hash_data(params)
        elif action == 'verify':
            return self._verify_hash(params)
        elif action == 'generate_salt':
            return self._generate_salt(params)
        elif action == 'hmac':
            return self._generate_hmac(params)
        elif action == 'verify_hmac':
            return self._verify_hmac(params)
        elif action == 'hash_file':
            return self._hash_file(params)
        elif action == 'hash_records':
            return self._hash_records(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _hash_data(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Hash data using specified algorithm."""
        data = params.get('data')
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        algorithm = params.get('algorithm', 'sha256').lower()
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )
        
        encoding = params.get('encoding', 'utf-8')
        as_hex = params.get('as_hex', True)
        as_base64 = params.get('as_base64', False)
        
        if isinstance(data, (dict, list)):
            data = json.dumps(data, sort_keys=True, default=str)
        
        if isinstance(data, str):
            data = data.encode(encoding)
        
        hash_func = getattr(hashlib, algorithm)
        digest = hash_func(data).digest()
        
        if as_base64:
            result = base64.b64encode(digest).decode(encoding)
        elif as_hex:
            result = digest.hex()
        else:
            result = digest
        
        return ActionResult(
            success=True,
            message=f"Hashed with {algorithm}",
            data={
                'algorithm': algorithm,
                'hash': result,
                'length': len(result)
            }
        )
    
    def _verify_hash(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify data against a hash."""
        data = params.get('data')
        expected_hash = params.get('hash')
        
        if data is None or expected_hash is None:
            return ActionResult(
                success=False,
                message="data and hash are required"
            )
        
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')
        
        if isinstance(data, str):
            data = data.encode(encoding)
        
        hash_func = getattr(hashlib, algorithm)
        actual_hash = hash_func(data).hexdigest()
        
        is_valid = secrets.compare_digest(actual_hash, expected_hash)
        
        return ActionResult(
            success=is_valid,
            message=f"Hash verification: {'passed' if is_valid else 'failed'}",
            data={
                'valid': is_valid,
                'expected': expected_hash,
                'actual': actual_hash
            }
        )
    
    def _generate_salt(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate a cryptographic salt."""
        length = params.get('length', 32)
        as_hex = params.get('as_hex', True)
        as_base64 = params.get('as_base64', False)
        
        salt = secrets.token_bytes(length)
        
        if as_base64:
            result = base64.b64encode(salt).decode('utf-8')
        elif as_hex:
            result = salt.hex()
        else:
            result = salt
        
        return ActionResult(
            success=True,
            message=f"Generated salt ({length} bytes)",
            data={
                'salt': result,
                'length': length
            }
        )
    
    def _generate_hmac(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate HMAC for data."""
        data = params.get('data')
        secret = params.get('secret')
        
        if data is None or secret is None:
            return ActionResult(
                success=False,
                message="data and secret are required"
            )
        
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')
        
        if isinstance(data, str):
            data = data.encode(encoding)
        if isinstance(secret, str):
            secret = secret.encode(encoding)
        
        hmac_func = getattr(hmac, algorithm)
        signature = hmac_func(secret, data).digest()
        
        as_hex = params.get('as_hex', True)
        if as_hex:
            result = signature.hex()
        else:
            result = base64.b64encode(signature).decode(encoding)
        
        return ActionResult(
            success=True,
            message=f"Generated HMAC with {algorithm}",
            data={
                'algorithm': algorithm,
                'hmac': result
            }
        )
    
    def _verify_hmac(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify HMAC for data."""
        data = params.get('data')
        secret = params.get('secret')
        expected_hmac = params.get('hmac')
        
        if data is None or secret is None or expected_hmac is None:
            return ActionResult(
                success=False,
                message="data, secret, and hmac are required"
            )
        
        algorithm = params.get('algorithm', 'sha256').lower()
        encoding = params.get('encoding', 'utf-8')
        
        if isinstance(data, str):
            data = data.encode(encoding)
        if isinstance(secret, str):
            secret = secret.encode(encoding)
        
        hmac_func = getattr(hmac, algorithm)
        signature = hmac_func(secret, data).digest()
        actual_hmac = signature.hex()
        
        is_valid = secrets.compare_digest(actual_hmac, expected_hmac)
        
        return ActionResult(
            success=is_valid,
            message=f"HMAC verification: {'passed' if is_valid else 'failed'}",
            data={
                'valid': is_valid
            }
        )
    
    def _hash_file(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Hash file contents."""
        file_path = params.get('file_path')
        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"File not found: {file_path}")
        
        algorithm = params.get('algorithm', 'sha256').lower()
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )
        
        chunk_size = params.get('chunk_size', 8192)
        
        hash_func = getattr(hashlib, algorithm)()
        
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hash_func.update(chunk)
            
            file_hash = hash_func.hexdigest()
            file_size = os.path.getsize(file_path)
            
            return ActionResult(
                success=True,
                message=f"Hashed file with {algorithm}",
                data={
                    'algorithm': algorithm,
                    'hash': file_hash,
                    'file_path': file_path,
                    'file_size': file_size
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to hash file: {e}"
            )
    
    def _hash_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Hash records and add hash field."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        hash_field = params.get('hash_field', '_hash')
        algorithm = params.get('algorithm', 'sha256').lower()
        fields = params.get('fields')
        
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )
        
        hash_func = getattr(hashlib, algorithm)
        
        for record in records:
            if fields:
                data = {k: record.get(k) for k in fields if k in record}
            else:
                data = {k: v for k, v in record.items() if not k.startswith('_')}
            
            data_str = json.dumps(data, sort_keys=True, default=str)
            record[hash_field] = hash_func(data_str.encode()).hexdigest()
        
        return ActionResult(
            success=True,
            message=f"Hashed {len(records)} records",
            data={
                'count': len(records),
                'hash_field': hash_field,
                'algorithm': algorithm
            }
        )
