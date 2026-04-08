"""Data fingerprint action module for RabAI AutoClick.

Provides data fingerprinting for deduplication,
integrity verification, and change detection.
"""

import hashlib
import hmac
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
import base64
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFingerprintAction(BaseAction):
    """Generate and manage data fingerprints for deduplication.
    
    Supports multiple fingerprinting algorithms,
    fingerprint comparison, and integrity checking.
    """
    action_type = "data_fingerprint"
    display_name = "数据指纹"
    description = "数据指纹识别，用于去重和完整性验证"
    SUPPORTED_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'crc32', 'xxhash']
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fingerprint operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (generate, verify, compare,
                   find_duplicates), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'generate')
        
        if action == 'generate':
            return self._generate_fingerprint(params)
        elif action == 'verify':
            return self._verify_fingerprint(params)
        elif action == 'compare':
            return self._compare_fingerprints(params)
        elif action == 'find_duplicates':
            return self._find_duplicates(params)
        elif action == 'generate_batch':
            return self._generate_batch(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _generate_fingerprint(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate a fingerprint for data."""
        data = params.get('data')
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        algorithm = params.get('algorithm', 'sha256').lower()
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}"
            )
        
        as_hex = params.get('as_hex', True)
        as_base64 = params.get('as_base64', False)
        include_metadata = params.get('include_metadata', False)
        
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data, sort_keys=True, default=str)
        else:
            data_str = str(data)
        
        data_bytes = data_str.encode('utf-8')
        
        if algorithm == 'crc32':
            import zlib
            fingerprint = format(zlib.crc32(data_bytes) & 0xffffffff, '08x')
        elif algorithm == 'xxhash':
            try:
                import xxhash
                fingerprint = xxhash.xxh64(data_bytes).hexdigest()
            except ImportError:
                fingerprint = hashlib.sha256(data_bytes).hexdigest()
        else:
            hash_func = getattr(hashlib, algorithm)
            fingerprint = hash_func(data_bytes).digest()
            
            if as_base64:
                fingerprint = base64.b64encode(fingerprint).decode('utf-8')
            else:
                fingerprint = fingerprint.hex()
        
        result = {
            'fingerprint': fingerprint,
            'algorithm': algorithm,
            'length': len(fingerprint)
        }
        
        if include_metadata:
            result['data_type'] = type(data).__name__
            result['data_size'] = len(data_str)
        
        return ActionResult(
            success=True,
            message=f"Generated {algorithm} fingerprint",
            data=result
        )
    
    def _verify_fingerprint(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify data against a fingerprint."""
        data = params.get('data')
        expected_fingerprint = params.get('fingerprint')
        
        if data is None or expected_fingerprint is None:
            return ActionResult(
                success=False,
                message="data and fingerprint are required"
            )
        
        algorithm = params.get('algorithm', 'sha256').lower()
        
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data, sort_keys=True, default=str)
        else:
            data_str = str(data)
        
        data_bytes = data_str.encode('utf-8')
        
        if algorithm == 'crc32':
            import zlib
            actual = format(zlib.crc32(data_bytes) & 0xffffffff, '08x')
        else:
            hash_func = getattr(hashlib, algorithm)
            actual = hash_func(data_bytes).hexdigest()
        
        is_valid = hmac.compare_digest(actual.lower(), expected_fingerprint.lower())
        
        return ActionResult(
            success=is_valid,
            message=f"Fingerprint verification: {'passed' if is_valid else 'failed'}",
            data={
                'valid': is_valid,
                'expected': expected_fingerprint,
                'actual': actual
            }
        )
    
    def _compare_fingerprints(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare two fingerprints."""
        fingerprint1 = params.get('fingerprint1')
        fingerprint2 = params.get('fingerprint2')
        
        if not fingerprint1 or not fingerprint2:
            return ActionResult(
                success=False,
                message="fingerprint1 and fingerprint2 are required"
            )
        
        are_equal = hmac.compare_digest(
            fingerprint1.lower(),
            fingerprint2.lower()
        )
        
        return ActionResult(
            success=True,
            message=f"Fingerprints: {'identical' if are_equal else 'different'}",
            data={
                'identical': are_equal
            }
        )
    
    def _find_duplicates(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find duplicate records based on fingerprint."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        algorithm = params.get('algorithm', 'sha256').lower()
        key_field = params.get('key_field')
        
        fingerprints = {}
        duplicates = []
        
        for idx, record in enumerate(records):
            if isinstance(record, dict):
                data_str = json.dumps(record, sort_keys=True, default=str)
            else:
                data_str = str(record)
            
            data_bytes = data_str.encode('utf-8')
            
            if algorithm == 'crc32':
                import zlib
                fp = format(zlib.crc32(data_bytes) & 0xffffffff, '08x')
            else:
                hash_func = getattr(hashlib, algorithm)
                fp = hash_func(data_bytes).hexdigest()
            
            if key_field and isinstance(record, dict):
                record_key = record.get(key_field, idx)
            else:
                record_key = idx
            
            if fp in fingerprints:
                duplicates.append({
                    'fingerprint': fp,
                    'records': [fingerprints[fp], record_key]
                })
            else:
                fingerprints[fp] = record_key
        
        unique_count = len(records) - sum(len(d['records']) for d in duplicates)
        
        return ActionResult(
            success=True,
            message=f"Found {len(duplicates)} duplicate groups",
            data={
                'duplicates': duplicates,
                'duplicate_count': len(duplicates),
                'unique_count': unique_count,
                'total_records': len(records)
            }
        )
    
    def _generate_batch(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate fingerprints for a batch of records."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        algorithm = params.get('algorithm', 'sha256').lower()
        key_field = params.get('key_field')
        
        results = []
        
        for idx, record in enumerate(records):
            if isinstance(record, dict):
                data_str = json.dumps(record, sort_keys=True, default=str)
            else:
                data_str = str(record)
            
            data_bytes = data_str.encode('utf-8')
            
            if algorithm == 'crc32':
                import zlib
                fp = format(zlib.crc32(data_bytes) & 0xffffffff, '08x')
            else:
                hash_func = getattr(hashlib, algorithm)
                fp = hash_func(data_bytes).hexdigest()
            
            if key_field and isinstance(record, dict):
                record_key = record.get(key_field, idx)
            else:
                record_key = idx
            
            results.append({
                'key': record_key,
                'fingerprint': fp,
                'algorithm': algorithm
            })
        
        return ActionResult(
            success=True,
            message=f"Generated {len(results)} fingerprints",
            data={
                'fingerprints': results,
                'count': len(results)
            }
        )
