"""Data Fingerprint action module for RabAI AutoClick.

Provides data fingerprinting operations:
- FingerprintHashAction: Generate hash fingerprint
- FingerprintChecksumAction: Generate checksum
- FingerprintSchemaAction: Generate schema fingerprint
- FingerprintCompareAction: Compare fingerprints
"""

from __future__ import annotations

import sys
import os
import hashlib
from typing import Any, Dict, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FingerprintHashAction(BaseAction):
    """Generate hash fingerprint."""
    action_type = "fingerprint_hash"
    display_name = "哈希指纹"
    description = "生成哈希指纹"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute hash fingerprinting."""
        data = params.get('data', '')
        algorithm = params.get('algorithm', 'md5')
        output_var = params.get('output_var', 'fingerprint')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if isinstance(resolved_data, (dict, list)):
                data_str = json.dumps(resolved_data, sort_keys=True)
            else:
                data_str = str(resolved_data)

            if algorithm == 'md5':
                fingerprint = hashlib.md5(data_str.encode()).hexdigest()
            elif algorithm == 'sha1':
                fingerprint = hashlib.sha1(data_str.encode()).hexdigest()
            elif algorithm == 'sha256':
                fingerprint = hashlib.sha256(data_str.encode()).hexdigest()
            elif algorithm == 'sha512':
                fingerprint = hashlib.sha512(data_str.encode()).hexdigest()
            else:
                fingerprint = hashlib.md5(data_str.encode()).hexdigest()

            result = {
                'fingerprint': fingerprint,
                'algorithm': algorithm,
                'length': len(fingerprint),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Generated {algorithm} fingerprint: {fingerprint[:16]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash fingerprint error: {e}")


class FingerprintChecksumAction(BaseAction):
    """Generate checksum."""
    action_type = "fingerprint_checksum"
    display_name = "校验和指纹"
    description = "生成校验和"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute checksum generation."""
        file_path = params.get('file_path', '')
        data = params.get('data', None)
        algorithm = params.get('algorithm', 'md5')
        output_var = params.get('output_var', 'checksum')

        try:
            import json

            if file_path:
                resolved_path = context.resolve_value(file_path) if context else file_path

                if not os.path.exists(resolved_path):
                    return ActionResult(success=False, message=f"File not found: {resolved_path}")

                with open(resolved_path, 'rb') as f:
                    content = f.read()

                if algorithm == 'md5':
                    checksum = hashlib.md5(content).hexdigest()
                elif algorithm == 'sha1':
                    checksum = hashlib.sha1(content).hexdigest()
                elif algorithm == 'sha256':
                    checksum = hashlib.sha256(content).hexdigest()
                else:
                    checksum = hashlib.md5(content).hexdigest()

                result = {
                    'checksum': checksum,
                    'algorithm': algorithm,
                    'file_path': resolved_path,
                    'size_bytes': len(content),
                }
            else:
                resolved_data = context.resolve_value(data) if context else data

                if isinstance(resolved_data, (dict, list)):
                    data_str = json.dumps(resolved_data, sort_keys=True)
                else:
                    data_str = str(resolved_data)

                if algorithm == 'md5':
                    checksum = hashlib.md5(data_str.encode()).hexdigest()
                elif algorithm == 'sha1':
                    checksum = hashlib.sha1(data_str.encode()).hexdigest()
                elif algorithm == 'sha256':
                    checksum = hashlib.sha256(data_str.encode()).hexdigest()
                else:
                    checksum = hashlib.md5(data_str.encode()).hexdigest()

                result = {
                    'checksum': checksum,
                    'algorithm': algorithm,
                    'data_size': len(data_str),
                }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Generated {algorithm} checksum: {checksum[:16]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checksum error: {e}")


class FingerprintSchemaAction(BaseAction):
    """Generate schema fingerprint."""
    action_type = "fingerprint_schema"
    display_name = "Schema指纹"
    description = "生成Schema指纹"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute schema fingerprinting."""
        schema = params.get('schema', {})
        output_var = params.get('output_var', 'schema_fingerprint')

        if not schema:
            return ActionResult(success=False, message="schema is required")

        try:
            import json

            resolved_schema = context.resolve_value(schema) if context else schema

            normalized = json.dumps(resolved_schema, sort_keys=True)
            fingerprint = hashlib.sha256(normalized.encode()).hexdigest()

            field_count = len(resolved_schema)
            type_info = {k: type(v).__name__ for k, v in resolved_schema.items()}

            result = {
                'fingerprint': fingerprint,
                'field_count': field_count,
                'types': type_info,
                'schema': resolved_schema,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Schema fingerprint: {fingerprint[:16]}... ({field_count} fields)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema fingerprint error: {e}")


class FingerprintCompareAction(BaseAction):
    """Compare fingerprints."""
    action_type = "fingerprint_compare"
    display_name = "指纹对比"
    description = "对比指纹"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute fingerprint comparison."""
        fingerprint1 = params.get('fingerprint1', '')
        fingerprint2 = params.get('fingerprint2', '')
        output_var = params.get('output_var', 'compare_result')

        if not fingerprint1 or not fingerprint2:
            return ActionResult(success=False, message="fingerprint1 and fingerprint2 are required")

        try:
            resolved_fp1 = context.resolve_value(fingerprint1) if context else fingerprint1
            resolved_fp2 = context.resolve_value(fingerprint2) if context else fingerprint2

            match = resolved_fp1 == resolved_fp2

            result = {
                'match': match,
                'fingerprint1': resolved_fp1[:16] + '...' if len(resolved_fp1) > 16 else resolved_fp1,
                'fingerprint2': resolved_fp2[:16] + '...' if len(resolved_fp2) > 16 else resolved_fp2,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Fingerprints {'match' if match else 'do not match'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint compare error: {e}")
