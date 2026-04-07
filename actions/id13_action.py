"""ID13 action module for RabAI AutoClick.

Provides additional ID generation operations:
- IDUUIDAction: Generate UUID
- IDRandomAction: Generate random ID
- IDHashAction: Generate hash ID
- IDTimestampAction: Generate timestamp-based ID
- IDSequenceAction: Generate sequence ID
- IDEncodeAction: Encode ID
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IDUUIDAction(BaseAction):
    """Generate UUID."""
    action_type = "id13_uuid"
    display_name = "生成UUID"
    description = "生成UUID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID generation.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with UUID.
        """
        version = params.get('version', 4)
        output_var = params.get('output_var', 'uuid_id')

        try:
            import uuid

            resolved_version = int(context.resolve_value(version)) if version else 4

            if resolved_version == 1:
                result = str(uuid.uuid1())
            elif resolved_version == 4:
                result = str(uuid.uuid4())
            else:
                result = str(uuid.uuid4())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成UUID: {result}",
                data={
                    'uuid': result,
                    'version': resolved_version,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 4, 'output_var': 'uuid_id'}


class IDRandomAction(BaseAction):
    """Generate random ID."""
    action_type = "id13_random"
    display_name = "生成随机ID"
    description = "生成随机ID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random ID generation.

        Args:
            context: Execution context.
            params: Dict with length, charset, output_var.

        Returns:
            ActionResult with random ID.
        """
        length = params.get('length', 16)
        charset = params.get('charset', 'alphanumeric')
        output_var = params.get('output_var', 'random_id')

        try:
            import random
            import string

            resolved_length = int(context.resolve_value(length)) if length else 16
            resolved_charset = context.resolve_value(charset) if charset else 'alphanumeric'

            if resolved_charset == 'alphanumeric':
                chars = string.ascii_letters + string.digits
            elif resolved_charset == 'alpha':
                chars = string.ascii_letters
            elif resolved_charset == 'digit':
                chars = string.digits
            elif resolved_charset == 'hex':
                chars = string.hexdigits.lower()
            else:
                chars = string.ascii_letters + string.digits

            result = ''.join(random.choice(chars) for _ in range(resolved_length))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机ID: {result}",
                data={
                    'id': result,
                    'length': resolved_length,
                    'charset': resolved_charset,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 16, 'charset': 'alphanumeric', 'output_var': 'random_id'}


class IDHashAction(BaseAction):
    """Generate hash ID."""
    action_type = "id13_hash"
    display_name = "生成哈希ID"
    description = "生成哈希ID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hash ID generation.

        Args:
            context: Execution context.
            params: Dict with value, algorithm, length, output_var.

        Returns:
            ActionResult with hash ID.
        """
        value = params.get('value', '')
        algorithm = params.get('algorithm', 'md5')
        length = params.get('length', None)
        output_var = params.get('output_var', 'hash_id')

        try:
            import hashlib

            resolved_value = context.resolve_value(value)
            resolved_algorithm = context.resolve_value(algorithm) if algorithm else 'md5'
            resolved_length = int(context.resolve_value(length)) if length else None

            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode('utf-8')

            hash_func = getattr(hashlib, resolved_algorithm, hashlib.md5)
            result = hash_func(resolved_value).hexdigest()

            if resolved_length:
                result = result[:resolved_length]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成哈希ID: {result}",
                data={
                    'id': result,
                    'algorithm': resolved_algorithm,
                    'length': resolved_length or len(result),
                    'output_var': output_var
                }
            )
        except AttributeError:
            return ActionResult(
                success=False,
                message=f"不支持的哈希算法: {algorithm}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成哈希ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'md5', 'length': None, 'output_var': 'hash_id'}


class IDTimestampAction(BaseAction):
    """Generate timestamp-based ID."""
    action_type = "id13_timestamp"
    display_name = "生成时间戳ID"
    description = "生成时间戳ID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timestamp ID generation.

        Args:
            context: Execution context.
            params: Dict with prefix, suffix, output_var.

        Returns:
            ActionResult with timestamp ID.
        """
        prefix = params.get('prefix', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'timestamp_id')

        try:
            import time

            resolved_prefix = context.resolve_value(prefix) if prefix else ''
            resolved_suffix = context.resolve_value(suffix) if suffix else ''

            timestamp = int(time.time() * 1000000)  # Microseconds

            result = f"{resolved_prefix}{timestamp}{resolved_suffix}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成时间戳ID: {result}",
                data={
                    'id': result,
                    'timestamp': timestamp,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成时间戳ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'prefix': '', 'suffix': '', 'output_var': 'timestamp_id'}


class IDSequenceAction(BaseAction):
    """Generate sequence ID."""
    action_type = "id13_sequence"
    display_name = "生成序列ID"
    description = "生成序列ID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sequence ID generation.

        Args:
            context: Execution context.
            params: Dict with name, start, step, output_var.

        Returns:
            ActionResult with sequence ID.
        """
        name = params.get('name', 'default')
        start = params.get('start', 1)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'sequence_id')

        try:
            import time

            resolved_name = context.resolve_value(name) if name else 'default'
            resolved_start = int(context.resolve_value(start)) if start else 1
            resolved_step = int(context.resolve_value(step)) if step else 1

            # Simple sequence implementation using timestamp
            current_time = int(time.time() * 1000)
            result = resolved_start + (current_time % 1000000) * resolved_step

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成序列ID: {result}",
                data={
                    'id': result,
                    'name': resolved_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成序列ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 1, 'step': 1, 'output_var': 'sequence_id'}


class IDEncodeAction(BaseAction):
    """Encode ID."""
    action_type = "id13_encode"
    display_name = "编码ID"
    description = "编码ID"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ID encoding.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with encoded ID.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'base62')
        output_var = params.get('output_var', 'encoded_id')

        try:
            import base64

            resolved_value = context.resolve_value(value)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'base62'

            if isinstance(resolved_value, str):
                resolved_value = resolved_value.encode('utf-8')

            if resolved_encoding == 'base64':
                result = base64.b64encode(resolved_value).decode('ascii')
            elif resolved_encoding == 'base62':
                result = base64.b64encode(resolved_value).decode('ascii').rstrip('=')
            elif resolved_encoding == 'base32':
                result = base64.b32encode(resolved_value).decode('ascii')
            else:
                result = str(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"编码ID: {result[:20]}...",
                data={
                    'id': result,
                    'encoding': resolved_encoding,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"编码ID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'encoding']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encoded_id'}