"""Hash action module for RabAI AutoClick.

Provides hash/crypto operations:
- HashMd5Action: Calculate MD5 hash
- HashSha256Action: Calculate SHA-256 hash
- HashSha1Action: Calculate SHA-1 hash
- HashFileAction: Calculate file hash
"""

import hashlib
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashMd5Action(BaseAction):
    """Calculate MD5 hash."""
    action_type = "hash_md5"
    display_name = "MD5哈希"
    description = "计算MD5哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute MD5 hash calculation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hash value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = hashlib.md5(resolved.encode()).hexdigest()
            elif isinstance(resolved, bytes):
                result = hashlib.md5(resolved).hexdigest()
            else:
                result = hashlib.md5(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"MD5: {result}",
                data={
                    'hash': result,
                    'algorithm': 'md5',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"MD5计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashSha256Action(BaseAction):
    """Calculate SHA-256 hash."""
    action_type = "hash_sha256"
    display_name = "SHA256哈希"
    description = "计算SHA-256哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA-256 hash calculation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hash value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = hashlib.sha256(resolved.encode()).hexdigest()
            elif isinstance(resolved, bytes):
                result = hashlib.sha256(resolved).hexdigest()
            else:
                result = hashlib.sha256(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA256: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': 'sha256',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA256计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashSha1Action(BaseAction):
    """Calculate SHA-1 hash."""
    action_type = "hash_sha1"
    display_name = "SHA1哈希"
    description = "计算SHA-1哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SHA-1 hash calculation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hash value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'hash_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = hashlib.sha1(resolved.encode()).hexdigest()
            elif isinstance(resolved, bytes):
                result = hashlib.sha1(resolved).hexdigest()
            else:
                result = hashlib.sha1(str(resolved).encode()).hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SHA1: {result}",
                data={
                    'hash': result,
                    'algorithm': 'sha1',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SHA1计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hash_result'}


class HashFileAction(BaseAction):
    """Calculate file hash."""
    action_type = "hash_file"
    display_name = "文件哈希"
    description = "计算文件哈希值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file hash calculation.

        Args:
            context: Execution context.
            params: Dict with file_path, algorithm, output_var.

        Returns:
            ActionResult with file hash.
        """
        file_path = params.get('file_path', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'hash_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid_algos = ['md5', 'sha1', 'sha256', 'sha512']
        valid, msg = self.validate_in(algorithm.lower(), valid_algos, 'algorithm')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            if not os.path.isfile(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"路径不是文件: {resolved_path}"
                )

            hash_func = hashlib.new(algorithm.lower())

            with open(resolved_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_func.update(chunk)

            result = hash_func.hexdigest()
            context.set(output_var, result)

            file_size = os.path.getsize(resolved_path)

            return ActionResult(
                success=True,
                message=f"文件哈希: {result[:16]}...",
                data={
                    'hash': result,
                    'algorithm': algorithm.lower(),
                    'file_path': resolved_path,
                    'file_size': file_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件哈希计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'hash_result'}