"""HMAC action module for RabAI AutoClick.

Provides HMAC operations:
- HmacGenerateAction: Generate HMAC
- HmacVerifyAction: Verify HMAC
- HmacMd5Action: HMAC-MD5
- HmacSha256Action: HMAC-SHA256
"""

import hashlib
import hmac as hmac_module
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HmacGenerateAction(BaseAction):
    """Generate HMAC."""
    action_type = "hmac_generate"
    display_name = "生成HMAC"
    description = "生成HMAC"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generate.

        Args:
            context: Execution context.
            params: Dict with data, key, algorithm, output_var.

        Returns:
            ActionResult with HMAC.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'hmac_value')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)
            resolved_algo = context.resolve_value(algorithm)

            algo_map = {
                'md5': hashlib.md5,
                'sha1': hashlib.sha1,
                'sha256': hashlib.sha256,
                'sha384': hashlib.sha384,
                'sha512': hashlib.sha512,
            }

            algo = algo_map.get(resolved_algo, hashlib.sha256)

            h = hmac_module.new(
                resolved_key.encode('utf-8'),
                resolved_data.encode('utf-8'),
                algo
            )
            result = h.hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC生成完成",
                data={'hmac': result, 'algorithm': resolved_algo, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'hmac_value'}


class HmacVerifyAction(BaseAction):
    """Verify HMAC."""
    action_type = "hmac_verify"
    display_name = "验证HMAC"
    description = "验证HMAC"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute verify.

        Args:
            context: Execution context.
            params: Dict with data, key, hmac, algorithm, output_var.

        Returns:
            ActionResult with verification result.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        hmac_val = params.get('hmac', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'hmac_valid')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(hmac_val, str, 'hmac')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)
            resolved_hmac = context.resolve_value(hmac_val)
            resolved_algo = context.resolve_value(algorithm)

            algo_map = {
                'md5': hashlib.md5,
                'sha1': hashlib.sha1,
                'sha256': hashlib.sha256,
                'sha384': hashlib.sha384,
                'sha512': hashlib.sha512,
            }

            algo = algo_map.get(resolved_algo, hashlib.sha256)

            h = hmac_module.new(
                resolved_key.encode('utf-8'),
                resolved_data.encode('utf-8'),
                algo
            )
            computed = h.hexdigest()

            valid = hmac_module.compare_digest(computed, resolved_hmac)
            context.set(output_var, valid)

            return ActionResult(
                success=True,
                message=f"HMAC验证: {'通过' if valid else '失败'}",
                data={'valid': valid, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC验证失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key', 'hmac']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'sha256', 'output_var': 'hmac_valid'}


class HmacMd5Action(BaseAction):
    """HMAC-MD5."""
    action_type = "hmac_md5"
    display_name = "HMAC-MD5"
    description = "HMAC-MD5"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC-MD5.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with HMAC-MD5.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'hmac_md5')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            h = hmac_module.new(
                resolved_key.encode('utf-8'),
                resolved_data.encode('utf-8'),
                hashlib.md5
            )
            result = h.hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC-MD5: {result}",
                data={'hmac': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC-MD5失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hmac_md5'}


class HmacSha256Action(BaseAction):
    """HMAC-SHA256."""
    action_type = "hmac_sha256"
    display_name = "HMAC-SHA256"
    description = "HMAC-SHA256"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HMAC-SHA256.

        Args:
            context: Execution context.
            params: Dict with data, key, output_var.

        Returns:
            ActionResult with HMAC-SHA256.
        """
        data = params.get('data', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'hmac_sha256')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_key = context.resolve_value(key)

            h = hmac_module.new(
                resolved_key.encode('utf-8'),
                resolved_data.encode('utf-8'),
                hashlib.sha256
            )
            result = h.hexdigest()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HMAC-SHA256: {result[:16]}...",
                data={'hmac': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC-SHA256失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hmac_sha256'}
