"""API Response action module for RabAI AutoClick.

Provides API response handling operations:
- ResponseParseAction: Parse API response
- ResponseTransformAction: Transform API response
- ResponseValidateAction: Validate API response
- ResponseCacheAction: Cache API response
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResponseParseAction(BaseAction):
    """Parse API response."""
    action_type = "response_parse"
    display_name = "响应解析"
    description = "解析API响应"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute response parsing."""
        response = params.get('response', '')
        format_type = params.get('format', 'json')
        output_var = params.get('output_var', 'parsed_response')

        if not response:
            return ActionResult(success=False, message="response is required")

        try:
            resolved_response = context.resolve_value(response) if context else response

            if isinstance(resolved_response, str):
                if format_type == 'json':
                    parsed = json.loads(resolved_response)
                else:
                    parsed = resolved_response
            else:
                parsed = resolved_response

            result = {
                'parsed': parsed,
                'format': format_type,
                'type': type(parsed).__name__,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Parsed {format_type} response"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response parse error: {e}")


class ResponseTransformAction(BaseAction):
    """Transform API response."""
    action_type = "response_transform"
    display_name = "响应转换"
    description = "转换API响应"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute response transformation."""
        response = params.get('response', {})
        transforms = params.get('transforms', [])
        output_var = params.get('output_var', 'transformed_response')

        if not response:
            return ActionResult(success=False, message="response is required")

        try:
            resolved_response = context.resolve_value(response) if context else response
            resolved_transforms = context.resolve_value(transforms) if context else transforms

            transformed = resolved_response

            for transform in resolved_transforms:
                op = transform.get('operation', '')

                if op == 'flatten' and isinstance(transformed, dict):
                    flattened = {}
                    for k, v in transformed.items():
                        if isinstance(v, dict):
                            for sub_k, sub_v in v.items():
                                flattened[f"{k}_{sub_k}"] = sub_v
                        else:
                            flattened[k] = v
                    transformed = flattened

                elif op == 'select':
                    fields = transform.get('fields', [])
                    if isinstance(transformed, dict):
                        transformed = {k: v for k, v in transformed.items() if k in fields}

                elif op == 'rename':
                    old_name = transform.get('old_name', '')
                    new_name = transform.get('new_name', '')
                    if isinstance(transformed, dict) and old_name in transformed:
                        transformed[new_name] = transformed.pop(old_name)

                elif op == 'extract':
                    path = transform.get('path', '')
                    if isinstance(transformed, dict):
                        for key in path.split('.'):
                            transformed = transformed.get(key, {})
                    transformed = {'extracted': transformed}

            result = {
                'transformed': transformed,
                'transforms_applied': len(resolved_transforms),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Applied {len(resolved_transforms)} transforms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response transform error: {e}")


class ResponseValidateAction(BaseAction):
    """Validate API response."""
    action_type = "response_validate"
    display_name: "响应验证"
    description = "验证API响应"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute response validation."""
        response = params.get('response', {})
        rules = params.get('rules', [])
        output_var = params.get('output_var', 'validation_result')

        if not response:
            return ActionResult(success=False, message="response is required")

        try:
            resolved_response = context.resolve_value(response) if context else response
            resolved_rules = context.resolve_value(rules) if context else rules

            errors = []
            for rule in resolved_rules:
                field = rule.get('field', '')
                check_type = rule.get('type', 'exists')
                expected = rule.get('expected', None)

                value = resolved_response
                for key in field.split('.'):
                    value = value.get(key, None) if isinstance(value, dict) else None
                    if value is None:
                        break

                if check_type == 'exists':
                    if value is None:
                        errors.append(f"Field '{field}' does not exist")
                elif check_type == 'equals':
                    if value != expected:
                        errors.append(f"Field '{field}' expected {expected}, got {value}")
                elif check_type == 'type':
                    expected_type = expected
                    if expected_type == 'string' and not isinstance(value, str):
                        errors.append(f"Field '{field}' expected string")
                    elif expected_type == 'number' and not isinstance(value, (int, float)):
                        errors.append(f"Field '{field}' expected number")

            result = {
                'valid': len(errors) == 0,
                'errors': errors,
                'error_count': len(errors),
            }

            return ActionResult(
                success=len(errors) == 0,
                data={output_var: result},
                message=f"Validation {'passed' if not errors else f'failed: {len(errors)} errors'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response validate error: {e}")


class ResponseCacheAction(BaseAction):
    """Cache API response."""
    action_type = "response_cache"
    display_name = "响应缓存"
    description = "缓存API响应"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._cache = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute response caching."""
        key = params.get('key', '')
        response = params.get('response', None)
        ttl = params.get('ttl', 300)
        operation = params.get('operation', 'get')
        output_var = params.get('output_var', 'cache_result')

        try:
            resolved_key = context.resolve_value(key) if context else key

            if operation == 'set' and response is not None:
                resolved_response = context.resolve_value(response) if context else response
                self._cache[resolved_key] = {
                    'response': resolved_response,
                    'cached_at': os.time.time(),
                    'expires_at': os.time.time() + ttl,
                }
                result = {'cached': True, 'key': resolved_key, 'ttl': ttl}
            elif operation == 'get':
                if resolved_key in self._cache:
                    entry = self._cache[resolved_key]
                    if entry['expires_at'] > os.time.time():
                        result = {'hit': True, 'response': entry['response'], 'key': resolved_key}
                    else:
                        result = {'hit': False, 'reason': 'expired', 'key': resolved_key}
                else:
                    result = {'hit': False, 'reason': 'not_found', 'key': resolved_key}
            elif operation == 'clear':
                self._cache.clear()
                result = {'cleared': True}

            return ActionResult(success=True, data={output_var: result}, message=f"Cache {operation}: {result.get('hit', result.get('cached', result.get('cleared', False)))}")
        except Exception as e:
            return ActionResult(success=False, message=f"Response cache error: {e}")
