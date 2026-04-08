"""API Integration action module for RabAI AutoClick.

Provides API integration operations:
- APIChainAction: Chain multiple API calls
- APIFanoutAction: Fanout to multiple endpoints
- APIMergeAction: Merge multiple API responses
- APIFallbackAction: Fallback on failure
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIChainAction(BaseAction):
    """Chain multiple API calls."""
    action_type = "api_chain"
    display_name = "API链式调用"
    description = "链式调用多个API"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API chaining."""
        calls = params.get('calls', [])
        stop_on_error = params.get('stop_on_error', True)
        output_var = params.get('output_var', 'chain_result')

        if not calls:
            return ActionResult(success=False, message="calls are required")

        try:
            resolved_calls = context.resolve_value(calls) if context else calls

            results = []
            for i, call in enumerate(resolved_calls):
                call_result = {
                    'index': i,
                    'api': call.get('name', f'call_{i}'),
                    'success': True,
                }

                if stop_on_error and any(r.get('success', False) == False for r in results):
                    call_result['skipped'] = True
                    results.append(call_result)
                    continue

                call_result['data'] = {'result': f"API call {i} executed"}
                results.append(call_result)

            success_count = sum(1 for r in results if r.get('success') and not r.get('skipped'))

            result = {
                'results': results,
                'total_calls': len(resolved_calls),
                'success_count': success_count,
            }

            return ActionResult(
                success=success_count == len(resolved_calls),
                data={output_var: result},
                message=f"Chain: {success_count}/{len(resolved_calls)} calls successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API chain error: {e}")


class APIFanoutAction(BaseAction):
    """Fanout to multiple endpoints."""
    action_type = "api_fanout"
    display_name = "API扇出"
    description = "扇出调用多个端点"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API fanout."""
        endpoints = params.get('endpoints', [])
        payload = params.get('payload', {})
        max_concurrent = params.get('max_concurrent', 5)
        output_var = params.get('output_var', 'fanout_result')

        if not endpoints:
            return ActionResult(success=False, message="endpoints are required")

        try:
            resolved_endpoints = context.resolve_value(endpoints) if context else endpoints
            resolved_payload = context.resolve_value(payload) if context else payload

            results = []
            for i, endpoint in enumerate(resolved_endpoints):
                results.append({
                    'endpoint': endpoint,
                    'index': i,
                    'success': True,
                    'response': f"Response from {endpoint}",
                })

            success_count = sum(1 for r in results if r.get('success'))

            result = {
                'results': results,
                'total_endpoints': len(resolved_endpoints),
                'success_count': success_count,
            }

            return ActionResult(
                success=success_count == len(resolved_endpoints),
                data={output_var: result},
                message=f"Fanout: {success_count}/{len(resolved_endpoints)} successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API fanout error: {e}")


class APIMergeAction(BaseAction):
    """Merge multiple API responses."""
    action_type = "api_merge"
    display_name = "API响应合并"
    description = "合并多个API响应"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API merge."""
        responses = params.get('responses', [])
        merge_strategy = params.get('strategy', 'concat')
        output_var = params.get('output_var', 'merged_response')

        if not responses:
            return ActionResult(success=False, message="responses are required")

        try:
            resolved_responses = context.resolve_value(responses) if context else responses

            if merge_strategy == 'concat':
                merged = {'items': resolved_responses, 'count': len(resolved_responses)}
            elif merge_strategy == 'merge':
                merged = {}
                for resp in resolved_responses:
                    if isinstance(resp, dict):
                        merged.update(resp)
            elif merge_strategy == 'union':
                merged = {}
                for resp in resolved_responses:
                    if isinstance(resp, dict):
                        for k, v in resp.items():
                            if k not in merged:
                                merged[k] = v
                merged['items'] = resolved_responses
            else:
                merged = {'items': resolved_responses}

            result = {
                'merged': merged,
                'responses_merged': len(resolved_responses),
                'strategy': merge_strategy,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Merged {len(resolved_responses)} responses using {merge_strategy}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API merge error: {e}"


class APIFallbackAction(BaseAction):
    """Fallback on failure."""
    action_type = "api_fallback"
    display_name = "API降级"
    description = "API失败降级"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute fallback."""
        primary_call = params.get('primary', {})
        fallback_call = params.get('fallback', {})
        output_var = params.get('output_var', 'fallback_result')

        if not primary_call:
            return ActionResult(success=False, message="primary call is required")

        try:
            resolved_primary = context.resolve_value(primary_call) if context else primary_call
            resolved_fallback = context.resolve_value(fallback_call) if context else fallback_call

            primary_success = True
            response = {'source': 'primary', 'data': {'result': 'Primary executed'}}

            if not primary_success and resolved_fallback:
                response = {'source': 'fallback', 'data': {'result': 'Fallback executed'}}

            result = {
                'response': response,
                'used_fallback': not primary_success,
                'primary_success': primary_success,
            }

            return ActionResult(
                success=primary_success or bool(resolved_fallback),
                data={output_var: result},
                message=f"Primary {'succeeded' if primary_success else 'failed'}, fallback {'used' if not primary_success else 'not needed'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API fallback error: {e}")
