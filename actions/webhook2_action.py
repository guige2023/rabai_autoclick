"""Webhook delivery action module for RabAI AutoClick.

Provides webhook operations:
- WebhookDeliverAction: Deliver webhook POST
- WebhookDeliverGetAction: Deliver webhook GET
- WebhookBatchAction: Deliver batch webhooks
- WebhookRetryAction: Retry failed webhook delivery
"""

from __future__ import annotations

import json
import sys
import os
import time
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebhookDeliverAction(BaseAction):
    """Deliver webhook POST request."""
    action_type = "webhook_deliver"
    display_name = "Webhook发送"
    description = "发送Webhook请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook delivery."""
        url = params.get('url', '')
        payload = params.get('payload', {})
        headers = params.get('headers', {})
        method = params.get('method', 'POST')
        timeout = params.get('timeout', 30)
        retry_count = params.get('retry_count', 0)
        output_var = params.get('output_var', 'webhook_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_payload = context.resolve_value(payload) if context else payload
            resolved_headers = context.resolve_value(headers) if context else headers
            resolved_retry = context.resolve_value(retry_count) if context else retry_count

            last_error = None
            for attempt in range(resolved_retry + 1):
                try:
                    body = json.dumps(resolved_payload).encode('utf-8') if resolved_payload else None
                    headers_copy = {**resolved_headers, 'Content-Type': 'application/json', 'User-Agent': 'RabAI-Webhook/1.0'}

                    request = urllib.request.Request(resolved_url, data=body, headers=headers_copy, method=method.upper())
                    with urllib.request.urlopen(request, timeout=timeout) as resp:
                        content = resp.read().decode('utf-8')
                        try:
                            response_data = json.loads(content)
                        except json.JSONDecodeError:
                            response_data = content

                        result = {
                            'delivered': True,
                            'status_code': resp.status,
                            'response': response_data,
                            'attempt': attempt + 1,
                        }
                        if context:
                            context.set(output_var, result)
                        return ActionResult(success=True, message=f"Webhook delivered (attempt {attempt + 1})", data=result)
                except urllib.error.HTTPError as e:
                    last_error = str(e)
                    if attempt < resolved_retry:
                        time.sleep(2 ** attempt)
                except Exception as e:
                    last_error = str(e)
                    if attempt < resolved_retry:
                        time.sleep(2 ** attempt)

            return ActionResult(success=False, message=f"Webhook failed after {resolved_retry + 1} attempts: {last_error}")
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook delivery error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'payload': {}, 'headers': {}, 'method': 'POST', 'timeout': 30,
            'retry_count': 0, 'output_var': 'webhook_result'
        }


class WebhookDeliverGetAction(BaseAction):
    """Deliver webhook GET request."""
    action_type = "webhook_deliver_get"
    display_name = "Webhook GET"
    description = "发送Webhook GET请求"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook GET delivery."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'webhook_get_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            headers_copy = {**resolved_headers, 'User-Agent': 'RabAI-Webhook/1.0'}
            request = urllib.request.Request(resolved_url, headers=headers_copy, method='GET')

            with urllib.request.urlopen(request, timeout=timeout) as resp:
                content = resp.read().decode('utf-8')
                try:
                    response_data = json.loads(content)
                except json.JSONDecodeError:
                    response_data = content

                result = {'status_code': resp.status, 'response': response_data}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"GET {resolved_url} -> {resp.status}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook GET error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 30, 'output_var': 'webhook_get_result'}


class WebhookBatchAction(BaseAction):
    """Deliver batch webhooks."""
    action_type = "webhook_batch"
    display_name = "Webhook批量发送"
    description = "批量发送Webhook"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook batch delivery."""
        webhooks = params.get('webhooks', [])  # [{url, payload, method}]
        max_concurrent = params.get('max_concurrent', 5)
        output_var = params.get('output_var', 'webhook_batch_result')

        if not webhooks:
            return ActionResult(success=False, message="webhooks is required")

        try:
            import urllib.request
            import concurrent.futures

            resolved_webhooks = context.resolve_value(webhooks) if context else webhooks

            def deliver(wb):
                url = wb.get('url', '')
                payload = wb.get('payload', {})
                method = wb.get('method', 'POST')
                headers = wb.get('headers', {})
                headers_copy = {**headers, 'Content-Type': 'application/json'}

                body = json.dumps(payload).encode('utf-8') if payload else None
                request = urllib.request.Request(url, data=body, headers=headers_copy, method=method.upper())

                try:
                    with urllib.request.urlopen(request, timeout=30) as resp:
                        return {'success': True, 'url': url, 'status': resp.status}
                except Exception as e:
                    return {'success': False, 'url': url, 'error': str(e)}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                results = list(executor.map(deliver, resolved_webhooks))

            success_count = sum(1 for r in results if r.get('success', False))
            if context:
                context.set(output_var, results)
            return ActionResult(
                success=success_count == len(results),
                message=f"Webhook batch: {success_count}/{len(results)} succeeded",
                data={'results': results, 'success_count': success_count, 'total': len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook batch error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['webhooks']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_concurrent': 5, 'output_var': 'webhook_batch_result'}


class WebhookStatusAction(BaseAction):
    """Check webhook delivery status."""
    action_type = "webhook_status"
    display_name = "Webhook状态"
    description = "检查Webhook状态"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook status check."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'webhook_status_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import urllib.request

            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            headers_copy = {**resolved_headers, 'User-Agent': 'RabAI-Webhook/1.0'}
            request = urllib.request.Request(resolved_url, headers=headers_copy, method='HEAD')

            start_time = time.time()
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                elapsed = round((time.time() - start_time) * 1000, 2)

            result = {
                'reachable': True,
                'status_code': resp.status,
                'elapsed_ms': elapsed,
                'headers': dict(resp.headers),
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Webhook reachable: {resp.status}", data=result)
        except urllib.error.HTTPError as e:
            result = {'reachable': True, 'status_code': e.code, 'error': str(e)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Webhook returns HTTP {e.code}", data=result)
        except Exception as e:
            result = {'reachable': False, 'error': str(e)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=False, message=f"Webhook unreachable: {str(e)}", data=result)

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 10, 'output_var': 'webhook_status_result'}
