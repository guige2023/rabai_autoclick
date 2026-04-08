"""API Async action module for RabAI AutoClick.

Provides async API request handling with callbacks,
webhooks, and long-polling support.
"""

import json
import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, Future

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiAsyncClientAction(BaseAction):
    """Perform async API requests with callbacks and futures.

    Non-blocking API calls with result handling,
    timeout, and automatic retry.
    """
    action_type = "api_async_client"
    display_name = "API异步客户端"
    description = "异步API调用，支持回调和Future"

    _executor = ThreadPoolExecutor(max_workers=10)

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute async API request.

        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   callback, timeout, wait_for_result.

        Returns:
            ActionResult with future_id or result.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            method = params.get('method', 'GET').upper()
            headers = params.get('headers', {})
            body = params.get('body')
            callback = params.get('callback')
            timeout = params.get('timeout', 30)
            wait_for_result = params.get('wait_for_result', True)

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            def make_request() -> Dict:
                req_start = time.time()
                try:
                    body_bytes = None
                    if body:
                        if isinstance(body, str):
                            body_bytes = body.encode('utf-8')
                        elif isinstance(body, dict):
                            body_bytes = json.dumps(body).encode('utf-8')
                            headers.setdefault('Content-Type', 'application/json')
                        else:
                            body_bytes = body

                    req = Request(url, data=body_bytes, headers=headers, method=method)
                    with urlopen(req, timeout=timeout) as resp:
                        latency = int((time.time() - req_start) * 1000)
                        try:
                            resp_data = json.loads(resp.read())
                        except Exception:
                            resp_data = resp.read().decode('utf-8', errors='ignore')

                        result = {
                            'success': True,
                            'status': resp.status,
                            'data': resp_data,
                            'latency_ms': latency,
                        }

                        # Execute callback if provided
                        if callback and callable(callback):
                            try:
                                callback(result)
                            except Exception as e:
                                result['callback_error'] = str(e)

                        return result

                except HTTPError as e:
                    return {
                        'success': False,
                        'status': e.code,
                        'error': e.read().decode('utf-8', errors='ignore') if e.fp else str(e),
                        'latency_ms': int((time.time() - req_start) * 1000),
                    }
                except Exception as e:
                    return {
                        'success': False,
                        'error': str(e),
                        'latency_ms': int((time.time() - req_start) * 1000),
                    }

            future = self._executor.submit(make_request)

            if wait_for_result:
                try:
                    result = future.result(timeout=timeout)
                    duration = time.time() - start_time
                    return ActionResult(
                        success=result.get('success', False),
                        message=f"Async request {'OK' if result.get('success') else 'FAIL'}",
                        data=result,
                        duration=duration,
                    )
                except Exception as e:
                    duration = time.time() - start_time
                    return ActionResult(
                        success=False,
                        message=f"Async request error: {str(e)}",
                        duration=duration,
                    )
            else:
                # Return immediately with future ID
                future_id = id(future)
                if not hasattr(context, '_async_futures'):
                    context._async_futures = {}
                context._async_futures[future_id] = future

                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Async request submitted (id={future_id})",
                    data={'future_id': future_id, 'status': 'pending'},
                    duration=duration,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Async client error: {str(e)}",
                duration=duration,
            )


class ApiWebhookHandlerAction(BaseAction):
    """Handle incoming webhooks and dispatch to handlers.

    Validates webhook signatures, parses payloads,
    and routes to registered handlers.
    """
    action_type = "api_webhook_handler"
    display_name = "Webhook处理器"
    description = "处理传入的Webhook并分发给处理器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle a webhook.

        Args:
            context: Execution context.
            params: Dict with keys: payload, headers, signature_header,
                   secret, handlers (map of event_type to handler).

        Returns:
            ActionResult with handler results.
        """
        start_time = time.time()
        try:
            payload = params.get('payload')
            headers = params.get('headers', {})
            signature_header = params.get('signature_header', 'X-Signature')
            secret = params.get('secret')
            handlers = params.get('handlers', {})

            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    pass

            # Validate signature
            if secret and signature_header:
                signature = headers.get(signature_header, '')
                expected = self._compute_signature(str(payload), secret)
                if not self._verify_signature(signature, expected):
                    return ActionResult(
                        success=False,
                        message="Invalid webhook signature",
                        duration=time.time() - start_time,
                    )

            # Extract event type
            event_type = payload.get('type', payload.get('event', 'unknown')) if isinstance(payload, dict) else 'unknown'

            # Route to handler
            handler = handlers.get(event_type)
            if not handler:
                for pattern, h in handlers.items():
                    if pattern != '*' and event_type.startswith(pattern.replace('*', '')):
                        handler = h
                        break

            if not handler:
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"No handler for event type: {event_type}",
                    data={'event_type': event_type, 'handled': False},
                    duration=duration,
                )

            if callable(handler):
                try:
                    result = handler(payload, headers, context)
                    duration = time.time() - start_time
                    return ActionResult(
                        success=True,
                        message=f"Webhook handled: {event_type}",
                        data={'event_type': event_type, 'handled': True, 'result': result},
                        duration=duration,
                    )
                except Exception as e:
                    duration = time.time() - start_time
                    return ActionResult(
                        success=False,
                        message=f"Handler error: {str(e)}",
                        data={'event_type': event_type, 'handled': True, 'error': str(e)},
                        duration=duration,
                    )

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Handler registered for: {event_type}",
                data={'event_type': event_type, 'handled': False, 'result': None},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Webhook handler error: {str(e)}",
                duration=duration,
            )

    def _compute_signature(self, payload: str, secret: str) -> str:
        """Compute HMAC signature for payload."""
        import hmac
        import hashlib
        return hmac.new(secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

    def _verify_signature(self, signature: str, expected: str) -> bool:
        """Verify webhook signature using timing-safe comparison."""
        import hmac
        return hmac.compare_digest(signature, expected)


class ApiLongPollingAction(BaseAction):
    """Long-polling API client that waits for events.

    Repeatedly polls until condition is met or
    timeout expires.
    """
    action_type = "api_long_polling"
    display_name = "API长轮询"
    description = "长轮询API直到条件满足"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Long-poll for condition.

        Args:
            context: Execution context.
            params: Dict with keys: url, condition_fn, timeout,
                   poll_interval, max_attempts, headers.

        Returns:
            ActionResult with poll result.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            condition_fn = params.get('condition_fn')
            timeout = params.get('timeout', 60)
            poll_interval = params.get('poll_interval', 2)
            max_attempts = params.get('max_attempts', 30)
            headers = params.get('headers', {})

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            if not callable(condition_fn):
                return ActionResult(
                    success=False,
                    message="condition_fn must be callable",
                    duration=time.time() - start_time,
                )

            attempts = 0
            deadline = time.time() + timeout

            while attempts < max_attempts and time.time() < deadline:
                attempts += 1
                try:
                    req = Request(url, headers=headers)
                    with urlopen(req, timeout=30) as resp:
                        data = json.loads(resp.read())

                    if condition_fn(data, context):
                        duration = time.time() - start_time
                        return ActionResult(
                            success=True,
                            message=f"Condition met on attempt {attempts}",
                            data={'data': data, 'attempts': attempts, 'duration': duration},
                            duration=duration,
                        )

                except Exception as e:
                    pass

                if attempts < max_attempts and time.time() < deadline:
                    time.sleep(min(poll_interval, deadline - time.time()))

            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Long poll timeout after {attempts} attempts",
                data={'attempts': attempts, 'timeout': timeout},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Long polling error: {str(e)}",
                duration=duration,
            )
