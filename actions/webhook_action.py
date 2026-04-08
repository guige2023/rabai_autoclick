"""Webhook action module for RabAI AutoClick.

Provides webhook handling including signature verification,
event parsing, and routing to handlers.
"""

import time
import hmac
import hashlib
import json
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WebhookReceiverAction(BaseAction):
    """Receive and parse incoming webhooks.
    
    Parses webhook payloads, validates signatures,
    extracts event metadata, and routes to handlers.
    """
    action_type = "webhook_receiver"
    display_name = "Webhook接收器"
    description = "接收并解析Webhook请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Receive and parse webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: request, secret, signature_header,
                   event_type_field, required_fields.
        
        Returns:
            ActionResult with parsed webhook data.
        """
        request = params.get('request', {})
        secret = params.get('secret', '')
        signature_header = params.get('signature_header', 'X-Signature')
        event_type_field = params.get('event_type_field', 'event')
        required_fields = params.get('required_fields', [])
        start_time = time.time()

        headers = request.get('headers', {})
        body = request.get('body', {})

        if not isinstance(body, dict):
            try:
                body = json.loads(body) if isinstance(body, str) else {}
            except:
                return ActionResult(
                    success=False,
                    message="Failed to parse webhook body as JSON"
                )

        if secret:
            signature = headers.get(signature_header, '')
            if not self._verify_signature(body, secret, signature):
                return ActionResult(
                    success=False,
                    message="Webhook signature verification failed",
                    data={'error': 'invalid_signature'}
                )

        missing_fields = [f for f in required_fields if f not in body]
        if missing_fields:
            return ActionResult(
                success=False,
                message=f"Missing required fields: {missing_fields}",
                data={'missing_fields': missing_fields}
            )

        event_type = body.get(event_type_field, 'unknown')
        return ActionResult(
            success=True,
            message=f"Webhook received: {event_type}",
            data={
                'event_type': event_type,
                'payload': body,
                'headers': headers,
                'verified': bool(secret)
            },
            duration=time.time() - start_time
        )

    def _verify_signature(self, body: Any, secret: str, signature: str) -> bool:
        """Verify webhook signature."""
        if not signature:
            return False
        try:
            body_str = json.dumps(body, sort_keys=True) if isinstance(body, dict) else str(body)
            expected = hmac.new(secret.encode(), body_str.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(f"sha256={expected}", signature)
        except:
            return False


class WebhookSenderAction(BaseAction):
    """Send webhook notifications to external endpoints.
    
    Sends HTTP POST requests with JSON payloads
    and optional HMAC signatures.
    """
    action_type = "webhook_sender"
    display_name = "Webhook发送器"
    description = "发送Webhook通知"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, payload, secret, headers,
                   timeout, retry_count.
        
        Returns:
            ActionResult with send result.
        """
        import urllib.request
        import urllib.parse

        url = params.get('url', '')
        payload = params.get('payload', {})
        secret = params.get('secret', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        retry_count = params.get('retry_count', 0)
        start_time = time.time()

        if not url:
            return ActionResult(success=False, message="url is required")

        body = json.dumps(payload).encode() if isinstance(payload, (dict, list)) else str(payload).encode()
        req_headers = {'Content-Type': 'application/json', **headers}

        if secret:
            signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
            req_headers['X-Signature'] = f"sha256={signature}"

        last_error = None
        for attempt in range(retry_count + 1):
            try:
                req = urllib.request.Request(url, data=body, headers=req_headers, method='POST')
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    response_body = resp.read().decode()
                    return ActionResult(
                        success=True,
                        message=f"Webhook sent successfully (attempt {attempt + 1})",
                        data={
                            'status_code': resp.status,
                            'response': response_body,
                            'attempt': attempt + 1
                        },
                        duration=time.time() - start_time
                    )
            except Exception as e:
                last_error = str(e)
                if attempt < retry_count:
                    time.sleep(2 ** attempt)

        return ActionResult(
            success=False,
            message=f"Webhook send failed after {retry_count + 1} attempts: {last_error}",
            data={'error': last_error, 'attempts': retry_count + 1}
        )


class WebhookRouterAction(BaseAction):
    """Route webhook events to appropriate handlers.
    
    Matches incoming event types to registered handlers
    and dispatches to the appropriate action.
    """
    action_type = "webhook_router"
    display_name = "Webhook路由"
    description = "路由Webhook事件到处理器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route webhook to handler.
        
        Args:
            context: Execution context.
            params: Dict with keys: event_type, payload, routes (list
                   of {event_pattern, handler_action, handler_params}).
        
        Returns:
            ActionResult with routing result.
        """
        event_type = params.get('event_type', '')
        payload = params.get('payload', {})
        routes = params.get('routes', [])
        start_time = time.time()

        if not routes:
            return ActionResult(
                success=True,
                message=f"No routes defined for event: {event_type}",
                data={'routed': False, 'event_type': event_type}
            )

        for route in routes:
            pattern = route.get('event_pattern', '*')
            if self._matches_pattern(event_type, pattern):
                handler_action = route.get('handler_action', '')
                handler_params = route.get('handler_params', {})
                merged_params = {**handler_params, 'payload': payload}

                result = self._execute_handler(handler_action, merged_params)
                return ActionResult(
                    success=result.success,
                    message=f"Routed to '{handler_action}' for event '{event_type}'",
                    data={
                        'routed': True,
                        'event_type': event_type,
                        'handler': handler_action,
                        'result': result.data
                    },
                    duration=time.time() - start_time
                )

        return ActionResult(
            success=True,
            message=f"No route matched for event: {event_type}",
            data={'routed': False, 'event_type': event_type}
        )

    def _matches_pattern(self, event_type: str, pattern: str) -> bool:
        """Check if event type matches route pattern."""
        if pattern == '*':
            return True
        if pattern.startswith('*') and pattern.endswith('*'):
            return pattern[1:-1] in event_type
        if pattern.startswith('*'):
            return event_type.endswith(pattern[1:])
        if pattern.endswith('*'):
            return event_type.startswith(pattern[:-1])
        return event_type == pattern

    def _execute_handler(
        self,
        action_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a handler action."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_name)
            if action:
                return action.execute(None, params)
        except ImportError:
            pass
        return ActionResult(success=False, message=f"Handler '{action_name}' not found")
