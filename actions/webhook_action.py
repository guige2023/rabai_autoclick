"""Webhook action module for RabAI AutoClick.

Provides webhook operations:
- WebhookReceiveAction: Receive webhook payload
- WebhookSendAction: Send webhook notification
- WebhookVerifyAction: Verify webhook signature
- WebhookFilterAction: Filter webhook events
"""

from __future__ import annotations

import sys
import os
import hmac
import hashlib
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebhookReceiveAction(BaseAction):
    """Receive webhook payload."""
    action_type = "webhook_receive"
    display_name = "Webhook接收"
    description = "接收Webhook载荷"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook receive."""
        payload_var = params.get('payload_var', 'webhook_payload')
        headers_var = params.get('headers_var', 'webhook_headers')
        output_var = params.get('output_var', 'webhook_data')

        try:
            result = {
                'payload': {},
                'headers': {},
                'received': True,
            }

            return ActionResult(
                success=True,
                data={
                    output_var: result,
                    payload_var: {},
                    headers_var: {},
                },
                message="Webhook received"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook receive error: {e}")


class WebhookSendAction(BaseAction):
    """Send webhook notification."""
    action_type = "webhook_send"
    display_name = "Webhook发送"
    description = "发送Webhook通知"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook send."""
        url = params.get('url', '')
        payload = params.get('payload', {})
        headers = params.get('headers', {})
        secret = params.get('secret', '')
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'webhook_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests

            resolved_url = context.resolve_value(url) if context else url
            resolved_payload = context.resolve_value(payload) if context else payload
            resolved_headers = context.resolve_value(headers) if context else headers

            request_headers = {'Content-Type': 'application/json'}
            request_headers.update(resolved_headers)

            body = json.dumps(resolved_payload)

            if secret:
                signature = hmac.new(
                    secret.encode('utf-8'),
                    body.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                request_headers['X-Webhook-Signature'] = f'sha256={signature}'

            response = requests.post(
                resolved_url,
                data=body,
                headers=request_headers,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'sent': response.ok,
                'response': response.text[:500] if response.text else '',
            }

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"Webhook sent: {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook send error: {e}")


class WebhookVerifyAction(BaseAction):
    """Verify webhook signature."""
    action_type = "webhook_verify"
    display_name = "Webhook验证"
    description = "验证Webhook签名"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook verification."""
        payload = params.get('payload', '')
        signature = params.get('signature', '')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'sha256')
        output_var = params.get('output_var', 'verification_result')

        if not payload or not signature or not secret:
            return ActionResult(success=False, message="payload, signature, and secret are required")

        try:
            resolved_payload = context.resolve_value(payload) if context else payload
            resolved_signature = context.resolve_value(signature) if context else signature

            if isinstance(resolved_payload, dict):
                body = json.dumps(resolved_payload)
            else:
                body = str(resolved_payload)

            if algorithm == 'sha256':
                expected = hmac.new(
                    secret.encode('utf-8'),
                    body.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
            elif algorithm == 'sha1':
                expected = hmac.new(
                    secret.encode('utf-8'),
                    body.encode('utf-8'),
                    hashlib.sha1
                ).hexdigest()
            else:
                return ActionResult(success=False, message=f"Unsupported algorithm: {algorithm}")

            expected_sig = f"{algorithm}={expected}"
            verified = hmac.compare_digest(expected_sig, resolved_signature)

            result = {
                'verified': verified,
                'algorithm': algorithm,
                'signature_provided': resolved_signature[:50] if resolved_signature else '',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message="Signature verified" if verified else "Signature mismatch"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook verify error: {e}")


class WebhookFilterAction(BaseAction):
    """Filter webhook events."""
    action_type = "webhook_filter"
    display_name = "Webhook过滤"
    description = "过滤Webhook事件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook filter."""
        payload = params.get('payload', {})
        event_types = params.get('event_types', [])
        required_fields = params.get('required_fields', [])
        exclude_fields = params.get('exclude_fields', [])
        output_var = params.get('output_var', 'filtered_payload')

        if not payload:
            return ActionResult(success=False, message="payload is required")

        try:
            resolved_payload = context.resolve_value(payload) if context else payload

            if event_types:
                event_type = resolved_payload.get('type') or resolved_payload.get('event', '')
                if event_type not in event_types:
                    return ActionResult(
                        success=False,
                        message=f"Event type '{event_type}' not in allowed types"
                    )

            missing_fields = [f for f in required_fields if f not in resolved_payload]
            if missing_fields:
                return ActionResult(
                    success=False,
                    message=f"Missing required fields: {missing_fields}"
                )

            filtered = {k: v for k, v in resolved_payload.items() if k not in exclude_fields}

            result = {
                'filtered': filtered,
                'event_type': resolved_payload.get('type', resolved_payload.get('event', '')),
                'passed': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message="Webhook filtered successfully"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook filter error: {e}")
