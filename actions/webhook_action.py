"""Webhook action module for RabAI AutoClick.

Provides webhook sending, receiving, and verification actions
for event-driven automation workflows.
"""

import hashlib
import hmac
import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WebhookSendAction(BaseAction):
    """Send webhook HTTP POST request with signature verification support.
    
    Sends JSON payloads to webhook endpoints with optional HMAC signature
    for payload integrity verification.
    """
    action_type = "webhook_send"
    display_name = "发送Webhook"
    description = "向Webhook端点发送HTTP POST请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send a webhook.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - url: Webhook endpoint URL (required)
                - payload: Dict or JSON string payload to send (required)
                - secret: HMAC secret for signature (optional)
                - signature_header: Header name for signature (default X-Signature-256)
                - headers: Additional headers dict (optional)
                - timeout: Request timeout in seconds (default 10)
        
        Returns:
            ActionResult with webhook delivery status.
        """
        url = params.get('url', '')
        if not url:
            return ActionResult(success=False, message="url is required")
        
        payload = params.get('payload')
        if payload is None:
            return ActionResult(success=False, message="payload is required")
        
        secret = params.get('secret', '')
        signature_header = params.get('signature_header', 'X-Signature-256')
        timeout = params.get('timeout', 10)
        
        # Serialize payload
        if isinstance(payload, dict):
            body_str = json.dumps(payload, ensure_ascii=False)
        elif isinstance(payload, str):
            body_str = payload
        else:
            return ActionResult(success=False, message="payload must be dict or JSON string")
        
        body_bytes = body_str.encode('utf-8')
        
        # Build headers
        headers = {str(k): str(v) for k, v in params.get('headers', {}).items()}
        headers['Content-Type'] = 'application/json'
        headers['User-Agent'] = 'RabAI-AutoClick-Webhook/1.0'
        headers['X-Webhook-Timestamp'] = str(int(time.time()))
        
        # Add HMAC signature if secret provided
        if secret:
            timestamp = headers['X-Webhook-Timestamp']
            signed_payload = f"{timestamp}.{body_str}"
            signature = hmac.new(
                secret.encode('utf-8'),
                signed_payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers[signature_header] = f"sha256={signature}"
        
        try:
            request = Request(url, data=body_bytes, headers=headers, method='POST')
            start = time.time()
            with urlopen(request, timeout=timeout) as response:
                elapsed = time.time() - start
                response_body = response.read().decode('utf-8')
                parsed = response_body
                try:
                    parsed = json.loads(response_body)
                except json.JSONDecodeError:
                    pass
                
                return ActionResult(
                    success=response.status < 400,
                    message=f"Webhook delivered: HTTP {response.status} in {elapsed:.2f}s",
                    data={
                        'status_code': response.status,
                        'response': parsed,
                        'elapsed': elapsed,
                        'signature_sent': bool(secret)
                    }
                )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"Webhook failed: HTTP {e.code}",
                data={'status_code': e.code, 'error': str(e)}
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Webhook delivery error: {e.reason}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook error: {e}",
                data={'error': str(e)}
            )


class WebhookVerifyAction(BaseAction):
    """Verify incoming webhook signature.
    
    Validates HMAC signatures on incoming webhook requests
    to ensure payload authenticity.
    """
    action_type = "webhook_verify"
    display_name = "验证Webhook签名"
    description = "验证Webhook请求的HMAC签名"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify webhook signature.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - payload: Raw request body string or dict (required)
                - signature: Signature from request header (required)
                - secret: HMAC secret key (required)
                - signature_header: Expected signature header format (default X-Signature-256)
                - timestamp: Request timestamp (optional, for replay protection)
                - tolerance: Max age of request in seconds (default 300)
        
        Returns:
            ActionResult with verification result.
        """
        payload = params.get('payload')
        signature = params.get('signature', '')
        secret = params.get('secret', '')
        signature_header = params.get('signature_header', 'X-Signature-256')
        timestamp = params.get('timestamp', 0)
        tolerance = params.get('tolerance', 300)
        
        if not payload:
            return ActionResult(success=False, message="payload is required")
        if not signature:
            return ActionResult(success=False, message="signature is required")
        if not secret:
            return ActionResult(success=False, message="secret is required")
        
        # Get body string
        if isinstance(payload, dict):
            body_str = json.dumps(payload, ensure_ascii=False)
        else:
            body_str = str(payload)
        
        # Timestamp check for replay protection
        if timestamp > 0:
            current_time = int(time.time())
            if abs(current_time - int(timestamp)) > tolerance:
                return ActionResult(
                    success=False,
                    message=f"Timestamp outside tolerance window ({tolerance}s)",
                    data={'timestamp_mismatch': True}
                )
        
        # Parse signature (supports "sha256=xxx" format)
        sig_value = signature
        if '=' in signature:
            sig_value = signature.split('=', 1)[1]
        
        # Compute expected signature
        signed_payload = f"{timestamp}.{body_str}" if timestamp else body_str
        expected = hmac.new(
            secret.encode('utf-8'),
            signed_payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        is_valid = hmac.compare_digest(expected, sig_value)
        
        return ActionResult(
            success=is_valid,
            message="Signature verified" if is_valid else "Signature mismatch",
            data={
                'valid': is_valid,
                'algorithm': 'sha256',
                'timestamp_checked': timestamp > 0
            }
        )


class WebhookRetryAction(BaseAction):
    """Retry failed webhook deliveries with exponential backoff.
    
    Queues failed webhooks for automatic retry with configurable
    backoff strategy and max attempts.
    """
    action_type = "webhook_retry"
    display_name = "Webhook重试"
    description = "自动重试失败的Webhook发送"
    
    def __init__(self) -> None:
        super().__init__()
        self._queue: List[Dict[str, Any]] = []
        self._retry_counts: Dict[int, int] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retry webhook delivery.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - url: Webhook URL (required)
                - payload: Webhook payload dict (required)
                - secret: HMAC secret (optional)
                - max_retries: Max retry attempts (default 5)
                - base_delay: Base delay in seconds (default 2)
                - backoff_factor: Multiplier for each retry (default 2)
                - max_delay: Max delay cap in seconds (default 60)
                - headers: Additional headers (optional)
        
        Returns:
            ActionResult with retry status.
        """
        url = params.get('url', '')
        payload = params.get('payload')
        max_retries = params.get('max_retries', 5)
        base_delay = params.get('base_delay', 2)
        backoff_factor = params.get('backoff_factor', 2)
        max_delay = params.get('max_delay', 60)
        
        if not url:
            return ActionResult(success=False, message="url is required")
        if payload is None:
            return ActionResult(success=False, message="payload is required")
        
        headers = {str(k): str(v) for k, v in params.get('headers', {}).items()}
        secret = params.get('secret', '')
        
        attempt = self._retry_counts.get(id(params), 0)
        
        if isinstance(payload, dict):
            body_str = json.dumps(payload, ensure_ascii=False)
        else:
            body_str = str(payload)
        
        body_bytes = body_str.encode('utf-8')
        request_headers = dict(headers)
        request_headers['Content-Type'] = 'application/json'
        request_headers['X-Webhook-Retry'] = str(attempt)
        
        if secret:
            import base64
            sig_payload = body_str
            sig = hmac.new(secret.encode(), sig_payload.encode(), hashlib.sha256).hexdigest()
            request_headers['X-Signature-256'] = f"sha256={sig}"
        
        try:
            request = Request(url, data=body_bytes, headers=request_headers, method='POST')
            with urlopen(request, timeout=10) as response:
                self._retry_counts[id(params)] = 0
                return ActionResult(
                    success=True,
                    message=f"Webhook delivered on attempt {attempt + 1}",
                    data={'attempt': attempt + 1, 'status_code': response.status}
                )
        except Exception as e:
            self._retry_counts[id(params)] = attempt + 1
            
            if attempt + 1 >= max_retries:
                return ActionResult(
                    success=False,
                    message=f"Max retries ({max_retries}) exhausted: {e}",
                    data={'attempts': attempt + 1, 'error': str(e)}
                )
            
            delay = min(base_delay * (backoff_factor ** attempt), max_delay)
            return ActionResult(
                success=False,
                message=f"Retry {attempt + 1}/{max_retries} failed, next in {delay}s",
                data={
                    'attempts': attempt + 1,
                    'max_retries': max_retries,
                    'retry_delay': delay,
                    'error': str(e),
                    'can_retry': True
                }
            )
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current retry queue status."""
        return {
            'queued': len(self._queue),
            'retry_counts': dict(self._retry_counts)
        }
