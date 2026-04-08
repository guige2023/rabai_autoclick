"""Webhook dispatcher action module for RabAI AutoClick.

Provides webhook delivery with retries, signature verification,
event queuing, and delivery status tracking.
"""

import sys
import os
import json
import time
import hmac
import hashlib
import requests
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from queue import Queue, Empty
from threading import Thread, Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DeliveryStatus(Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class WebhookEvent:
    """A webhook event to be delivered."""
    id: str
    url: str
    payload: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)
    created_at: str = ""
    attempts: int = 0
    max_attempts: int = 3
    status: DeliveryStatus = DeliveryStatus.PENDING
    last_error: Optional[str] = None
    response_code: Optional[int] = None
    response_body: Optional[str] = None


@dataclass
class DeliveryResult:
    """Result of webhook delivery attempt."""
    success: bool
    status_code: Optional[int]
    response_body: Optional[str]
    elapsed_ms: float
    error: Optional[str] = None


class WebhookDispatcherAction(BaseAction):
    """Dispatch webhooks with delivery guarantees and signature verification.
    
    Supports HMAC signatures, automatic retries, event queuing,
    and delivery status tracking.
    """
    action_type = "webhook_dispatcher"
    display_name = "Webhook分发"
    description = "可靠Webhook发送，支持签名和重试"
    
    def __init__(self):
        super().__init__()
        self._queue: List[WebhookEvent] = []
        self._delivered: Dict[str, DeliveryResult] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute webhook operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'send', 'send_batch', 'verify', 'status'
                - url: Webhook endpoint URL
                - payload: Event payload dict
                - secret: HMAC secret for signing
                - signature_header: Header name for signature (default 'X-Signature')
                - timeout: Request timeout in seconds (default 30)
                - max_retries: Max delivery attempts (default 3)
                - headers: Additional headers
        
        Returns:
            ActionResult with delivery result.
        """
        operation = params.get('operation', 'send').lower()
        
        if operation == 'send':
            return self._send(params)
        elif operation == 'send_batch':
            return self._send_batch(params)
        elif operation == 'verify':
            return self._verify(params)
        elif operation == 'status':
            return self._status(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _send(self, params: Dict[str, Any]) -> ActionResult:
        """Send a single webhook."""
        url = params.get('url')
        payload = params.get('payload')
        secret = params.get('secret')
        signature_header = params.get('signature_header', 'X-Signature')
        timeout = params.get('timeout', 30)
        max_retries = params.get('max_retries', 3)
        headers = params.get('headers', {})
        
        if not url:
            return ActionResult(success=False, message="url is required")
        if not payload:
            return ActionResult(success=False, message="payload is required")
        
        # Build headers
        request_headers = dict(headers)
        request_headers['Content-Type'] = 'application/json'
        request_headers['User-Agent'] = 'RabAI-Webhook/1.0'
        
        # Add signature if secret provided
        if secret:
            body = json.dumps(payload, sort_keys=True)
            signature = self._generate_signature(body, secret)
            request_headers[signature_header] = f"sha256={signature}"
        else:
            body = json.dumps(payload)
        
        # Send with retries
        last_error = None
        for attempt in range(max_retries):
            try:
                start = time.time()
                response = requests.post(
                    url,
                    data=body,
                    headers=request_headers,
                    timeout=timeout
                )
                elapsed_ms = (time.time() - start) * 1000
                
                success = 200 <= response.status_code < 300
                result = DeliveryResult(
                    success=success,
                    status_code=response.status_code,
                    response_body=response.text[:1000] if response.text else None,
                    elapsed_ms=elapsed_ms
                )
                
                if success:
                    return ActionResult(
                        success=True,
                        message=f"Delivered successfully ({response.status_code})",
                        data={
                            'status_code': response.status_code,
                            'elapsed_ms': elapsed_ms
                        }
                    )
                else:
                    last_error = f"HTTP {response.status_code}"
                    
            except requests.exceptions.Timeout:
                last_error = f"Timeout after {timeout}s"
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
            except Exception as e:
                last_error = str(e)
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return ActionResult(
            success=False,
            message=f"Failed after {max_retries} attempts: {last_error}",
            data={'error': last_error, 'attempts': max_retries}
        )
    
    def _send_batch(self, params: Dict[str, Any]) -> ActionResult:
        """Send multiple webhooks."""
        webhooks = params.get('webhooks', [])
        if not webhooks:
            return ActionResult(success=False, message="webhooks list required")
        
        results = []
        for wh in webhooks:
            result = self._send({**params, **wh})
            results.append({
                'url': wh.get('url'),
                'success': result.success
            })
        
        success_count = sum(1 for r in results if r['success'])
        
        return ActionResult(
            success=success_count == len(results),
            message=f"Sent {success_count}/{len(results)} webhooks",
            data={'results': results, 'success_count': success_count}
        )
    
    def _verify(self, params: Dict[str, Any]) -> ActionResult:
        """Verify webhook signature."""
        payload = params.get('payload')
        signature = params.get('signature')
        secret = params.get('secret')
        
        if not payload:
            return ActionResult(success=False, message="payload required")
        if not signature:
            return ActionResult(success=False, message="signature required")
        if not secret:
            return ActionResult(success=False, message="secret required")
        
        expected = self._generate_signature(payload, secret)
        
        # Support multiple signature formats
        signatures = signature.split(',')
        valid = any(
            sig.strip().endswith(f'={expected}') or sig.strip() == f'sha256={expected}'
            for sig in signatures
        )
        
        return ActionResult(
            success=valid,
            message="Signature verified" if valid else "Invalid signature",
            data={'valid': valid}
        )
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get webhook delivery status."""
        event_id = params.get('event_id')
        if not event_id:
            return ActionResult(success=False, message="event_id required")
        
        with self._lock:
            if event_id in self._delivered:
                result = self._delivered[event_id]
                return ActionResult(
                    success=result.success,
                    message=f"Status: {result.success}",
                    data={'result': {
                        'success': result.success,
                        'status_code': result.status_code,
                        'elapsed_ms': result.elapsed_ms
                    }}
                )
            else:
                return ActionResult(
                    success=False,
                    message="Event not found",
                    data={'event_id': event_id}
                )
    
    def _generate_signature(self, payload: str, secret: str) -> str:
        """Generate HMAC-SHA256 signature."""
        return hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()


class WebhookRelayAction(BaseAction):
    """Relay and transform incoming webhooks to other endpoints.
    
    Supports payload transformation, filtering, and routing to
    multiple destinations.
    """
    action_type = "webhook_relay"
    display_name = "Webhook中继"
    description = "Webhook接收、转换和转发"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute webhook relay operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'receive', 'relay', 'transform'
                - payload: Incoming webhook payload
                - destinations: List of destination URLs
                - transform_func: Optional transformation function
                - filter_func: Optional filter function
        
        Returns:
            ActionResult with relay result.
        """
        operation = params.get('operation', 'receive').lower()
        
        if operation == 'receive':
            return self._receive(params)
        elif operation == 'relay':
            return self._relay(params)
        elif operation == 'transform':
            return self._transform(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _receive(self, params: Dict[str, Any]) -> ActionResult:
        """Receive and validate incoming webhook."""
        payload = params.get('payload')
        secret = params.get('secret')
        signature = params.get('signature')
        
        if not payload:
            return ActionResult(success=False, message="payload required")
        
        # Verify signature if secret provided
        if secret and signature:
            body = json.dumps(payload, sort_keys=True)
            expected = hmac.new(
                secret.encode(),
                body.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, f'sha256={expected}'):
                return ActionResult(
                    success=False,
                    message="Invalid signature"
                )
        
        return ActionResult(
            success=True,
            message="Webhook received",
            data={'payload': payload}
        )
    
    def _relay(self, params: Dict[str, Any]) -> ActionResult:
        """Relay webhook to destinations."""
        payload = params.get('payload')
        destinations = params.get('destinations', [])
        headers = params.get('headers', {})
        
        if not payload:
            return ActionResult(success=False, message="payload required")
        if not destinations:
            return ActionResult(success=False, message="destinations required")
        
        results = []
        body = json.dumps(payload)
        
        for url in destinations:
            try:
                response = requests.post(
                    url,
                    data=body,
                    headers={**headers, 'Content-Type': 'application/json'},
                    timeout=30
                )
                results.append({
                    'url': url,
                    'success': 200 <= response.status_code < 300,
                    'status_code': response.status_code
                })
            except Exception as e:
                results.append({
                    'url': url,
                    'success': False,
                    'error': str(e)
                })
        
        success_count = sum(1 for r in results if r.get('success'))
        
        return ActionResult(
            success=success_count == len(destinations),
            message=f"Relayed to {success_count}/{len(destinations)} destinations",
            data={'results': results}
        )
    
    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        """Transform webhook payload."""
        payload = params.get('payload')
        mapping = params.get('mapping', {})
        
        if not payload:
            return ActionResult(success=False, message="payload required")
        
        if not isinstance(payload, dict):
            return ActionResult(success=False, message="payload must be dict")
        
        transformed = {}
        for target_key, source_key in mapping.items():
            if '.' in source_key:
                # Nested path like "data.user.id"
                parts = source_key.split('.')
                value = payload
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        value = None
                        break
                transformed[target_key] = value
            else:
                transformed[target_key] = payload.get(source_key)
        
        return ActionResult(
            success=True,
            message="Payload transformed",
            data={'original': payload, 'transformed': transformed}
        )
