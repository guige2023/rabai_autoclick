"""Webhook handler action module for RabAI AutoClick.

Handles incoming webhook requests with signature verification,
event parsing, and routing to appropriate handlers.
"""

import hashlib
import hmac
import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventType(Enum):
    """Supported webhook event types."""
    UNKNOWN = "unknown"
    PING = "ping"
    CONFIRMATION = "confirmation"
    NOTIFICATION = "notification"
    ACTION = "action"
    DATA_UPDATE = "data_update"
    ERROR = "error"


@dataclass
class WebhookEvent:
    """Parsed webhook event."""
    event_type: EventType
    event_id: Optional[str]
    timestamp: float
    data: Dict[str, Any]
    headers: Dict[str, str]
    raw_body: str
    signature: Optional[str] = None
    verified: bool = False


class WebhookHandlerAction(BaseAction):
    """Webhook handler action with signature verification.
    
    Validates webhook signatures using HMAC-SHA256,
    parses event types, and routes to registered handlers.
    """
    action_type = "webhook_handler"
    display_name = "Webhook处理器"
    description = "Webhook签名验证与事件路由"
    
    def __init__(self):
        super().__init__()
        self._handlers: Dict[EventType, List[Callable]] = {
            event_type: [] for event_type in EventType
        }
        self._secret: Optional[str] = None
        self._tolerance: int = 300
    
    def set_secret(self, secret: str, tolerance: int = 300) -> None:
        """Set webhook secret for signature verification.
        
        Args:
            secret: Shared secret for HMAC signature.
            tolerance: Time tolerance in seconds for timestamp validation.
        """
        self._secret = secret
        self._tolerance = tolerance
    
    def register_handler(self, event_type: EventType, handler: Callable) -> None:
        """Register a handler for a specific event type.
        
        Args:
            event_type: Type of event to handle.
            handler: Callable that takes WebhookEvent and returns ActionResult.
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process incoming webhook request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                body: Raw request body (string or bytes)
                headers: Request headers dict
                signature_header: Name of signature header (default 'X-Signature')
                timestamp_header: Name of timestamp header (default 'X-Timestamp')
                secret: Webhook secret (if not set via set_secret)
                tolerance: Time tolerance in seconds.
        
        Returns:
            ActionResult with parsed event and handler responses.
        """
        body = params.get('body', b'')
        headers = params.get('headers', {})
        raw_body = body.decode('utf-8', errors='replace') if isinstance(body, bytes) else body
        signature_header = params.get('signature_header', 'X-Signature')
        timestamp_header = params.get('timestamp_header', 'X-Timestamp')
        
        secret = params.get('secret') or self._secret
        tolerance = params.get('tolerance', self._tolerance)
        
        event = self._parse_event(raw_body, headers, signature_header, timestamp_header)
        
        if secret and event.signature:
            event.verified = self._verify_signature(raw_body, event.signature, secret, tolerance)
            if not event.verified:
                return ActionResult(
                    success=False,
                    message="Signature verification failed",
                    data={'event': self._event_to_dict(event)}
                )
        
        event_type = self._classify_event(event)
        event.event_type = event_type
        
        handler_results = self._dispatch_event(event)
        
        return ActionResult(
            success=True,
            message=f"Processed {event_type.value} event",
            data={
                'event': self._event_to_dict(event),
                'handler_count': len(handler_results),
                'handler_results': handler_results
            }
        )
    
    def _parse_event(
        self,
        raw_body: str,
        headers: Dict[str, str],
        sig_header: str,
        ts_header: str
    ) -> WebhookEvent:
        """Parse raw webhook into WebhookEvent."""
        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError:
            data = {'raw': raw_body}
        
        signature = headers.get(sig_header) or headers.get(sig_header.lower()) or headers.get(sig_header.upper())
        timestamp_str = headers.get(ts_header) or headers.get(ts_header.lower()) or headers.get(ts_header.upper())
        
        try:
            timestamp = float(timestamp_str) if timestamp_str else time.time()
        except (ValueError, TypeError):
            timestamp = time.time()
        
        event_id = data.get('event_id') or data.get('id') or headers.get('X-Event-ID')
        
        return WebhookEvent(
            event_type=EventType.UNKNOWN,
            event_id=event_id,
            timestamp=timestamp,
            data=data,
            headers=headers,
            raw_body=raw_body,
            signature=signature
        )
    
    def _verify_signature(
        self,
        body: str,
        signature: str,
        secret: str,
        tolerance: int
    ) -> bool:
        """Verify HMAC-SHA256 signature."""
        try:
            timestamp, sig = signature.split('t=')
            timestamp = float(timestamp)
        except (ValueError, AttributeError):
            return False
        
        if abs(time.time() - timestamp) > tolerance:
            return False
        
        expected = hmac.new(
            secret.encode('utf-8'),
            f"{timestamp}.{body}".encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(f"t={timestamp},{sig}", f"t={timestamp},{expected}")
    
    def _classify_event(self, event: WebhookEvent) -> EventType:
        """Classify event type from event data."""
        data = event.data
        event_id = str(event.event_id or '').lower()
        
        if event_id in ('ping', 'hook.verify') or data.get('type') == 'ping':
            return EventType.PING
        elif data.get('type') in ('confirmation', 'subscription_confirmation'):
            return EventType.CONFIRMATION
        elif data.get('type') == 'error' or data.get('error'):
            return EventType.ERROR
        elif 'action' in event_id or data.get('action'):
            return EventType.ACTION
        elif 'update' in event_id or 'change' in event_id:
            return EventType.DATA_UPDATE
        else:
            return EventType.NOTIFICATION
    
    def _dispatch_event(self, event: WebhookEvent) -> List[Dict[str, Any]]:
        """Dispatch event to registered handlers."""
        results = []
        handlers = self._handlers.get(event.event_type, []) + self._handlers.get(EventType.UNKNOWN, [])
        
        for handler in handlers:
            try:
                result = handler(event)
                if isinstance(result, ActionResult):
                    results.append({
                        'success': result.success,
                        'message': result.message,
                        'data': result.data
                    })
                else:
                    results.append({'success': True, 'result': result})
            except Exception as e:
                results.append({'success': False, 'error': str(e)})
        
        return results
    
    def _event_to_dict(self, event: WebhookEvent) -> Dict[str, Any]:
        """Convert WebhookEvent to dict."""
        return {
            'event_type': event.event_type.value,
            'event_id': event.event_id,
            'timestamp': event.timestamp,
            'data': event.data,
            'verified': event.verified,
            'signature': event.signature
        }
