"""API Event Handler Action Module.

Provides event-driven API handling including webhooks, SSE,
event dispatching, and subscription management.
"""

import sys
import os
import json
import time
import hmac
import hashlib
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventDispatcherAction(BaseAction):
    """Dispatch and handle events with subscriber pattern.
    
    Supports event filtering, priority ordering, and async handling.
    """
    action_type = "event_dispatcher"
    display_name = "事件调度"
    description = "基于订阅者模式的事件调度，支持过滤和优先级排序"

    def __init__(self):
        super().__init__()
        self._subscribers: Dict[str, List[Dict]] = defaultdict(list)
        self._event_history: List[Dict] = []
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Dispatch an event to all subscribers.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - event_type: Type/name of the event.
                - event_data: Data to pass to subscribers.
                - filter: Optional filter condition.
                - async: Whether to dispatch asynchronously.
                - max_depth: Max propagation depth.
        
        Returns:
            ActionResult with dispatch results or error.
        """
        event_type = params.get('event_type', '')
        event_data = params.get('event_data', {})
        event_filter = params.get('filter', None)
        async_dispatch = params.get('async', False)
        max_depth = params.get('max_depth', 10)
        output_var = params.get('output_var', 'dispatch_result')

        if not event_type:
            return ActionResult(
                success=False,
                message="Parameter 'event_type' is required"
            )

        try:
            # Create event object
            event = {
                'type': event_type,
                'data': event_data,
                'timestamp': datetime.now().isoformat(),
                'depth': 0,
            }

            # Apply filter if provided
            if event_filter and not self._matches_filter(event, event_filter):
                context.variables[output_var] = {'dispatched': 0, 'skipped': True}
                return ActionResult(success=True, data={'dispatched': 0})

            # Get subscribers
            subscribers = self._subscribers.get(event_type, [])

            # Sort by priority (higher first)
            subscribers = sorted(subscribers, key=lambda x: x.get('priority', 0), reverse=True)

            results = []
            for subscriber in subscribers[:max_depth]:
                handler = subscriber.get('handler')
                if handler:
                    try:
                        if async_dispatch:
                            thread = threading.Thread(
                                target=self._call_handler,
                                args=(handler, event, subscriber.get('id'))
                            )
                            thread.start()
                            results.append({'id': subscriber.get('id'), 'async': True})
                        else:
                            result = self._call_handler(handler, event, subscriber.get('id'))
                            results.append(result)
                    except Exception as e:
                        results.append({
                            'id': subscriber.get('id'),
                            'error': str(e)
                        })

            # Record in history
            with self._lock:
                self._event_history.append({
                    'event': event,
                    'results': results,
                    'subscriber_count': len(subscribers)
                })
                # Keep last 1000 events
                if len(self._event_history) > 1000:
                    self._event_history = self._event_history[-1000:]

            context.variables[output_var] = {
                'dispatched': len(results),
                'event': event
            }
            return ActionResult(
                success=True,
                data={'dispatched': len(results), 'event': event},
                message=f"Event '{event_type}' dispatched to {len(results)} subscribers"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Event dispatch failed: {str(e)}"
            )

    def _call_handler(self, handler: Callable, event: Dict, handler_id: str) -> Dict:
        """Call an event handler."""
        try:
            result = handler(event['data'])
            return {'id': handler_id, 'success': True, 'result': result}
        except Exception as e:
            return {'id': handler_id, 'success': False, 'error': str(e)}

    def _matches_filter(self, event: Dict, filter_expr: Dict) -> bool:
        """Check if event matches a filter expression."""
        for key, expected in filter_expr.items():
            if key in event and event[key] != expected:
                return False
            if key in event.get('data', {}) and event['data'][key] != expected:
                return False
        return True


class EventSubscribeAction(BaseAction):
    """Subscribe a handler to an event type."""
    action_type = "event_subscribe"
    display_name = "事件订阅"
    description = "订阅指定类型的事件处理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Subscribe a handler to an event.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - event_type: Type of event to subscribe to.
                - handler: Handler function or var name.
                - handler_var: Context variable containing handler.
                - priority: Subscriber priority (higher = earlier).
                - subscriber_id: Unique subscriber identifier.
        
        Returns:
            ActionResult with subscription confirmation or error.
        """
        event_type = params.get('event_type', '')
        handler_var = params.get('handler_var', None)
        priority = params.get('priority', 0)
        subscriber_id = params.get('subscriber_id', f"sub_{int(time.time()*1000)}")

        if not event_type:
            return ActionResult(
                success=False,
                message="Parameter 'event_type' is required"
            )

        try:
            # Get handler from context if specified
            handler = None
            if handler_var:
                handler = context.variables.get(handler_var)

            if not handler:
                return ActionResult(
                    success=False,
                    message=f"Handler not found in context: {handler_var}"
                )

            # Register subscription
            dispatcher = self._get_dispatcher(context)
            dispatcher._subscribers[event_type].append({
                'id': subscriber_id,
                'handler': handler,
                'priority': priority,
                'subscribed_at': datetime.now().isoformat()
            })

            return ActionResult(
                success=True,
                data={'subscriber_id': subscriber_id, 'event_type': event_type},
                message=f"Subscribed '{subscriber_id}' to '{event_type}'"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Event subscribe failed: {str(e)}"
            )

    def _get_dispatcher(self, context: Any) -> EventDispatcherAction:
        """Get or create the event dispatcher."""
        if not hasattr(context, '_event_dispatcher'):
            context._event_dispatcher = EventDispatcherAction()
        return context._event_dispatcher


class WebhookReceiverAction(BaseAction):
    """Receive and validate incoming webhooks.
    
    Supports signature verification, IP filtering, and payload parsing.
    """
    action_type = "webhook_receiver"
    display_name = "Webhook接收"
    description = "接收并验证传入的Webhook请求，支持签名验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Process an incoming webhook request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - payload: Webhook payload (dict or JSON string).
                - headers: Request headers.
                - secret: Webhook secret for signature verification.
                - signature_header: Header containing signature.
                - verify_signature: Whether to verify signature.
                - allowed_ips: List of allowed IP addresses.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with validation result or error.
        """
        payload = params.get('payload', {})
        headers = params.get('headers', {})
        secret = params.get('secret', '')
        signature_header = params.get('signature_header', 'X-Signature')
        verify_signature = params.get('verify_signature', True)
        allowed_ips = params.get('allowed_ips', [])
        output_var = params.get('output_var', 'webhook_data')

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return ActionResult(
                    success=False,
                    message="Invalid JSON payload"
                )

        try:
            result = {
                'valid': True,
                'payload': payload,
                'headers': headers,
                'verified': False
            }

            # Verify signature
            if verify_signature and secret:
                signature = headers.get(signature_header, headers.get(signature_header.lower(), ''))
                if not signature:
                    result['valid'] = False
                    result['error'] = f"Missing signature header: {signature_header}"
                elif not self._verify_signature(payload, signature, secret):
                    result['valid'] = False
                    result['error'] = "Invalid signature"
                    result['verified'] = False
                else:
                    result['verified'] = True

            # Check IP whitelist
            if allowed_ips:
                client_ip = headers.get('X-Forwarded-For', '').split(',')[0].strip()
                client_ip = client_ip or headers.get('X-Real-IP', '0.0.0.0')
                if client_ip not in allowed_ips:
                    result['valid'] = False
                    result['error'] = f"IP {client_ip} not in allowed list"

            context.variables[output_var] = result
            return ActionResult(
                success=result['valid'],
                data=result,
                message="Webhook verified" if result['valid'] else result.get('error', 'Verification failed')
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook processing failed: {str(e)}"
            )

    def _verify_signature(self, payload: Any, signature: str, secret: str) -> bool:
        """Verify webhook signature using HMAC-SHA256."""
        try:
            if isinstance(payload, dict):
                payload_bytes = json.dumps(payload, separators=(',', ':')).encode()
            else:
                payload_bytes = str(payload).encode()

            expected = hmac.new(
                secret.encode(),
                payload_bytes,
                hashlib.sha256
            ).hexdigest()

            return hmac.compare_digest(f"sha256={expected}", signature)
        except Exception:
            return False


class SSEPublisherAction(BaseAction):
    """Publish Server-Sent Events (SSE) to clients.
    
    Supports event streaming, reconnection handling, and event filtering.
    """
    action_type = "sse_publisher"
    display_name = "SSE发布"
    description = "发布Server-Sent Events，支持事件流和重连处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Publish an SSE event.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - event: Event data to publish.
                - event_type: SSE event type (default: 'message').
                - event_id: Unique event ID for retry handling.
                - retry: Retry timeout in milliseconds.
                - comment: Comment line to include.
                - output_var: Variable name to store SSE format.
        
        Returns:
            ActionResult with SSE formatted string or error.
        """
        event_data = params.get('event', {})
        event_type = params.get('event_type', 'message')
        event_id = params.get('event_id', str(int(time.time() * 1000)))
        retry = params.get('retry', None)
        comment = params.get('comment', None)
        output_var = params.get('output_var', 'sse_data')

        try:
            lines = []

            # Add comment
            if comment:
                lines.append(f": {comment}")

            # Add retry
            if retry:
                lines.append(f"retry: {retry}")

            # Add event ID
            lines.append(f"id: {event_id}")

            # Add event type
            if event_type != 'message':
                lines.append(f"event: {event_type}")

            # Add data
            if isinstance(event_data, dict):
                data_lines = json.dumps(event_data, ensure_ascii=False).split('\n')
            else:
                data_lines = str(event_data).split('\n')

            for line in data_lines:
                lines.append(f"data: {line}")

            sse_output = '\n'.join(lines) + '\n\n'

            context.variables[output_var] = sse_output
            return ActionResult(
                success=True,
                data={'sse': sse_output, 'event_id': event_id},
                message=f"SSE event published: {event_id}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SSE publishing failed: {str(e)}"
            )


class EventBusAction(BaseAction):
    """Central event bus for pub/sub across the application.
    
    Provides a shared event bus with topic-based routing.
    """
    action_type = "event_bus"
    display_name = "事件总线"
    description = "应用内共享的事件总线，支持基于主题的路由"

    def __init__(self):
        super().__init__()
        self._topics: Dict[str, List[Callable]] = defaultdict(list)
        self._bus_lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Publish or subscribe to the event bus.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'publish', 'subscribe', 'unsubscribe'.
                - topic: Topic name.
                - message: Message to publish (for publish).
                - handler_var: Handler variable (for subscribe).
        
        Returns:
            ActionResult with operation result or error.
        """
        operation = params.get('operation', 'publish')
        topic = params.get('topic', '')
        message = params.get('message', {})
        handler_var = params.get('handler_var', None)

        if not topic:
            return ActionResult(
                success=False,
                message="Parameter 'topic' is required"
            )

        try:
            if operation == 'publish':
                return self._publish(topic, message, context)
            elif operation == 'subscribe':
                return self._subscribe(topic, handler_var, context)
            elif operation == 'unsubscribe':
                return self._unsubscribe(topic, handler_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Event bus operation failed: {str(e)}"
            )

    def _publish(self, topic: str, message: Dict, context: Any) -> ActionResult:
        """Publish a message to a topic."""
        handlers = self._topics.get(topic, [])
        results = []

        for handler in handlers:
            try:
                result = handler(message)
                results.append({'success': True, 'result': result})
            except Exception as e:
                results.append({'success': False, 'error': str(e)})

        return ActionResult(
            success=True,
            data={'topic': topic, 'delivered': len(handlers), 'results': results},
            message=f"Published to '{topic}': {len(handlers)} handlers"
        )

    def _subscribe(self, topic: str, handler_var: str, context: Any) -> ActionResult:
        """Subscribe a handler to a topic."""
        handler = context.variables.get(handler_var) if handler_var else None
        if not handler:
            return ActionResult(
                success=False,
                message=f"Handler not found: {handler_var}"
            )

        with self._bus_lock:
            self._topics[topic].append(handler)

        return ActionResult(
            success=True,
            data={'topic': topic, 'subscribed': True},
            message=f"Subscribed to topic '{topic}'"
        )

    def _unsubscribe(self, topic: str, handler_var: str) -> ActionResult:
        """Unsubscribe a handler from a topic."""
        handler = None
        if handler_var and hasattr(self, '_context'):
            handler = self._context.variables.get(handler_var)

        with self._bus_lock:
            if topic in self._topics and handler:
                try:
                    self._topics[topic].remove(handler)
                except ValueError:
                    pass

        return ActionResult(
            success=True,
            data={'topic': topic, 'unsubscribed': True},
            message=f"Unsubscribed from topic '{topic}'"
        )
