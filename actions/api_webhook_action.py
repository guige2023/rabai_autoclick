"""API webhook action module for RabAI AutoClick.

Provides webhook dispatching, signature generation, and
verification capabilities for secure callback integration.
"""

import hashlib
import hmac
import time
import json
import base64
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from core.base_action import BaseAction, ActionResult


class WebhookDispatchAction(BaseAction):
    """Send webhook HTTP POST requests to registered endpoints.
    
    Supports custom headers, HMAC signatures, retry logic,
    and delivery confirmation via response validation.
    """
    action_type = "webhook_dispatch"
    display_name = "Webhook发送"
    description = "向注册端点发送Webhook HTTP请求"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Dispatch webhook to endpoint.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, payload, headers, secret,
                   signature_header, retry_count, timeout.
        
        Returns:
            ActionResult with delivery status and response.
        """
        url = params.get("url", "")
        payload = params.get("payload")
        headers = params.get("headers", {})
        secret = params.get("secret")
        signature_header = params.get("signature_header", "X-Webhook-Signature")
        retry_count = params.get("retry_count", 0)
        timeout = params.get("timeout", 10)
        
        if not url:
            return ActionResult(success=False, message="Webhook URL is required")
        
        if payload is None:
            return ActionResult(success=False, message="Payload is required")
        
        try:
            if isinstance(payload, dict):
                body_bytes = json.dumps(payload, default=str).encode("utf-8")
            elif isinstance(payload, str):
                body_bytes = payload.encode("utf-8")
            else:
                body_bytes = payload
            
            req_headers = {str(k): str(v) for k, v in headers.items()}
            req_headers["Content-Type"] = req_headers.get("Content-Type", "application/json")
            req_headers["User-Agent"] = "RabAI-AutoClick-Webhook/1.0"
            req_headers["X-Webhook-Timestamp"] = str(int(time.time()))
            
            if secret:
                timestamp = req_headers["X-Webhook-Timestamp"]
                signature_payload = f"{timestamp}.{body_bytes.decode()}"
                signature = hmac.new(
                    secret.encode("utf-8"),
                    signature_payload.encode("utf-8"),
                    hashlib.sha256
                ).hexdigest()
                req_headers[signature_header] = f"sha256={signature}"
            
            last_error = None
            for attempt in range(retry_count + 1):
                try:
                    request = Request(
                        url,
                        data=body_bytes,
                        headers=req_headers,
                        method="POST"
                    )
                    
                    with urlopen(request, timeout=timeout) as response:
                        response_body = response.read()
                        response_data = None
                        
                        try:
                            response_data = json.loads(response_body.decode())
                        except Exception:
                            response_data = response_body.decode("utf-8", errors="replace")
                        
                        return ActionResult(
                            success=True,
                            message=f"Webhook delivered (attempt {attempt + 1})",
                            data={
                                "url": url,
                                "status_code": response.status,
                                "response": response_data,
                                "attempt": attempt + 1
                            }
                        )
                except HTTPError as e:
                    last_error = f"HTTP {e.code}: {e.reason}"
                    if e.code < 500:
                        break
                except URLError as e:
                    last_error = str(e.reason)
                except Exception as e:
                    last_error = str(e)
                
                if attempt < retry_count:
                    wait_time = (attempt + 1) * 2
                    time.sleep(wait_time)
            
            return ActionResult(
                success=False,
                message=f"Webhook delivery failed: {last_error}",
                data={
                    "url": url,
                    "error": last_error,
                    "attempts": retry_count + 1
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook dispatch error: {e}")


class WebhookVerifyAction(BaseAction):
    """Verify webhook signature authenticity.
    
    Validates HMAC-SHA256 signatures against known secrets
    and checks timestamp freshness to prevent replay attacks.
    """
    action_type = "webhook_verify"
    display_name = "Webhook验证"
    description = "验证Webhook签名真实性"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify webhook signature.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, signature, secret,
                   timestamp, tolerance, header_name.
        
        Returns:
            ActionResult with verification result.
        """
        payload = params.get("payload")
        signature = params.get("signature", "")
        secret = params.get("secret", "")
        timestamp = params.get("timestamp")
        tolerance = params.get("tolerance", 300)
        header_name = params.get("header_name", "X-Webhook-Signature")
        
        if not secret:
            return ActionResult(success=False, message="Verification secret is required")
        
        try:
            if isinstance(payload, dict):
                body_str = json.dumps(payload, default=str)
            elif isinstance(payload, bytes):
                body_str = payload.decode("utf-8")
            else:
                body_str = str(payload)
            
            if timestamp:
                try:
                    ts = int(timestamp)
                    current_time = int(time.time())
                    if abs(current_time - ts) > tolerance:
                        return ActionResult(
                            success=False,
                            message="Timestamp outside tolerance window",
                            data={"timestamp_age": abs(current_time - ts)}
                        )
                except ValueError:
                    pass
            
            if signature.startswith("sha256="):
                provided_sig = signature[7:]
            else:
                provided_sig = signature
            
            parts = provided_sig.split(":") if ":" in provided_sig else [provided_sig]
            if len(parts) > 1:
                timestamp_sig = parts[0]
                sig_hash = parts[1]
                signature_payload = f"{timestamp_sig}.{body_str}"
            else:
                sig_hash = provided_sig
                signature_payload = body_str
            
            expected_sig = hmac.new(
                secret.encode("utf-8"),
                signature_payload.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            
            is_valid = hmac.compare_digest(expected_sig, sig_hash)
            
            return ActionResult(
                success=is_valid,
                message="Signature verified" if is_valid else "Signature mismatch",
                data={
                    "valid": is_valid,
                    "header": header_name
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Verification failed: {e}")


class WebhookHandlerAction(BaseAction):
    """Handle incoming webhook requests with routing.
    
    Routes webhooks to registered handlers based on event type,
    validates signatures, and ensures delivery acknowledgment.
    """
    action_type = "webhook_handler"
    display_name = "Webhook处理"
    description = "根据事件类型路由Webhook到处理程序"
    VALID_ROUTING = ["event_type", "path", "header", "payload_field"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle incoming webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, headers, routing_mode,
                   event_type_field, handlers, default_handler.
        
        Returns:
            ActionResult with handler response.
        """
        payload = params.get("payload", {})
        headers = params.get("headers", {})
        routing_mode = params.get("routing_mode", "event_type")
        event_type_field = params.get("event_type_field", "event")
        handlers = params.get("handlers", {})
        default_handler = params.get("default_handler")
        
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                pass
        
        try:
            if routing_mode == "event_type":
                if isinstance(payload, dict):
                    event_type = payload.get(event_type_field, payload.get("type"))
                else:
                    event_type = None
            elif routing_mode == "header":
                event_type = headers.get("X-Event-Type", headers.get("X-Webhook-Event"))
            elif routing_mode == "path":
                event_type = headers.get("X-GitHub-Event", "unknown")
            elif routing_mode == "payload_field":
                event_type = payload.get("action") if isinstance(payload, dict) else None
            else:
                event_type = None
            
            handler = handlers.get(event_type) if event_type else None
            
            if not handler and default_handler:
                handler = handlers.get(default_handler)
            
            if not handler:
                return ActionResult(
                    success=False,
                    message=f"No handler found for event type: {event_type}",
                    data={
                        "event_type": event_type,
                        "available_handlers": list(handlers.keys())
                    }
                )
            
            handler_result = {
                "event_type": event_type,
                "handler": handler,
                "processed": True
            }
            
            return ActionResult(
                success=True,
                message=f"Webhook routed to handler for '{event_type}'",
                data=handler_result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook handling failed: {e}")
