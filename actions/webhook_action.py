"""Webhook action module for RabAI AutoClick.

Provides webhook trigger and handler actions for event-driven
automation workflows.
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
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WebhookTriggerAction(BaseAction):
    """Trigger a webhook (send POST/GET to webhook URL).
    
    Supports custom headers, JSON payload, HMAC signature
    for security, and retry on failure.
    """
    action_type = "webhook_trigger"
    display_name = "Webhook触发"
    description = "向指定URL发送Webhook请求，支持签名和重试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Trigger a webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - url: str (webhook endpoint)
                - method: str (POST/GET/PUT, default POST)
                - headers: dict
                - payload: dict or str
                - secret: str (for HMAC signature)
                - signature_header: str
                - timeout: int
                - retry_count: int
                - save_to_var: str
        
        Returns:
            ActionResult with webhook response.
        """
        url = params.get('url', '')
        method = params.get('method', 'POST').upper()
        headers = params.get('headers', {})
        payload = params.get('payload', {})
        secret = params.get('secret', '')
        signature_header = params.get('signature_header', 'X-Webhook-Signature')
        timeout = params.get('timeout', 10)
        retry_count = params.get('retry_count', 0)
        save_to_var = params.get('save_to_var', None)

        if not url:
            return ActionResult(success=False, message="Webhook URL is required")

        headers = dict(headers)
        body_bytes = None

        if payload:
            if isinstance(payload, dict):
                body_str = json.dumps(payload, ensure_ascii=False)
            else:
                body_str = str(payload)
            body_bytes = body_str.encode('utf-8')
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
        elif method != 'GET':
            body_bytes = b''

        # Add HMAC signature
        if secret and body_bytes:
            signature = hmac.new(
                secret.encode('utf-8'),
                body_bytes,
                hashlib.sha256
            ).hexdigest()
            headers[signature_header] = signature

        last_error = None
        for attempt in range(retry_count + 1):
            try:
                import urllib.request
                import urllib.error
                
                req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
                start = time.time()
                
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    elapsed_ms = int((time.time() - start) * 1000)
                    resp_body = resp.read()
                    try:
                        resp_data = json.loads(resp_body.decode('utf-8'))
                    except:
                        resp_data = resp_body.decode('utf-8', errors='replace')
                    
                    result = {
                        'status_code': resp.status,
                        'body': resp_data,
                        'elapsed_ms': elapsed_ms,
                        'attempt': attempt + 1,
                    }
                    
                    if save_to_var and context:
                        context.variables[save_to_var] = result
                    
                    return ActionResult(
                        success=True,
                        data=result,
                        message=f"Webhook delivered: HTTP {resp.status} in {elapsed_ms}ms"
                    )
            except urllib.error.HTTPError as e:
                last_error = f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')}"
            except Exception as e:
                last_error = str(e)
            
            if attempt < retry_count:
                time.sleep(0.5 * (attempt + 1))
        
        return ActionResult(
            success=False,
            message=f"Webhook failed after {retry_count + 1} attempts: {last_error}"
        )


class WebhookServerAction(BaseAction):
    """Start a local webhook server to receive events.
    
    Runs a lightweight HTTP server on a specified port,
    dispatches incoming requests to registered handlers.
    """
    action_type = "webhook_server"
    display_name = "Webhook服务"
    description = "启动本地Webhook服务器接收外部事件"

    _server = None
    _thread = None
    _handlers: Dict[str, Callable] = {}
    _server_lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Start or control a webhook server.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: str (start/stop/status)
                - port: int (default 8080)
                - host: str (default 0.0.0.0)
                - route: str (path to register)
                - handler_var: str (variable to store webhook data)
                - timeout: int (seconds to wait for event, 0=non-blocking)
        
        Returns:
            ActionResult with received event data.
        """
        action = params.get('action', 'start')
        port = params.get('port', 8080)
        host = params.get('host', '0.0.0.0')
        route = params.get('route', '/webhook')
        handler_var = params.get('handler_var', 'webhook_event')
        timeout = params.get('timeout', 30)

        if action == 'start':
            return self._start_server(port, host, route, handler_var, context)
        elif action == 'stop':
            return self._stop_server()
        elif action == 'status':
            return self._server_status()
        else:
            return ActionResult(success=False, message=f"Unknown action: {action}")

    def _start_server(self, port: int, host: str, route: str, handler_var: str, context: Any) -> ActionResult:
        """Start the webhook server."""
        with self._server_lock:
            if self._server is not None:
                return ActionResult(success=True, message=f"Server already running on {host}:{port}")

            # Event for synchronization
            event_received = threading.Event()
            received_data = [None]

            class _WebhookHandler(BaseHTTPRequestHandler):
                def log_message(self, format, *args):
                    pass  # Suppress logging

                def do_POST(self):
                    if self.path != route:
                        self.send_response(404)
                        self.end_headers()
                        return
                    
                    content_length = int(self.headers.get('Content-Length', 0))
                    body = self.rfile.read(content_length)
                    
                    try:
                        data = json.loads(body.decode('utf-8'))
                    except:
                        data = body.decode('utf-8', errors='replace')
                    
                    received_data[0] = {
                        'path': self.path,
                        'method': 'POST',
                        'headers': dict(self.headers),
                        'body': data,
                        'timestamp': datetime.now().isoformat(),
                    }
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'status': 'received'}).encode())
                    
                    event_received.set()

                def do_GET(self):
                    if self.path == route or self.path.startswith(route + '?'):
                        self.send_response(200)
                        self.send_header('Content-Type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'status': 'listening'}).encode())
                    else:
                        self.send_response(404)
                        self.end_headers()

            def _run_server():
                self._server = HTTPServer((host, port), _WebhookHandler)
                self._server.serve_forever()

            self._thread = threading.Thread(target=_run_server, daemon=True)
            self._thread.start()
            time.sleep(0.2)  # Give server time to start

            # Wait for event with timeout
            if timeout > 0:
                event_received.wait(timeout=timeout)
            
            if received_data[0] is not None:
                if context and handler_var:
                    context.variables[handler_var] = received_data[0]
                return ActionResult(
                    success=True,
                    data=received_data[0],
                    message=f"Webhook received: {received_data[0]['path']}"
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Server started on {host}:{port}, waiting for webhook..."
                )

    def _stop_server(self) -> ActionResult:
        """Stop the webhook server."""
        with self._server_lock:
            if self._server:
                self._server.shutdown()
                self._server = None
                self._thread = None
                return ActionResult(success=True, message="Server stopped")
            return ActionResult(success=False, message="Server not running")

    def _server_status(self) -> ActionResult:
        """Get server status."""
        if self._server:
            return ActionResult(success=True, message="Server is running")
        return ActionResult(success=True, message="Server is not running")


class WebhookSignatureAction(BaseAction):
    """Verify webhook HMAC signature.
    
    Validates the signature of incoming webhook requests
    to ensure authenticity.
    """
    action_type = "webhook_verify"
    display_name = "Webhook签名验证"
    description = "验证Webhook请求的HMAC签名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Verify a webhook signature.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - secret: str
                - payload: str or bytes (raw body)
                - signature: str (from header)
                - algorithm: str (sha256/sha512, default sha256)
                - header_name: str (default X-Webhook-Signature)
                - save_to_var: str
        
        Returns:
            ActionResult with verification result.
        """
        secret = params.get('secret', '')
        payload = params.get('payload', '')
        signature = params.get('signature', '')
        algorithm = params.get('algorithm', 'sha256')
        header_name = params.get('header_name', 'X-Webhook-Signature')
        save_to_var = params.get('save_to_var', 'signature_valid')

        if not secret:
            return ActionResult(success=False, message="Secret is required")
        if not signature:
            return ActionResult(success=False, message="Signature is required")

        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        elif not isinstance(payload, bytes):
            payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')

        alg = algorithm.lower().replace('-', '')
        if alg == 'sha256':
            expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
        elif alg == 'sha512':
            expected = hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest()
        else:
            expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

        # Support different signature formats
        sig_clean = signature
        if sig_clean.startswith('sha256='):
            sig_clean = sig_clean[7:]
        elif sig_clean.startswith('v1='):
            sig_clean = sig_clean[3:]

        valid = hmac.compare_digest(expected, sig_clean)

        if context and save_to_var:
            context.variables[save_to_var] = valid

        return ActionResult(
            success=True,
            data={'valid': valid, 'algorithm': algorithm},
            message=f"Signature {'valid' if valid else 'invalid'}"
        )
