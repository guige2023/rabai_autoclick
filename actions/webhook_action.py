"""Webhook action module for RabAI AutoClick.

Provides webhook operations:
- WebhookSendAction: Send webhook request
- WebhookListenAction: Start webhook listener
- WebhookServerAction: Start HTTP server for webhooks
- WebhookVerifyAction: Verify webhook signature
- WebhookRetryAction: Retry failed webhook
"""

import json
import hashlib
import hmac
import time
import os
import subprocess
from typing import Any, Dict, List, Optional
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebhookSendAction(BaseAction):
    """Send webhook request."""
    action_type = "webhook_send"
    display_name = "发送Webhook"
    description = "发送Webhook请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute send.

        Args:
            context: Execution context.
            params: Dict with url, method, body, headers, secret, output_var.

        Returns:
            ActionResult with response.
        """
        url = params.get('url', '')
        method = params.get('method', 'POST')
        body = params.get('body', '')
        headers = params.get('headers', {})
        secret = params.get('secret', '')
        output_var = params.get('output_var', 'webhook_response')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(url)
            resolved_method = context.resolve_value(method)
            resolved_body = context.resolve_value(body) if body else ''
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_secret = context.resolve_value(secret) if secret else ''
            resolved_timeout = context.resolve_value(timeout)

            if isinstance(resolved_body, dict):
                encoded_body = json.dumps(resolved_body).encode('utf-8')
                content_type = 'application/json'
            else:
                encoded_body = str(resolved_body).encode('utf-8')
                content_type = 'text/plain'

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method=resolved_method.upper()
            )
            request.add_header('Content-Type', content_type)

            # Add signature if secret provided
            if resolved_secret:
                signature = hmac.new(
                    resolved_secret.encode('utf-8'),
                    encoded_body,
                    hashlib.sha256
                ).hexdigest()
                request.add_header('X-Webhook-Signature', f'sha256={signature}')

            request.add_header('X-Webhook-Timestamp', str(int(time.time())))

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = resp.read().decode('utf-8')
                status = resp.status

                try:
                    data = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    data = response_body

                result = {'status': status, 'body': data}
                context.set(output_var, result)

                return ActionResult(
                    success=status < 400,
                    message=f"Webhook {resolved_method} -> {status}",
                    data={'status': status, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook发送失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'method': 'POST', 'body': '', 'headers': {}, 'secret': '',
            'output_var': 'webhook_response', 'timeout': 30
        }


class WebhookVerifyAction(BaseAction):
    """Verify webhook signature."""
    action_type = "webhook_verify"
    display_name = "验证Webhook签名"
    description = "验证Webhook请求签名"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute verify.

        Args:
            context: Execution context.
            params: Dict with payload, signature, secret, output_var.

        Returns:
            ActionResult with verification result.
        """
        payload = params.get('payload', '')
        signature = params.get('signature', '')
        secret = params.get('secret', '')
        output_var = params.get('output_var', 'webhook_valid')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_payload = context.resolve_value(payload) if payload else ''
            resolved_sig = context.resolve_value(signature) if signature else ''
            resolved_secret = context.resolve_value(secret) if secret else ''

            if isinstance(resolved_payload, dict):
                payload_bytes = json.dumps(resolved_payload).encode('utf-8')
            else:
                payload_bytes = str(resolved_payload).encode('utf-8')

            expected = hmac.new(
                resolved_secret.encode('utf-8'),
                payload_bytes,
                hashlib.sha256
            ).hexdigest()

            if resolved_sig.startswith('sha256='):
                resolved_sig = resolved_sig[7:]

            valid = hmac.compare_digest(expected, resolved_sig)
            context.set(output_var, valid)

            return ActionResult(
                success=True,
                message=f"签名验证: {'通过' if valid else '失败'}",
                data={'valid': valid, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"签名验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['payload', 'signature', 'secret']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'webhook_valid'}


class WebhookRetryAction(BaseAction):
    """Retry failed webhook."""
    action_type = "webhook_retry"
    display_name = "重试Webhook"
    description = "重试发送失败的Webhook"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute retry.

        Args:
            context: Execution context.
            params: Dict with webhook_data, max_retries, output_var.

        Returns:
            ActionResult with response.
        """
        webhook_data = params.get('webhook_data', {})
        max_retries = params.get('max_retries', 3)
        output_var = params.get('output_var', 'webhook_retry_response')

        valid, msg = self.validate_type(webhook_data, dict, 'webhook_data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(webhook_data)
            resolved_retries = context.resolve_value(max_retries)

            url = resolved_data.get('url', '')
            method = resolved_data.get('method', 'POST')
            body = resolved_data.get('body', '')
            headers = resolved_data.get('headers', {})
            secret = resolved_data.get('secret', '')

            if not url:
                return ActionResult(
                    success=False,
                    message="webhook_data中缺少url"
                )

            last_error = None
            for attempt in range(int(resolved_retries)):
                try:
                    import urllib.request

                    if isinstance(body, dict):
                        encoded_body = json.dumps(body).encode('utf-8')
                        content_type = 'application/json'
                    else:
                        encoded_body = str(body).encode('utf-8')
                        content_type = 'text/plain'

                    request = urllib.request.Request(
                        url,
                        data=encoded_body,
                        method=method.upper()
                    )
                    request.add_header('Content-Type', content_type)

                    if secret:
                        signature = hmac.new(
                            secret.encode('utf-8'),
                            encoded_body,
                            hashlib.sha256
                        ).hexdigest()
                        request.add_header('X-Webhook-Signature', f'sha256={signature}')

                    request.add_header('X-Webhook-Timestamp', str(int(time.time())))
                    request.add_header('X-Webhook-Retry', str(attempt + 1))

                    for k, v in headers.items():
                        request.add_header(k, str(v))

                    with urllib.request.urlopen(request, timeout=30) as resp:
                        response_body = resp.read().decode('utf-8')
                        status = resp.status

                        try:
                            data = json.loads(response_body)
                        except (json.JSONDecodeError, ValueError):
                            data = response_body

                        result = {'status': status, 'body': data, 'attempts': attempt + 1}
                        context.set(output_var, result)

                        return ActionResult(
                            success=True,
                            message=f"Webhook重试成功 (尝试 {attempt + 1})",
                            data=result
                        )

                except Exception as e:
                    last_error = str(e)
                    time.sleep(2 ** attempt)

            return ActionResult(
                success=False,
                message=f"Webhook重试全部失败: {last_error}",
                data={'error': last_error, 'attempts': resolved_retries}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook重试失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['webhook_data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_retries': 3, 'output_var': 'webhook_retry_response'}


class WebhookBatchSendAction(BaseAction):
    """Send multiple webhooks in batch."""
    action_type = "webhook_batch_send"
    display_name = "批量发送Webhook"
    description = "批量发送多个Webhook请求"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch send.

        Args:
            context: Execution context.
            params: Dict with webhooks, output_var.

        Returns:
            ActionResult with batch results.
        """
        webhooks = params.get('webhooks', [])
        output_var = params.get('output_var', 'webhook_batch_results')

        valid, msg = self.validate_type(webhooks, list, 'webhooks')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_webhooks = context.resolve_value(webhooks)

            if not isinstance(resolved_webhooks, list):
                return ActionResult(
                    success=False,
                    message="webhooks参数必须是列表"
                )

            results = []
            for i, wh in enumerate(resolved_webhooks):
                url = wh.get('url', '')
                method = wh.get('method', 'POST')
                body = wh.get('body', {})

                try:
                    import urllib.request

                    if isinstance(body, dict):
                        encoded_body = json.dumps(body).encode('utf-8')
                    else:
                        encoded_body = str(body).encode('utf-8')

                    request = urllib.request.Request(
                        url,
                        data=encoded_body,
                        method=method.upper()
                    )
                    request.add_header('Content-Type', 'application/json')

                    with urllib.request.urlopen(request, timeout=30) as resp:
                        results.append({
                            'index': i,
                            'url': url,
                            'success': True,
                            'status': resp.status
                        })
                except Exception as e:
                    results.append({
                        'index': i,
                        'url': url,
                        'success': False,
                        'error': str(e)
                    })

            success_count = sum(1 for r in results if r.get('success', False))

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"批量Webhook完成: {success_count}/{len(results)} 成功",
                data={'total': len(results), 'success': success_count, 'results': results, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"批量Webhook失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['webhooks']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'webhook_batch_results'}


class WebhookListenAction(BaseAction):
    """Listen for webhook (simple HTTP server)."""
    action_type = "webhook_listen"
    display_name = "监听Webhook"
    description = "启动Webhook监听服务器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute listen.

        Args:
            context: Execution context.
            params: Dict with port, path, output_var.

        Returns:
            ActionResult indicating server started.
        """
        port = params.get('port', 8080)
        path = params.get('path', '/webhook')
        output_var = params.get('output_var', 'webhook_payload')
        timeout = params.get('timeout', 60)

        valid, msg = self.validate_type(port, int, 'port')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_port = context.resolve_value(port)
            resolved_path = context.resolve_value(path)
            resolved_timeout = context.resolve_value(timeout)

            class WebhookHandler(BaseHTTPRequestHandler):
                webhook_payload = None

                def do_POST(self):
                    content_length = int(self.headers.get('Content-Length', 0))
                    body = self.rfile.read(content_length).decode('utf-8')

                    try:
                        data = json.loads(body)
                    except (json.JSONDecodeError, ValueError):
                        data = body

                    WebhookHandler.webhook_payload = data

                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'status': 'received'}).encode('utf-8'))

                def log_message(self, format, *args):
                    pass

            server = HTTPServer(('0.0.0.0', int(resolved_port)), WebhookHandler)

            # Run server in background thread
            def run_server():
                server.serve_forever()

            thread = Thread(target=run_server, daemon=True)
            thread.start()

            # Wait for webhook or timeout
            start = time.time()
            payload = None
            while time.time() - start < int(resolved_timeout):
                if WebhookHandler.webhook_payload is not None:
                    payload = WebhookHandler.webhook_payload
                    break
                time.sleep(0.5)

            server.shutdown()

            if payload is not None:
                context.set(output_var, payload)

                return ActionResult(
                    success=True,
                    message=f"收到Webhook: {str(payload)[:50]}",
                    data={'payload': payload, 'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Webhook监听超时 ({int(resolved_timeout)}s)"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook监听失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '/webhook', 'output_var': 'webhook_payload', 'timeout': 60}
