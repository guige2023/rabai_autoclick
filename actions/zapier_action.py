"""Zapier platform integration for RabAI AutoClick.

Provides actions to trigger Zapier webhooks and manage Zapier integrations.
"""

import json
import time
import sys
import os
import hashlib
import hmac
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ZapierWebhookAction(BaseAction):
    """Trigger a Zapier webhook to start a Zap.

    Supports authentication via bearer tokens or signature verification.
    """
    action_type = "zapier_webhook"
    display_name = "Zapier Webhook"
    description = "触发Zapier Webhook启动自动化流程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Trigger a Zapier webhook.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: The webhook URL from Zapier
                - data: Data to send (dict)
                - method: HTTP method (POST or GET)
                - headers: Optional custom headers
                - timeout: Request timeout in seconds

        Returns:
            ActionResult with webhook response.
        """
        import urllib.request
        import urllib.error

        webhook_url = params.get('webhook_url')
        data = params.get('data', {})
        method = params.get('method', 'POST').upper()
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")

        try:
            if method == 'GET':
                # Append data as query params
                query = '&'.join(f"{k}={str(v)}" for k, v in data.items())
                url = f"{webhook_url}?{query}" if query else webhook_url
                req = urllib.request.Request(url, method='GET')
            else:
                json_data = json.dumps(data).encode('utf-8')
                req = urllib.request.Request(webhook_url, data=json_data, method='POST')
                headers = {**headers, 'Content-Type': 'application/json'}

            for k, v in headers.items():
                req.add_header(k, v)

            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode('utf-8')
                return ActionResult(
                    success=True,
                    message="Webhook triggered successfully",
                    data={
                        'status_code': resp.status,
                        'body': body,
                        'headers': dict(resp.headers)
                    }
                )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8') if e.fp else ''
            return ActionResult(
                success=False,
                message=f"Webhook HTTP error: {e.code} {e.reason}",
                data={'status_code': e.code, 'body': body}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook error: {str(e)}")


class ZapierAuthAction(BaseAction):
    """Validate Zapier API key and fetch account info.

    Uses Zapier's authentication endpoint to verify credentials.
    """
    action_type = "zapier_auth"
    display_name = "Zapier认证"
    description = "验证Zapier API密钥并获取账户信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Authenticate with Zapier.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Zapier API key
                - check_scope: Whether to verify scopes

        Returns:
            ActionResult with auth status and user info.
        """
        import urllib.request
        import urllib.error

        api_key = params.get('api_key') or os.environ.get('ZAPIER_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="ZAPIER_API_KEY is required")

        try:
            req = urllib.request.Request(
                'https://api.zapier.com/v1/user',
                headers={'Authorization': f'Bearer {api_key}'}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                return ActionResult(
                    success=True,
                    message="Zapier authentication successful",
                    data=data
                )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"Auth failed: {e.code}",
                data={'status_code': e.code}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")


class ZapierZapListAction(BaseAction):
    """List all Zaps in a Zapier account.

    Requires a valid API key with read access.
    """
    action_type = "zapier_zap_list"
    display_name = "Zapier Zap列表"
    description = "列出账户中所有Zap"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """List Zaps from Zapier account.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Zapier API key
                - limit: Max results (default 50)
                - offset: Pagination offset

        Returns:
            ActionResult with list of Zaps.
        """
        import urllib.request
        import urllib.error

        api_key = params.get('api_key') or os.environ.get('ZAPIER_API_KEY')
        limit = params.get('limit', 50)
        offset = params.get('offset', 0)

        if not api_key:
            return ActionResult(success=False, message="ZAPIER_API_KEY is required")

        try:
            url = f"https://api.zapier.com/v1/zaps?limit={limit}&offset={offset}"
            req = urllib.request.Request(
                url,
                headers={'Authorization': f'Bearer {api_key}'}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                zaps = data.get('zaps', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(zaps)} Zaps",
                    data={'zaps': zaps, 'total': data.get('count', len(zaps))}
                )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"List Zaps failed: {e.code}",
                data={'status_code': e.code}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"List error: {str(e)}")


class ZapierZapRunAction(BaseAction):
    """Trigger a specific Zap by its ID and retrieve run status.

    Allows programmatic triggering and monitoring of Zap runs.
    """
    action_type = "zapier_zap_run"
    display_name = "Zapier Zap运行"
    description = "触发指定Zap并获取运行状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Trigger a Zap run.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Zapier API key
                - zap_id: The Zap ID to trigger
                - poll_interval: Seconds between status checks
                - max_polls: Maximum poll attempts

        Returns:
            ActionResult with run ID and status.
        """
        import urllib.request
        import urllib.error
        import time as time_module

        api_key = params.get('api_key') or os.environ.get('ZAPIER_API_KEY')
        zap_id = params.get('zap_id')
        poll_interval = params.get('poll_interval', 2)
        max_polls = params.get('max_polls', 15)

        if not api_key:
            return ActionResult(success=False, message="ZAPIER_API_KEY is required")
        if not zap_id:
            return ActionResult(success=False, message="zap_id is required")

        try:
            # Trigger the zap
            req = urllib.request.Request(
                f'https://api.zapier.com/v1/zaps/{zap_id}/trigger',
                method='POST',
                headers={'Authorization': f'Bearer {api_key}'}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                trigger_data = json.loads(resp.read().decode('utf-8'))

            run_id = trigger_data.get('run_id')
            if not run_id:
                return ActionResult(success=False, message="No run_id returned from trigger")

            # Poll for completion
            for _ in range(max_polls):
                time_module.sleep(poll_interval)
                req2 = urllib.request.Request(
                    f'https://api.zapier.com/v1/zaps/runs/{run_id}',
                    headers={'Authorization': f'Bearer {api_key}'}
                )
                with urllib.request.urlopen(req2, timeout=15) as resp2:
                    run_data = json.loads(resp2.read().decode('utf-8'))

                status = run_data.get('status', '')
                if status in ('success', 'error', 'stopped'):
                    return ActionResult(
                        success=(status == 'success'),
                        message=f"Zap run {status}",
                        data=run_data
                    )

            return ActionResult(
                success=True,
                message="Zap triggered, polling timed out",
                data={'run_id': run_id, 'status': 'polling_timeout'}
            )
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"Zap run failed: {e.code}",
                data={'status_code': e.code}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Run error: {str(e)}")
