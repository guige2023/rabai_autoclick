"""Webhook action module for RabAI AutoClick.

Provides webhook handling actions for receiving and
processing incoming webhooks from external services.
"""

import hashlib
import hmac
import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WebhookReceiveAction(BaseAction):
    """Receive and validate incoming webhooks.
    
    Handles webhook verification and parsing.
    """
    action_type = "webhook_receive"
    display_name = "接收Webhook"
    description = "接收和处理Webhook请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Receive webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, headers, signature_header,
                   secret, algorithm, verify_signature.
        
        Returns:
            ActionResult with validated webhook data.
        """
        payload = params.get('payload', {})
        headers = params.get('headers', {})
        signature_header = params.get('signature_header', 'X-Signature')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'sha256')
        verify_signature = params.get('verify_signature', True)

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return ActionResult(success=False, message="Invalid JSON payload")

        is_valid = True
        if verify_signature and secret:
            signature = headers.get(signature_header, '')
            if not signature:
                is_valid = False
            else:
                expected = self._compute_signature(str(payload), secret, algorithm)
                is_valid = hmac.compare_digest(signature, expected)

        event_type = headers.get('X-Event-Type', headers.get('X-GitHub-Event', 'generic'))
        delivery_id = headers.get('X-Delivery-ID', headers.get('X-GitHub-Delivery', ''))

        return ActionResult(
            success=is_valid,
            message=f"Webhook received: {'valid' if is_valid else 'invalid signature'}",
            data={
                'valid': is_valid,
                'event_type': event_type,
                'delivery_id': delivery_id,
                'payload': payload,
                'headers': headers
            }
        )

    def _compute_signature(self, payload: str, secret: str, algorithm: str) -> str:
        """Compute webhook signature."""
        if algorithm == 'sha256':
            return hmac.new(
                secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
        elif algorithm == 'sha1':
            return hmac.new(
                secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha1
            ).hexdigest()
        return ''


class WebhookParseAction(BaseAction):
    """Parse webhook payload based on provider.
    
    Normalizes payloads from different webhook providers.
    """
    action_type = "webhook_parse"
    display_name = "Webhook解析"
    description = "解析Webhook提供商数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse webhook payload.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, provider, normalize.
                   providers: github, gitlab, slack, stripe, generic.
        
        Returns:
            ActionResult with parsed data.
        """
        payload = params.get('payload', {})
        provider = params.get('provider', 'generic')
        normalize = params.get('normalize', True)

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return ActionResult(success=False, message="Invalid JSON payload")

        parsed = {}

        if provider == 'github':
            parsed = self._parse_github(payload)
        elif provider == 'gitlab':
            parsed = self._parse_gitlab(payload)
        elif provider == 'slack':
            parsed = self._parse_slack(payload)
        elif provider == 'stripe':
            parsed = self._parse_stripe(payload)
        else:
            parsed = payload

        if normalize:
            normalized = {
                'event': parsed.get('event', 'unknown'),
                'action': parsed.get('action', ''),
                'data': parsed.get('data', parsed),
                'timestamp': parsed.get('timestamp', time.time())
            }
            parsed = normalized

        return ActionResult(
            success=True,
            message=f"Parsed {provider} webhook",
            data={'parsed': parsed, 'provider': provider}
        )

    def _parse_github(self, payload: Dict) -> Dict:
        """Parse GitHub webhook payload."""
        return {
            'event': 'github',
            'action': payload.get('action', ''),
            'data': {
                'repository': payload.get('repository', {}).get('full_name'),
                'sender': payload.get('sender', {}).get('login'),
                'ref': payload.get('ref'),
                'commits': len(payload.get('commits', []))
            }
        }

    def _parse_gitlab(self, payload: Dict) -> Dict:
        """Parse GitLab webhook payload."""
        return {
            'event': 'gitlab',
            'action': payload.get('object_kind', 'unknown'),
            'data': {
                'project': payload.get('project', {}).get('path_with_namespace'),
                'user': payload.get('user', {}).get('name')
            }
        }

    def _parse_slack(self, payload: Dict) -> Dict:
        """Parse Slack webhook payload."""
        return {
            'event': 'slack',
            'action': payload.get('type', 'event_callback'),
            'data': {
                'team': payload.get('team_id'),
                'user': payload.get('event', {}).get('user'),
                'text': payload.get('event', {}).get('text')
            }
        }

    def _parse_stripe(self, payload: Dict) -> Dict:
        """Parse Stripe webhook payload."""
        return {
            'event': 'stripe',
            'action': payload.get('type', 'unknown'),
            'data': {
                'id': payload.get('data', {}).get('object', {}).get('id'),
                'amount': payload.get('data', {}).get('object', {}).get('amount'),
                'currency': payload.get('data', {}).get('object', {}).get('currency')
            }
        }


class WebhookRespondAction(BaseAction):
    """Send webhook response.
    
    Generates webhook acknowledgement responses.
    """
    action_type = "webhook_respond"
    display_name = "Webhook响应"
    description = "发送Webhook响应"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send webhook response.
        
        Args:
            context: Execution context.
            params: Dict with keys: status_code, body, headers.
        
        Returns:
            ActionResult with response data.
        """
        status_code = params.get('status_code', 200)
        body = params.get('body', '')
        headers = params.get('headers', {})

        response = {
            'status_code': status_code,
            'body': body,
            'headers': headers,
            'timestamp': time.time()
        }

        return ActionResult(
            success=status_code < 400,
            message=f"Webhook response: {status_code}",
            data=response
        )


class WebhookRetryAction(BaseAction):
    """Retry failed webhook deliveries.
    
    Implements webhook retry logic with backoff.
    """
    action_type = "webhook_retry"
    display_name = "Webhook重试"
    description = "Webhook失败重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retry webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: webhook_url, payload, headers,
                   max_retries, backoff_base, secret.
        
        Returns:
            ActionResult with retry result.
        """
        webhook_url = params.get('webhook_url', '')
        payload = params.get('payload', {})
        headers = params.get('headers', {})
        max_retries = params.get('max_retries', 3)
        backoff_base = params.get('backoff_base', 2)
        secret = params.get('secret', '')

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")

        try:
            import urllib.request
            import urllib.error

            payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)
            
            if secret:
                signature = hmac.new(
                    secret.encode('utf-8'),
                    payload_str.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                headers['X-Signature'] = signature

            headers['Content-Type'] = headers.get('Content-Type', 'application/json')

            for attempt in range(max_retries):
                try:
                    data = payload_str.encode('utf-8')
                    req = urllib.request.Request(
                        webhook_url,
                        data=data,
                        headers=headers,
                        method='POST'
                    )

                    with urllib.request.urlopen(req, timeout=30) as response:
                        status = response.status
                        
                        if status < 400:
                            return ActionResult(
                                success=True,
                                message=f"Webhook delivered on attempt {attempt + 1}",
                                data={'attempts': attempt + 1, 'status': status}
                            )
                
                except urllib.error.HTTPError as e:
                    last_error = f"HTTP {e.code}"
                    if e.code >= 500:
                        continue
                    return ActionResult(
                        success=False,
                        message=f"Webhook failed: {last_error}",
                        data={'attempts': attempt + 1, 'error': last_error}
                    )
                
                except Exception as e:
                    last_error = str(e)

                if attempt < max_retries - 1:
                    wait_time = backoff_base ** attempt
                    time.sleep(wait_time)

            return ActionResult(
                success=False,
                message=f"Webhook failed after {max_retries} attempts: {last_error}",
                data={'attempts': max_retries, 'error': last_error}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook retry failed: {str(e)}")


class WebhookFilterAction(BaseAction):
    """Filter webhooks based on conditions.
    
    Applies filtering rules to webhook events.
    """
    action_type = "webhook_filter"
    display_name = "Webhook过滤"
    description = "Webhook条件过滤"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter webhook.
        
        Args:
            context: Execution context.
            params: Dict with keys: payload, rules, filter_action.
                   rules: list of {field, operator, value}.
        
        Returns:
            ActionResult with filter result.
        """
        payload = params.get('payload', {})
        rules = params.get('rules', [])
        filter_action = params.get('filter_action', 'include')

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return ActionResult(success=False, message="Invalid JSON payload")

        if not rules:
            return ActionResult(
                success=True,
                message="No rules, passing through",
                data={'passed': True, 'payload': payload}
            )

        matched = True
        for rule in rules:
            field = rule.get('field', '')
            operator = rule.get('operator', '==')
            value = rule.get('value')

            actual_value = self._get_nested_value(payload, field)
            
            rule_matched = self._evaluate_condition(actual_value, operator, value)
            
            if not rule_matched:
                matched = False
                break

        passes = matched if filter_action == 'include' else not matched

        return ActionResult(
            success=True,
            message=f"Webhook {'passed' if passes else 'filtered'}: {rules[0] if rules else 'no rules'}",
            data={
                'passed': passes,
                'matched': matched,
                'payload': payload if passes else None
            }
        )

    def _get_nested_value(self, obj: Any, path: str) -> Any:
        """Get nested value from object using dot notation."""
        if not path:
            return obj
        
        for key in path.split('.'):
            if isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
            if obj is None:
                return None
        return obj

    def _evaluate_condition(self, actual: Any, operator: str, expected: Any) -> bool:
        """Evaluate condition."""
        if operator == '==':
            return actual == expected
        elif operator == '!=':
            return actual != expected
        elif operator == '>':
            return float(actual) > float(expected)
        elif operator == '<':
            return float(actual) < float(expected)
        elif operator == '>=':
            return float(actual) >= float(expected)
        elif operator == '<=':
            return float(actual) <= float(expected)
        elif operator == 'in':
            return expected in str(actual)
        elif operator == 'contains':
            return str(expected) in str(actual)
        elif operator == 'startswith':
            return str(actual).startswith(str(expected))
        elif operator == 'endswith':
            return str(actual).endswith(str(expected))
        elif operator == 'exists':
            return actual is not None
        elif operator == 'empty':
            return actual is None or actual == ''
        
        return False
