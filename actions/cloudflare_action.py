"""Cloudflare integration for RabAI AutoClick.

Provides actions to manage DNS records, zones, firewall rules, and CDN purge on Cloudflare.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CloudflareDNSAction(BaseAction):
    """Manage Cloudflare DNS records.

    Handles DNS record CRUD and zone management.
    """
    action_type = "cloudflare_dns"
    display_name = "Cloudflare DNS"
    description = "管理Cloudflare DNS记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Cloudflare DNS.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Cloudflare API token
                - zone_id: Zone ID (or look up by name)
                - operation: list | create | update | delete | purge_cache
                - record_id: Record ID (for update/delete)
                - name: Record name
                - type: Record type (A, AAAA, CNAME, MX, TXT, etc.)
                - content: Record content (IP, hostname, etc.)
                - ttl: TTL in seconds
                - proxied: Whether to proxy through Cloudflare
                - priority: MX priority

        Returns:
            ActionResult with DNS data.
        """
        api_token = params.get('api_token') or os.environ.get('CLOUDFLARE_API_TOKEN')
        zone_id = params.get('zone_id')

        if not api_token:
            return ActionResult(success=False, message="CLOUDFLARE_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json',
        }

        try:
            if not zone_id:
                zone_name = params.get('zone_name') or os.environ.get('CLOUDFLARE_ZONE')
                if zone_name:
                    req = urllib.request.Request(
                        f'https://api.cloudflare.com/client/v4/zones?name={zone_name}',
                        headers=headers
                    )
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        zones = json.loads(resp.read().decode('utf-8')).get('result', [])
                    if zones:
                        zone_id = zones[0]['id']

            if not zone_id:
                return ActionResult(success=False, message="zone_id or zone_name is required")

            operation = params.get('operation', 'list')

            if operation == 'list':
                url = f'https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records'
                query_params = []
                if params.get('type'):
                    query_params.append(f'type={params["type"]}')
                if params.get('name'):
                    query_params.append(f'name={params["name"]}')
                if query_params:
                    url += '?' + '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                records = result.get('result', [])
                return ActionResult(success=True, message=f"Found {len(records)} DNS records", data={'records': records})

            elif operation == 'create':
                name = params.get('name')
                record_type = params.get('type', 'A')
                content = params.get('content')

                if not name or not content:
                    return ActionResult(success=False, message="name and content are required")

                payload = {
                    'name': name,
                    'type': record_type,
                    'content': content,
                    'ttl': params.get('ttl', 1),  # 1 = auto
                    'proxied': params.get('proxied', False),
                }
                if params.get('priority'):
                    payload['priority'] = params['priority']

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                record = result.get('result', {})
                return ActionResult(success=True, message=f"DNS record {name} created", data={'record_id': record.get('id')})

            elif operation == 'update':
                record_id = params.get('record_id')
                if not record_id:
                    return ActionResult(success=False, message="record_id is required")

                payload = {}
                for key in ['name', 'content', 'ttl', 'proxied']:
                    if key in params:
                        payload[key] = params[key]

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records/{record_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="DNS record updated", data={'record': result.get('result')})

            elif operation == 'delete':
                record_id = params.get('record_id')
                if not record_id:
                    return ActionResult(success=False, message="record_id is required")

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records/{record_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="DNS record deleted", data=result)

            elif operation == 'purge_cache':
                url = f'https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache'
                payload = {}
                if params.get('files'):
                    payload['files'] = params['files']
                elif params.get('prefixes'):
                    payload['prefixes'] = params['prefixes']

                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Cache purged", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Cloudflare API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Cloudflare error: {str(e)}")


class CloudflareFirewallAction(BaseAction):
    """Manage Cloudflare firewall rules and access policies.

    Handles WAF rules and IP filtering.
    """
    action_type = "cloudflare_firewall"
    display_name = "Cloudflare防火墙"
    description = "管理Cloudflare防火墙规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Cloudflare firewall rules.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Cloudflare API token
                - zone_id: Zone ID
                - operation: list | create | delete
                - rule_id: Rule ID (for delete)
                - expression: Firewall rule expression
                - description: Rule description
                - action: block | challenge | allow | js_challenge

        Returns:
            ActionResult with firewall data.
        """
        api_token = params.get('api_token') or os.environ.get('CLOUDFLARE_API_TOKEN')
        zone_id = params.get('zone_id')

        if not api_token or not zone_id:
            return ActionResult(success=False, message="CLOUDFLARE_API_TOKEN and zone_id are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json',
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/firewall/rules',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                rules = result.get('result', [])
                return ActionResult(success=True, message=f"Found {len(rules)} firewall rules", data={'rules': rules})

            elif operation == 'create':
                expression = params.get('expression')
                action = params.get('action', 'block')

                if not expression:
                    return ActionResult(success=False, message="expression is required")

                payload = [{
                    'expression': expression,
                    'description': params.get('description', ''),
                    'action': action,
                    'paused': params.get('paused', False),
                }]

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/firewall/rules',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                rule = result.get('result', [{}])[0]
                return ActionResult(success=True, message=f"Firewall rule created", data={'rule_id': rule.get('id')})

            elif operation == 'delete':
                rule_id = params.get('rule_id')
                if not rule_id:
                    return ActionResult(success=False, message="rule_id is required")

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/zones/{zone_id}/firewall/rules/{rule_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Firewall rule deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Cloudflare API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Cloudflare error: {str(e)}")


class CloudflareWorkersAction(BaseAction):
    """Manage Cloudflare Workers and KV namespaces.

    Handles worker scripts and KV storage operations.
    """
    action_type = "cloudflare_workers"
    display_name = "Cloudflare Workers"
    description = "管理Cloudflare Workers和KV存储"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Cloudflare Workers.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Cloudflare API token
                - account_id: Cloudflare account ID
                - operation: list_scripts | deploy_script | get_kv | put_kv | delete_kv
                - script_name: Worker script name
                - script_content: Worker script content
                - kv_namespace: KV namespace ID
                - kv_key: KV key
                - kv_value: KV value

        Returns:
            ActionResult with worker data.
        """
        api_token = params.get('api_token') or os.environ.get('CLOUDFLARE_API_TOKEN')
        account_id = params.get('account_id') or os.environ.get('CLOUDFLARE_ACCOUNT_ID')

        if not api_token or not account_id:
            return ActionResult(success=False, message="CLOUDFLARE_API_TOKEN and account_id are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/javascript',
        }

        try:
            operation = params.get('operation', 'list_scripts')

            if operation == 'list_scripts':
                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/accounts/{account_id}/workers/scripts',
                    headers={'Authorization': f'Bearer {api_token}'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                scripts = result.get('result', [])
                return ActionResult(success=True, message=f"Found {len(scripts)} worker scripts", data={'scripts': scripts})

            elif operation == 'deploy_script':
                script_name = params.get('script_name')
                script_content = params.get('script_content')

                if not script_name or not script_content:
                    return ActionResult(success=False, message="script_name and script_content are required")

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/accounts/{account_id}/workers/scripts/{script_name}',
                    data=script_content.encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Worker {script_name} deployed", data=result)

            elif operation == 'get_kv':
                kv_namespace = params.get('kv_namespace')
                kv_key = params.get('kv_key')

                if not kv_namespace or not kv_key:
                    return ActionResult(success=False, message="kv_namespace and kv_key are required")

                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/accounts/{account_id}/storage/kv/namespaces/{kv_namespace}/values/{kv_key}',
                    headers={'Authorization': f'Bearer {api_token}'}
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    value = resp.read().decode('utf-8')
                return ActionResult(success=True, message=f"KV value retrieved for {kv_key}", data={'value': value})

            elif operation == 'put_kv':
                kv_namespace = params.get('kv_namespace')
                kv_key = params.get('kv_key')
                kv_value = params.get('kv_value')

                if not all([kv_namespace, kv_key, kv_value]):
                    return ActionResult(success=False, message="kv_namespace, kv_key, and kv_value are required")

                data = json.dumps({'value': kv_value}).encode('utf-8') if isinstance(kv_value, (dict, list)) else kv_value.encode('utf-8')
                req = urllib.request.Request(
                    f'https://api.cloudflare.com/client/v4/accounts/{account_id}/storage/kv/namespaces/{kv_namespace}/values/{kv_key}',
                    data=data,
                    method='PUT',
                    headers={'Authorization': f'Bearer {api_token}', 'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"KV value set for {kv_key}", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Cloudflare API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Cloudflare error: {str(e)}")
