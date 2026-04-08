"""Salesforce integration for RabAI AutoClick.

Provides actions to manage Salesforce CRM objects, SOQL queries, and REST API operations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SalesforceAuthAction(BaseAction):
    """Authenticate with Salesforce and manage session tokens.

    Supports username/password flow and refresh token flow.
    """
    action_type = "salesforce_auth"
    display_name = "Salesforce认证"
    description = "Salesforce OAuth认证和会话管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Authenticate with Salesforce.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: authenticate | refresh | revoke
                - client_id: Connected App consumer key
                - client_secret: Connected App consumer secret
                - username: Salesforce username
                - password: Salesforce password
                - security_token: Security token
                - login_url: My Domain login URL
                - refresh_token: For token refresh flow
                - instance_url: Current instance URL (for refresh/revoke)
                - access_token: Current access token (for refresh/revoke)

        Returns:
            ActionResult with auth tokens and instance info.
        """
        import urllib.request
        import urllib.error

        operation = params.get('operation', 'authenticate')

        try:
            if operation == 'authenticate':
                client_id = params.get('client_id') or os.environ.get('SF_CLIENT_ID')
                client_secret = params.get('client_secret') or os.environ.get('SF_CLIENT_SECRET')
                username = params.get('username') or os.environ.get('SF_USERNAME')
                password = params.get('password') or os.environ.get('SF_PASSWORD')
                security_token = params.get('security_token') or os.environ.get('SF_SECURITY_TOKEN', '')
                login_url = params.get('login_url') or os.environ.get('SF_LOGIN_URL', 'https://login.salesforce.com')

                if not all([client_id, client_secret, username, password]):
                    return ActionResult(success=False, message="client_id, client_secret, username, and password are required")

                data = {
                    'grant_type': 'password',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'username': username,
                    'password': password + security_token,
                }

                req = urllib.request.Request(
                    f'{login_url}/services/oauth2/token',
                    data=urllib.parse.urlencode(data).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                # Store tokens in context for reuse
                context._sf_access_token = result.get('access_token')
                context._sf_instance_url = result.get('instance_url')
                context._sf_refresh_token = result.get('refresh_token')

                return ActionResult(
                    success=True,
                    message="Salesforce authenticated",
                    data={
                        'instance_url': result.get('instance_url'),
                        'token_type': result.get('token_type'),
                    }
                )

            elif operation == 'refresh':
                client_id = params.get('client_id') or os.environ.get('SF_CLIENT_ID')
                client_secret = params.get('client_secret') or os.environ.get('SF_CLIENT_SECRET')
                refresh_token = params.get('refresh_token') or getattr(context, '_sf_refresh_token', None)
                login_url = params.get('login_url') or os.environ.get('SF_LOGIN_URL', 'https://login.salesforce.com')

                if not all([client_id, client_secret, refresh_token]):
                    return ActionResult(success=False, message="client_id, client_secret, and refresh_token are required")

                data = {
                    'grant_type': 'refresh_token',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'refresh_token': refresh_token,
                }

                req = urllib.request.Request(
                    f'{login_url}/services/oauth2/token',
                    data=urllib.parse.urlencode(data).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                context._sf_access_token = result.get('access_token')
                context._sf_instance_url = result.get('instance_url')

                return ActionResult(success=True, message="Token refreshed", data={'instance_url': result.get('instance_url')})

            elif operation == 'revoke':
                access_token = params.get('access_token') or getattr(context, '_sf_access_token', None)
                if not access_token:
                    return ActionResult(success=False, message="access_token is required")

                req = urllib.request.Request(
                    'https://login.salesforce.com/services/oauth2/revoke',
                    data=urllib.parse.urlencode({'token': access_token}).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                with urllib.request.urlopen(req, timeout=15):
                    pass

                context._sf_access_token = None
                context._sf_instance_url = None
                return ActionResult(success=True, message="Token revoked")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Salesforce auth error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Salesforce error: {str(e)}")


class SalesforceQueryAction(BaseAction):
    """Execute SOQL queries and SOSL searches against Salesforce.

    Supports SELECT, INSERT, UPDATE, DELETE operations.
    """
    action_type = "salesforce_query"
    display_name = "Salesforce查询"
    description = "执行SOQL查询和SOSL搜索"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SOQL/SOSL on Salesforce.

        Args:
            context: Execution context.
            params: Dict with keys:
                - access_token: Access token (or from context)
                - instance_url: Instance URL (or from context)
                - operation: query | search | insert | update | delete | upsert
                - soql: SOQL query string
                - sosl: SOSL search string
                - object_type: SObject type (for DML)
                - object_id: Record ID (for update/delete)
                - fields: Dict of field values (for insert/update/upsert)
                - external_id_field: Field name for upsert

        Returns:
            ActionResult with query results.
        """
        import urllib.parse

        access_token = params.get('access_token') or getattr(context, '_sf_access_token', None)
        instance_url = params.get('instance_url') or getattr(context, '_sf_instance_url', None)

        if not access_token or not instance_url:
            return ActionResult(success=False, message="Not authenticated with Salesforce")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            operation = params.get('operation', 'query')

            if operation == 'query':
                soql = params.get('soql')
                if not soql:
                    return ActionResult(success=False, message="soql is required for query")

                url = f'{instance_url}/services/data/v58.0/query?q={urllib.parse.quote(soql)}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                records = result.get('records', [])
                return ActionResult(
                    success=True,
                    message=f"Query returned {len(records)} records",
                    data={'records': records, 'totalSize': result.get('totalSize'), 'done': result.get('done')}
                )

            elif operation == 'search':
                sosl = params.get('sosl')
                if not sosl:
                    return ActionResult(success=False, message="sosl is required for search")

                url = f'{instance_url}/services/data/v58.0/search?q={urllib.parse.quote(sosl)}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                return ActionResult(
                    success=True,
                    message=f"Search returned {len(result)} results",
                    data={'results': result}
                )

            elif operation in ('insert', 'update', 'delete', 'upsert'):
                object_type = params.get('object_type')
                if not object_type:
                    return ActionResult(success=False, message="object_type is required for DML")

                object_id = params.get('object_id')
                fields = params.get('fields', {})

                if operation == 'insert':
                    url = f'{instance_url}/services/data/v58.0/sobjects/{object_type}'
                    method = 'POST'
                elif operation == 'update':
                    if not object_id:
                        return ActionResult(success=False, message="object_id is required for update")
                    url = f'{instance_url}/services/data/v58.0/sobjects/{object_type}/{object_id}'
                    method = 'PATCH'
                elif operation == 'delete':
                    if not object_id:
                        return ActionResult(success=False, message="object_id is required for delete")
                    url = f'{instance_url}/services/data/v58.0/sobjects/{object_type}/{object_id}'
                    method = 'DELETE'
                elif operation == 'upsert':
                    if not object_id:
                        return ActionResult(success=False, message="object_id is required for upsert")
                    external_id = params.get('external_id_field', 'ExternalId')
                    url = f'{instance_url}/services/data/v58.0/sobjects/{object_type}/{external_id}/{object_id}'
                    method = 'PATCH'

                req = urllib.request.Request(
                    url,
                    data=json.dumps(fields).encode('utf-8') if method != 'DELETE' else None,
                    method=method,
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 204 else {'success': True}

                return ActionResult(success=True, message=f"{operation.capitalize()} completed", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Salesforce API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Salesforce error: {str(e)}")


class SalesforceWorkflowAction(BaseAction):
    """Manage Salesforce workflows and approval processes.

    Handles process builder, flows, and approval actions.
    """
    action_type = "salesforce_workflow"
    display_name = "Salesforce工作流"
    description = "管理Salesforce工作流和审批流程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Salesforce workflows.

        Args:
            context: Execution context.
            params: Dict with keys:
                - access_token: Access token
                - instance_url: Instance URL
                - operation: submit_approval | approve | reject | recall | list_processes
                - object_type: SObject type
                - object_id: Record ID for approval
                - approver_id: Approver user ID
                - comments: Approval comments
                - next_approver: ID of next approver

        Returns:
            ActionResult with workflow result.
        """
        access_token = params.get('access_token') or getattr(context, '_sf_access_token', None)
        instance_url = params.get('instance_url') or getattr(context, '_sf_instance_url', None)

        if not access_token or not instance_url:
            return ActionResult(success=False, message="Not authenticated with Salesforce")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            operation = params.get('operation', 'list_processes')

            if operation == 'submit_approval':
                object_id = params.get('object_id')
                if not object_id:
                    return ActionResult(success=False, message="object_id is required")

                payload = {
                    'objectId': object_id,
                    'comments': params.get('comments', ''),
                }
                if params.get('approver_id'):
                    payload['approverIds'] = [params['approver_id']]
                if params.get('next_approver'):
                    payload['nextApproverIds'] = [params['next_approver']]

                req = urllib.request.Request(
                    f'{instance_url}/services/data/v58.0/process/approvals',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Approval submitted", data=result)

            elif operation == 'approve':
                workitem_id = params.get('workitem_id')
                if not workitem_id:
                    return ActionResult(success=False, message="workitem_id is required")

                payload = {
                    'action': 'Approve',
                    'comments': params.get('comments', 'Approved'),
                    'workitemId': workitem_id,
                }

                req = urllib.request.Request(
                    f'{instance_url}/services/data/v58.0/process/approvals',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Approval granted", data=result)

            elif operation == 'reject':
                workitem_id = params.get('workitem_id')
                if not workitem_id:
                    return ActionResult(success=False, message="workitem_id is required")

                payload = {
                    'action': 'Reject',
                    'comments': params.get('comments', 'Rejected'),
                    'workitemId': workitem_id,
                }

                req = urllib.request.Request(
                    f'{instance_url}/services/data/v58.0/process/approvals',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Approval rejected", data=result)

            elif operation == 'recall':
                process_instance_id = params.get('process_instance_id')
                if not process_instance_id:
                    return ActionResult(success=False, message="process_instance_id is required")

                payload = {'contextId': process_instance_id}

                req = urllib.request.Request(
                    f'{instance_url}/services/data/v58.0/process/approvals/recall',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Approval recalled", data=result)

            elif operation == 'list_processes':
                object_id = params.get('object_id')
                url = f'{instance_url}/services/data/v58.0/process/approvals'
                if object_id:
                    url += f'?contextId={object_id}'

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Processes retrieved", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Salesforce API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Salesforce error: {str(e)}")
