"""Supabase integration for RabAI AutoClick.

Provides actions to manage Supabase database, auth, storage, and realtime subscriptions.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SupabaseDBAction(BaseAction):
    """Execute queries against Supabase PostgreSQL database.

    Supports SELECT, INSERT, UPDATE, DELETE operations.
    """
    action_type = "supabase_db"
    display_name = "Supabase数据库"
    description = "执行Supabase PostgreSQL数据库操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute database operations on Supabase.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Supabase project URL
                - anon_key: Supabase anon key
                - service_key: Supabase service role key (for admin operations)
                - operation: select | insert | update | delete | rpc
                - table: Table name
                - filters: List of filter dicts {column, operator, value}
                - data: Dict of column: value for insert/update
                - select: Columns to select (default *)
                - limit: Row limit
                - order_by: Dict {column, ascending}

        Returns:
            ActionResult with query results.
        """
        import urllib.request
        import urllib.error

        url = params.get('url') or os.environ.get('SUPABASE_URL')
        anon_key = params.get('anon_key') or os.environ.get('SUPABASE_ANON_KEY')
        service_key = params.get('service_key') or os.environ.get('SUPABASE_SERVICE_KEY')
        table = params.get('table')

        if not url or not anon_key:
            return ActionResult(success=False, message="url and anon_key/service_key are required")

        # Use service key if provided for admin access
        key = service_key or anon_key
        operation = params.get('operation', 'select')

        headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }

        try:
            if operation == 'select':
                if not table:
                    return ActionResult(success=False, message="table is required for select")

                query_params = []
                select_cols = params.get('select', '*')
                query_params.append(f"select={select_cols}")

                if params.get('filters'):
                    for f in params['filters']:
                        col = f.get('column', '')
                        op = f.get('operator', 'eq')
                        val = f.get('value', '')
                        query_params.append(f"{col}={op}.{val}")

                if params.get('limit'):
                    query_params.append(f"limit={params['limit']}")

                if params.get('order_by'):
                    ob = params['order_by']
                    asc = 'asc' if ob.get('ascending', True) else 'desc'
                    query_params.append(f"order={ob['column']}.{asc}")

                query = '&'.join(query_params)
                req = urllib.request.Request(
                    f'{url}/rest/v1/{table}?{query}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    rows = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Selected {len(rows)} rows", data={'rows': rows, 'count': len(rows)})

            elif operation == 'insert':
                if not table or not params.get('data'):
                    return ActionResult(success=False, message="table and data are required for insert")

                req = urllib.request.Request(
                    f'{url}/rest/v1/{table}',
                    data=json.dumps(params['data']).encode('utf-8'),
                    method='POST',
                    headers={**headers, 'Prefer': 'return=representation'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Row inserted", data={'result': result})

            elif operation == 'update':
                if not table or not params.get('data'):
                    return ActionResult(success=False, message="table and data are required for update")

                filters = params.get('filters', [])
                if not filters:
                    return ActionResult(success=False, message="filters are required for update")

                query_params = []
                for f in filters:
                    col = f.get('column', '')
                    op = f.get('operator', 'eq')
                    val = f.get('value', '')
                    query_params.append(f"{col}={op}.{val}")

                query = '&'.join(query_params)
                req = urllib.request.Request(
                    f'{url}/rest/v1/{table}?{query}',
                    data=json.dumps(params['data']).encode('utf-8'),
                    method='PATCH',
                    headers={**headers, 'Prefer': 'return=representation'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Rows updated", data={'result': result})

            elif operation == 'delete':
                if not table:
                    return ActionResult(success=False, message="table is required for delete")

                filters = params.get('filters', [])
                if not filters:
                    return ActionResult(success=False, message="filters are required for delete")

                query_params = []
                for f in filters:
                    col = f.get('column', '')
                    op = f.get('operator', 'eq')
                    val = f.get('value', '')
                    query_params.append(f"{col}={op}.{val}")

                query = '&'.join(query_params)
                req = urllib.request.Request(
                    f'{url}/rest/v1/{table}?{query}',
                    method='DELETE',
                    headers={**headers, 'Prefer': 'return=representation'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Rows deleted", data={'result': result})

            elif operation == 'rpc':
                if not table:
                    return ActionResult(success=False, message="table (function name) is required for rpc")

                args = params.get('args', {})

                req = urllib.request.Request(
                    f'{url}/rest/v1/rpc/{table}',
                    data=json.dumps(args).encode('utf-8'),
                    method='POST',
                    headers={**headers, 'Prefer': 'return=representation'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="RPC executed", data={'result': result})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Supabase API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Supabase error: {str(e)}")


class SupabaseStorageAction(BaseAction):
    """Manage Supabase Storage buckets and files.

    Handles file upload, download, and bucket management.
    """
    action_type = "supabase_storage"
    display_name = "Supabase存储"
    description = "管理Supabase Storage存储桶和文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Supabase Storage.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Supabase project URL
                - anon_key: Supabase anon key
                - service_key: Supabase service role key
                - operation: upload | download | delete | list | create_bucket
                - bucket: Bucket name
                - path: File path in bucket
                - file_data: Base64 encoded file data (for upload)
                - content_type: MIME type

        Returns:
            ActionResult with storage data.
        """
        import urllib.request
        import urllib.error

        url = params.get('url') or os.environ.get('SUPABASE_URL')
        anon_key = params.get('anon_key') or os.environ.get('SUPABASE_ANON_KEY')
        service_key = params.get('service_key') or os.environ.get('SUPABASE_SERVICE_KEY')

        if not url or not service_key:
            return ActionResult(success=False, message="url and service_key are required for storage operations")

        operation = params.get('operation', 'list')
        bucket = params.get('bucket')

        headers = {
            'Authorization': f'Bearer {service_key}',
            'apikey': service_key,
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'create_bucket':
                if not bucket:
                    return ActionResult(success=False, message="bucket is required")

                payload = {
                    'id': bucket,
                    'name': bucket,
                    'public': params.get('public', False),
                }

                req = urllib.request.Request(
                    f'{url}/storage/v1/bucket',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Bucket {bucket} created", data=result)

            elif operation == 'upload':
                if not bucket or not params.get('path') or not params.get('file_data'):
                    return ActionResult(success=False, message="bucket, path, and file_data are required")

                import base64
                file_data = params['file_data']
                if isinstance(file_data, str):
                    file_bytes = base64.b64decode(file_data)
                else:
                    file_bytes = file_data

                content_type = params.get('content_type', 'application/octet-stream')
                upload_headers = {
                    'Authorization': f'Bearer {service_key}',
                    'apikey': service_key,
                    'Content-Type': content_type,
                    'x-upsert': str(params.get('upsert', False)).lower(),
                }

                req = urllib.request.Request(
                    f'{url}/storage/v1/object/{bucket}/{params["path"]}',
                    data=file_bytes,
                    method='POST',
                    headers=upload_headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"File uploaded to {bucket}/{params['path']}", data=result)

            elif operation == 'download':
                if not bucket or not params.get('path'):
                    return ActionResult(success=False, message="bucket and path are required")

                download_headers = {
                    'Authorization': f'Bearer {service_key}',
                    'apikey': service_key,
                }

                req = urllib.request.Request(
                    f'{url}/storage/v1/object/{bucket}/{params["path"]}',
                    headers=download_headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = resp.read()
                return ActionResult(success=True, message="File downloaded", data={'size': len(data), 'data': base64.b64encode(data).decode()})

            elif operation == 'list':
                if not bucket:
                    return ActionResult(success=False, message="bucket is required")

                prefix = params.get('prefix', '')
                req = urllib.request.Request(
                    f'{url}/storage/v1/object/list/{bucket}?prefix={prefix}',
                    headers=download_headers if 'download_headers' in dir() else {
                        'Authorization': f'Bearer {service_key}',
                        'apikey': service_key,
                    }
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    files = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(files)} files", data={'files': files})

            elif operation == 'delete':
                if not bucket or not params.get('path'):
                    return ActionResult(success=False, message="bucket and path are required")

                req = urllib.request.Request(
                    f'{url}/storage/v1/object/{bucket}/{params["path"]}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 200 else {}
                return ActionResult(success=True, message=f"File deleted from {bucket}", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Supabase API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Supabase error: {str(e)}")


class SupabaseAuthAction(BaseAction):
    """Manage Supabase Auth users and sessions.

    Handles user creation, authentication, and management.
    """
    action_type = "supabase_auth"
    display_name = "Supabase认证"
    description = "管理Supabase Auth用户和会话"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Supabase Auth.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Supabase project URL
                - anon_key: Supabase anon key
                - service_key: Supabase service role key
                - operation: signup | signin | signout | get_user | list_users | delete_user
                - email: User email
                - password: User password
                - phone: User phone
                - user_id: User ID (for delete)

        Returns:
            ActionResult with auth data.
        """
        import urllib.request
        import urllib.error

        url = params.get('url') or os.environ.get('SUPABASE_URL')
        anon_key = params.get('anon_key') or os.environ.get('SUPABASE_ANON_KEY')
        service_key = params.get('service_key') or os.environ.get('SUPABASE_SERVICE_KEY')

        if not url or not anon_key:
            return ActionResult(success=False, message="url and anon_key are required")

        operation = params.get('operation', 'signup')

        try:
            if operation == 'signup':
                email = params.get('email')
                password = params.get('password')
                phone = params.get('phone')

                if not email and not phone:
                    return ActionResult(success=False, message="email or phone is required")

                payload = {}
                if email:
                    payload['email'] = email
                if password:
                    payload['password'] = password
                if phone:
                    payload['phone'] = phone

                req = urllib.request.Request(
                    f'{url}/auth/v1/signup',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers={'apikey': anon_key, 'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="User signed up", data={'session': result.get('session')})

            elif operation == 'signin':
                email = params.get('email')
                phone = params.get('phone')
                password = params.get('password')

                payload = {}
                if email:
                    payload['email'] = email
                if phone:
                    payload['phone'] = phone
                if password:
                    payload['password'] = password

                req = urllib.request.Request(
                    f'{url}/auth/v1/token?grant_type=password',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers={'apikey': anon_key, 'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="User signed in", data={'session': result.get('session'), 'user': result.get('user')})

            elif operation == 'get_user':
                access_token = params.get('access_token')
                if not access_token:
                    return ActionResult(success=False, message="access_token is required")

                req = urllib.request.Request(
                    f'{url}/auth/v1/user',
                    headers={'Authorization': f'Bearer {access_token}', 'apikey': anon_key}
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="User retrieved", data={'user': result})

            elif operation == 'list_users':
                if not service_key:
                    return ActionResult(success=False, message="service_key is required for listing users")

                req = urllib.request.Request(
                    f'{url}/auth/v1/admin/users',
                    headers={'Authorization': f'Bearer {service_key}', 'apikey': service_key}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                users = result.get('users', [])
                return ActionResult(success=True, message=f"Found {len(users)} users", data={'users': users})

            elif operation == 'delete_user':
                if not service_key:
                    return ActionResult(success=False, message="service_key is required for deleting users")
                user_id = params.get('user_id')
                if not user_id:
                    return ActionResult(success=False, message="user_id is required")

                req = urllib.request.Request(
                    f'{url}/auth/v1/admin/users/{user_id}',
                    method='DELETE',
                    headers={'Authorization': f'Bearer {service_key}', 'apikey': service_key}
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 200 else {}
                return ActionResult(success=True, message="User deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Supabase API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Supabase error: {str(e)}")
