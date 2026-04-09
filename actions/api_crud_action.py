"""API CRUD Action Module.

Provides Create, Read, Update, Delete operations for API resources.
"""

import time
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APICreateAction(BaseAction):
    """Create resources via API.
    
    Handles POST requests to create new resources.
    """
    action_type = "api_create"
    display_name = "API创建资源"
    description = "通过API创建新资源"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, data, headers, auth.
        
        Returns:
            ActionResult with created resource.
        """
        url = params.get('url', '')
        data = params.get('data', {})
        headers = params.get('headers', {})
        auth = params.get('auth', {})
        
        if not url:
            return ActionResult(
                success=False,
                data=None,
                error="URL is required"
            )
        
        try:
            import urllib.request
            
            headers['Content-Type'] = headers.get('Content-Type', 'application/json')
            
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8') if isinstance(data, dict) else data,
                headers=headers,
                method='POST'
            )
            
            if auth:
                import base64
                credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                encoded = base64.b64encode(credentials.encode()).decode()
                req.add_header('Authorization', f'Basic {encoded}')
            
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read().decode()
                try:
                    result = json.loads(response_data)
                except:
                    result = response_data
                
                return ActionResult(
                    success=True,
                    data={
                        'resource': result,
                        'status_code': response.status,
                        'url': url
                    },
                    error=None
                )
                
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                data={'status_code': e.code},
                error=f"HTTP error: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Create failed: {str(e)}"
            )


class APIReadAction(BaseAction):
    """Read resources via API.
    
    Handles GET requests to retrieve resources.
    """
    action_type = "api_read"
    display_name = "API读取资源"
    description = "通过API读取资源"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, params, headers, auth.
        
        Returns:
            ActionResult with retrieved resource.
        """
        url = params.get('url', '')
        query_params = params.get('params', {})
        headers = params.get('headers', {})
        auth = params.get('auth', {})
        
        if not url:
            return ActionResult(
                success=False,
                data=None,
                error="URL is required"
            )
        
        try:
            import urllib.request
            import urllib.parse
            
            # Add query parameters
            if query_params:
                url_with_params = f"{url}?{urllib.parse.urlencode(query_params)}"
            else:
                url_with_params = url
            
            req = urllib.request.Request(url_with_params, headers=headers, method='GET')
            
            if auth:
                import base64
                credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                encoded = base64.b64encode(credentials.encode()).decode()
                req.add_header('Authorization', f'Basic {encoded}')
            
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read().decode()
                try:
                    result = json.loads(response_data)
                except:
                    result = response_data
                
                return ActionResult(
                    success=True,
                    data={
                        'resource': result,
                        'status_code': response.status,
                        'headers': dict(response.headers)
                    },
                    error=None
                )
                
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                data={'status_code': e.code},
                error=f"HTTP error: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Read failed: {str(e)}"
            )


class APIUpdateAction(BaseAction):
    """Update resources via API.
    
    Handles PUT/PATCH requests to update existing resources.
    """
    action_type = "api_update"
    display_name = "API更新资源"
    description = "通过API更新资源"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute update operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, data, method, headers, auth.
        
        Returns:
            ActionResult with updated resource.
        """
        url = params.get('url', '')
        data = params.get('data', {})
        method = params.get('method', 'PUT')
        headers = params.get('headers', {})
        auth = params.get('auth', {})
        
        if not url:
            return ActionResult(
                success=False,
                data=None,
                error="URL is required"
            )
        
        if method not in ('PUT', 'PATCH'):
            return ActionResult(
                success=False,
                data=None,
                error=f"Invalid method: {method}. Use PUT or PATCH"
            )
        
        try:
            import urllib.request
            
            headers['Content-Type'] = headers.get('Content-Type', 'application/json')
            
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8') if isinstance(data, dict) else data,
                headers=headers,
                method=method
            )
            
            if auth:
                import base64
                credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                encoded = base64.b64encode(credentials.encode()).decode()
                req.add_header('Authorization', f'Basic {encoded}')
            
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read().decode()
                try:
                    result = json.loads(response_data)
                except:
                    result = response_data
                
                return ActionResult(
                    success=True,
                    data={
                        'resource': result,
                        'status_code': response.status,
                        'method': method
                    },
                    error=None
                )
                
        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                data={'status_code': e.code},
                error=f"HTTP error: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Update failed: {str(e)}"
            )


class APIDeleteAction(BaseAction):
    """Delete resources via API.
    
    Handles DELETE requests to remove resources.
    """
    action_type = "api_delete"
    display_name = "API删除资源"
    description = "通过API删除资源"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, headers, auth.
        
        Returns:
            ActionResult with delete result.
        """
        url = params.get('url', '')
        headers = params.get('headers', {})
        auth = params.get('auth', {})
        
        if not url:
            return ActionResult(
                success=False,
                data=None,
                error="URL is required"
            )
        
        try:
            import urllib.request
            
            req = urllib.request.Request(url, headers=headers, method='DELETE')
            
            if auth:
                import base64
                credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                encoded = base64.b64encode(credentials.encode()).decode()
                req.add_header('Authorization', f'Basic {encoded}')
            
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = response.read().decode()
                try:
                    result = json.loads(response_data)
                except:
                    result = {'deleted': True}
                
                return ActionResult(
                    success=True,
                    data={
                        'result': result,
                        'status_code': response.status
                    },
                    error=None
                )
                
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return ActionResult(
                    success=True,
                    data={'status_code': 404, 'message': 'Resource not found'},
                    error=None
                )
            return ActionResult(
                success=False,
                data={'status_code': e.code},
                error=f"HTTP error: {e.code}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Delete failed: {str(e)}"
            )


class APIBatchCrudAction(BaseAction):
    """Batch CRUD operations via API.
    
    Executes multiple create/read/update/delete operations in a batch.
    """
    action_type = "api_batch_crud"
    display_name = "API批量操作"
    description = "批量执行API的CRUD操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch CRUD operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: operations, continue_on_error.
        
        Returns:
            ActionResult with batch results.
        """
        operations = params.get('operations', [])
        continue_on_error = params.get('continue_on_error', True)
        
        if not operations:
            return ActionResult(
                success=False,
                data=None,
                error="No operations specified"
            )
        
        results = []
        success_count = 0
        error_count = 0
        
        for i, op in enumerate(operations):
            op_type = op.get('type', '')
            op_params = op.get('params', {})
            
            try:
                if op_type == 'create':
                    action = APICreateAction()
                    result = action.execute(context, op_params)
                elif op_type == 'read':
                    action = APIReadAction()
                    result = action.execute(context, op_params)
                elif op_type == 'update':
                    action = APIUpdateAction()
                    result = action.execute(context, op_params)
                elif op_type == 'delete':
                    action = APIDeleteAction()
                    result = action.execute(context, op_params)
                else:
                    result = ActionResult(
                        success=False,
                        data=None,
                        error=f"Unknown operation type: {op_type}"
                    )
                
                results.append({
                    'index': i,
                    'type': op_type,
                    'success': result.success,
                    'data': result.data,
                    'error': result.error
                })
                
                if result.success:
                    success_count += 1
                else:
                    error_count += 1
                    if not continue_on_error:
                        break
                        
            except Exception as e:
                results.append({
                    'index': i,
                    'type': op_type,
                    'success': False,
                    'error': str(e)
                })
                error_count += 1
                if not continue_on_error:
                    break
        
        return ActionResult(
            success=error_count == 0,
            data={
                'results': results,
                'total': len(operations),
                'success_count': success_count,
                'error_count': error_count
            },
            error=None if error_count == 0 else f"{error_count} operations failed"
        )


def register_actions():
    """Register all API CRUD actions."""
    return [
        APICreateAction,
        APIReadAction,
        APIUpdateAction,
        APIDeleteAction,
        APIBatchCrudAction,
    ]
