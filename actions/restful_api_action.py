"""RESTful API action module for RabAI AutoClick.

Provides REST API operations:
- RestGetAction: GET request
- RestPostAction: POST request
- RestPutAction: PUT request
- RestDeleteAction: DELETE request
- RestPatchAction: PATCH request
- RestBatchAction: Batch multiple requests
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RestGetAction(BaseAction):
    """Perform GET request."""
    action_type = "rest_get"
    display_name = "REST GET"
    description = "发送GET请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GET request."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        params_dict = params.get('params', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'rest_response')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers
            resolved_params = context.resolve_value(params_dict) if context else params_dict

            response = requests.get(
                resolved_url,
                headers=resolved_headers,
                params=resolved_params,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'body': response.text,
                'json': None,
            }

            try:
                result['json'] = response.json()
                result['body'] = None
            except Exception:
                pass

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"GET {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GET error: {e}")


class RestPostAction(BaseAction):
    """Perform POST request."""
    action_type = "rest_post"
    display_name = "REST POST"
    description = "发送POST请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute POST request."""
        url = params.get('url', '')
        data = params.get('data', None)
        json_data = params.get('json', None)
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'rest_response')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            resolved_url = context.resolve_value(url) if context else url
            resolved_data = context.resolve_value(data) if context else data
            resolved_json = context.resolve_value(json_data) if context else json_data
            resolved_headers = context.resolve_value(headers) if context else headers

            response = requests.post(
                resolved_url,
                data=resolved_data,
                json=resolved_json,
                headers=resolved_headers,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'body': response.text,
                'json': None,
            }

            try:
                result['json'] = response.json()
                result['body'] = None
            except Exception:
                pass

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"POST {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"POST error: {e}")


class RestPutAction(BaseAction):
    """Perform PUT request."""
    action_type = "rest_put"
    display_name = "REST PUT"
    description = "发送PUT请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute PUT request."""
        url = params.get('url', '')
        data = params.get('data', None)
        json_data = params.get('json', None)
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'rest_response')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            resolved_url = context.resolve_value(url) if context else url
            resolved_data = context.resolve_value(data) if context else data
            resolved_json = context.resolve_value(json_data) if context else json_data

            response = requests.put(
                resolved_url,
                data=resolved_data,
                json=resolved_json,
                headers=headers,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'body': response.text,
                'json': None,
            }

            try:
                result['json'] = response.json()
            except Exception:
                pass

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"PUT {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PUT error: {e}")


class RestDeleteAction(BaseAction):
    """Perform DELETE request."""
    action_type = "rest_delete"
    display_name = "REST DELETE"
    description = "发送DELETE请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DELETE request."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'rest_response')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            resolved_url = context.resolve_value(url) if context else url

            response = requests.delete(
                resolved_url,
                headers=headers,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'body': response.text,
                'json': None,
            }

            try:
                result['json'] = response.json()
            except Exception:
                pass

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"DELETE {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DELETE error: {e}")


class RestPatchAction(BaseAction):
    """Perform PATCH request."""
    action_type = "rest_patch"
    display_name = "REST PATCH"
    description = "发送PATCH请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute PATCH request."""
        url = params.get('url', '')
        data = params.get('data', None)
        json_data = params.get('json', None)
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'rest_response')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            resolved_url = context.resolve_value(url) if context else url

            response = requests.patch(
                resolved_url,
                data=data,
                json=json_data,
                headers=headers,
                timeout=timeout
            )

            result = {
                'status_code': response.status_code,
                'body': response.text,
                'json': None,
            }

            try:
                result['json'] = response.json()
            except Exception:
                pass

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"PATCH {response.status_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PATCH error: {e}")


class RestBatchAction(BaseAction):
    """Perform batch REST requests."""
    action_type = "rest_batch"
    display_name = "REST批量请求"
    description = "批量发送REST请求"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch requests."""
        requests_list = params.get('requests', [])
        max_workers = params.get('max_workers', 5)
        output_var = params.get('output_var', 'batch_results')

        if not requests_list:
            return ActionResult(success=False, message="requests list is required")

        try:
            import requests
            from concurrent.futures import ThreadPoolExecutor, as_completed

            results = []

            def make_request(req):
                method = req.get('method', 'get').lower()
                url = req.get('url', '')
                return {'method': method, 'url': url, 'status': 'pending'}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(make_request, r): r for r in requests_list}
                for future in as_completed(futures):
                    results.append(future.result())

            return ActionResult(
                success=True,
                data={output_var: results},
                message=f"Batch {len(results)} requests"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {e}")
