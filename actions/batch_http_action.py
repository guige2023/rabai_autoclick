"""Batch HTTP action module for RabAI AutoClick.

Provides batch HTTP request processing with
concurrency control, retry, and response aggregation.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchHTTPAction(BaseAction):
    """Execute multiple HTTP requests in batch.
    
    Supports concurrent execution, retry logic,
    rate limiting, and response aggregation.
    """
    action_type = "batch_http"
    display_name = "批量HTTP"
    description = "批量HTTP请求并发执行，支持重试和限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - requests: list of {url, method, headers, data, params}
                - max_concurrency: int, max parallel requests
                - timeout: int, per-request timeout
                - retry_count: int, retries on failure
                - retry_delay: float, delay between retries
                - fail_fast: bool, stop on first failure
                - aggregate_type: str (list/dict/indexed)
                - save_to_var: str
        
        Returns:
            ActionResult with aggregated responses.
        """
        requests_list = params.get('requests', [])
        max_concurrency = params.get('max_concurrency', 5)
        timeout = params.get('timeout', 30)
        retry_count = params.get('retry_count', 0)
        retry_delay = params.get('retry_delay', 1.0)
        fail_fast = params.get('fail_fast', False)
        aggregate_type = params.get('aggregate_type', 'list')
        save_to_var = params.get('save_to_var', None)

        if not requests_list:
            return ActionResult(success=False, message="No requests provided")

        start_time = time.time()
        results = []
        errors = []
        total = len(requests_list)

        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_req = {}
            for i, req in enumerate(requests_list):
                future = executor.submit(
                    self._execute_request, req, timeout, retry_count, retry_delay
                )
                future_to_req[future] = (i, req)

            for future in as_completed(future_to_req):
                i, req = future_to_req[future]
                try:
                    result = future.result()
                    results.append({
                        'index': i,
                        'url': req.get('url', ''),
                        'success': result['success'],
                        'status': result.get('status'),
                        'data': result.get('data'),
                        'error': result.get('error'),
                        'duration': result.get('duration', 0)
                    })
                    if fail_fast and not result['success']:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                except Exception as e:
                    errors.append({'index': i, 'error': str(e)})

        succeeded = sum(1 for r in results if r['success'])
        failed = total - succeeded

        # Aggregate results
        if aggregate_type == 'list':
            aggregated = [r['data'] for r in results]
        elif aggregate_type == 'dict':
            aggregated = {r['index']: r for r in results}
        elif aggregate_type == 'indexed':
            aggregated = results
        else:
            aggregated = results

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = aggregated

        return ActionResult(
            success=failed == 0,
            message=f"Batch: {succeeded}/{total} succeeded, {failed} failed",
            data={
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
                'results': aggregated,
                'duration': time.time() - start_time
            }
        )

    def _execute_request(
        self, req: Dict, timeout: int, retry_count: int, retry_delay: float
    ) -> Dict:
        """Execute a single HTTP request with retry."""
        url = req.get('url', '')
        method = req.get('method', 'GET').upper()
        headers = req.get('headers', {})
        data = req.get('data', None)
        params = req.get('params', {})

        if not url:
            return {'success': False, 'error': 'URL required'}

        # Build URL with query params
        if params:
            sep = '&' if '?' in url else '?'
            param_str = '&'.join(f'{k}={v}' for k, v in params.items())
            url = f"{url}{sep}{param_str}"

        start_time = time.time()
        last_error = None

        for attempt in range(retry_count + 1):
            try:
                req_data = None
                if data and method in ('POST', 'PUT', 'PATCH'):
                    if isinstance(data, dict):
                        req_data = json.dumps(data).encode('utf-8')
                        headers = {**headers, 'Content-Type': 'application/json'}
                    else:
                        req_data = str(data).encode('utf-8')

                http_request = Request(url, data=req_data, headers=headers, method=method)
                with urlopen(http_request, timeout=timeout) as response:
                    body = response.read().decode('utf-8')
                    try:
                        json_data = json.loads(body)
                    except Exception:
                        json_data = body

                    return {
                        'success': True,
                        'status': response.status,
                        'data': json_data,
                        'duration': time.time() - start_time
                    }

            except HTTPError as e:
                last_error = f"HTTP {e.code}: {e.reason}"
            except URLError as e:
                last_error = f"URL Error: {e.reason}"
            except Exception as e:
                last_error = str(e)

            if attempt < retry_count:
                time.sleep(retry_delay * (attempt + 1))

        return {
            'success': False,
            'error': last_error,
            'duration': time.time() - start_time
        }

    def get_required_params(self) -> List[str]:
        return ['requests']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_concurrency': 5,
            'timeout': 30,
            'retry_count': 0,
            'retry_delay': 1.0,
            'fail_fast': False,
            'aggregate_type': 'list',
            'save_to_var': None,
        }
