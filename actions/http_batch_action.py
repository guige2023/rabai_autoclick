"""HTTP batch action module for RabAI AutoClick.

Provides batch HTTP request capabilities including
parallel execution, rate limiting, and aggregated results.
"""

import sys
import os
import json
import time
import threading
import queue
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchHttpAction(BaseAction):
    """Execute multiple HTTP requests in batch.
    
    Supports parallel execution, rate limiting,
    partial failure handling, and result aggregation.
    """
    action_type = "batch_http"
    display_name = "批量HTTP请求"
    description = "批量执行多个HTTP请求，支持并行和限流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a batch of HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - requests: list of dicts, each with:
                    url, method, headers, data, params, timeout
                - parallel: bool (default True)
                - max_workers: int (default 5)
                - rate_limit: int (requests per second, 0=unlimited)
                - stop_on_error: bool (default False)
                - save_to_var: str
        
        Returns:
            ActionResult with batch results.
        """
        requests_list = params.get('requests', [])
        parallel = params.get('parallel', True)
        max_workers = params.get('max_workers', 5)
        rate_limit = params.get('rate_limit', 0)
        stop_on_error = params.get('stop_on_error', False)
        save_to_var = params.get('save_to_var', 'batch_results')

        if not requests_list:
            return ActionResult(success=False, message="No requests provided")

        results = []
        success_count = 0
        error_count = 0

        if parallel and len(requests_list) > 1:
            results, success_count, error_count = self._execute_parallel(
                requests_list, max_workers, rate_limit, stop_on_error
            )
        else:
            for req in requests_list:
                result = self._execute_single(req)
                results.append(result)
                if result.get('success'):
                    success_count += 1
                else:
                    error_count += 1
                    if stop_on_error:
                        break
                if rate_limit > 0:
                    time.sleep(1.0 / rate_limit)

        summary = {
            'total': len(requests_list),
            'success': success_count,
            'errors': error_count,
            'results': results,
        }

        if context and save_to_var:
            context.variables[save_to_var] = summary

        return ActionResult(
            success=error_count == 0,
            data=summary,
            message=f"Batch: {success_count}/{len(requests_list)} succeeded"
        )

    def _execute_single(self, req: Dict) -> Dict:
        """Execute a single HTTP request."""
        import urllib.request
        import urllib.error

        url = req.get('url', '')
        method = req.get('method', 'GET').upper()
        headers = req.get('headers', {})
        body = req.get('data', None)
        query_params = req.get('params', {})
        timeout = req.get('timeout', 30)

        body_bytes = None
        if body is not None and method != 'GET':
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
            else:
                body_bytes = str(body).encode('utf-8')

        if query_params:
            parsed = urllib.parse.urlparse(url)
            q = urllib.parse.parse_qsl(parsed.query)
            q.extend(list(query_params.items()))
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urllib.parse.urlencode(q)}"

        try:
            start = time.time()
            r = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                elapsed_ms = int((time.time() - start) * 1000)
                resp_body = resp.read()
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except:
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {
                    'success': True,
                    'url': url,
                    'status_code': resp.status,
                    'body': body_data,
                    'elapsed_ms': elapsed_ms,
                }
        except urllib.error.HTTPError as e:
            return {
                'success': False,
                'url': url,
                'status_code': e.code,
                'error': f"HTTP {e.code}",
            }
        except Exception as e:
            return {
                'success': False,
                'url': url,
                'error': str(e),
            }

    def _execute_parallel(self, requests_list: List, max_workers: int, 
                         rate_limit: int, stop_on_error: bool) -> tuple:
        """Execute requests in parallel with rate limiting."""
        results = []
        success_count = 0
        error_count = 0
        min_interval = 1.0 / rate_limit if rate_limit > 0 else 0
        last_request_time = [0.0]
        lock = threading.Lock()

        def throttled_execute(req: Dict, index: int) -> Dict:
            nonlocal success_count, error_count
            
            if min_interval > 0:
                with lock:
                    wait = min_interval - (time.time() - last_request_time[0])
                    if wait > 0:
                        time.sleep(wait)
                    last_request_time[0] = time.time()
            
            result = self._execute_single(req)
            result['index'] = index
            
            with lock:
                if result.get('success'):
                    success_count += 1
                else:
                    error_count += 1
            
            return result

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(throttled_execute, req, i): i
                for i, req in enumerate(requests_list)
            }
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                if stop_on_error and not result.get('success'):
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break

        # Sort by index
        results.sort(key=lambda x: x.get('index', 0))
        return results, success_count, error_count


import urllib.parse


class HttpChainAction(BaseAction):
    """Execute a chain of HTTP requests where each response
    feeds into the next request.
    
    Supports extracting values from responses using JSONPath
    or dot notation, and passing them as variables.
    """
    action_type = "http_chain"
    display_name = "HTTP链式请求"
    description = "链式执行多个HTTP请求，上一个响应作为下一个输入"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a chain of HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - steps: list of dicts, each with:
                    url, method, headers, data, extract (list of extractors)
                - extractors: dict mapping var names to JSONPath expressions
                - save_to_var: str
        
        Returns:
            ActionResult with final chain result.
        """
        steps = params.get('steps', [])
        extractors = params.get('extractors', {})
        save_to_var = params.get('save_to_var', 'chain_result')

        if not steps:
            return ActionResult(success=False, message="No steps provided")

        chain_vars = {}
        final_result = None

        for i, step in enumerate(steps):
            url = self._interpolate(step.get('url', ''), chain_vars)
            method = step.get('method', 'GET').upper()
            headers = step.get('headers', {})
            body = step.get('body', None)
            timeout = step.get('timeout', 30)

            # Interpolate body
            if isinstance(body, dict):
                body = {k: self._interpolate(str(v), chain_vars) for k, v in body.items()}
            elif isinstance(body, str):
                body = self._interpolate(body, chain_vars)

            # Interpolate headers
            headers = {k: self._interpolate(str(v), chain_vars) for k, v in headers.items()}

            # Execute request
            result = self._execute_single({
                'url': url,
                'method': method,
                'headers': headers,
                'data': body,
                'timeout': timeout,
            })

            if not result.get('success'):
                return ActionResult(
                    success=False,
                    data={'step': i, 'url': url, 'result': result},
                    message=f"Chain failed at step {i}: {result.get('error', 'unknown')}"
                )

            # Extract values
            response_body = result.get('body')
            for var_name, path in extractors.items():
                extracted = self._extract(response_body, path)
                chain_vars[var_name] = extracted

            # Also extract from step-specific extractors
            for extractor in step.get('extract', []):
                var_name = extractor.get('as', f'extracted_{i}')
                path = extractor.get('path', '')
                extracted = self._extract(response_body, path)
                chain_vars[var_name] = extracted

            final_result = result

        if context and save_to_var:
            context.variables[save_to_var] = chain_vars

        return ActionResult(
            success=True,
            data={'chain_vars': chain_vars, 'final_response': final_result},
            message=f"Chain completed: {len(steps)} steps"
        )

    def _interpolate(self, template: str, vars: Dict) -> str:
        """Simple variable interpolation: {{var_name}}."""
        result = template
        for k, v in vars.items():
            result = result.replace(f'{{{{{k}}}}}', str(v))
        return result

    def _extract(self, data: Any, path: str) -> Any:
        """Extract value from data using dot notation path."""
        if not path:
            return data
        
        current = data
        for part in path.split('.'):
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    return None
            else:
                return None
            
            if current is None:
                return None
        
        return current

    def _execute_single(self, req: Dict) -> Dict:
        """Execute a single HTTP request."""
        import urllib.request
        import urllib.error
        import urllib.parse

        url = req.get('url', '')
        method = req.get('method', 'GET').upper()
        headers = req.get('headers', {})
        body = req.get('data', None)
        timeout = req.get('timeout', 30)

        body_bytes = None
        if body is not None and method != 'GET':
            if isinstance(body, dict):
                body_bytes = json.dumps(body).encode('utf-8')
            else:
                body_bytes = str(body).encode('utf-8')

        try:
            start = time.time()
            r = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                elapsed_ms = int((time.time() - start) * 1000)
                resp_body = resp.read()
                try:
                    body_data = json.loads(resp_body.decode('utf-8'))
                except:
                    body_data = resp_body.decode('utf-8', errors='replace')
                
                return {
                    'success': True,
                    'status_code': resp.status,
                    'body': body_data,
                    'elapsed_ms': elapsed_ms,
                }
        except urllib.error.HTTPError as e:
            return {
                'success': False,
                'status_code': e.code,
                'error': f"HTTP {e.code}",
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
            }
