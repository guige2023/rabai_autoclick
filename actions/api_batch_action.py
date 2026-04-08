"""API batch action module for RabAI AutoClick.

Provides API batch request processing with support for
request bundling, parallel execution, and result aggregation.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiBatchAction(BaseAction):
    """API batch action for processing multiple requests.
    
    Supports batch request execution with concurrency control,
    error handling, and result aggregation.
    """
    action_type = "api_batch"
    display_name = "API批量处理"
    description = "API批量请求处理"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                requests: List of request definitions
                concurrency: Max concurrent requests
                stop_on_error: Stop on first error
                continue_on_error: Continue after errors
                timeout: Per-request timeout.
        
        Returns:
            ActionResult with batch results.
        """
        requests = params.get('requests', [])
        concurrency = params.get('concurrency', 5)
        stop_on_error = params.get('stop_on_error', False)
        continue_on_error = params.get('continue_on_error', True)
        timeout = params.get('timeout', 30)
        
        if not requests:
            return ActionResult(success=False, message="No requests provided")
        
        if len(requests) == 1:
            return self._execute_single(requests[0], timeout)
        
        return self._execute_batch(
            requests, concurrency, stop_on_error, continue_on_error, timeout
        )
    
    def _execute_single(
        self,
        request: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute a single request."""
        result = self._do_request(request, timeout)
        
        return ActionResult(
            success=result['success'],
            message=result.get('error', 'Request completed'),
            data={
                'results': [result],
                'total': 1,
                'successful': 1 if result['success'] else 0,
                'failed': 0 if result['success'] else 1
            }
        )
    
    def _execute_batch(
        self,
        requests: List[Dict[str, Any]],
        concurrency: int,
        stop_on_error: bool,
        continue_on_error: bool,
        timeout: int
    ) -> ActionResult:
        """Execute batch requests with concurrency control."""
        results = []
        successful = 0
        failed = 0
        errors = []
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(self._do_request, req, timeout): (i, req)
                for i, req in enumerate(requests)
            }
            
            for future in as_completed(futures):
                i, req = futures[future]
                
                try:
                    result = future.result()
                    results.append({**result, 'index': i})
                    
                    if result.get('success'):
                        successful += 1
                    else:
                        failed += 1
                        errors.append({
                            'index': i,
                            'error': result.get('error'),
                            'request': self._summarize_request(req)
                        })
                        
                        if stop_on_error:
                            for f in futures:
                                f.cancel()
                            break
                except Exception as e:
                    failed += 1
                    errors.append({
                        'index': i,
                        'error': str(e),
                        'request': self._summarize_request(req)
                    })
                    
                    if stop_on_error:
                        for f in futures:
                            f.cancel()
                        break
        
        success = failed == 0 or continue_on_error
        
        return ActionResult(
            success=success,
            message=f"Batch completed: {successful}/{len(requests)} successful",
            data={
                'results': results,
                'total': len(requests),
                'successful': successful,
                'failed': failed,
                'errors': errors[:10]
            }
        )
    
    def _do_request(
        self,
        request: Dict[str, Any],
        timeout: int
    ) -> Dict[str, Any]:
        """Execute a single HTTP request."""
        url = request.get('url', '')
        method = request.get('method', 'GET')
        headers = request.get('headers', {})
        body = request.get('body')
        
        if not url:
            return {'success': False, 'error': 'URL is required'}
        
        data = None
        if body:
            if isinstance(body, dict):
                data = json.dumps(body).encode('utf-8')
                headers = {**headers, 'Content-Type': 'application/json'}
            elif isinstance(body, str):
                data = body.encode('utf-8')
            else:
                data = body
        
        try:
            req = Request(url, data=data, headers=headers, method=method)
            
            with urlopen(req, timeout=timeout) as response:
                body_bytes = response.read()
                
                try:
                    body_json = json.loads(body_bytes.decode('utf-8', errors='replace'))
                    body_result = body_json
                except json.JSONDecodeError:
                    body_result = body_bytes.decode('utf-8', errors='replace')
                
                return {
                    'success': True,
                    'status': response.status,
                    'body': body_result,
                    'headers': dict(response.headers)
                }
        except HTTPError as e:
            body = e.read() if e.fp else b''
            return {
                'success': False,
                'error': f"HTTP {e.code}: {e.reason}",
                'status': e.code,
                'body': body.decode('utf-8', errors='replace') if body else ''
            }
        except URLError as e:
            return {
                'success': False,
                'error': f"URL error: {str(e.reason)}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _summarize_request(self, request: Dict) -> Dict:
        """Summarize request for error reporting."""
        return {
            'url': request.get('url', ''),
            'method': request.get('method', 'GET')
        }
