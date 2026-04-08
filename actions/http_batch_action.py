"""HTTP batch action module for RabAI AutoClick.

Provides batch HTTP request capabilities with concurrent execution,
request queuing, result aggregation, and failure handling.
"""

import json
import time
import sys
import os
import asyncio
from typing import Any, Dict, List, Optional, Union, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpBatchAction(BaseAction):
    """Execute multiple HTTP requests in batch with concurrency control.
    
    Supports concurrent request execution, request queuing,
    result aggregation, and configurable failure handling.
    """
    action_type = "http_batch"
    display_name = "HTTP批量请求"
    description = "批量执行HTTP请求，支持并发控制和结果聚合"
    MAX_CONCURRENT = 10
    DEFAULT_TIMEOUT = 30
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests (list of request configs),
                   max_concurrent, continue_on_failure, timeout.
        
        Returns:
            ActionResult with batch execution results.
        """
        requests_config = params.get('requests', [])
        if not requests_config:
            return ActionResult(success=False, message="No requests provided")
        
        if not isinstance(requests_config, list):
            return ActionResult(success=False, message="requests must be a list")
        
        max_concurrent = params.get('max_concurrent', self.MAX_CONCURRENT)
        continue_on_failure = params.get('continue_on_failure', True)
        timeout = params.get('timeout', self.DEFAULT_TIMEOUT)
        
        results = []
        lock = threading.Lock()
        
        def execute_single(req: Dict[str, Any]) -> Dict[str, Any]:
            """Execute a single request and return result."""
            url = req.get('url', '')
            method = req.get('method', 'GET').upper()
            headers = req.get('headers', {})
            body = req.get('body')
            req_timeout = req.get('timeout', timeout)
            expected_status = req.get('expected_status', 200)
            
            start_time = time.time()
            result = {
                'url': url,
                'method': method,
                'success': False,
                'status_code': None,
                'body': None,
                'error': None,
                'elapsed': 0
            }
            
            if not url:
                result['error'] = 'URL is required'
                return result
            
            try:
                request_body = None
                if body:
                    if isinstance(body, dict):
                        request_body = json.dumps(body).encode('utf-8')
                    elif isinstance(body, str):
                        request_body = body.encode('utf-8')
                    elif isinstance(body, bytes):
                        request_body = body
                
                headers_dict = {str(k): str(v) for k, v in headers.items()} if headers else {}
                if body and 'Content-Type' not in headers_dict:
                    headers_dict['Content-Type'] = 'application/json'
                
                request = Request(url, data=request_body, headers=headers_dict, method=method)
                
                with urlopen(request, timeout=req_timeout) as response:
                    status_code = response.status
                    response_body = response.read().decode('utf-8')
                    elapsed = time.time() - start_time
                    
                    content_type = dict(response.headers).get('Content-Type', '')
                    parsed_body = response_body
                    if 'application/json' in content_type:
                        try:
                            parsed_body = json.loads(response_body)
                        except json.JSONDecodeError:
                            pass
                    
                    if isinstance(expected_status, list):
                        success = status_code in expected_status
                    else:
                        success = status_code == expected_status
                    
                    result.update({
                        'success': success,
                        'status_code': status_code,
                        'body': parsed_body,
                        'elapsed': elapsed
                    })
                    
            except HTTPError as e:
                result['error'] = f"HTTP {e.code}: {e.reason}"
                result['status_code'] = e.code
            except URLError as e:
                result['error'] = f"URL Error: {e.reason}"
            except TimeoutError:
                result['error'] = f"Timeout after {req_timeout}s"
            except Exception as e:
                result['error'] = str(e)
            
            return result
        
        try:
            with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                future_to_req = {
                    executor.submit(execute_single, req): idx 
                    for idx, req in enumerate(requests_config)
                }
                
                for future in as_completed(future_to_req):
                    idx = future_to_req[future]
                    try:
                        result = future.result()
                        with lock:
                            results.append({'index': idx, **result})
                    except Exception as e:
                        with lock:
                            results.append({
                                'index': idx,
                                'success': False,
                                'error': str(e)
                            })
            
            results.sort(key=lambda x: x['index'])
            
            total = len(results)
            successful = sum(1 for r in results if r.get('success', False))
            failed = total - successful
            
            all_success = failed == 0 if not continue_on_failure else False
            
            return ActionResult(
                success=all_success,
                message=f"Batch: {successful}/{total} succeeded, {failed} failed",
                data={
                    'total': total,
                    'successful': successful,
                    'failed': failed,
                    'results': results
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Batch execution failed: {e}",
                data={'error': str(e)}
            )
