"""Batch API action module for RabAI AutoClick.

Provides batch request processing with concurrency control,
request bundling, and response aggregation.
"""

import asyncio
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchApiAction(BaseAction):
    """Batch API action for processing multiple requests efficiently.
    
    Supports concurrent request execution with semaphore-based
    concurrency limiting, request bundling, and response aggregation.
    """
    action_type = "batch_api"
    display_name = "批量API"
    description = "并发批量API请求处理与响应聚合"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch API requests.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                requests: List[Dict] with url, method, headers, body per request
                concurrency: Max concurrent requests (default 5)
                bundle_size: Number of requests per bundle (default 1)
                stop_on_error: Stop on first error (default False)
                timeout: Per-request timeout in seconds.
        
        Returns:
            ActionResult with all responses and success count.
        """
        requests = params.get('requests', [])
        concurrency = params.get('concurrency', 5)
        stop_on_error = params.get('stop_on_error', False)
        timeout = params.get('timeout', 30)
        
        if not requests:
            return ActionResult(success=False, message="No requests provided")
        
        if len(requests) == 1:
            return self._execute_single(requests[0], timeout)
        
        return self._execute_batch(requests, concurrency, stop_on_error, timeout)
    
    def _execute_single(self, req: Dict[str, Any], timeout: int) -> ActionResult:
        """Execute a single request."""
        result = self._do_request(req, timeout)
        return ActionResult(
            success=result['success'],
            message=result.get('error', 'Success'),
            data={'responses': [result], 'total': 1, 'successful': 1 if result['success'] else 0}
        )
    
    def _execute_batch(
        self,
        requests: List[Dict[str, Any]],
        concurrency: int,
        stop_on_error: bool,
        timeout: int
    ) -> ActionResult:
        """Execute batch requests with concurrency control."""
        results = []
        successful = 0
        failed = 0
        semaphore = asyncio.Semaphore(concurrency)
        
        async def run_all():
            tasks = [self._run_with_semaphore(req, semaphore, timeout) for req in requests]
            return await asyncio.gather(*tasks, return_exceptions=True)
        
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        raw_results = loop.run_until_complete(run_all())
        
        for i, result in enumerate(raw_results):
            if isinstance(result, Exception):
                results.append({'success': False, 'error': str(result), 'index': i})
                failed += 1
                if stop_on_error:
                    break
            elif isinstance(result, dict):
                results.append({**result, 'index': i})
                if result.get('success'):
                    successful += 1
                else:
                    failed += 1
                    if stop_on_error:
                        break
        
        return ActionResult(
            success=failed == 0,
            message=f"Batch complete: {successful} successful, {failed} failed",
            data={
                'responses': results,
                'total': len(requests),
                'successful': successful,
                'failed': failed
            }
        )
    
    async def _run_with_semaphore(
        self,
        req: Dict[str, Any],
        semaphore: asyncio.Semaphore,
        timeout: int
    ) -> Dict[str, Any]:
        """Run request with semaphore concurrency control."""
        async with semaphore:
            return await asyncio.to_thread(self._do_request, req, timeout)
    
    def _do_request(self, req: Dict[str, Any], timeout: int) -> Dict[str, Any]:
        """Execute a single HTTP request."""
        url = req.get('url', '')
        method = req.get('method', 'GET')
        headers = req.get('headers', {})
        body = req.get('body')
        
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
            request = Request(url, data=data, headers=headers, method=method)
            with urlopen(request, timeout=timeout) as response:
                body_bytes = response.read()
                return {
                    'success': True,
                    'status': response.status,
                    'body': body_bytes.decode('utf-8', errors='replace'),
                    'headers': dict(response.headers)
                }
        except HTTPError as e:
            return {'success': False, 'error': f'HTTP {e.code}: {e.reason}'}
        except URLError as e:
            return {'success': False, 'error': f'URL error: {str(e.reason)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
