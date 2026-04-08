"""
API Batch Action Module.

Executes multiple API requests in batch with concurrency control,
request batching, response aggregation, and error handling.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import json
import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchMode(Enum):
    """Batch execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CHUNKED = "chunked"
    PRIORITY = "priority"


class RequestMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class BatchRequest:
    """A single request in a batch."""
    id: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3
    expected_status: int = 200

    def __post_init__(self):
        if isinstance(self.method, str):
            self.method = self.method.upper()


@dataclass
class BatchResponse:
    """Response from a batch request."""
    request_id: str
    status_code: int
    headers: Dict[str, str]
    body: Any
    latency_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class BatchResult:
    """Result of a batch execution."""
    total: int
    succeeded: int
    failed: int
    total_duration_ms: float
    responses: List[BatchResponse]
    errors: List[Dict[str, Any]]
    rate_limit_hits: int = 0


class ApiBatchAction(BaseAction):
    """Batch API execution action.
    
    Executes multiple API requests with configurable concurrency,
    batching strategies, and comprehensive error handling.
    """
    action_type = "api_batch"
    display_name = "API批量请求"
    description = "批量执行多个API请求"
    
    def __init__(self):
        super().__init__()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._lock = threading.Lock()
        self._rate_limiter: Optional[Dict[str, Any]] = None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a batch of API requests.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - requests: List of request dicts
                - mode: Execution mode (sequential/parallel/chunked/priority)
                - max_concurrency: Max parallel requests (default 10)
                - chunk_size: Size of chunks for chunked mode (default 5)
                - global_timeout: Overall timeout in seconds (default 300)
                
        Returns:
            ActionResult with batch execution results.
        """
        start_time = time.time()
        
        requests = params.get("requests", [])
        mode = BatchMode(params.get("mode", "parallel"))
        max_concurrency = params.get("max_concurrency", 10)
        chunk_size = params.get("chunk_size", 5)
        global_timeout = params.get("global_timeout", 300)
        
        if not requests:
            return ActionResult(
                success=False,
                message="No requests provided",
                duration=time.time() - start_time
            )
        
        batch_requests = [self._parse_request(r) for r in requests]
        
        try:
            if mode == BatchMode.SEQUENTIAL:
                result = self._execute_sequential(batch_requests, global_timeout)
            elif mode == BatchMode.PARALLEL:
                result = self._execute_parallel(batch_requests, max_concurrency, global_timeout)
            elif mode == BatchMode.CHUNKED:
                result = self._execute_chunked(batch_requests, chunk_size, global_timeout)
            elif mode == BatchMode.PRIORITY:
                result = self._execute_priority(batch_requests, max_concurrency, global_timeout)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown batch mode: {mode}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=result.failed == 0,
                message=f"Batch completed: {result.succeeded}/{result.total} succeeded",
                data={
                    "total": result.total,
                    "succeeded": result.succeeded,
                    "failed": result.failed,
                    "total_duration_ms": result.total_duration_ms,
                    "rate_limit_hits": result.rate_limit_hits,
                    "responses": [
                        {"request_id": r.request_id, "status_code": r.status_code,
                         "latency_ms": r.latency_ms, "success": r.success}
                        for r in result.responses
                    ],
                    "errors": result.errors
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Batch execution failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _parse_request(self, req: Dict[str, Any]) -> BatchRequest:
        """Parse a request dict into a BatchRequest."""
        return BatchRequest(
            id=req.get("id", str(hash(str(req)))),
            url=req.get("url", ""),
            method=req.get("method", "GET"),
            headers=req.get("headers", {}),
            body=req.get("body"),
            timeout=req.get("timeout", 30.0),
            priority=req.get("priority", 0),
            max_retries=req.get("max_retries", 3),
            expected_status=req.get("expected_status", 200)
        )
    
    def _execute_request(self, request: BatchRequest) -> BatchResponse:
        """Execute a single HTTP request."""
        req_start = time.time()
        try:
            body_data = None
            if request.body is not None:
                if isinstance(request.body, dict):
                    body_data = json.dumps(request.body).encode("utf-8")
                elif isinstance(request.body, str):
                    body_data = request.body.encode("utf-8")
                else:
                    body_data = str(request.body).encode("utf-8")
            
            headers = dict(request.headers)
            if body_data and "Content-Type" not in headers:
                headers["Content-Type"] = "application/json"
            
            req = Request(request.url, data=body_data, headers=headers, method=request.method)
            
            with urlopen(req, timeout=request.timeout) as response:
                response_body = response.read()
                try:
                    body = json.loads(response_body)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    body = response_body.decode("utf-8", errors="replace")
                
                return BatchResponse(
                    request_id=request.id,
                    status_code=response.status,
                    headers=dict(response.headers),
                    body=body,
                    latency_ms=(time.time() - req_start) * 1000,
                    success=response.status == request.expected_status
                )
                
        except HTTPError as e:
            return BatchResponse(
                request_id=request.id,
                status_code=e.code,
                headers=dict(e.headers) if e.headers else {},
                body=e.read().decode("utf-8", errors="replace") if e.fp else None,
                latency_ms=(time.time() - req_start) * 1000,
                success=False,
                error=f"HTTP {e.code}: {str(e)}"
            )
        except URLError as e:
            return BatchResponse(
                request_id=request.id,
                status_code=0,
                headers={},
                body=None,
                latency_ms=(time.time() - req_start) * 1000,
                success=False,
                error=f"URL error: {str(e)}"
            )
        except Exception as e:
            return BatchResponse(
                request_id=request.id,
                status_code=0,
                headers={},
                body=None,
                latency_ms=(time.time() - req_start) * 1000,
                success=False,
                error=f"Request failed: {str(e)}"
            )
    
    def _execute_sequential(self, requests: List[BatchRequest], timeout: float) -> BatchResult:
        """Execute requests sequentially."""
        responses = []
        errors = []
        start_time = time.time()
        
        for req in requests:
            if time.time() - start_time > timeout:
                errors.append({"request_id": req.id, "error": "Global timeout exceeded"})
                continue
            response = self._execute_request(req)
            responses.append(response)
            if not response.success:
                errors.append({"request_id": req.id, "error": response.error})
        
        succeeded = sum(1 for r in responses if r.success)
        failed = len(responses) - succeeded
        
        return BatchResult(
            total=len(requests),
            succeeded=succeeded,
            failed=failed,
            total_duration_ms=(time.time() - start_time) * 1000,
            responses=responses,
            errors=errors
        )
    
    def _execute_parallel(self, requests: List[BatchRequest], max_concurrency: int, timeout: float) -> BatchResult:
        """Execute requests in parallel with concurrency limit."""
        responses = []
        errors = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_req = {executor.submit(self._execute_request, req): req for req in requests}
            
            for future in as_completed(future_to_req, timeout=timeout):
                req = future_to_req[future]
                try:
                    response = future.result()
                    responses.append(response)
                    if not response.success:
                        errors.append({"request_id": req.id, "error": response.error})
                except Exception as e:
                    errors.append({"request_id": req.id, "error": str(e)})
        
        succeeded = sum(1 for r in responses if r.success)
        failed = len(responses) - succeeded
        
        return BatchResult(
            total=len(requests),
            succeeded=succeeded,
            failed=failed,
            total_duration_ms=(time.time() - start_time) * 1000,
            responses=responses,
            errors=errors
        )
    
    def _execute_chunked(self, requests: List[BatchRequest], chunk_size: int, timeout: float) -> BatchResult:
        """Execute requests in chunks."""
        all_responses = []
        all_errors = []
        start_time = time.time()
        
        for i in range(0, len(requests), chunk_size):
            if time.time() - start_time > timeout:
                break
            chunk = requests[i:i + chunk_size]
            chunk_result = self._execute_parallel(chunk, chunk_size, timeout - (time.time() - start_time))
            all_responses.extend(chunk_result.responses)
            all_errors.extend(chunk_result.errors)
        
        succeeded = sum(1 for r in all_responses if r.success)
        failed = len(all_responses) - succeeded
        
        return BatchResult(
            total=len(requests),
            succeeded=succeeded,
            failed=failed,
            total_duration_ms=(time.time() - start_time) * 1000,
            responses=all_responses,
            errors=all_errors
        )
    
    def _execute_priority(self, requests: List[BatchRequest], max_concurrency: int, timeout: float) -> BatchResult:
        """Execute requests by priority order."""
        sorted_requests = sorted(requests, key=lambda r: -r.priority)
        return self._execute_parallel(sorted_requests, max_concurrency, timeout)
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate batch parameters."""
        if "requests" not in params:
            return False, "Missing required parameter: requests"
        if not isinstance(params["requests"], list):
            return False, "Parameter 'requests' must be a list"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["requests"]
