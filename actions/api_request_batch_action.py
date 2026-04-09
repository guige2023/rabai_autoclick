"""API request batch action module for RabAI AutoClick.

Provides batched API request execution with concurrency control,
request queuing, and result aggregation.
"""

import time
import json
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.base_action import BaseAction, ActionResult


class ApiBatchRequestAction(BaseAction):
    """Execute multiple API requests in batch with concurrency control.
    
    Processes requests in parallel with configurable worker count,
    collects results, and provides aggregate reporting.
    """
    action_type = "api_batch_request"
    display_name = "API批量请求"
    description = "并发执行多个API请求并聚合结果"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch API requests.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests, max_workers, timeout,
                   continue_on_error, aggregate.
        
        Returns:
            ActionResult with batch results and statistics.
        """
        requests_list = params.get("requests", [])
        max_workers = params.get("max_workers", 5)
        timeout = params.get("timeout", 30)
        continue_on_error = params.get("continue_on_error", True)
        aggregate = params.get("aggregate", True)
        
        if not requests_list:
            return ActionResult(success=False, message="No requests to execute")
        
        results = []
        errors = []
        start_time = time.time()
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_req = {
                    executor.submit(
                        self._execute_single_request,
                        req,
                        timeout
                    ): req for req in requests_list
                }
                
                for future in as_completed(future_to_req):
                    req = future_to_req[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        error = {
                            "request": req.get("id", "unknown"),
                            "error": str(e)
                        }
                        errors.append(error)
                        if not continue_on_error:
                            break
            
            total_time = time.time() - start_time
            
            success_count = sum(1 for r in results if r.get("success"))
            failure_count = len(results) + len(errors) - success_count
            
            aggregated = None
            if aggregate and results:
                aggregated = self._aggregate_results(results)
            
            return ActionResult(
                success=failure_count == 0,
                message=f"Batch: {success_count} succeeded, {failure_count} failed in {total_time:.2f}s",
                data={
                    "total": len(requests_list),
                    "succeeded": success_count,
                    "failed": failure_count,
                    "elapsed": total_time,
                    "results": results[:100],
                    "errors": errors[:20],
                    "aggregated": aggregated
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch execution failed: {e}")
    
    def _execute_single_request(
        self,
        req: Dict[str, Any],
        timeout: int
    ) -> Dict[str, Any]:
        url = req.get("url", "")
        method = req.get("method", "GET").upper()
        headers = req.get("headers", {})
        body = req.get("body")
        req_id = req.get("id", url)
        
        req_start = time.time()
        
        try:
            body_bytes = None
            if body:
                if isinstance(body, dict):
                    body_bytes = json.dumps(body).encode()
                elif isinstance(body, str):
                    body_bytes = body.encode()
                else:
                    body_bytes = body
            
            request = Request(
                url,
                data=body_bytes,
                headers={str(k): str(v) for k, v in headers.items()},
                method=method
            )
            
            with urlopen(request, timeout=timeout) as response:
                response_body = response.read()
                response_data = None
                
                try:
                    response_data = json.loads(response_body.decode())
                except Exception:
                    response_data = response_body.decode("utf-8", errors="replace")
                
                return {
                    "id": req_id,
                    "success": True,
                    "status_code": response.status,
                    "duration": time.time() - req_start,
                    "data": response_data
                }
        except HTTPError as e:
            return {
                "id": req_id,
                "success": False,
                "status_code": e.code,
                "error": e.reason,
                "duration": time.time() - req_start
            }
        except Exception as e:
            return {
                "id": req_id,
                "success": False,
                "error": str(e),
                "duration": time.time() - req_start
            }
    
    def _aggregate_results(self, results: List[Dict]) -> Dict[str, Any]:
        durations = [r.get("duration", 0) for r in results]
        status_codes = [r.get("status_code") for r in results if "status_code" in r]
        
        return {
            "total_requests": len(results),
            "avg_duration": sum(durations) / len(durations) if durations else 0,
            "min_duration": min(durations) if durations else 0,
            "max_duration": max(durations) if durations else 0,
            "status_codes": {code: status_codes.count(code) for code in set(status_codes) if code}
        }


class ApiBatchChunkAction(BaseAction):
    """Process large datasets by chunking into smaller API batches.
    
    Splits large arrays into chunks and processes each chunk
    as a separate batch with progress tracking.
    """
    action_type = "api_batch_chunk"
    display_name = "API分块批处理"
    description = "将大数据集分块为小批次处理"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process data in chunks.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, url, chunk_size, max_workers,
                   request_template.
        
        Returns:
            ActionResult with all processed results.
        """
        data = params.get("data", [])
        url = params.get("url", "")
        chunk_size = params.get("chunk_size", 100)
        max_workers = params.get("max_workers", 3)
        request_template = params.get("request_template", {})
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="Data must be a list")
        
        if not data:
            return ActionResult(success=False, message="No data to process")
        
        chunks = [
            data[i:i + chunk_size]
            for i in range(0, len(data), chunk_size)
        ]
        
        all_results = []
        total_errors = 0
        start_time = time.time()
        
        try:
            for i, chunk in enumerate(chunks):
                chunk_start = time.time()
                
                batch_params = {
                    "requests": [
                        {
                            "id": f"chunk_{i}_{j}",
                            "url": url,
                            "method": request_template.get("method", "POST"),
                            "headers": request_template.get("headers", {}),
                            "body": {
                                **request_template.get("body", {}),
                                "item": item
                            }
                        }
                        for j, item in enumerate(chunk)
                    ],
                    "max_workers": max_workers,
                    "continue_on_error": True
                }
                
                batch_result = ApiBatchRequestAction().execute(context, batch_params)
                
                all_results.extend(batch_result.data.get("results", []))
                total_errors += len(batch_result.data.get("errors", []))
                
                chunk_time = time.time() - chunk_start
                
                if context and hasattr(context, "_progress"):
                    context._progress = (i + 1) / len(chunks)
            
            total_time = time.time() - start_time
            
            return ActionResult(
                success=total_errors == 0,
                message=f"Chunked processing: {len(chunks)} chunks, {len(all_results)} results in {total_time:.1f}s",
                data={
                    "total_chunks": len(chunks),
                    "chunk_size": chunk_size,
                    "total_results": len(all_results),
                    "errors": total_errors,
                    "elapsed": total_time,
                    "results": all_results
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chunk processing failed: {e}")
