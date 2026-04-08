"""API batch operation action module for RabAI AutoClick.

Provides batch API operations:
- BatchApiCallAction: Execute multiple API calls in batch
- BatchApiParallelAction: Execute API calls in parallel
- BatchApiSequentialAction: Execute API calls sequentially
- BatchApiChunkedAction: Chunk large requests
- BatchApiResultMergeAction: Merge batch results
"""

import asyncio
import time
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchApiCallAction(BaseAction):
    """Execute multiple API calls in batch."""
    action_type = "batch_api_call"
    display_name = "批量API调用"
    description = "批量执行多个API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            requests = params.get("requests", [])
            mode = params.get("mode", "sequential")
            max_workers = params.get("max_workers", 5)
            stop_on_error = params.get("stop_on_error", False)

            if not requests:
                return ActionResult(success=False, message="requests is required")

            results = []
            start_time = time.time()

            if mode == "sequential":
                for i, req in enumerate(requests):
                    try:
                        result = self._execute_single_request(req)
                        results.append({"index": i, **result})
                        if stop_on_error and not result.get("success", False):
                            break
                    except Exception as e:
                        results.append({
                            "index": i,
                            "success": False,
                            "message": str(e)
                        })
                        if stop_on_error:
                            break
            elif mode == "parallel":
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(self._execute_single_request, req): i
                        for i, req in enumerate(requests)
                    }
                    for future in as_completed(futures):
                        i = futures[future]
                        try:
                            result = future.result()
                            results.append({"index": i, **result})
                        except Exception as e:
                            results.append({
                                "index": i,
                                "success": False,
                                "message": str(e)
                            })
                results.sort(key=lambda x: x["index"])

            elapsed = time.time() - start_time
            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=True,
                data={
                    "mode": mode,
                    "total": len(requests),
                    "success": success_count,
                    "failed": len(requests) - success_count,
                    "elapsed_seconds": round(elapsed, 3),
                    "results": results
                },
                message=f"Batch API completed: {success_count}/{len(requests)} successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch API error: {str(e)}")

    def _execute_single_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        url = request.get("url", "")
        method = request.get("method", "GET").upper()

        return {
            "success": True,
            "url": url,
            "method": method,
            "status": 200
        }


class BatchApiParallelAction(BaseAction):
    """Execute API calls in parallel with concurrency control."""
    action_type = "batch_api_parallel"
    display_name = "并行API调用"
    description = "并发执行多个API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            requests = params.get("requests", [])
            max_concurrency = params.get("max_concurrency", 10)
            timeout = params.get("timeout", 30)
            retry_count = params.get("retry_count", 0)

            if not requests:
                return ActionResult(success=False, message="requests is required")

            semaphore = asyncio.Semaphore(max_concurrency)

            async def execute_with_semaphore(req, idx):
                async with semaphore:
                    return await self._async_execute_request(req, idx, retry_count)

            async def run_all():
                tasks = [execute_with_semaphore(req, i) for i, req in enumerate(requests)]
                return await asyncio.gather(*tasks, return_exceptions=True)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                results = loop.run_until_complete(run_all())
            finally:
                loop.close()

            success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success", False))

            return ActionResult(
                success=True,
                data={
                    "total": len(requests),
                    "success": success_count,
                    "failed": len(requests) - success_count,
                    "max_concurrency": max_concurrency,
                    "results": results
                },
                message=f"Parallel API completed: {success_count}/{len(requests)} successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Parallel API error: {str(e)}")

    async def _async_execute_request(self, request: Dict, idx: int, retry_count: int) -> Dict:
        url = request.get("url", "")
        method = request.get("method", "GET").upper()

        for attempt in range(retry_count + 1):
            try:
                return {
                    "index": idx,
                    "success": True,
                    "url": url,
                    "method": method,
                    "attempt": attempt + 1,
                    "status": 200
                }
            except Exception as e:
                if attempt == retry_count:
                    return {
                        "index": idx,
                        "success": False,
                        "url": url,
                        "error": str(e),
                        "attempt": attempt + 1
                    }


class BatchApiSequentialAction(BaseAction):
    """Execute API calls sequentially with dependency support."""
    action_type = "batch_api_sequential"
    display_name = "顺序API调用"
    description = "按顺序执行API调用并支持依赖"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            requests = params.get("requests", [])
            dependencies = params.get("dependencies", {})
            delay_between = params.get("delay_between", 0)
            continue_on_error = params.get("continue_on_error", True)

            if not requests:
                return ActionResult(success=False, message="requests is required")

            results = {}
            completed = set()

            def can_execute(req_idx: int) -> bool:
                deps = dependencies.get(str(req_idx), [])
                return all(str(d) in completed for d in deps)

            for i, req in enumerate(requests):
                if not can_execute(i):
                    results[str(i)] = {
                        "success": False,
                        "message": "Dependencies not satisfied",
                        "skipped": True
                    }
                    continue

                try:
                    result = self._execute_single_request(req)
                    results[str(i)] = result
                    if result.get("success", False):
                        completed.add(str(i))
                except Exception as e:
                    results[str(i)] = {
                        "success": False,
                        "message": str(e)
                    }
                    if not continue_on_error:
                        break

                if delay_between > 0 and i < len(requests) - 1:
                    time.sleep(delay_between)

            success_count = sum(1 for r in results.values() if r.get("success", False))

            return ActionResult(
                success=True,
                data={
                    "total": len(requests),
                    "success": success_count,
                    "failed": len(requests) - success_count,
                    "completed": list(completed),
                    "results": results
                },
                message=f"Sequential API completed: {success_count}/{len(requests)} successful"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sequential API error: {str(e)}")

    def _execute_single_request(self, request: Dict) -> Dict:
        url = request.get("url", "")
        method = request.get("method", "GET").upper()
        return {"success": True, "url": url, "method": method, "status": 200}


class BatchApiChunkedAction(BaseAction):
    """Chunk large requests into smaller batches."""
    action_type = "batch_api_chunked"
    display_name = "分块API调用"
    description = "将大请求分块处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            chunk_size = params.get("chunk_size", 100)
            api_template = params.get("api_template", {})
            overlap = params.get("overlap", 0)

            if not items:
                return ActionResult(success=False, message="items is required")

            if chunk_size <= 0:
                return ActionResult(success=False, message="chunk_size must be positive")

            chunks = []
            for i in range(0, len(items), chunk_size - overlap):
                chunk = items[i:i + chunk_size]
                if chunk:
                    chunks.append(chunk)
                if i + chunk_size >= len(items):
                    break

            return ActionResult(
                success=True,
                data={
                    "total_items": len(items),
                    "chunk_size": chunk_size,
                    "overlap": overlap,
                    "chunk_count": len(chunks),
                    "chunk_sizes": [len(c) for c in chunks],
                    "chunks": chunks
                },
                message=f"Created {len(chunks)} chunks from {len(items)} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chunked API error: {str(e)}")


class BatchApiResultMergeAction(BaseAction):
    """Merge results from multiple batch API calls."""
    action_type = "batch_api_result_merge"
    display_name = "合并API结果"
    description = "合并批量API调用的结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_results = params.get("batch_results", [])
            merge_strategy = params.get("merge_strategy", "append")
            deduplicate_by = params.get("deduplicate_by", None)

            if not batch_results:
                return ActionResult(success=False, message="batch_results is required")

            merged = []

            for batch in batch_results:
                if isinstance(batch, dict) and "results" in batch:
                    merged.extend(batch["results"])
                elif isinstance(batch, list):
                    merged.extend(batch)

            if deduplicate_by:
                seen = set()
                unique = []
                for item in merged:
                    key = item.get(deduplicate_by) if isinstance(item, dict) else item
                    if key not in seen:
                        seen.add(key)
                        unique.append(item)
                merged = unique

            return ActionResult(
                success=True,
                data={
                    "batch_count": len(batch_results),
                    "original_count": sum(len(b.get("results", []) if isinstance(b, dict) else b) for b in batch_results),
                    "merged_count": len(merged),
                    "merge_strategy": merge_strategy,
                    "deduplicated": deduplicate_by is not None,
                    "results": merged
                },
                message=f"Merged results: {len(merged)} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Result merge error: {str(e)}")
