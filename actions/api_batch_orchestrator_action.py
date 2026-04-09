"""API batch orchestrator action module for RabAI AutoClick.

Provides batch orchestration for API operations:
- ApiBatchOrchestratorAction: Orchestrate multiple API batches
- ApiBatchSchedulerAction: Schedule batch execution
- ApiBatchMonitorAction: Monitor batch progress
- ApiBatchResultAggregatorAction: Aggregate batch results
"""

import time
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchState(Enum):
    """Batch execution states."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ApiBatchOrchestratorAction(BaseAction):
    """Orchestrate multiple API batches with dependencies."""
    action_type = "api_batch_orchestrator"
    display_name = "API批量编排器"
    description = "编排多批次API执行"

    def __init__(self):
        super().__init__()
        self._batch_queue: List[Dict[str, Any]] = []
        self._results: Dict[str, Any] = {}
        self._state = BatchState.PENDING

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batches = params.get("batches", [])
            max_concurrent = params.get("max_concurrent", 3)
            fail_fast = params.get("fail_fast", False)
            callback = params.get("callback")

            if not batches:
                return ActionResult(success=False, message="batches list is required")

            self._state = BatchState.RUNNING
            self._results = {}
            completed = 0
            total = len(batches)

            with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                future_to_batch = {
                    executor.submit(self._execute_batch, batch): idx
                    for idx, batch in enumerate(batches)
                }

                for future in as_completed(future_to_batch):
                    idx = future_to_batch[future]
                    try:
                        result = future.result()
                        self._results[f"batch_{idx}"] = result
                        completed += 1

                        if fail_fast and not result.get("success", False):
                            executor.shutdown(wait=False, cancel_futures=True)
                            self._state = BatchState.FAILED
                            return ActionResult(
                                success=False,
                                message=f"Batch {idx} failed, stopping",
                                data={"completed": completed, "total": total, "results": self._results}
                            )
                    except Exception as e:
                        self._results[f"batch_{idx}"] = {"success": False, "error": str(e)}
                        completed += 1

            all_success = all(r.get("success", False) for r in self._results.values())
            self._state = BatchState.COMPLETED if all_success else BatchState.FAILED

            return ActionResult(
                success=all_success,
                message=f"Orchestrated {completed}/{total} batches",
                data={"completed": completed, "total": total, "results": self._results, "state": self._state.value}
            )
        except Exception as e:
            self._state = BatchState.FAILED
            return ActionResult(success=False, message=f"Batch orchestrator error: {e}")

    def _execute_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single batch."""
        requests = batch.get("requests", [])
        batch_type = batch.get("type", "sequential")

        if batch_type == "parallel":
            return self._execute_parallel(requests)
        return self._execute_sequential(requests)

    def _execute_sequential(self, requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute requests sequentially."""
        results = []
        for req in requests:
            result = self._execute_request(req)
            results.append(result)
            if not result.get("success", False):
                return {"success": False, "results": results, "failed_at": len(results) - 1}
        return {"success": True, "results": results}

    def _execute_parallel(self, requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute requests in parallel."""
        with ThreadPoolExecutor(max_workers=min(len(requests), 5)) as executor:
            futures = [executor.submit(self._execute_request, req) for req in requests]
            results = [f.result() for f in as_completed(futures)]
        return {"success": True, "results": results}

    def _execute_request(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single API request."""
        import urllib.request
        import json as json_module

        url = req.get("url", "")
        method = req.get("method", "GET").upper()
        headers = req.get("headers", {})
        body = req.get("body")

        if not url:
            return {"success": False, "error": "URL is required"}

        try:
            request = urllib.request.Request(url, method=method, headers=headers)
            if body:
                request.data = json_module.dumps(body).encode() if isinstance(body, dict) else str(body).encode()

            with urllib.request.urlopen(request, timeout=30) as response:
                content = response.read().decode()
                return {"success": True, "content": content, "status": response.status}
        except Exception as e:
            return {"success": False, "error": str(e)}


class ApiBatchSchedulerAction(BaseAction):
    """Schedule batch API execution."""
    action_type = "api_batch_scheduler"
    display_name = "API批量调度器"
    description = "调度批量API执行时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batches = params.get("batches", [])
            schedule_time = params.get("schedule_time")
            interval = params.get("interval")
            max_executions = params.get("max_executions", 1)

            if not batches:
                return ActionResult(success=False, message="batches list is required")

            schedule_info = {
                "batches_count": len(batches),
                "schedule_time": schedule_time,
                "interval": interval,
                "max_executions": max_executions,
                "scheduled_at": datetime.now().isoformat(),
            }

            if schedule_time:
                try:
                    from datetime import datetime as dt
                    st = dt.fromisoformat(schedule_time)
                    schedule_info["delay_seconds"] = max(0, (st - dt.now()).total_seconds())
                except (ValueError, TypeError):
                    schedule_info["delay_seconds"] = 0

            return ActionResult(
                success=True,
                message="Batch scheduled successfully",
                data=schedule_info
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch scheduler error: {e}")


class ApiBatchMonitorAction(BaseAction):
    """Monitor batch execution progress."""
    action_type = "api_batch_monitor"
    display_name = "API批量监控器"
    description = "监控批量执行进度"

    def __init__(self):
        super().__init__()
        self._batch_status: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_id = params.get("batch_id")
            operation = params.get("operation", "status")

            if operation == "status":
                if batch_id and batch_id in self._batch_status:
                    return ActionResult(success=True, message="Batch status retrieved", data=self._batch_status[batch_id])
                return ActionResult(success=True, message="No active batch", data={"active_batches": list(self._batch_status.keys())})

            elif operation == "register":
                if not batch_id:
                    return ActionResult(success=False, message="batch_id required")
                self._batch_status[batch_id] = {
                    "state": BatchState.PENDING.value,
                    "progress": 0.0,
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "started_at": datetime.now().isoformat(),
                }
                return ActionResult(success=True, message=f"Batch {batch_id} registered")

            elif operation == "update":
                if not batch_id or batch_id not in self._batch_status:
                    return ActionResult(success=False, message="Batch not found")
                self._batch_status[batch_id].update({
                    k: v for k, v in params.items()
                    if k in ("state", "progress", "total", "completed", "failed")
                })
                return ActionResult(success=True, message="Batch updated")

            elif operation == "cancel":
                if batch_id and batch_id in self._batch_status:
                    self._batch_status[batch_id]["state"] = BatchState.CANCELLED.value
                    return ActionResult(success=True, message=f"Batch {batch_id} cancelled")
                return ActionResult(success=False, message="Batch not found")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Batch monitor error: {e}")


class ApiBatchResultAggregatorAction(BaseAction):
    """Aggregate results from multiple batches."""
    action_type = "api_batch_result_aggregator"
    display_name = "API批量结果聚合器"
    description = "聚合多个批次的结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            batch_results = params.get("batch_results", {})
            aggregation_type = params.get("aggregation_type", "all")
            filter_failed = params.get("filter_failed", False)

            if not batch_results:
                return ActionResult(success=False, message="batch_results required")

            aggregated = {
                "total_batches": len(batch_results),
                "successful_batches": 0,
                "failed_batches": 0,
                "all_results": [],
                "errors": [],
            }

            for batch_key, result in batch_results.items():
                if isinstance(result, dict):
                    if result.get("success", False):
                        aggregated["successful_batches"] += 1
                        aggregated["all_results"].append(result)
                    else:
                        aggregated["failed_batches"] += 1
                        aggregated["errors"].append({"batch": batch_key, "error": result.get("error", "Unknown")})

            if filter_failed:
                aggregated["all_results"] = [r for r in aggregated["all_results"] if r.get("success", False)]

            if aggregation_type == "first":
                aggregated["aggregated_content"] = aggregated["all_results"][0] if aggregated["all_results"] else None
            elif aggregation_type == "last":
                aggregated["aggregated_content"] = aggregated["all_results"][-1] if aggregated["all_results"] else None
            elif aggregation_type == "merge":
                merged = {}
                for r in aggregated["all_results"]:
                    if isinstance(r, dict) and "content" in r:
                        try:
                            import json
                            merged.update(json.loads(r["content"]) if isinstance(r["content"], str) else r["content"])
                        except Exception:
                            pass
                aggregated["aggregated_content"] = merged

            return ActionResult(
                success=aggregated["failed_batches"] == 0,
                message=f"Aggregated {aggregated['total_batches']} batches: {aggregated['successful_batches']} success, {aggregated['failed_batches']} failed",
                data=aggregated
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Result aggregator error: {e}")
