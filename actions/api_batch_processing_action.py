"""API batch processing action module for RabAI AutoClick.

Provides batch processing for API operations:
- BatchRequestAction: Process multiple API requests in batch
- BatchResponseHandlerAction: Handle batch API responses
- BatchErrorCollectorAction: Collect and summarize batch errors
- BatchSizeOptimizerAction: Optimize batch sizes dynamically
- BatchThrottlerAction: Throttle batch requests to avoid rate limits
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchRequestAction(BaseAction):
    """Process multiple API requests in batch."""
    action_type = "batch_request"
    display_name = "批量请求"
    description = "批量处理多个API请求"
    
    def __init__(self):
        super().__init__()
        self._batch_size = 10
        self._pending_requests: List[Dict] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            requests = params.get("requests", [])
            batch_size = params.get("batch_size", 10)
            parallel = params.get("parallel", False)
            
            if not requests:
                return ActionResult(success=False, message="No requests provided")
            
            batches = self._create_batches(requests, batch_size)
            results = []
            
            for i, batch in enumerate(batches):
                if parallel:
                    batch_results = self._execute_parallel(batch)
                else:
                    batch_results = self._execute_sequential(batch)
                
                results.extend(batch_results)
                
                if params.get("early_terminate_on_failure") and \
                   any(not r.get("success") for r in batch_results):
                    break
            
            return ActionResult(
                success=True,
                message=f"Batch processing complete",
                data={
                    "total_requests": len(requests),
                    "batches": len(batches),
                    "results": results,
                    "successful": sum(1 for r in results if r.get("success")),
                    "failed": sum(1 for r in results if not r.get("success"))
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _create_batches(self, items: List[Any], batch_size: int) -> List[List[Any]]:
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    
    def _execute_sequential(self, batch: List[Dict]) -> List[Dict]:
        results = []
        for req in batch:
            result = {
                "request_id": req.get("id", "unknown"),
                "success": True,
                "message": "Simulated success"
            }
            results.append(result)
        return results
    
    def _execute_parallel(self, batch: List[Dict]) -> List[Dict]:
        return self._execute_sequential(batch)


class BatchResponseHandlerAction(BaseAction):
    """Handle batch API responses."""
    action_type = "batch_response_handler"
    display_name = "批量响应处理"
    description = "处理批量API响应"
    
    def __init__(self):
        super().__init__()
        self._response_cache: Dict[str, Any] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            responses = params.get("responses", [])
            handler_type = params.get("handler_type", "aggregate")
            group_by = params.get("group_by")
            
            if handler_type == "aggregate":
                return self._handle_aggregate(responses, params)
            elif handler_type == "group":
                return self._handle_group(responses, group_by)
            elif handler_type == "filter":
                return self._handle_filter(responses, params)
            elif handler_type == "transform":
                return self._handle_transform(responses, params)
            else:
                return ActionResult(success=False, message=f"Unknown handler type: {handler_type}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _handle_aggregate(self, responses: List[Dict], params: Dict[str, Any]) -> ActionResult:
        aggregation = params.get("aggregation", "count")
        
        successful = [r for r in responses if r.get("success", True)]
        failed = [r for r in responses if not r.get("success", True)]
        
        if aggregation == "count":
            value = len(responses)
        elif aggregation == "success_rate":
            value = len(successful) / len(responses) if responses else 0
        elif aggregation == "list":
            value = responses
        else:
            value = len(responses)
        
        return ActionResult(
            success=True,
            message="Aggregate response handled",
            data={
                "handler_type": "aggregate",
                "aggregation": aggregation,
                "value": value,
                "total": len(responses),
                "successful": len(successful),
                "failed": len(failed)
            }
        )
    
    def _handle_group(self, responses: List[Dict], group_by: str) -> ActionResult:
        if not group_by:
            return ActionResult(success=False, message="group_by is required")
        
        groups: Dict[str, List] = defaultdict(list)
        
        for response in responses:
            key = response.get(group_by, "unknown")
            groups[key].append(response)
        
        return ActionResult(
            success=True,
            message="Responses grouped",
            data={
                "handler_type": "group",
                "group_by": group_by,
                "groups": {k: len(v) for k, v in groups.items()},
                "total_groups": len(groups)
            }
        )
    
    def _handle_filter(self, responses: List[Dict], params: Dict[str, Any]) -> ActionResult:
        filter_key = params.get("filter_key")
        filter_value = params.get("filter_value")
        
        if not filter_key:
            return ActionResult(success=False, message="filter_key is required")
        
        filtered = [
            r for r in responses 
            if r.get(filter_key) == filter_value
        ]
        
        return ActionResult(
            success=True,
            message=f"Filtered {len(filtered)} responses",
            data={
                "handler_type": "filter",
                "filter_key": filter_key,
                "filter_value": filter_value,
                "original_count": len(responses),
                "filtered_count": len(filtered),
                "filtered": filtered
            }
        )
    
    def _handle_transform(self, responses: List[Dict], params: Dict[str, Any]) -> ActionResult:
        transform_fn = params.get("transform_fn")
        
        if not callable(transform_fn):
            transform_fn = lambda x: {"id": x.get("id"), "status": x.get("status", "unknown")}
        
        transformed = [transform_fn(r) for r in responses]
        
        return ActionResult(
            success=True,
            message=f"Transformed {len(transformed)} responses",
            data={
                "handler_type": "transform",
                "original_count": len(responses),
                "transformed_count": len(transformed),
                "transformed": transformed
            }
        )


class BatchErrorCollectorAction(BaseAction):
    """Collect and summarize batch errors."""
    action_type = "batch_error_collector"
    display_name = "批量错误收集"
    description = "收集并汇总批量错误"
    
    def __init__(self):
        super().__init__()
        self._errors: List[Dict] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "collect")
            
            if operation == "collect":
                return self._collect_errors(params)
            elif operation == "summarize":
                return self._summarize_errors()
            elif operation == "clear":
                return self._clear_errors()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _collect_errors(self, params: Dict[str, Any]) -> ActionResult:
        error = params.get("error")
        request_id = params.get("request_id")
        severity = params.get("severity", "error")
        
        error_entry = {
            "error": error,
            "request_id": request_id,
            "severity": severity,
            "timestamp": datetime.now().isoformat()
        }
        
        self._errors.append(error_entry)
        
        return ActionResult(
            success=True,
            message="Error collected",
            data={
                "error_entry": error_entry,
                "total_errors": len(self._errors)
            }
        )
    
    def _summarize_errors(self) -> ActionResult:
        if not self._errors:
            return ActionResult(
                success=True,
                message="No errors to summarize",
                data={"error_count": 0}
            )
        
        by_severity = defaultdict(int)
        by_request = defaultdict(int)
        
        for err in self._errors:
            by_severity[err.get("severity", "error")] += 1
            if err.get("request_id"):
                by_request[err.get("request_id")] += 1
        
        return ActionResult(
            success=True,
            message="Error summary generated",
            data={
                "total_errors": len(self._errors),
                "by_severity": dict(by_severity),
                "by_request": dict(by_request),
                "recent_errors": self._errors[-10:]
            }
        )
    
    def _clear_errors(self) -> ActionResult:
        count = len(self._errors)
        self._errors.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared {count} errors",
            data={"cleared_count": count}
        )


class BatchSizeOptimizerAction(BaseAction):
    """Optimize batch sizes dynamically."""
    action_type = "batch_size_optimizer"
    display_name = "批量大小优化"
    description = "动态优化批量大小"
    
    def __init__(self):
        super().__init__()
        self._history: List[Dict] = []
        self._optimal_size = 10
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "optimize")
            
            if operation == "optimize":
                return self._optimize(params)
            elif operation == "record":
                return self._record_performance(params)
            elif operation == "status":
                return self._get_status()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _optimize(self, params: Dict[str, Any]) -> ActionResult:
        target_time = params.get("target_time", 1.0)
        
        if len(self._history) < 3:
            return ActionResult(
                success=True,
                message="Insufficient data for optimization",
                data={
                    "optimal_size": self._optimal_size,
                    "history_count": len(self._history)
                }
            )
        
        recent = self._history[-10:]
        successful = [h for h in recent if h.get("success")]
        
        if successful:
            avg_time = sum(h.get("time", 1.0) for h in successful) / len(successful)
            
            if avg_time > target_time:
                self._optimal_size = max(1, self._optimal_size - 1)
            elif avg_time < target_time * 0.5:
                self._optimal_size += 1
        
        return ActionResult(
            success=True,
            message="Batch size optimized",
            data={
                "optimal_size": self._optimal_size,
                "avg_time": avg_time if successful else None,
                "history_count": len(self._history)
            }
        )
    
    def _record_performance(self, params: Dict[str, Any]) -> ActionResult:
        batch_size = params.get("batch_size", 10)
        duration = params.get("duration", 1.0)
        success = params.get("success", True)
        
        self._history.append({
            "batch_size": batch_size,
            "time": duration,
            "success": success,
            "timestamp": datetime.now().isoformat()
        })
        
        return ActionResult(
            success=True,
            message="Performance recorded",
            data={
                "batch_size": batch_size,
                "duration": duration,
                "total_records": len(self._history)
            }
        )
    
    def _get_status(self) -> ActionResult:
        return ActionResult(
            success=True,
            message="Batch optimizer status",
            data={
                "optimal_size": self._optimal_size,
                "history_count": len(self._history),
                "recent_performance": self._history[-5:] if self._history else []
            }
        )


class BatchThrottlerAction(BaseAction):
    """Throttle batch requests to avoid rate limits."""
    action_type = "batch_throttler"
    display_name = "批量限流"
    description = "批量请求限流控制"
    
    def __init__(self):
        super().__init__()
        self._rate_limit = 100
        self._window = 60
        self._requests: List[datetime] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            batch_size = params.get("batch_size", 1)
            
            if operation == "check":
                return self._check_throttle(batch_size)
            elif operation == "configure":
                return self._configure(params)
            elif operation == "status":
                return self._get_status()
            elif operation == "reset":
                return self._reset()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check_throttle(self, batch_size: int) -> ActionResult:
        now = datetime.now()
        cutoff = now.timestamp() - self._window
        
        self._requests = [r for r in self._requests if r.timestamp() > cutoff]
        
        available = self._rate_limit - len(self._requests)
        
        if available >= batch_size:
            for _ in range(batch_size):
                self._requests.append(now)
            
            return ActionResult(
                success=True,
                message="Request allowed",
                data={
                    "allowed": True,
                    "batch_size": batch_size,
                    "available_quota": available - batch_size,
                    "rate_limit": self._rate_limit,
                    "window_seconds": self._window
                }
            )
        else:
            wait_time = self._calculate_wait_time()
            
            return ActionResult(
                success=False,
                message="Rate limit exceeded",
                data={
                    "allowed": False,
                    "batch_size": batch_size,
                    "available_quota": available,
                    "rate_limit": self._rate_limit,
                    "window_seconds": self._window,
                    "wait_seconds": wait_time
                }
            )
    
    def _calculate_wait_time(self) -> float:
        if not self._requests:
            return 0
        
        oldest = min(self._requests)
        elapsed = (datetime.now() - oldest).total_seconds()
        return max(0, self._window - elapsed)
    
    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        rate_limit = params.get("rate_limit")
        window = params.get("window")
        
        if rate_limit is not None:
            self._rate_limit = rate_limit
        if window is not None:
            self._window = window
        
        return ActionResult(
            success=True,
            message="Throttler configured",
            data={
                "rate_limit": self._rate_limit,
                "window_seconds": self._window
            }
        )
    
    def _get_status(self) -> ActionResult:
        now = datetime.now()
        cutoff = now.timestamp() - self._window
        self._requests = [r for r in self._requests if r.timestamp() > cutoff]
        
        return ActionResult(
            success=True,
            message="Throttler status",
            data={
                "rate_limit": self._rate_limit,
                "window_seconds": self._window,
                "current_usage": len(self._requests),
                "available": self._rate_limit - len(self._requests)
            }
        )
    
    def _reset(self) -> ActionResult:
        count = len(self._requests)
        self._requests.clear()
        
        return ActionResult(
            success=True,
            message=f"Reset throttler, cleared {count} requests",
            data={"cleared_count": count}
        )
