"""Automation pipeline action module for RabAI AutoClick.

Provides pipeline orchestration for automation workflows:
- SequentialPipelineAction: Execute actions sequentially
- ParallelPipelineAction: Execute actions in parallel
- ConditionalPipelineAction: Execute based on conditions
- RetryPipelineAction: Retry failed pipeline steps
- PipelineMonitorAction: Monitor pipeline execution
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
import concurrent.futures

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineStep:
    """Represents a step in a pipeline."""
    
    def __init__(self, name: str, action: Callable, params: Dict[str, Any] = None):
        self.name = name
        self.action = action
        self.params = params or {}
        self.result: Optional[ActionResult] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success(self) -> bool:
        return self.result.success if self.result else False


class SequentialPipelineAction(BaseAction):
    """Execute actions sequentially."""
    action_type = "sequential_pipeline"
    display_name = "顺序流水线"
    description = "按顺序执行自动化步骤"
    
    def __init__(self):
        super().__init__()
        self._pipeline: List[PipelineStep] = []
        self._results: List[ActionResult] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            stop_on_failure = params.get("stop_on_failure", True)
            
            self._results = []
            failed_step = None
            
            for i, step_config in enumerate(steps):
                step_name = step_config.get("name", f"step_{i}")
                step_action = step_config.get("action")
                step_params = step_config.get("params", {})
                
                step = PipelineStep(step_name, step_action, step_params)
                step.start_time = datetime.now()
                
                try:
                    if callable(step_action):
                        result = step_action(context, step_params)
                    else:
                        result = ActionResult(success=True, message="No-op step")
                    
                    step.result = result
                    self._results.append(result)
                    
                except Exception as e:
                    step.result = ActionResult(success=False, message=str(e))
                    self._results.append(step.result)
                
                step.end_time = datetime.now()
                
                if stop_on_failure and not step.success:
                    failed_step = step_name
                    break
            
            success = failed_step is None
            message = f"Pipeline completed" if success else f"Pipeline failed at: {failed_step}"
            
            return ActionResult(
                success=success,
                message=message,
                data={
                    "total_steps": len(steps),
                    "completed_steps": len(self._results),
                    "failed_step": failed_step,
                    "results": [{"name": s.name, "success": s.success, "duration": s.duration} 
                               for s in self._pipeline[:len(self._results)]]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ParallelPipelineAction(BaseAction):
    """Execute actions in parallel."""
    action_type = "parallel_pipeline"
    display_name = "并行流水线"
    description = "并行执行自动化步骤"
    
    def __init__(self):
        super().__init__()
        self._results: Dict[str, ActionResult] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            max_workers = params.get("max_workers", 4)
            fail_fast = params.get("fail_fast", True)
            
            self._results = {}
            
            def run_step(step_config: Dict[str, Any]) -> tuple:
                step_name = step_config.get("name", "unnamed")
                step_action = step_config.get("action")
                step_params = step_config.get("params", {})
                
                start_time = datetime.now()
                
                try:
                    if callable(step_action):
                        result = step_action(context, step_params)
                    else:
                        result = ActionResult(success=True, message="No-op step")
                except Exception as e:
                    result = ActionResult(success=False, message=str(e))
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                return (step_name, result, duration)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(run_step, step): step for step in steps}
                
                for future in concurrent.futures.as_completed(futures):
                    if fail_fast:
                        name, result, duration = future.result()
                        if not result.success:
                            executor.shutdown(wait=False)
                            return ActionResult(
                                success=False,
                                message=f"Pipeline failed at: {name}",
                                data={
                                    "failed_step": name,
                                    "duration": duration,
                                    "error": result.message
                                }
                            )
                    
                    name, result, duration = future.result()
                    self._results[name] = {
                        "result": result,
                        "duration": duration
                    }
            
            return ActionResult(
                success=True,
                message="Parallel pipeline completed",
                data={
                    "total_steps": len(steps),
                    "results": {name: {"success": v["result"].success, 
                                      "duration": v["duration"]} 
                               for name, v in self._results.items()}
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ConditionalPipelineAction(BaseAction):
    """Execute based on conditions."""
    action_type = "conditional_pipeline"
    display_name = "条件流水线"
    description = "根据条件执行自动化步骤"
    
    def __init__(self):
        super().__init__()
        self._conditions: Dict[str, Callable] = {}
        self._branches: Dict[str, List[Dict]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition_expr = params.get("condition")
            branches = params.get("branches", {})
            default_branch = params.get("default_branch")
            
            condition_met = self._evaluate_condition(condition_expr, context, params)
            
            branch_to_execute = None
            if condition_met and "true" in branches:
                branch_to_execute = "true"
            elif not condition_met and "false" in branches:
                branch_to_execute = "false"
            elif default_branch and default_branch in branches:
                branch_to_execute = default_branch
            
            if not branch_to_execute:
                return ActionResult(
                    success=True,
                    message="No branch selected",
                    data={"condition_result": condition_met}
                )
            
            branch_steps = branches[branch_to_execute]
            results = []
            
            for step_config in branch_steps:
                step_action = step_config.get("action")
                step_params = step_config.get("params", {})
                
                try:
                    if callable(step_action):
                        result = step_action(context, step_params)
                    else:
                        result = ActionResult(success=True, message="No-op step")
                    results.append(result)
                except Exception as e:
                    results.append(ActionResult(success=False, message=str(e)))
            
            all_success = all(r.success for r in results)
            
            return ActionResult(
                success=all_success,
                message=f"Branch '{branch_to_execute}' executed",
                data={
                    "branch": branch_to_execute,
                    "condition_result": condition_met,
                    "steps_executed": len(results),
                    "all_success": all_success
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _evaluate_condition(self, condition: Any, context: Any, params: Dict[str, Any]) -> bool:
        if condition is None:
            return True
        elif isinstance(condition, bool):
            return condition
        elif callable(condition):
            return condition(context, params)
        else:
            return bool(condition)


class RetryPipelineAction(BaseAction):
    """Retry failed pipeline steps."""
    action_type = "retry_pipeline"
    display_name = "重试流水线"
    description = "重试失败的流水线步骤"
    
    def __init__(self):
        super().__init__()
        self._retry_counts: Dict[str, int] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            max_retries = params.get("max_retries", 3)
            retry_on = params.get("retry_on", None)
            
            self._retry_counts = {f"step_{i}": 0 for i in range(len(steps))}
            results = []
            
            for i, step_config in enumerate(steps):
                step_name = step_config.get("name", f"step_{i}")
                step_action = step_config.get("action")
                step_params = step_config.get("params", {})
                
                retry_count = 0
                last_result = None
                
                while retry_count <= max_retries:
                    try:
                        if callable(step_action):
                            result = step_action(context, step_params)
                        else:
                            result = ActionResult(success=True, message="No-op step")
                        
                        last_result = result
                        
                        if result.success:
                            break
                        
                        if retry_on and not retry_on(result):
                            break
                        
                    except Exception as e:
                        last_result = ActionResult(success=False, message=str(e))
                    
                    retry_count += 1
                    self._retry_counts[step_name] = retry_count
                
                results.append({
                    "step": step_name,
                    "success": last_result.success if last_result else False,
                    "retries": retry_count,
                    "message": last_result.message if last_result else "No result"
                })
            
            all_success = all(r["success"] for r in results)
            
            return ActionResult(
                success=all_success,
                message="Pipeline execution completed",
                data={
                    "total_steps": len(steps),
                    "successful": sum(1 for r in results if r["success"]),
                    "failed": sum(1 for r in results if not r["success"]),
                    "retry_counts": self._retry_counts,
                    "step_results": results
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PipelineMonitorAction(BaseAction):
    """Monitor pipeline execution."""
    action_type = "pipeline_monitor"
    display_name = "流水线监控"
    description = "监控流水线执行状态"
    
    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, List[float]] = {}
        self._execution_log: List[Dict] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "status")
            pipeline_id = params.get("pipeline_id")
            
            if operation == "start":
                return self._start_monitoring(pipeline_id, params)
            elif operation == "log":
                return self._log_step(pipeline_id, params)
            elif operation == "status":
                return self._get_status(pipeline_id)
            elif operation == "metrics":
                return self._get_metrics(pipeline_id)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _start_monitoring(self, pipeline_id: str, params: Dict[str, Any]) -> ActionResult:
        if pipeline_id:
            self._metrics[pipeline_id] = []
        
        return ActionResult(
            success=True,
            message=f"Monitoring started for pipeline: {pipeline_id}",
            data={"pipeline_id": pipeline_id}
        )
    
    def _log_step(self, pipeline_id: str, params: Dict[str, Any]) -> ActionResult:
        step_name = params.get("step_name")
        duration = params.get("duration")
        success = params.get("success", True)
        
        log_entry = {
            "pipeline_id": pipeline_id,
            "step_name": step_name,
            "duration": duration,
            "success": success,
            "timestamp": datetime.now().isoformat()
        }
        
        self._execution_log.append(log_entry)
        
        if pipeline_id and duration:
            if pipeline_id not in self._metrics:
                self._metrics[pipeline_id] = []
            self._metrics[pipeline_id].append(duration)
        
        return ActionResult(
            success=True,
            message=f"Step logged: {step_name}",
            data={"log_entry": log_entry}
        )
    
    def _get_status(self, pipeline_id: str) -> ActionResult:
        pipeline_logs = [l for l in self._execution_log if l.get("pipeline_id") == pipeline_id]
        
        if not pipeline_logs:
            return ActionResult(
                success=True,
                message="No logs found",
                data={"pipeline_id": pipeline_id, "status": "unknown"}
            )
        
        total_steps = len(pipeline_logs)
        successful = sum(1 for l in pipeline_logs if l.get("success"))
        failed = total_steps - successful
        
        return ActionResult(
            success=True,
            message="Pipeline status retrieved",
            data={
                "pipeline_id": pipeline_id,
                "total_steps": total_steps,
                "successful": successful,
                "failed": failed,
                "status": "completed" if failed == 0 else "failed"
            }
        )
    
    def _get_metrics(self, pipeline_id: str) -> ActionResult:
        durations = self._metrics.get(pipeline_id, [])
        
        if not durations:
            return ActionResult(
                success=True,
                message="No metrics available",
                data={"pipeline_id": pipeline_id, "metrics": None}
            )
        
        return ActionResult(
            success=True,
            message="Metrics retrieved",
            data={
                "pipeline_id": pipeline_id,
                "metrics": {
                    "count": len(durations),
                    "total": sum(durations),
                    "average": sum(durations) / len(durations),
                    "min": min(durations),
                    "max": max(durations)
                }
            }
        )
