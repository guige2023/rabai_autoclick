"""Automation polling action module for RabAI AutoClick.

Provides polling mechanisms for monitoring conditions, waiting for
state changes, and periodic task execution with backoff strategies.
"""

import time
from typing import Any, Dict, List, Optional, Union, Callable

from core.base_action import BaseAction, ActionResult


class AutomationPollAction(BaseAction):
    """Poll a condition until it becomes true or timeout expires.
    
    Supports custom condition functions, interval configuration,
    and maximum attempt limits.
    """
    action_type = "automation_poll"
    display_name = "轮询等待"
    description = "轮询条件直到为真或超时"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Poll until condition is met.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition_type, target_value, compare_op,
                   interval, max_attempts, timeout.
        
        Returns:
            ActionResult with poll result and attempt count.
        """
        condition_type = params.get("condition_type", "value")
        target_value = params.get("target_value")
        compare_op = params.get("compare_op", "eq")
        interval = params.get("interval", 1.0)
        max_attempts = params.get("max_attempts", 30)
        timeout = params.get("timeout", 60)
        
        source_value = params.get("source_value")
        source_path = params.get("source_path", "data")
        
        start_time = time.time()
        attempts = 0
        
        try:
            while attempts < max_attempts:
                attempts += 1
                elapsed = time.time() - start_time
                
                if elapsed > timeout:
                    return ActionResult(
                        success=False,
                        message=f"Timeout after {elapsed:.1f}s ({attempts} attempts)",
                        data={"attempts": attempts, "elapsed": elapsed}
                    )
                
                current_value = source_value
                
                if condition_type == "value":
                    matched = self._compare(current_value, target_value, compare_op)
                elif condition_type == "exists":
                    matched = current_value is not None
                elif condition_type == "truthy":
                    matched = bool(current_value)
                else:
                    matched = False
                
                if matched:
                    return ActionResult(
                        success=True,
                        message=f"Condition met after {attempts} attempts ({elapsed:.2f}s)",
                        data={
                            "attempts": attempts,
                            "elapsed": elapsed,
                            "final_value": current_value
                        }
                    )
                
                if attempts < max_attempts:
                    time.sleep(interval)
            
            return ActionResult(
                success=False,
                message=f"Max attempts ({max_attempts}) reached",
                data={"attempts": attempts, "elapsed": time.time() - start_time}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Poll failed: {e}")
    
    def _compare(self, actual: Any, expected: Any, op: str) -> bool:
        try:
            if op == "eq":
                return actual == expected
            elif op == "ne":
                return actual != expected
            elif op == "gt":
                return float(actual) > float(expected)
            elif op == "ge":
                return float(actual) >= float(expected)
            elif op == "lt":
                return float(actual) < float(expected)
            elif op == "le":
                return float(actual) <= float(expected)
            elif op == "contains":
                return expected in actual if actual else False
            elif op == "in":
                return actual in expected
            elif op == "regex":
                import re
                return bool(re.match(expected, str(actual)))
            return False
        except (ValueError, TypeError):
            return False


class AutomationRetryPollAction(BaseAction):
    """Poll with exponential backoff when condition is not met.
    
    Increases wait interval exponentially between attempts.
    Useful for resilient waiting against unreliable services.
    """
    action_type = "automation_retry_poll"
    display_name = "指数退避轮询"
    description = "条件不满足时指数增加等待间隔的轮询"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Poll with exponential backoff.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition, initial_interval, max_interval,
                   multiplier, max_attempts, timeout, jitter.
        
        Returns:
            ActionResult with poll result and timing.
        """
        initial_interval = params.get("initial_interval", 1.0)
        max_interval = params.get("max_interval", 60.0)
        multiplier = params.get("multiplier", 2.0)
        max_attempts = params.get("max_attempts", 10)
        timeout = params.get("timeout", 120)
        jitter = params.get("jitter", 0.1)
        
        condition_expr = params.get("condition_expr")
        source_value = params.get("source_value")
        expected_value = params.get("expected_value")
        
        start_time = time.time()
        attempts = 0
        interval = initial_interval
        total_sleep = 0.0
        
        try:
            while attempts < max_attempts:
                attempts += 1
                elapsed = time.time() - start_time
                
                if elapsed > timeout:
                    return ActionResult(
                        success=False,
                        message=f"Timeout after {elapsed:.1f}s",
                        data={"attempts": attempts, "total_sleep": total_sleep}
                    )
                
                matched = False
                if condition_expr:
                    try:
                        matched = eval(condition_expr, {"value": source_value})
                    except Exception:
                        pass
                elif expected_value is not None:
                    matched = source_value == expected_value
                
                if matched:
                    return ActionResult(
                        success=True,
                        message=f"Condition met at attempt {attempts}",
                        data={
                            "attempts": attempts,
                            "elapsed": elapsed,
                            "total_sleep": total_sleep,
                            "final_interval": interval
                        }
                    )
                
                if attempts < max_attempts:
                    sleep_time = min(interval, max_interval)
                    if jitter > 0:
                        import random
                        sleep_time *= (1 + random.uniform(-jitter, jitter))
                    
                    time.sleep(sleep_time)
                    total_sleep += sleep_time
                    interval = min(interval * multiplier, max_interval)
            
            return ActionResult(
                success=False,
                message=f"Max attempts ({max_attempts}) with backoff",
                data={"attempts": attempts, "total_sleep": total_sleep}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry poll failed: {e}")


class AutomationPeriodicAction(BaseAction):
    """Execute a task periodically at fixed intervals.
    
    Runs a callback or nested actions at regular intervals
    for a specified duration or number of iterations.
    """
    action_type = "automation_periodic"
    display_name = "定期执行"
    description = "按固定间隔定期执行任务"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute task periodically.
        
        Args:
            context: Execution context.
            params: Dict with keys: interval, max_iterations, max_duration,
                   action_type, action_params.
        
        Returns:
            ActionResult with execution summary.
        """
        interval = params.get("interval", 5.0)
        max_iterations = params.get("max_iterations", 10)
        max_duration = params.get("max_duration", 300)
        action_type = params.get("action_type")
        action_params = params.get("action_params", {})
        
        start_time = time.time()
        iterations = 0
        results = []
        errors = []
        
        try:
            while iterations < max_iterations:
                iterations += 1
                elapsed = time.time() - start_time
                
                if elapsed > max_duration:
                    break
                
                iter_start = time.time()
                
                if action_type:
                    try:
                        action = context.get_action(action_type)
                        if action:
                            result = action.execute(context, action_params)
                            results.append({
                                "iteration": iterations,
                                "success": result.success,
                                "duration": time.time() - iter_start,
                                "message": result.message
                            })
                        else:
                            errors.append(f"Iteration {iterations}: Unknown action type '{action_type}'")
                    except Exception as e:
                        errors.append(f"Iteration {iterations}: {e}")
                else:
                    results.append({
                        "iteration": iterations,
                        "success": True,
                        "duration": 0,
                        "message": "Periodic tick"
                    })
                
                if iterations < max_iterations:
                    time.sleep(interval)
            
            total_elapsed = time.time() - start_time
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Periodic execution: {iterations} iterations in {total_elapsed:.1f}s",
                data={
                    "iterations": iterations,
                    "total_elapsed": total_elapsed,
                    "results": results,
                    "errors": errors,
                    "success_count": sum(1 for r in results if r["success"])
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Periodic execution failed: {e}")
