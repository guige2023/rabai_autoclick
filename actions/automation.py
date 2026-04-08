"""Automation action module for RabAI AutoClick.

Provides automation workflow actions including retry logic,
circuit breaker, throttle, debounce, and batch processing.
"""

import os
import sys
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RetryAction(BaseAction):
    """Retry a condition or action until success.
    
    Supports exponential backoff, max attempts,
    and success condition checking.
    """
    action_type = "retry"
    display_name = "重试"
    description = "重试操作直到成功，支持指数退避"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Retry an operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: condition_var, expected_value,
                   max_attempts, delay, backoff, save_to_var.
        
        Returns:
            ActionResult with retry result.
        """
        condition_var = params.get('condition_var', None)
        expected_value = params.get('expected_value', None)
        max_attempts = params.get('max_attempts', 5)
        delay = params.get('delay', 1.0)
        backoff = params.get('backoff', 2.0)
        save_to_var = params.get('save_to_var', None)

        attempt = 0
        current_delay = delay

        while attempt < max_attempts:
            attempt += 1

            # Check condition
            if condition_var and hasattr(context, 'variables'):
                value = context.variables.get(condition_var)
                if expected_value is not None:
                    condition_met = (value == expected_value)
                else:
                    condition_met = bool(value)
            else:
                # No condition - always succeed
                condition_met = True

            if condition_met:
                result_data = {
                    'success': True,
                    'attempts': attempt,
                    'condition_var': condition_var,
                    'condition_met': True
                }
                if save_to_var:
                    context.variables[save_to_var] = result_data
                return ActionResult(
                    success=True,
                    message=f"重试成功: 第 {attempt} 次尝试",
                    data=result_data
                )

            # Wait before next attempt
            if attempt < max_attempts:
                time.sleep(current_delay)
                current_delay *= backoff

        result_data = {
            'success': False,
            'attempts': max_attempts,
            'condition_var': condition_var,
            'condition_met': False
        }
        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=False,
            message=f"重试失败: {max_attempts} 次尝试后仍未满足条件",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'condition_var': None,
            'expected_value': None,
            'max_attempts': 5,
            'delay': 1.0,
            'backoff': 2.0,
            'save_to_var': None
        }


class ThrottleAction(BaseAction):
    """Throttle action execution rate.
    
    Ensures minimum interval between executions.
    """
    action_type = "throttle"
    display_name = "限速"
    description = "限制操作执行频率"

    _last_executions: Dict[str, float] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Throttle execution.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, interval, save_to_var.
        
        Returns:
            ActionResult with throttle result.
        """
        key = params.get('key', 'default')
        interval = params.get('interval', 1.0)
        save_to_var = params.get('save_to_var', None)

        now = time.time()

        with self._lock:
            last_exec = self._last_executions.get(key, 0)
            elapsed = now - last_exec

            if elapsed < interval:
                wait_time = interval - elapsed
                result_data = {
                    'throttled': True,
                    'wait_time': wait_time,
                    'elapsed': elapsed,
                    'interval': interval
                }
                if save_to_var:
                    context.variables[save_to_var] = result_data
                return ActionResult(
                    success=True,
                    message=f"限速: 需等待 {wait_time:.2f}s",
                    data=result_data
                )

            self._last_executions[key] = now

        result_data = {
            'throttled': False,
            'elapsed': elapsed,
            'interval': interval
        }
        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"执行通过: 距上次 {elapsed:.2f}s",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': 'default',
            'interval': 1.0,
            'save_to_var': None
        }


class DebounceAction(BaseAction):
    """Debounce rapid successive calls.
    
    Ignores calls within cooldown period.
    """
    action_type = "debounce"
    display_name = "防抖"
    description = "忽略频繁调用，在静默期后生效"

    _last_calls: Dict[str, float] = {}
    _lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Debounce execution.
        
        Args:
            context: Execution context.
            params: Dict with keys: key, cooldown, save_to_var.
        
        Returns:
            ActionResult with debounce result.
        """
        key = params.get('key', 'default')
        cooldown = params.get('cooldown', 1.0)
        save_to_var = params.get('save_to_var', None)

        now = time.time()

        with self._lock:
            last_call = self._last_calls.get(key, 0)
            elapsed = now - last_call

            if elapsed < cooldown:
                remaining = cooldown - elapsed
                result_data = {
                    'debounced': True,
                    'remaining': remaining,
                    'elapsed': elapsed
                }
                if save_to_var:
                    context.variables[save_to_var] = result_data
                return ActionResult(
                    success=True,
                    message=f"防抖: 忽略 ({remaining:.2f}s remaining)",
                    data=result_data
                )

            self._last_calls[key] = now

        result_data = {
            'debounced': False,
            'elapsed': elapsed
        }
        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message="防抖: 执行通过",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': 'default',
            'cooldown': 1.0,
            'save_to_var': None
        }


class BatchProcessAction(BaseAction):
    """Process items in batches.
    
    Splits large lists into chunks for batch processing.
    """
    action_type = "batch_process"
    display_name = "批量处理"
    description = "分批处理列表数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Batch process items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, batch_size,
                   overlap, save_to_var.
        
        Returns:
            ActionResult with batch information.
        """
        items = params.get('items', [])
        batch_size = params.get('batch_size', 10)
        overlap = params.get('overlap', 0)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        if batch_size <= 0:
            return ActionResult(
                success=False,
                message=f"Invalid batch_size: {batch_size}"
            )

        batches = []
        step = batch_size - overlap if overlap > 0 else batch_size

        for i in range(0, len(items), step):
            batch = items[i:i + batch_size]
            batches.append({
                'index': len(batches),
                'items': batch,
                'count': len(batch),
                'start': i,
                'end': min(i + batch_size, len(items))
            })

        result_data = {
            'batches': batches,
            'total_items': len(items),
            'batch_count': len(batches),
            'batch_size': batch_size
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"分批完成: {len(items)} 项 -> {len(batches)} 批",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items', 'batch_size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'overlap': 0,
            'save_to_var': None
        }
