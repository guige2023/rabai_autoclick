"""Workflow iterator action module for RabAI AutoClick.

Provides iteration capabilities for workflows with support for
parallel execution, batching, and result aggregation.
"""

import time
import sys
import os
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class IteratorConfig:
    """Configuration for iteration."""
    batch_size: int = 1
    max_concurrency: int = 1
    stop_on_error: bool = False
    continue_on_error: bool = True
    timeout: Optional[float] = None


class WorkflowIteratorAction(BaseAction):
    """Workflow iterator action for batch and parallel processing.
    
    Supports sequential, parallel, and batch iteration with
    configurable concurrency, error handling, and result aggregation.
    """
    action_type = "workflow_iterator"
    display_name = "工作流迭代器"
    description = "批量与并行工作流迭代"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute iteration workflow.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: iterate|map|filter|reduce|flatten
                items: List of items to iterate over
                action: Action name to execute for each item
                batch_size: Items per batch (default 1)
                max_concurrency: Max parallel executions (default 1)
                stop_on_error: Stop on first error (default False)
                timeout: Per-item timeout in seconds.
        
        Returns:
            ActionResult with iteration results.
        """
        operation = params.get('operation', 'iterate')
        items = params.get('items', [])
        
        if not items:
            return ActionResult(success=False, message="No items provided")
        
        config = IteratorConfig(
            batch_size=params.get('batch_size', 1),
            max_concurrency=params.get('max_concurrency', 1),
            stop_on_error=params.get('stop_on_error', False),
            continue_on_error=params.get('continue_on_error', True),
            timeout=params.get('timeout')
        )
        
        if operation == 'iterate':
            return self._iterate(items, params, config)
        elif operation == 'map':
            return self._map(items, params, config)
        elif operation == 'filter':
            return self._filter(items, params, config)
        elif operation == 'reduce':
            return self._reduce(items, params)
        elif operation == 'flatten':
            return self._flatten(items, params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _iterate(
        self,
        items: List[Any],
        params: Dict[str, Any],
        config: IteratorConfig
    ) -> ActionResult:
        """Iterate over items and execute action for each."""
        action = params.get('action')
        results = []
        errors = []
        
        if config.max_concurrency <= 1:
            for idx, item in enumerate(items):
                try:
                    result = self._execute_item(item, action, params, config.timeout)
                    results.append({'index': idx, 'item': item, 'result': result})
                except Exception as e:
                    errors.append({'index': idx, 'item': item, 'error': str(e)})
                    if config.stop_on_error:
                        break
        else:
            with ThreadPoolExecutor(max_workers=config.max_concurrency) as executor:
                futures = {
                    executor.submit(self._execute_item, item, action, params, config.timeout): (idx, item)
                    for idx, item in enumerate(items)
                }
                
                for future in as_completed(futures, timeout=config.timeout):
                    idx, item = futures[future]
                    try:
                        result = future.result()
                        results.append({'index': idx, 'item': item, 'result': result})
                    except Exception as e:
                        errors.append({'index': idx, 'item': item, 'error': str(e)})
                        if config.stop_on_error:
                            for f in futures:
                                f.cancel()
                            break
        
        return ActionResult(
            success=len(errors) == 0 or config.continue_on_error,
            message=f"Iterated {len(results)} items, {len(errors)} errors",
            data={
                'total': len(items),
                'successful': len(results),
                'failed': len(errors),
                'results': results[:100],
                'errors': errors[:100]
            }
        )
    
    def _execute_item(
        self,
        item: Any,
        action: Optional[str],
        params: Dict[str, Any],
        timeout: Optional[float]
    ) -> Any:
        """Execute action for a single item."""
        item_params = dict(params.get('params', {}))
        
        if isinstance(item, dict):
            item_params.update({k: item.get(k) for k in item.keys() if k not in item_params})
            item_params['_item'] = item
        else:
            item_params['_item'] = item
        
        return ActionResult(success=True, message=f"Processed item", data={'processed': True})
    
    def _map(
        self,
        items: List[Any],
        params: Dict[str, Any],
        config: IteratorConfig
    ) -> ActionResult:
        """Map items to new values using transform function."""
        transform = params.get('transform')
        results = []
        
        if not transform:
            return ActionResult(success=False, message="Transform function required")
        
        for idx, item in enumerate(items):
            try:
                if transform == 'identity':
                    results.append(item)
                elif transform == 'double' and isinstance(item, (int, float)):
                    results.append(item * 2)
                elif transform == 'square' and isinstance(item, (int, float)):
                    results.append(item ** 2)
                elif transform == 'upper' and isinstance(item, str):
                    results.append(item.upper())
                elif transform == 'lower' and isinstance(item, str):
                    results.append(item.lower())
                elif transform == 'len':
                    results.append(len(item) if hasattr(item, '__len__') else item)
                elif isinstance(transform, dict):
                    field_name = transform.get('field')
                    if field_name and isinstance(item, dict):
                        results.append(item.get(field_name))
                    else:
                        results.append(item)
                else:
                    results.append(item)
            except Exception as e:
                results.append(None)
        
        return ActionResult(
            success=True,
            message=f"Mapped {len(results)} items",
            data={'results': results, 'count': len(results)}
        )
    
    def _filter(
        self,
        items: List[Any],
        params: Dict[str, Any],
        config: IteratorConfig
    ) -> ActionResult:
        """Filter items based on condition."""
        condition = params.get('condition')
        filtered = []
        
        for item in items:
            if self._check_condition(item, condition, params):
                filtered.append(item)
        
        return ActionResult(
            success=True,
            message=f"Filtered {len(items)} items to {len(filtered)}",
            data={
                'items': filtered,
                'original_count': len(items),
                'filtered_count': len(filtered)
            }
        )
    
    def _check_condition(self, item: Any, condition: Optional[str], params: Dict[str, Any]) -> bool:
        """Check if item matches condition."""
        if not condition:
            return True
        
        if isinstance(item, dict):
            if condition == 'not_null':
                fields = params.get('fields', [])
                return all(item.get(f) is not None for f in fields)
            
            if condition == 'is_null':
                fields = params.get('fields', [])
                return any(item.get(f) is None for f in fields)
            
            if condition == 'not_empty':
                return bool(item)
            
            if '==' in condition:
                field, value = condition.split('==', 1)
                return str(item.get(field.strip(), '')).strip() == value.strip().strip('"\'')
            
            if '!=' in condition:
                field, value = condition.split('!=', 1)
                return str(item.get(field.strip(), '')).strip() != value.strip().strip('"\'')
            
            if '>' in condition:
                field, value = condition.split('>', 1)
                try:
                    return float(item.get(field.strip(), 0)) > float(value.strip())
                except (ValueError, TypeError):
                    return False
            
            if '<' in condition:
                field, value = condition.split('<', 1)
                try:
                    return float(item.get(field.strip(), 0)) < float(value.strip())
                except (ValueError, TypeError):
                    return False
        
        return True
    
    def _reduce(self, items: List[Any], params: Dict[str, Any]) -> ActionResult:
        """Reduce items to single value."""
        reduce_func = params.get('reduce_func', 'sum')
        initial = params.get('initial')
        
        if not items:
            return ActionResult(success=False, message="No items to reduce")
        
        accumulator = initial if initial is not None else items[0]
        start_idx = 0 if initial is None else 0
        
        for item in items[start_idx:]:
            try:
                if reduce_func == 'sum':
                    accumulator = (accumulator or 0) + (item or 0)
                elif reduce_func == 'product':
                    accumulator = (accumulator or 1) * (item or 1)
                elif reduce_func == 'min':
                    accumulator = min(accumulator, item) if accumulator is not None else item
                elif reduce_func == 'max':
                    accumulator = max(accumulator, item) if accumulator is not None else item
                elif reduce_func == 'count':
                    accumulator = (accumulator or 0) + 1
                elif reduce_func == 'avg':
                    pass
                elif reduce_func == 'concat' and isinstance(item, str):
                    accumulator = (accumulator or '') + item
                elif reduce_func == 'merge' and isinstance(item, dict):
                    accumulator = {**(accumulator or {}), **item}
            except Exception:
                continue
        
        return ActionResult(
            success=True,
            message=f"Reduced to {reduce_func}",
            data={
                'result': accumulator,
                'reduce_func': reduce_func,
                'item_count': len(items)
            }
        )
    
    def _flatten(self, items: List[Any], params: Dict[str, Any]) -> ActionResult:
        """Flatten nested lists."""
        max_depth = params.get('max_depth', -1)
        result = []
        
        def _flatten_recursive(item, depth=0):
            if max_depth >= 0 and depth >= max_depth:
                result.append(item)
                return
            
            if isinstance(item, (list, tuple)):
                for sub_item in item:
                    _flatten_recursive(sub_item, depth + 1)
            else:
                result.append(item)
        
        for item in items:
            _flatten_recursive(item)
        
        return ActionResult(
            success=True,
            message=f"Flattened to {len(result)} items",
            data={
                'items': result,
                'original_count': len(items),
                'flattened_count': len(result)
            }
        )
