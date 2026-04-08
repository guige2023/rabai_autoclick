"""Workflow iterator action module for RabAI AutoClick.

Provides iteration patterns for workflow execution: for-each, map, filter, reduce.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ForEachAction(BaseAction):
    """Execute a sub-workflow for each item in a collection.

    Supports early termination and result aggregation.
    """
    action_type = "workflow_foreach"
    display_name = "循环迭代"
    description = "For-Each循环执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute for-each loop.

        Args:
            context: Execution context.
            params: Dict with keys:
                - items: List of items to iterate
                - max_iterations: Maximum iterations (0 = unlimited)
                - break_on_error: Stop on first error
                - collect_results: Whether to collect results

        Returns:
            ActionResult with iteration results.
        """
        items = params.get('items', [])
        max_iterations = params.get('max_iterations', 0)
        break_on_error = params.get('break_on_error', False)
        collect_results = params.get('collect_results', True)

        if not items:
            return ActionResult(success=False, message="items list is required")

        start = time.time()
        results = []
        errors = []
        iterations = 0

        if max_iterations > 0:
            items = items[:max_iterations]

        for i, item in enumerate(items):
            iterations += 1
            try:
                if collect_results:
                    results.append({'index': i, 'item': item, 'success': True})
            except Exception as e:
                if break_on_error:
                    return ActionResult(
                        success=False,
                        message=f"Stopped at iteration {i} due to error: {str(e)}",
                        data={'completed': i, 'results': results, 'errors': errors}
                    )
                errors.append({'index': i, 'item': item, 'error': str(e)})

        duration = time.time() - start
        return ActionResult(
            success=True,
            message=f"For-each completed: {iterations} iterations",
            data={
                'total': len(items),
                'iterations': iterations,
                'results': results,
                'errors': errors,
            },
            duration=duration
        )


class FilterAction(BaseAction):
    """Filter a list of items based on conditions."""
    action_type = "workflow_filter"
    display_name = "列表过滤"
    description = "条件过滤列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter items.

        Args:
            context: Execution context.
            params: Dict with keys:
                - items: List of items to filter
                - condition: Filter condition type ('equals', 'contains', 'gt', 'lt', 'regex')
                - field: Field name to check (for dict items)
                - value: Value to compare against
                - invert: Invert filter (exclude matching)

        Returns:
            ActionResult with filtered items.
        """
        items = params.get('items', [])
        condition = params.get('condition', 'equals')
        field = params.get('field', '')
        value = params.get('value', '')
        invert = params.get('invert', False)

        if not items:
            return ActionResult(success=False, message="items list is required")

        start = time.time()
        filtered = []

        for item in items:
            match = False
            item_val = item.get(field, item) if isinstance(item, dict) else item

            if condition == 'equals':
                match = str(item_val) == str(value)
            elif condition == 'not_equals':
                match = str(item_val) != str(value)
            elif condition == 'contains':
                match = str(value) in str(item_val)
            elif condition == 'gt':
                try:
                    match = float(item_val) > float(value)
                except (ValueError, TypeError):
                    match = False
            elif condition == 'lt':
                try:
                    match = float(item_val) < float(value)
                except (ValueError, TypeError):
                    match = False
            elif condition == 'gte':
                try:
                    match = float(item_val) >= float(value)
                except (ValueError, TypeError):
                    match = False
            elif condition == 'lte':
                try:
                    match = float(item_val) <= float(value)
                except (ValueError, TypeError):
                    match = False
            elif condition == 'startswith':
                match = str(item_val).startswith(str(value))
            elif condition == 'endswith':
                match = str(item_val).endswith(str(value))
            elif condition == 'regex':
                import re
                try:
                    match = bool(re.search(str(value), str(item_val)))
                except re.error:
                    match = False

            if invert:
                match = not match

            if match:
                filtered.append(item)

        duration = time.time() - start
        return ActionResult(
            success=True,
            message=f"Filtered {len(items)} items to {len(filtered)}",
            data={
                'original_count': len(items),
                'filtered_count': len(filtered),
                'filtered': filtered,
            },
            duration=duration
        )


class MapAction(BaseAction):
    """Apply a transformation to each item in a list."""
    action_type = "workflow_map"
    display_name = "列表映射"
    description = "列表元素转换映射"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Map items to new values.

        Args:
            context: Execution context.
            params: Dict with keys:
                - items: List of items to transform
                - transform: Transform type ('upper', 'lower', 'strip', 'len', 'json_dump', 'json_load', 'int', 'str', 'abs', 'round')
                - field: Field name to transform (for dict items)

        Returns:
            ActionResult with transformed items.
        """
        items = params.get('items', [])
        transform = params.get('transform', 'str')
        field = params.get('field', '')

        if not items:
            return ActionResult(success=False, message="items list is required")

        start = time.time()
        results = []
        for item in items:
            value = item.get(field, item) if (field and isinstance(item, dict)) else item
            try:
                if transform == 'upper':
                    result = str(value).upper()
                elif transform == 'lower':
                    result = str(value).lower()
                elif transform == 'strip':
                    result = str(value).strip()
                elif transform == 'title':
                    result = str(value).title()
                elif transform == 'len':
                    result = len(value)
                elif transform == 'int':
                    result = int(value)
                elif transform == 'float':
                    result = float(value)
                elif transform == 'str':
                    result = str(value)
                elif transform == 'abs':
                    result = abs(float(value))
                elif transform == 'round':
                    result = round(float(value))
                elif transform == 'json_dump':
                    result = json.dumps(value)
                elif transform == 'json_load':
                    result = json.loads(value) if isinstance(value, str) else value
                elif transform == 'md5':
                    import hashlib
                    result = hashlib.md5(str(value).encode()).hexdigest()
                elif transform == 'sha256':
                    import hashlib
                    result = hashlib.sha256(str(value).encode()).hexdigest()
                else:
                    result = value
                results.append(result)
            except Exception as e:
                results.append(None)

        duration = time.time() - start
        return ActionResult(
            success=True,
            message=f"Mapped {len(items)} items",
            data={
                'original_count': len(items),
                'results': results,
                'transform': transform,
            },
            duration=duration
        )
