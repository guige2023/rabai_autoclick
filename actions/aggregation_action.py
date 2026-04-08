"""Aggregation action module for RabAI AutoClick.

Provides data aggregation operations including
sum, average, count, min, max, and group aggregation.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AggregateSumAction(BaseAction):
    """Sum numeric values in a list.
    
    Supports field extraction from dicts and type conversion.
    """
    action_type = "aggregate_sum"
    display_name = "求和"
    description = "对数值列表求和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sum values.
        
        Args:
            context: Execution context.
            params: Dict with keys: values, field, precision,
                   save_to_var.
        
        Returns:
            ActionResult with sum.
        """
        values = params.get('values', [])
        field = params.get('field', None)
        precision = params.get('precision', 2)
        save_to_var = params.get('save_to_var', None)

        if not values:
            return ActionResult(success=False, message="Values list is empty")

        try:
            if field:
                nums = []
                for item in values:
                    if isinstance(item, dict):
                        v = item.get(field, 0)
                    else:
                        v = getattr(item, field, 0)
                    nums.append(float(v))
            else:
                nums = [float(v) for v in values]

            total = sum(nums)

            result_data = {
                'sum': round(total, precision),
                'count': len(nums),
                'field': field
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"求和完成: {round(total, precision)}",
                data=result_data
            )

        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'precision': 2,
            'save_to_var': None
        }


class AggregateAverageAction(BaseAction):
    """Calculate average of numeric values.
    
    Supports mean and weighted average calculations.
    """
    action_type = "aggregate_average"
    display_name = "平均值"
    description = "计算数值平均值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate average.
        
        Args:
            context: Execution context.
            params: Dict with keys: values, field, weighted_by,
                   precision, save_to_var.
        
        Returns:
            ActionResult with average.
        """
        values = params.get('values', [])
        field = params.get('field', None)
        weighted_by = params.get('weighted_by', None)
        precision = params.get('precision', 2)
        save_to_var = params.get('save_to_var', None)

        if not values:
            return ActionResult(success=False, message="Values list is empty")

        try:
            if weighted_by:
                # Weighted average
                total_weighted = 0
                total_weight = 0
                for item in values:
                    if isinstance(item, dict):
                        v = float(item.get(field, 0))
                        w = float(item.get(weighted_by, 1))
                    else:
                        v = float(getattr(item, field, 0))
                        w = float(getattr(item, weighted_by, 1))
                    total_weighted += v * w
                    total_weight += w
                avg = total_weighted / total_weight if total_weight else 0
            else:
                # Simple average
                if field:
                    nums = [float(item.get(field, 0)) if isinstance(item, dict) else float(getattr(item, field, 0)) for item in values]
                else:
                    nums = [float(v) for v in values]
                avg = sum(nums) / len(nums)

            result_data = {
                'average': round(avg, precision),
                'count': len(values),
                'weighted': weighted_by is not None
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"平均值: {round(avg, precision)}",
                data=result_data
            )

        except (ValueError, TypeError, ZeroDivisionError) as e:
            return ActionResult(
                success=False,
                message=f"平均值计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'weighted_by': None,
            'precision': 2,
            'save_to_var': None
        }


class AggregateCountAction(BaseAction):
    """Count occurrences of values.
    
    Supports frequency counting and grouping.
    """
    action_type = "aggregate_count"
    display_name = "计数"
    description = "统计值出现次数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Count values.
        
        Args:
            context: Execution context.
            params: Dict with keys: values, field, top_n,
                   save_to_var.
        
        Returns:
            ActionResult with counts.
        """
        values = params.get('values', [])
        field = params.get('field', None)
        top_n = params.get('top_n', None)
        save_to_var = params.get('save_to_var', None)

        if not values:
            return ActionResult(success=False, message="Values list is empty")

        # Extract field if specified
        items = []
        for item in values:
            if field:
                if isinstance(item, dict):
                    items.append(item.get(field, None))
                else:
                    items.append(getattr(item, field, None))
            else:
                items.append(item)

        # Count
        counter = Counter(items)
        counts = dict(counter)

        # Sort by count
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)

        if top_n:
            sorted_counts = sorted_counts[:top_n]

        result_data = {
            'counts': dict(sorted_counts),
            'total': len(values),
            'unique': len(counts),
            'top': sorted_counts[0] if sorted_counts else None
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"计数完成: {len(values)} 项, {len(counts)} 个唯一值",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'top_n': None,
            'save_to_var': None
        }
