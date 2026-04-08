"""Set operation action module for RabAI AutoClick.

Provides set operations: union, intersection, difference,
symmetric difference, and subset checks.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Set, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SetOpAction(BaseAction):
    """Perform set operations on collections.
    
    Union, intersection, difference, symmetric
    difference, and subset checks.
    """
    action_type = "set_op"
    display_name = "集合运算"
    description = "集合运算：并集、交集、差集、对称差集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform set operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - set_a: list or set
                - set_b: list or set
                - operation: str (union/intersection/difference/symmetric_diff/issubset/issuperset)
                - key: str (field to use for comparison)
                - save_to_var: str
        
        Returns:
            ActionResult with set operation result.
        """
        set_a = params.get('set_a', [])
        set_b = params.get('set_b', [])
        operation = params.get('operation', 'union')
        key = params.get('key', '')
        save_to_var = params.get('save_to_var', 'set_result')

        # Convert to sets
        if key:
            a_keys = {self._get_key(item, key) for item in set_a}
            b_keys = {self._get_key(item, key) for item in set_b}
            a_set = set_a
            b_set = set_b
        else:
            a_keys = set(set_a)
            b_keys = set(set_b)
            a_set = set_a
            b_set = set_b

        result_set: Set = set()

        if operation == 'union':
            result_set = a_keys | b_keys
        elif operation == 'intersection':
            result_set = a_keys & b_keys
        elif operation == 'difference':
            result_set = a_keys - b_keys
        elif operation == 'symmetric_diff':
            result_set = a_keys ^ b_keys
        elif operation == 'issubset':
            is_subset = a_keys <= b_keys
            result = {
                'operation': 'issubset',
                'result': is_subset,
                'set_a_size': len(a_keys),
                'set_b_size': len(b_keys),
            }
            if context and save_to_var:
                context.variables[save_to_var] = result
            return ActionResult(success=True, data=result, message=f"A subset of B: {is_subset}")
        elif operation == 'issuperset':
            is_superset = a_keys >= b_keys
            result = {
                'operation': 'issuperset',
                'result': is_superset,
                'set_a_size': len(a_keys),
                'set_b_size': len(b_keys),
            }
            if context and save_to_var:
                context.variables[save_to_var] = result
            return ActionResult(success=True, data=result, message=f"A superset of B: {is_superset}")

        result = {
            'operation': operation,
            'result': list(result_set),
            'count': len(result_set),
            'set_a_size': len(a_keys),
            'set_b_size': len(b_keys),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"{operation}: {len(result_set)} items"
        )

    def _get_key(self, item: Any, key: str) -> Any:
        """Extract key from item."""
        if isinstance(item, dict):
            return item.get(key, id(item))
        return item


class SetCompareAction(BaseAction):
    """Compare two sets and report differences.
    
    Find items unique to each set and items in common.
    """
    action_type = "set_compare"
    display_name = "集合比较"
    description = "比较两个集合找出差异项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compare two sets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - set_a: list
                - set_b: list
                - key: str (field for comparison)
                - save_to_var: str
        
        Returns:
            ActionResult with comparison results.
        """
        set_a = params.get('set_a', [])
        set_b = params.get('set_b', [])
        key = params.get('key', '')
        save_to_var = params.get('save_to_var', 'compare_result')

        if key:
            a_keys = {self._get_key(item, key) for item in set_a}
            b_keys = {self._get_key(item, key) for item in set_b}
        else:
            a_keys = set(set_a)
            b_keys = set(set_b)

        in_a_only = list(a_keys - b_keys)
        in_b_only = list(b_keys - a_keys)
        in_both = list(a_keys & b_keys)

        result = {
            'in_a_only': in_a_only,
            'in_b_only': in_b_only,
            'in_both': in_both,
            'a_only_count': len(in_a_only),
            'b_only_count': len(in_b_only),
            'common_count': len(in_both),
            'total_unique': len(in_a_only) + len(in_b_only),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Comparison: {len(in_both)} common, {len(in_a_only)} in A only, {len(in_b_only)} in B only"
        )

    def _get_key(self, item: Any, key: str) -> Any:
        """Extract key from item."""
        if isinstance(item, dict):
            return item.get(key, id(item))
        return item


class SetPowerAction(BaseAction):
    """Compute power set of a collection.
    
    Generate all possible subsets of a set.
    """
    action_type = "set_power"
    display_name = "幂集"
    description = "计算集合的所有子集(幂集)"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compute power set.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - items: list
                - max_size: int (limit subset size)
                - save_to_var: str
        
        Returns:
            ActionResult with power set.
        """
        items = params.get('items', [])
        max_size = params.get('max_size', 0)
        save_to_var = params.get('save_to_var', 'power_result')

        if not items:
            return ActionResult(success=False, message="No items provided")

        n = len(items)
        power = []

        for mask in range(1 << n):
            subset = [items[i] for i in range(n) if mask & (1 << i)]
            if max_size > 0 and len(subset) > max_size:
                continue
            power.append(subset)

        result = {
            'power_set': power,
            'count': len(power),
            'original_size': n,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Power set of {n} items: {len(power)} subsets"
        )


class SetCartesianProductAction(BaseAction):
    """Compute Cartesian product of collections.
    
    Generate all combinations across multiple sets.
    """
    action_type = "set_cartesian"
    display_name = "笛卡尔积"
    description = "计算多个集合的笛卡尔积"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compute Cartesian product.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - sets: list of lists
                - repeat: int (repeat one set N times)
                - save_to_var: str
        
        Returns:
            ActionResult with product tuples.
        """
        sets = params.get('sets', [])
        repeat = params.get('repeat', 1)
        save_to_var = params.get('save_to_var', 'cartesian_result')

        if not sets:
            return ActionResult(success=False, message="No sets provided")

        import itertools

        try:
            if repeat > 1:
                product = list(itertools.product(*sets, repeat=repeat))
            else:
                product = list(itertools.product(*sets))
        except Exception as e:
            return ActionResult(success=False, message=f"Product error: {e}")

        result = {
            'product': product,
            'count': len(product),
            'num_sets': len(sets),
            'set_sizes': [len(s) for s in sets],
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Cartesian product: {len(product)} combinations"
        )
