"""Data join action module for RabAI AutoClick.

Provides data joining with support for inner, left, right,
outer, and cross joins on multiple datasets.
"""

import sys
import os
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    CROSS = "cross"
    ANTI = "anti"
    SEMI = "semi"


class DataJoinAction(BaseAction):
    """Join multiple datasets with various join types.
    
    Supports inner, left, right, outer, cross, anti,
    and semi joins on key fields.
    """
    action_type = "data_join"
    display_name = "数据连接"
    description = "数据连接：内连接/左连接/右连接/全连接/交叉连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Join datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (join/union/concat/lookup)
                - left_data: list of dicts
                - right_data: list of dicts
                - join_type: str (inner/left/right/outer/cross/anti/semi)
                - left_key: str, key field from left dataset
                - right_key: str, key field from right dataset
                - select_fields: list of {table, field, alias} specs
                - save_to_var: str
        
        Returns:
            ActionResult with joined data.
        """
        operation = params.get('operation', 'join')
        left_data = params.get('left_data', [])
        right_data = params.get('right_data', [])
        join_type = params.get('join_type', 'inner')
        left_key = params.get('left_key', '')
        right_key = params.get('right_key', '')
        select_fields = params.get('select_fields', [])
        save_to_var = params.get('save_to_var', None)

        if operation == 'join':
            return self._join(
                left_data, right_data, join_type, left_key, right_key,
                select_fields, save_to_var
            )
        elif operation == 'union':
            return self._union(left_data, right_data, save_to_var)
        elif operation == 'concat':
            return self._concat(params.get('datasets', []), save_to_var)
        elif operation == 'lookup':
            return self._lookup(left_data, right_data, left_key, right_key, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _join(
        self, left_data: List[Dict], right_data: List[Dict],
        join_type: str, left_key: str, right_key: str,
        select_fields: List, save_to_var: Optional[str]
    ) -> ActionResult:
        """Perform join operation."""
        if not left_data or not right_data:
            return ActionResult(success=False, message="Both datasets required")

        if join_type == 'cross':
            return self._cross_join(left_data, right_data, save_to_var)

        if not left_key or not right_key:
            return ActionResult(success=False, message="left_key and right_key required for non-cross join")

        # Build index on right dataset
        right_index = defaultdict(list)
        for record in right_data:
            key = record.get(right_key)
            right_index[key].append(record)

        results = []
        left_matches = defaultdict(bool)

        for left_record in left_data:
            left_val = left_record.get(left_key)
            matched_right = right_index.get(left_val, [])

            if not matched_right:
                if join_type in ('left', 'outer'):
                    results.append({**left_record, **{f'_right_{k}': None for k in right_data[0].keys()}})
            else:
                left_matches[left_val] = True
                for right_record in matched_right:
                    merged = {**left_record, **right_record}
                    results.append(merged)

        # Right-only and outer joins
        if join_type in ('right', 'outer'):
            for right_record in right_data:
                right_val = right_record.get(right_key)
                if not left_matches[right_val]:
                    results.append({**right_record, **{f'_left_{k}': None for k in left_data[0].keys()}})

        if join_type == 'anti':
            matched_keys = set()
            for lr in left_data:
                lv = lr.get(left_key)
                for rr in right_data:
                    if rr.get(right_key) == lv:
                        matched_keys.add(lv)
            results = [lr for lr in left_data if lr.get(left_key) not in matched_keys]
        elif join_type == 'semi':
            matched_keys = set()
            for lr in left_data:
                lv = lr.get(left_key)
                for rr in right_data:
                    if rr.get(right_key) == lv:
                        matched_keys.add(lv)
                        break
            results = [lr for lr in left_data if lr.get(left_key) in matched_keys]

        # Apply field selection
        if select_fields:
            results = self._select_fields(results, select_fields)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=True,
            message=f"Joined {len(results)} records ({join_type})",
            data=results
        )

    def _cross_join(
        self, left_data: List[Dict], right_data: List[Dict], save_to_var: Optional[str]
    ) -> ActionResult:
        """Cartesian product of both datasets."""
        results = [
            {**left_record, **right_record}
            for left_record in left_data
            for right_record in right_data
        ]

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=True,
            message=f"Cross join: {len(results)} records",
            data=results
        )

    def _union(
        self, left_data: List[Dict], right_data: List[Dict], save_to_var: Optional[str]
    ) -> ActionResult:
        """Union of two datasets (no duplicates)."""
        left_keys = set()
        results = []

        for record in left_data:
            key = self._record_key(record)
            if key not in left_keys:
                left_keys.add(key)
                results.append(record)

        for record in right_data:
            key = self._record_key(record)
            if key not in left_keys:
                left_keys.add(key)
                results.append(record)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=True,
            message=f"Union: {len(results)} unique records",
            data=results
        )

    def _concat(self, datasets: List[List[Dict]], save_to_var: Optional[str]) -> ActionResult:
        """Concatenate multiple datasets (allow duplicates)."""
        results = []
        for ds in datasets:
            results.extend(ds)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=True,
            message=f"Concatenated {len(results)} records from {len(datasets)} datasets",
            data=results
        )

    def _lookup(
        self, left_data: List[Dict], lookup_data: List[Dict],
        left_key: str, lookup_key: str, save_to_var: Optional[str]
    ) -> ActionResult:
        """Lookup values from second dataset into first."""
        lookup_index = {r.get(lookup_key): r for r in lookup_data}

        results = []
        for record in left_data:
            lv = record.get(left_key)
            matched = lookup_index.get(lv, {})
            new_record = {**record, '_lookup': matched}
            results.append(new_record)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=True,
            message=f"Lookup: {len(results)} records enriched",
            data=results
        )

    def _record_key(self, record: Dict) -> str:
        """Generate hashable key for a record."""
        return json.dumps(record, sort_keys=True, default=str)

    def _select_fields(self, results: List[Dict], select_fields: List) -> List[Dict]:
        """Apply field selection to results."""
        if not select_fields:
            return results

        selected = []
        for record in results:
            new_record = {}
            for spec in select_fields:
                table = spec.get('table', '')
                field_name = spec.get('field', '')
                alias = spec.get('alias', field_name)

                if table:
                    prefixed = f'_{table}_{field_name}'
                    if prefixed in record:
                        new_record[alias] = record[prefixed]
                else:
                    if field_name in record:
                        new_record[alias] = record[field_name]
            selected.append(new_record)

        return selected

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'left_data': [],
            'right_data': [],
            'datasets': [],
            'join_type': 'inner',
            'left_key': '',
            'right_key': '',
            'select_fields': [],
            'save_to_var': None,
        }
