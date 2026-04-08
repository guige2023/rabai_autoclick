"""Data merge action module for RabAI AutoClick.

Provides data merge/join operations: inner, left, right,
outer joins, union, and dedup.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JoinAction(BaseAction):
    """Join two datasets on a common key.
    
    Supports inner, left, right, outer, and cross joins.
    Handles duplicate columns with suffixing.
    """
    action_type = "data_join"
    display_name = "数据关联"
    description = "基于键关联两个数据集，支持多种连接类型"

    JOIN_TYPES = ['inner', 'left', 'right', 'outer', 'cross']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Join two datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - left: list of dicts (left dataset)
                - right: list of dicts (right dataset)
                - on: str (join key, required for non-cross join)
                - how: str (join type: inner/left/right/outer/cross)
                - left_suffix: str (suffix for duplicate columns from left)
                - right_suffix: str (suffix for duplicate columns from right)
                - indicator: bool (add _merge column showing source)
                - save_to_var: str
        
        Returns:
            ActionResult with joined result.
        """
        left = params.get('left', [])
        right = params.get('right', [])
        on = params.get('on', '')
        how = params.get('how', 'left')
        left_suffix = params.get('left_suffix', '_x')
        right_suffix = params.get('right_suffix', '_y')
        indicator = params.get('indicator', False)
        save_to_var = params.get('save_to_var', 'join_result')

        if not on and how != 'cross':
            return ActionResult(success=False, message="Join key 'on' is required")

        if how == 'cross':
            return self._cross_join(left, right, indicator, save_to_var, context)

        # Index right dataset by key
        right_index: Dict[Tuple, List[Dict]] = {}
        for r in right:
            key = self._get_key(r, on)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(r)

        # Determine all columns (handle suffixing)
        left_cols = set()
        right_cols = set()
        for l in left:
            left_cols.update(l.keys())
        for r in right:
            right_cols.update(r.keys())

        result = []
        right_keys_used = set()

        for l_row in left:
            key = self._get_key(l_row, on)
            right_matches = right_index.get(key, [])

            if not right_matches and how in ['inner', 'left']:
                # No match, for left join add as-is
                if how == 'left':
                    row = dict(l_row)
                    if indicator:
                        row['_merge'] = 'left_only'
                    result.append(row)

            for r_row in right_matches:
                right_keys_used.add(key)
                merged = self._merge_rows(l_row, r_row, on, left_cols, right_cols,
                                         left_suffix, right_suffix)
                if indicator:
                    merged['_merge'] = 'both'
                result.append(merged)

        # Right-only rows for outer/right join
        if how in ['outer', 'right']:
            for key, r_rows in right_index.items():
                if key not in right_keys_used:
                    for r_row in r_rows:
                        row = dict(r_row)
                        # Add left columns as empty/null
                        for col in left_cols - right_cols:
                            if col != on:
                                row[col + left_suffix if col in right_cols else col] = None
                        if indicator:
                            row['_merge'] = 'right_only'
                        result.append(row)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'rows': len(result), 'join_type': how},
            message=f"{how.capitalize()} join: {len(result)} rows"
        )

    def _get_key(self, row: Dict, on: str) -> Tuple:
        """Extract join key from row."""
        val = row.get(on, '')
        return (str(val),)

    def _merge_rows(self, left_row: Dict, right_row: Dict, on: str,
                    left_cols: set, right_cols: set,
                    left_suffix: str, right_suffix: str) -> Dict:
        """Merge two rows, handling duplicate columns."""
        merged = {}
        
        # Add all left columns
        for col, val in left_row.items():
            if col in right_cols and col != on:
                merged[col + left_suffix] = val
            else:
                merged[col] = val

        # Add all right columns
        for col, val in right_row.items():
            if col in left_cols and col != on:
                merged[col + right_suffix] = val
            elif col not in left_cols:
                merged[col] = val
            # Skip if col == on (already added from left)

        return merged

    def _cross_join(self, left: List, right: List, indicator: bool,
                    save_to_var: str, context: Any) -> ActionResult:
        """Execute a cross join."""
        result = []
        for l_row in left:
            for r_row in right:
                merged = dict(l_row)
                for col, val in r_row.items():
                    if col in l_row:
                        merged[col + '_right'] = val
                    else:
                        merged[col] = val
                if indicator:
                    merged['_merge'] = 'both'
                result.append(merged)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'rows': len(result), 'join_type': 'cross'},
            message=f"Cross join: {len(result)} rows"
        )


class UnionAction(BaseAction):
    """Union multiple datasets (stack rows).
    
    Supports concat, union all, and union distinct.
    Handles column alignment automatically.
    """
    action_type = "data_union"
    display_name = "数据合并"
    description = "合并多个数据集（堆叠行），支持去重和列对齐"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Union multiple datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - datasets: list of lists of dicts
                - how: str (all/distinct)
                - columns: list (explicit column order, optional)
                - save_to_var: str
        
        Returns:
            ActionResult with unioned result.
        """
        datasets = params.get('datasets', [])
        how = params.get('how', 'all')  # all or distinct
        explicit_columns = params.get('columns', [])
        save_to_var = params.get('save_to_var', 'union_result')

        if not datasets:
            return ActionResult(success=False, message="No datasets provided")

        # Collect all rows
        all_rows = []
        all_columns: set = set()

        for ds in datasets:
            for row in ds:
                all_columns.update(row.keys())
                all_rows.append(row)

        # Determine column order
        if explicit_columns:
            column_order = explicit_columns + [c for c in all_columns if c not in explicit_columns]
        else:
            # Use first dataset's column order
            if datasets and datasets[0]:
                column_order = list(datasets[0][0].keys()) + [c for c in all_columns if c not in datasets[0][0]]
            else:
                column_order = sorted(all_columns)

        # Align columns in each row
        aligned = []
        for row in all_rows:
            aligned_row = {col: row.get(col) for col in column_order}
            aligned.append(aligned_row)

        # Deduplicate if needed
        if how == 'distinct':
            seen = set()
            unique = []
            for row in aligned:
                key = tuple(sorted(row.items()))
                if key not in seen:
                    seen.add(key)
                    unique.append(row)
            aligned = unique

        if context and save_to_var:
            context.variables[save_to_var] = aligned

        return ActionResult(
            success=True,
            data={'rows': len(aligned), 'unique': how == 'distinct'},
            message=f"Union ({how}): {len(aligned)} rows"
        )


class DedupAction(BaseAction):
    """Remove duplicate rows from a dataset.
    
    Supports deduplication by all columns or specific keys,
    with options to keep first or last occurrence.
    """
    action_type = "dedup"
    display_name = "数据去重"
    description = "删除重复行，支持按指定键去重并保留首条或末条"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Remove duplicate rows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - subset: list of str (columns to check for dupes)
                - keep: str (first/last, default first)
                - save_to_var: str
        
        Returns:
            ActionResult with deduplicated result.
        """
        data = params.get('data', [])
        subset = params.get('subset', None)
        keep = params.get('keep', 'first')
        save_to_var = params.get('save_to_var', 'dedup_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        if subset:
            # Dedupe by specific columns
            seen: Dict[tuple, Any] = {}
            result = []
            
            for row in data:
                key = tuple(str(row.get(k, '')) for k in subset)
                if key not in seen:
                    seen[key] = row
                    if keep == 'first':
                        result.append(row)
                elif keep == 'last':
                    seen[key] = row
            
            if keep == 'last':
                result = list(seen.values())
        else:
            # Dedupe by entire row
            seen = set()
            result = []
            for row in data:
                key = tuple(sorted(row.items()))
                if key not in seen:
                    seen.add(key)
                    result.append(row)

        removed = len(data) - len(result)

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data={'original_rows': len(data), 'deduped_rows': len(result), 'removed': removed},
            message=f"Dedup: removed {removed} rows, {len(result)} remaining"
        )
