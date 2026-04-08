"""Data Shaper action module for RabAI AutoClick.

Reshapes data structures with projections, flattening,
and nested field access.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataShaperAction(BaseAction):
    """Shape data structures with projections and transforms.

    Projects fields, flattens nested objects, renames keys,
    and applies path-based transformations.
    """
    action_type = "data_shaper"
    display_name = "数据整形器"
    description = "通过投影和转换塑造数据结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Shape data.

        Args:
            context: Execution context.
            params: Dict with keys: data, shape (transformation spec),
                   flatten_depth, rename_map.

        Returns:
            ActionResult with shaped data.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            shape = params.get('shape', {})
            flatten_depth = params.get('flatten_depth', 0)
            rename_map = params.get('rename_map', {})
            pick_fields = params.get('pick_fields', None)
            omit_fields = params.get('omit_fields', None)

            if data is None:
                return ActionResult(success=False, message="data is required", duration=time.time() - start_time)

            result = data
            if isinstance(result, list):
                result = [self._shape_item(item, pick_fields, omit_fields, rename_map, flatten_depth) for item in result]
            elif isinstance(result, dict):
                result = self._shape_item(result, pick_fields, omit_fields, rename_map, flatten_depth)

            duration = time.time() - start_time
            return ActionResult(success=True, message="Data shaped", data={'result': result}, duration=duration)

        except Exception as e:
            return ActionResult(success=False, message=f"Shaper error: {str(e)}", duration=time.time() - start_time)

    def _shape_item(self, item: Dict, pick: Optional[List], omit: Optional[List], rename: Dict, depth: int) -> Dict:
        if not isinstance(item, dict):
            return item
        result = {}
        for k, v in item.items():
            if pick and k not in pick:
                continue
            if omit and k in omit:
                continue
            new_key = rename.get(k, k)
            if depth > 0 and isinstance(v, dict):
                result[new_key] = self._flatten_dict(v, depth)
            elif depth > 0 and isinstance(v, list) and v and isinstance(v[0], dict):
                result[new_key] = [self._flatten_dict(sub, depth) for sub in v]
            else:
                result[new_key] = v
        return result

    def _flatten_dict(self, d: Dict, depth: int, prefix: str = '', current: int = 0) -> Any:
        if current >= depth:
            return d
        result = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                result.update(self._flatten_dict(v, depth, key, current + 1))
            else:
                result[key] = v
        return result


class DataPivoterAction(BaseAction):
    """Pivot data between wide and long formats.

    Converts between wide (columns) and long (rows) formats
    for analytics and visualization.
    """
    action_type = "data_pivoter"
    display_name = "数据透视器"
    description = "在宽格式和长格式之间转换数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pivot data.

        Args:
            context: Execution context.
            params: Dict with keys: data, pivot_mode (wide_to_long/long_to_wide),
                   index_cols, value_cols, var_col.

        Returns:
            ActionResult with pivoted data.
        """
        start_time = time.time()
        try:
            data = params.get('data', [])
            pivot_mode = params.get('pivot_mode', 'wide_to_long')
            index_cols = params.get('index_cols', [])
            value_cols = params.get('value_cols', [])
            var_col = params.get('var_col', 'variable')
            val_col = params.get('val_col', 'value')

            if not isinstance(data, list):
                data = [data]

            if pivot_mode == 'wide_to_long':
                result = self._wide_to_long(data, index_cols, value_cols, var_col, val_col)
            else:
                result = self._long_to_wide(data, index_cols, var_col, val_col)

            duration = time.time() - start_time
            return ActionResult(success=True, message=f"Pivoted to {pivot_mode}", data={'result': result, 'count': len(result)}, duration=duration)

        except Exception as e:
            return ActionResult(success=False, message=f"Pivoter error: {str(e)}", duration=time.time() - start_time)

    def _wide_to_long(self, data: List[Dict], index: List, values: List, var_col: str, val_col: str) -> List[Dict]:
        result = []
        for row in data:
            base = {k: row[k] for k in index if k in row}
            for vcol in values:
                if vcol in row:
                    result.append({**base, var_col: vcol, val_col: row[vcol]})
        return result

    def _long_to_wide(self, data: List[Dict], index: List, var_col: str, val_col: str) -> List[Dict]:
        from collections import defaultdict
        groups = defaultdict(dict)
        for row in data:
            key = tuple(row.get(k) for k in index)
            var = row.get(var_col, '')
            val = row.get(val_col)
            groups[key][var] = val
        return [{**dict(zip(index, key)), **vals} for key, vals in groups.items()]
