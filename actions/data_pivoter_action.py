"""Data pivoter action module for RabAI AutoClick.

Provides pivot table operations to transform data from rows to
columns (pivot) and from columns to rows (unpivot Melt).
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PivotAction(BaseAction):
    """Pivot data from rows to columns.
    
    Transforms data so that unique values in a column
    become new columns with aggregated values.
    """
    action_type = "pivot"
    display_name = "透视表"
    description = "将行数据透视为列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, index (field to group by),
                   columns (field whose values become columns),
                   values (field to aggregate), aggfunc (sum|count|avg|first).
        
        Returns:
            ActionResult with pivoted data.
        """
        data = params.get('data', [])
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'first')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not all([index, columns, values]):
            return ActionResult(
                success=False,
                message="index, columns, and values fields are all required"
            )

        pivot = {}
        col_values = set()

        for row in data:
            idx_val = self._get_field(row, index)
            col_val = self._get_field(row, columns)
            val = self._get_field(row, values)
            col_values.add(col_val)

            key = idx_val
            if key not in pivot:
                pivot[key] = {}
            pivot[key][col_val] = val

        results = []
        for idx_val, col_data in pivot.items():
            result_row = {index: idx_val}
            for cv in col_values:
                result_row[cv] = col_data.get(cv)
            results.append(result_row)

        return ActionResult(
            success=True,
            message=f"Pivoted {len(data)} rows into {len(results)} pivot rows with {len(col_values)} columns",
            data={
                'pivoted': results,
                'columns': list(col_values),
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class UnpivotAction(BaseAction):
    """Unpivot/Melt data from columns to rows.
    
    Transforms wide format data (columns as variables)
    into long format (variable-value pairs).
    """
    action_type = "unpivot"
    display_name = "逆透视"
    description = "将列数据逆透视为行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unpivot wide data to long format.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, id_vars (list of fields
                   to keep as identifiers), value_vars (list of
                   fields to unpivot), var_name, value_name.
        
        Returns:
            ActionResult with unpivoted data.
        """
        data = params.get('data', [])
        id_vars = params.get('id_vars', [])
        value_vars = params.get('value_vars', [])
        var_name = params.get('var_name', 'variable')
        value_name = params.get('value_name', 'value')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not value_vars:
            if id_vars and data:
                first_row = data[0]
                all_fields = set()
                for row in data:
                    if isinstance(row, dict):
                        all_fields.update(row.keys())
                value_vars = [f for f in all_fields if f not in id_vars]

        results = []
        for row in data:
            base = {k: self._get_field(row, k) for k in id_vars if self._get_field(row, k) is not None}
            for var in value_vars:
                val = self._get_field(row, var)
                new_row = dict(base)
                new_row[var_name] = var
                new_row[value_name] = val
                results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Unpivoted {len(data)} rows into {len(results)} rows",
            data={
                'unpivoted': results,
                'count': len(results),
                'var_name': var_name,
                'value_name': value_name
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class TransposeAction(BaseAction):
    """Transpose rows and columns.
    
    Swaps rows and columns in data, turning
    row observations into columns.
    """
    action_type = "transpose"
    display_name = "转置"
    description = "转置数据行列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transpose data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, header_field (field
                   to use as column headers), value_field.
        
        Returns:
            ActionResult with transposed data.
        """
        data = params.get('data', [])
        header_field = params.get('header_field', '')
        value_field = params.get('value_field', '')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not data:
            return ActionResult(success=True, message="No data to transpose", data={'transposed': [], 'count': 0})

        if header_field:
            headers = [self._get_field(row, header_field) for row in data]
            if value_field:
                values = [[self._get_field(row, value_field)] for row in data]
            else:
                values = [[self._get_field(row, f) for f in headers] for row in data]
        else:
            headers = [f"col_{i}" for i in range(len(data[0]) if isinstance(data[0], dict) else len(data))]
            values = [[self._get_field(row, h) for h in headers] for row in data]

        transposed = []
        for col_idx in range(len(headers)):
            row_data = {'column': headers[col_idx]}
            for row_idx, row in enumerate(data):
                row_data[f"row_{row_idx}"] = values[row_idx][col_idx] if row_idx < len(values) and col_idx < len(values[row_idx]) else None
            transposed.append(row_data)

        return ActionResult(
            success=True,
            message=f"Transposed to {len(transposed)} columns from {len(data)} rows",
            data={
                'transposed': transposed,
                'count': len(transposed)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class CrossTabAction(BaseAction):
    """Create cross-tabulation (contingency table).
    
    Computes frequency counts or aggregations for
    combinations of two categorical variables.
    """
    action_type = "cross_tab"
    display_name = "交叉表"
    description = "创建两个分类变量的交叉表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create cross-tabulation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rows (field for row variable),
                   cols (field for column variable), values (optional
                   field to aggregate), aggfunc.
        
        Returns:
            ActionResult with cross-tabulation result.
        """
        data = params.get('data', [])
        rows = params.get('rows', '')
        cols = params.get('cols', '')
        values = params.get('values')
        aggfunc = params.get('aggfunc', 'count')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        table = {}
        row_vals = set()
        col_vals = set()

        for row in data:
            rv = self._get_field(row, rows)
            cv = self._get_field(row, cols)
            vv = self._get_field(row, values) if values else 1
            row_vals.add(rv)
            col_vals.add(cv)

            if rv not in table:
                table[rv] = {}
            if cv not in table[rv]:
                table[rv][cv] = []
            table[rv][cv].append(vv)

        results = []
        for rv in sorted(row_vals):
            result_row = {rows: rv}
            for cv in sorted(col_vals):
                vals = table.get(rv, {}).get(cv, [])
                if aggfunc == 'count':
                    result_row[cv] = len(vals)
                elif aggfunc == 'sum':
                    result_row[cv] = sum(vals)
                elif aggfunc == 'avg':
                    result_row[cv] = sum(vals) / len(vals) if vals else 0
                elif aggfunc == 'first':
                    result_row[cv] = vals[0] if vals else None
            results.append(result_row)

        return ActionResult(
            success=True,
            message=f"Cross-tab: {len(results)} rows x {len(col_vals)} columns",
            data={
                'cross_tab': results,
                'row_values': sorted(row_vals),
                'column_values': sorted(col_vals),
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value
