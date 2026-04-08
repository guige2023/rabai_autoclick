"""Data Normalize Action Module.

Provides data normalization and standardization utilities including
min-max scaling, z-score normalization, and encoding transformations.
"""

import sys
import os
import math
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataNormalizerAction(BaseAction):
    """Normalize and standardize data values.
    
    Supports min-max scaling, z-score, log, and power transformations.
    """
    action_type = "data_normalize"
    display_name = "数据标准化"
    description = "标准化和归一化数据值，支持多种变换方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Normalize data values.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list of numbers or dicts).
                - field: Field to normalize if data is list of dicts.
                - method: Normalization method.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with normalized data or error.
        """
        data = params.get('data', [])
        field = params.get('field', None)
        method = params.get('method', 'minmax')
        output_var = params.get('output_var', 'normalized')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Extract values
            if field and isinstance(data[0], dict) if data else False:
                values = [item.get(field, 0) for item in data]
            else:
                values = data

            # Apply normalization
            if method == 'minmax':
                normalized = self._minmax_scale(values)
            elif method == 'zscore':
                normalized = self._zscore_normalize(values)
            elif method == 'log':
                normalized = self._log_transform(values)
            elif method == 'power':
                normalized = self._power_transform(values, params.get('power', 0.5))
            elif method == 'robust':
                normalized = self._robust_scale(values)
            else:
                normalized = values

            # Store result
            if field and isinstance(data[0], dict):
                result = []
                for i, item in enumerate(data):
                    new_item = dict(item)
                    new_item[field] = normalized[i]
                    result.append(new_item)
            else:
                result = normalized

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'normalized': result, 'method': method},
                message=f"Normalized {len(result)} values using {method}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Normalization failed: {str(e)}"
            )

    def _minmax_scale(self, values: List[float]) -> List[float]:
        """Min-max scaling to [0, 1] range."""
        min_val = min(values)
        max_val = max(values)
        if max_val == min_val:
            return [0.5] * len(values)
        return [(v - min_val) / (max_val - min_val) for v in values]

    def _zscore_normalize(self, values: List[float]) -> List[float]:
        """Z-score normalization (standard score)."""
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1
        return [(v - mean) / std for v in values]

    def _log_transform(self, values: List[float]) -> List[float]:
        """Log transformation."""
        return [math.log(v + 1e-10) for v in values]

    def _power_transform(self, values: List[float], power: float) -> List[float]:
        """Power transformation."""
        return [math.copysign(abs(v) ** power, v) for v in values]

    def _robust_scale(self, values: List[float]) -> List[float]:
        """Robust scaling using median and IQR."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        median = sorted_vals[n // 2]
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1
        if iqr == 0:
            return [0.0] * len(values)
        return [(v - median) / iqr for v in values]


class EncodingTransformAction(BaseAction):
    """Transform data encodings (label, one-hot, ordinal).
    
    Supports encoding categorical data for ML pipelines.
    """
    action_type = "encoding_transform"
    display_name = "编码转换"
    description = "转换数据编码，支持标签、One-Hot和序号编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform data encodings.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - field: Field to encode if dicts.
                - encoding_type: 'label', 'onehot', 'ordinal', 'binary'.
                - categories: Predefined category list.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with encoded data or error.
        """
        data = params.get('data', [])
        field = params.get('field', None)
        encoding_type = params.get('encoding_type', 'label')
        categories = params.get('categories', None)
        output_var = params.get('output_var', 'encoded')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Get categories
            if not categories:
                if field:
                    categories = list(set(item.get(field) for item in data if isinstance(item, dict)))
                else:
                    categories = list(set(data))

            # Create encoder mapping
            if encoding_type == 'label':
                result = self._label_encode(data, field, categories)
            elif encoding_type == 'onehot':
                result = self._onehot_encode(data, field, categories)
            elif encoding_type == 'ordinal':
                result = self._ordinal_encode(data, field, categories)
            elif encoding_type == 'binary':
                result = self._binary_encode(data, field, categories)
            else:
                result = data

            context.variables[output_var] = result
            context.variables[f'{output_var}_categories'] = categories

            return ActionResult(
                success=True,
                data={'encoded': result, 'categories': categories},
                message=f"Encoded {len(data)} values using {encoding_type}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Encoding transform failed: {str(e)}"
            )

    def _label_encode(
        self, data: List, field: Optional[str], categories: List
    ) -> List:
        """Label encoding (categorical to integer)."""
        cat_to_idx = {cat: i for i, cat in enumerate(categories)}
        result = []
        for item in data:
            if isinstance(item, dict) and field:
                new_item = dict(item)
                new_item[f'{field}_encoded'] = cat_to_idx.get(item.get(field), -1)
                result.append(new_item)
            else:
                result.append(cat_to_idx.get(item, -1))
        return result

    def _onehot_encode(
        self, data: List, field: Optional[str], categories: List
    ) -> List[Dict]:
        """One-hot encoding."""
        result = []
        for item in data:
            value = item.get(field) if isinstance(item, dict) and field else item
            encoding = {f'onehot_{cat}': 1 if value == cat else 0 for cat in categories}
            if isinstance(item, dict):
                new_item = dict(item)
                new_item.update(encoding)
                result.append(new_item)
            else:
                result.append(encoding)
        return result

    def _ordinal_encode(
        self, data: List, field: Optional[str], categories: List
    ) -> List:
        """Ordinal encoding preserving order."""
        return self._label_encode(data, field, categories)

    def _binary_encode(
        self, data: List, field: Optional[str], categories: List
    ) -> List[Dict]:
        """Binary encoding of categories."""
        result = []
        n_bits = math.ceil(math.log2(len(categories))) if len(categories) > 1 else 1
        cat_to_idx = {cat: i for i, cat in enumerate(categories)}

        for item in data:
            value = item.get(field) if isinstance(item, dict) and field else item
            idx = cat_to_idx.get(value, 0)
            binary = format(idx, f'0{n_bits}b')
            encoding = {f'bit_{i}': int(b) for i, b in enumerate(binary)}

            if isinstance(item, dict):
                new_item = dict(item)
                new_item.update(encoding)
                result.append(new_item)
            else:
                result.append(encoding)

        return result


class DataImputerAction(BaseAction):
    """Handle missing values in datasets.
    
    Supports mean, median, mode, forward/backward fill, and constant imputation.
    """
    action_type = "data_imputer"
    display_name = "数据填充"
    description = "处理数据集中的缺失值，支持多种填充策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Impute missing values.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - fields: List of fields to impute.
                - strategy: 'mean', 'median', 'mode', 'ffill', 'bfill', 'constant'.
                - constant_value: Value for constant strategy.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with imputed data or error.
        """
        data = params.get('data', [])
        fields = params.get('fields', [])
        strategy = params.get('strategy', 'mean')
        constant_value = params.get('constant_value', 0)
        output_var = params.get('output_var', 'imputed')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            result = [dict(item) if isinstance(item, dict) else item for item in data]

            for field in fields:
                values = [item.get(field) for item in result if isinstance(item, dict)]
                non_null = [v for v in values if v is not None]

                if not non_null:
                    continue

                if strategy == 'mean':
                    fill_value = sum(non_null) / len(non_null)
                elif strategy == 'median':
                    sorted_vals = sorted(non_null)
                    n = len(sorted_vals)
                    fill_value = sorted_vals[n // 2]
                elif strategy == 'mode':
                    counter = Counter(non_null)
                    fill_value = counter.most_common(1)[0][0]
                elif strategy == 'constant':
                    fill_value = constant_value
                elif strategy == 'ffill':
                    fill_value = None
                    for item in result:
                        if isinstance(item, dict):
                            if item.get(field) is not None:
                                fill_value = item.get(field)
                            elif fill_value is not None:
                                item[field] = fill_value
                    continue
                elif strategy == 'bfill':
                    fill_value = None
                    for item in reversed(result):
                        if isinstance(item, dict):
                            if item.get(field) is not None:
                                fill_value = item.get(field)
                            elif fill_value is not None:
                                item[field] = fill_value
                    continue
                else:
                    fill_value = constant_value

                if strategy not in ('ffill', 'bfill'):
                    for item in result:
                        if isinstance(item, dict) and item.get(field) is None:
                            item[field] = fill_value

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'imputed': result, 'strategy': strategy},
                message=f"Imputed {len(fields)} fields using {strategy}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Imputation failed: {str(e)}"
            )


class OutlierHandlerAction(BaseAction):
    """Detect and handle outliers in data.
    
    Supports Z-score, IQR, and percentile-based outlier detection.
    """
    action_type = "outlier_handler"
    display_name = "异常值处理"
    description = "检测和处理数据中的异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Handle outliers in data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - field: Field to check for outliers.
                - method: 'zscore', 'iqr', 'percentile'.
                - threshold: Detection threshold.
                - action: 'remove', 'cap', 'mark'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with outlier handling result or error.
        """
        data = params.get('data', [])
        field = params.get('field', None)
        method = params.get('method', 'iqr')
        threshold = params.get('threshold', 3)
        action = params.get('action', 'mark')
        output_var = params.get('output_var', 'result')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            result = [dict(item) if isinstance(item, dict) else item for item in data]
            outlier_indices = []

            if field:
                values = [(i, item.get(field)) for i, item in enumerate(result) 
                         if isinstance(item, dict) and item.get(field) is not None]
                
                if not values:
                    return ActionResult(success=False, message=f"No values found for field: {field}")

                _, val_list = zip(*values)
                
                if method == 'zscore':
                    outlier_indices = self._zscore_outliers(val_list, threshold, [i for i, _ in values])
                elif method == 'iqr':
                    outlier_indices = self._iqr_outliers(val_list, threshold, [i for i, _ in values])
                elif method == 'percentile':
                    outlier_indices = self._percentile_outliers(val_list, threshold, [i for i, _ in values])

            if action == 'remove':
                result = [item for i, item in enumerate(result) if i not in outlier_indices]
            elif action == 'cap':
                bounds = self._get_bounds(result, field, method, threshold)
                for i, item in enumerate(result):
                    if isinstance(item, dict) and field:
                        val = item.get(field)
                        if val is not None:
                            if val < bounds['lower']:
                                item[field] = bounds['lower']
                            elif val > bounds['upper']:
                                item[field] = bounds['upper']
            elif action == 'mark':
                for i, item in enumerate(result):
                    if isinstance(item, dict):
                        item['_is_outlier'] = i in outlier_indices

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'result': result, 'outliers_removed': len(outlier_indices) if action == 'remove' else 0, 'outliers_found': len(outlier_indices)},
                message=f"Outlier handling completed: {len(outlier_indices)} outliers {action}ed"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Outlier handling failed: {str(e)}"
            )

    def _zscore_outliers(self, values: List[float], threshold: float, indices: List[int]) -> List[int]:
        """Detect outliers using Z-score."""
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
        if std == 0:
            return []
        return [indices[i] for i, v in enumerate(values) if abs((v - mean) / std) > threshold]

    def _iqr_outliers(self, values: List[float], multiplier: float, indices: List[int]) -> List[int]:
        """Detect outliers using IQR method."""
        sorted_vals = sorted(zip(indices, values), key=lambda x: x[1])
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4][1]
        q3 = sorted_vals[3 * n // 4][1]
        iqr = q3 - q1
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr
        return [idx for idx, val in zip(indices, values) if val < lower or val > upper]

    def _percentile_outliers(self, values: List[float], threshold: float, indices: List[int]) -> List[int]:
        """Detect outliers using percentile method."""
        sorted_vals = sorted(values)
        lower = sorted_vals[int(len(sorted_vals) * threshold / 100)]
        upper = sorted_vals[int(len(sorted_vals) * (100 - threshold) / 100)]
        return [indices[i] for i, v in enumerate(values) if v < lower or v > upper]

    def _get_bounds(self, data: List, field: str, method: str, threshold: float) -> Dict:
        """Get lower and upper bounds for capping."""
        values = [item.get(field) for item in data if isinstance(item, dict) and item.get(field) is not None]
        if not values:
            return {'lower': 0, 'upper': 0}

        if method == 'zscore':
            mean = sum(values) / len(values)
            std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
            return {'lower': mean - threshold * std, 'upper': mean + threshold * std}
        else:
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            q1 = sorted_vals[n // 4]
            q3 = sorted_vals[3 * n // 4]
            iqr = q3 - q1
            return {'lower': q1 - threshold * iqr, 'upper': q3 + threshold * iqr}
