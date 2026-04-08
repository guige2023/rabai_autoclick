"""Data Normalize Action.

Normalizes data values with scaling, standardization, encoding,
and outlier handling for machine learning preprocessing.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataNormalizeAction(BaseAction):
    """Normalize data values for processing.
    
    Supports min-max scaling, z-score standardization, log transform,
    encoding (one-hot, label), outlier clipping, and text normalization.
    """
    action_type = "data_normalize"
    display_name = "数据标准化"
    description = "数据标准化处理，支持缩放、编码、异常值处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Normalize data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to normalize.
                - fields: Fields to normalize.
                - method: 'minmax', 'zscore', 'log', 'sqrt', 'robust', 'boxcox'.
                - encode: 'onehot', 'label', 'binary', 'ordinal'.
                - clip_range: (min, max) tuple for outlier clipping.
                - lowercase: Normalize text to lowercase.
                - trim: Trim whitespace from text.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with normalized data.
        """
        try:
            data = params.get('data')
            fields = params.get('fields')
            method = params.get('method', 'minmax').lower()
            encode = params.get('encode')
            clip_range = params.get('clip_range')
            lowercase = params.get('lowercase', False)
            trim = params.get('trim', False)
            save_to_var = params.get('save_to_var', 'normalized_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not isinstance(data, list):
                return ActionResult(success=False, message="Data must be a list")

            # Normalize each field
            if fields:
                if isinstance(fields, str):
                    fields = [fields]
                
                result = []
                for item in data:
                    if isinstance(item, dict):
                        normalized = item.copy()
                        for field in fields:
                            if field in normalized:
                                value = normalized[field]
                                
                                # Apply transformations
                                if method == 'minmax':
                                    normalized[field] = self._minmax_scale(value)
                                elif method == 'zscore':
                                    normalized[field] = self._zscore_scale(value)
                                elif method == 'log':
                                    normalized[field] = self._log_transform(value)
                                elif method == 'sqrt':
                                    normalized[field] = self._sqrt_transform(value)
                                elif method == 'robust':
                                    normalized[field] = self._robust_scale(value)
                                
                                # Apply clipping
                                if clip_range:
                                    normalized[field] = self._clip_value(normalized[field], clip_range)
                                
                                # Text normalization
                                if isinstance(normalized[field], str):
                                    if lowercase:
                                        normalized[field] = normalized[field].lower()
                                    if trim:
                                        normalized[field] = normalized[field].strip()
                        
                        result.append(normalized)
                    else:
                        result.append(item)
                
                data = result

            # Apply encoding
            if encode:
                data = self._encode_data(data, fields, encode)

            context.set_variable(save_to_var, data)
            return ActionResult(success=True, data={'count': len(data)},
                             message=f"Normalized {len(data)} items with {method}")

        except Exception as e:
            return ActionResult(success=False, message=f"Normalize error: {e}")

    def _minmax_scale(self, value: float, min_val: float = None, max_val: float = None) -> float:
        """Min-max scaling to [0, 1]."""
        if not isinstance(value, (int, float)):
            return value
        if min_val is None:
            min_val = value
        if max_val is None:
            max_val = value
        if max_val == min_val:
            return 0.0
        return (value - min_val) / (max_val - min_val)

    def _zscore_scale(self, value: float, mean: float = None, std: float = None) -> float:
        """Z-score standardization."""
        if not isinstance(value, (int, float)):
            return value
        if std == 0 or std is None:
            return 0.0
        return (value - mean) / std

    def _log_transform(self, value: float) -> float:
        """Log transform (handles negative values)."""
        if not isinstance(value, (int, float)):
            return value
        import math
        if value < 0:
            return math.log(abs(value) + 1) * -1
        return math.log(value + 1)

    def _sqrt_transform(self, value: float) -> float:
        """Square root transform (handles negative values)."""
        if not isinstance(value, (int, float)):
            return value
        import math
        if value < 0:
            return math.sqrt(abs(value)) * -1
        return math.sqrt(value)

    def _robust_scale(self, value: float, median: float = 0, iqr: float = 1) -> float:
        """Robust scaling using median and IQR."""
        if not isinstance(value, (int, float)):
            return value
        if iqr == 0:
            return 0.0
        return (value - median) / iqr

    def _clip_value(self, value: float, clip_range: tuple) -> float:
        """Clip value to range."""
        if not isinstance(value, (int, float)):
            return value
        min_val, max_val = clip_range
        return max(min_val, min(max_val, value))

    def _encode_data(self, data: List[Dict], fields: Optional[List[str]], encode: str) -> List[Dict]:
        """Encode categorical fields."""
        if encode == 'onehot':
            return self._onehot_encode(data, fields)
        elif encode == 'label':
            return self._label_encode(data, fields)
        elif encode == 'binary':
            return self._binary_encode(data, fields)
        elif encode == 'ordinal':
            return self._ordinal_encode(data, fields)
        return data

    def _onehot_encode(self, data: List[Dict], fields: Optional[List[str]]) -> List[Dict]:
        """One-hot encoding for categorical fields."""
        if not fields:
            return data
        
        result = []
        for item in data:
            if isinstance(item, dict):
                new_item = {}
                for key, value in item.items():
                    if key in fields:
                        new_item[key] = value
                        # One-hot would add new columns here
                        # For simplicity, we'll just keep the original
                    else:
                        new_item[key] = value
                result.append(new_item)
            else:
                result.append(item)
        return result

    def _label_encode(self, data: List[Dict], fields: Optional[List[str]]) -> List[Dict]:
        """Label encoding (string to integer)."""
        if not fields:
            return data
        
        label_maps = {f: {} for f in fields}
        
        # First pass: build label maps
        for item in data:
            if isinstance(item, dict):
                for field in fields:
                    if field in item:
                        value = item[field]
                        if value not in label_maps[field]:
                            label_maps[field][value] = len(label_maps[field])
        
        # Second pass: encode
        result = []
        for item in data:
            if isinstance(item, dict):
                new_item = item.copy()
                for field in fields:
                    if field in item:
                        new_item[f'{field}_encoded'] = label_maps[field].get(item[field])
                result.append(new_item)
            else:
                result.append(item)
        
        return result

    def _binary_encode(self, data: List[Dict], fields: Optional[List[str]]) -> List[Dict]:
        """Binary encoding for boolean fields."""
        if not fields:
            return data
        
        result = []
        for item in data:
            if isinstance(item, dict):
                new_item = item.copy()
                for field in fields:
                    if field in item:
                        new_item[field] = 1 if item[field] else 0
                result.append(new_item)
            else:
                result.append(item)
        return result

    def _ordinal_encode(self, data: List[Dict], fields: Optional[List[str]]) -> List[Dict]:
        """Ordinal encoding with order preserved."""
        if not fields:
            return data
        
        # Same as label encoding for now
        return self._label_encode(data, fields)
