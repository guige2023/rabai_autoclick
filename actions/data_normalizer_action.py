"""Data normalizer action module for RabAI AutoClick.

Provides data normalization with standardization, min-max scaling,
robust scaling, and encoding capabilities.
"""

import sys
import os
import math
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NormalizationType(Enum):
    """Normalization types."""
    STANDARD = "standard"
    MIN_MAX = "min_max"
    ROBUST = "robust"
    LOG = "log"
    POWER = "power"


from enum import Enum


@dataclass
class ScaleParams:
    """Scaling parameters."""
    mean: float
    std: float
    min: float
    max: float
    median: float
    q1: float
    q3: float


class DataNormalizerAction(BaseAction):
    """Data normalizer action for scaling and encoding data.
    
    Supports standard scaling, min-max scaling, robust scaling,
    log transformation, and various encoding methods.
    """
    action_type = "data_normalizer"
    display_name = "数据标准化"
    description = "数据归一化与标准化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: normalize|encode|fit_transform|inverse
                data: Data to normalize
                method: Normalization method
                feature_range: Tuple of (min, max) for min-max scaling.
        
        Returns:
            ActionResult with normalized data.
        """
        operation = params.get('operation', 'normalize')
        
        if operation == 'normalize':
            return self._normalize(params)
        elif operation == 'encode':
            return self._encode(params)
        elif operation == 'fit_transform':
            return self._fit_transform(params)
        elif operation == 'inverse':
            return self._inverse_transform(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _normalize(self, params: Dict[str, Any]) -> ActionResult:
        """Normalize data."""
        data = params.get('data', [])
        method = params.get('method', 'standard')
        feature_range = params.get('feature_range', (0, 1))
        params_data = params.get('params')
        
        if not data and not params_data:
            return ActionResult(success=False, message="No data provided")
        
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return self._normalize_dict_list(data, method, feature_range)
            elif isinstance(data[0], (int, float)):
                return self._normalize_list(data, method, feature_range, params_data)
        
        return ActionResult(success=False, message="Unsupported data format")
    
    def _normalize_list(
        self,
        data: List[Union[int, float]],
        method: str,
        feature_range: tuple,
        fit_params: Optional[Dict] = None
    ) -> ActionResult:
        """Normalize a list of numbers."""
        if not data:
            return ActionResult(success=True, message="Empty data", data={'data': []})
        
        if method == 'standard' or method == 'zscore':
            mean = sum(data) / len(data)
            variance = sum((x - mean) ** 2 for x in data) / len(data)
            std = math.sqrt(variance) if variance > 0 else 1
            
            if fit_params:
                mean = fit_params.get('mean', mean)
                std = fit_params.get('std', std)
            
            normalized = [(x - mean) / std for x in data]
            
            return ActionResult(
                success=True,
                message=f"Standardized {len(data)} values",
                data={
                    'data': normalized,
                    'params': {'mean': mean, 'std': std}
                }
            )
        
        elif method == 'minmax' or method == 'min_max':
            min_val = min(data)
            max_val = max(data)
            range_val = max_val - min_val if max_val != min_val else 1
            
            if fit_params:
                min_val = fit_params.get('min', min_val)
                max_val = fit_params.get('max', max_val)
                range_val = max_val - min_val if max_val != min_val else 1
            
            min_target, max_target = feature_range
            normalized = [min_target + (x - min_val) / range_val * (max_target - min_target) for x in data]
            
            return ActionResult(
                success=True,
                message=f"Min-max scaled {len(data)} values",
                data={
                    'data': normalized,
                    'params': {'min': min_val, 'max': max_val, 'range': feature_range}
                }
            )
        
        elif method == 'robust' or method == 'robust_scaler':
            sorted_data = sorted(data)
            n = len(sorted_data)
            median = sorted_data[n // 2]
            q1 = sorted_data[n // 4]
            q3 = sorted_data[3 * n // 4]
            iqr = q3 - q1 if q3 != q1 else 1
            
            if fit_params:
                median = fit_params.get('median', median)
                q1 = fit_params.get('q1', q1)
                q3 = fit_params.get('q3', q3)
                iqr = q3 - q1 if q3 != q1 else 1
            
            normalized = [(x - median) / iqr for x in data]
            
            return ActionResult(
                success=True,
                message=f"Robust scaled {len(data)} values",
                data={
                    'data': normalized,
                    'params': {'median': median, 'q1': q1, 'q3': q3, 'iqr': iqr}
                }
            )
        
        elif method == 'log' or method == 'log_transform':
            normalized = []
            for x in data:
                if x > 0:
                    normalized.append(math.log(x))
                elif x == 0:
                    normalized.append(0)
                else:
                    normalized.append(float('nan'))
            
            return ActionResult(
                success=True,
                message=f"Log transformed {len(data)} values",
                data={'data': normalized}
            )
        
        elif method == 'power' or method == 'power_transform':
            if fit_params:
                power = fit_params.get('power', 0.5)
            else:
                log_data = [math.log(x) if x > 0 else 0 for x in data]
                power = 0.5
            
            normalized = [math.copysign(abs(x) ** power, x) if x != 0 else 0 for x in data]
            
            return ActionResult(
                success=True,
                message=f"Power transformed {len(data)} values",
                data={'data': normalized, 'params': {'power': power}}
            )
        
        return ActionResult(success=False, message=f"Unknown method: {method}")
    
    def _normalize_dict_list(
        self,
        data: List[Dict],
        method: str,
        feature_range: tuple
    ) -> ActionResult:
        """Normalize a list of dictionaries."""
        if not data:
            return ActionResult(success=True, message="Empty data", data={'data': []})
        
        numeric_fields = []
        for key in data[0].keys():
            if all(isinstance(item.get(key), (int, float)) for item in data if item.get(key) is not None):
                numeric_fields.append(key)
        
        if not numeric_fields:
            return ActionResult(success=False, message="No numeric fields found")
        
        all_params = {}
        normalized = []
        
        for item in data:
            new_item = dict(item)
            for field in numeric_fields:
                values = [d.get(field) for d in data if d.get(field) is not None]
                if not values:
                    continue
                
                result = self._normalize_list(values, method, feature_range)
                if result.success:
                    param_key = f"{field}_params"
                    all_params[param_key] = result.data['params']
                    
                    idx = [i for i, d in enumerate(data) if d.get(field) is not None].index(list(data).index(item) if item in data else 0)
                    
                    if hasattr(item, '__getitem__') and hasattr(item, '__class__'):
                        try:
                            normalized_values = result.data['data']
                            indices = [i for i, d in enumerate(data) if d.get(field) is not None]
                            if idx < len(normalized_values):
                                new_item[field] = normalized_values[idx]
                        except (IndexError, KeyError):
                            pass
            
            normalized.append(new_item)
        
        return ActionResult(
            success=True,
            message=f"Normalized {len(data)} items across {len(numeric_fields)} fields",
            data={
                'data': normalized,
                'params': all_params,
                'normalized_fields': numeric_fields
            }
        )
    
    def _encode(self, params: Dict[str, Any]) -> ActionResult:
        """Encode categorical data."""
        data = params.get('data', [])
        method = params.get('method', 'onehot')
        field = params.get('field')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if method == 'onehot' or method == 'one_hot':
            return self._one_hot_encode(data, field)
        elif method == 'label' or method == 'label_encode':
            return self._label_encode(data, field)
        elif method == 'ordinal':
            return self._ordinal_encode(data, field, params.get('mapping', {}))
        else:
            return ActionResult(success=False, message=f"Unknown encoding method: {method}")
    
    def _one_hot_encode(self, data: List[Any], field: Optional[str]) -> ActionResult:
        """One-hot encode."""
        items = data if field is None else [d.get(field) if isinstance(d, dict) else d for d in data]
        unique_values = sorted(set(items))
        
        encoding_map = {v: i for i, v in enumerate(unique_values)}
        
        one_hot = []
        for item in items:
            encoding = [0] * len(unique_values)
            if item in encoding_map:
                encoding[encoding_map[item]] = 1
            one_hot.append(encoding)
        
        return ActionResult(
            success=True,
            message=f"One-hot encoded {len(unique_values)} categories",
            data={
                'encoded': one_hot,
                'categories': unique_values,
                'mapping': encoding_map
            }
        )
    
    def _label_encode(self, data: List[Any], field: Optional[str]) -> ActionResult:
        """Label encode."""
        items = data if field is None else [d.get(field) if isinstance(d, dict) else d for d in data]
        unique_values = sorted(set(items))
        
        encoding_map = {v: i for i, v in enumerate(unique_values)}
        encoded = [encoding_map.get(item, -1) for item in items]
        
        return ActionResult(
            success=True,
            message=f"Label encoded {len(unique_values)} categories",
            data={
                'encoded': encoded,
                'categories': unique_values,
                'mapping': encoding_map
            }
        )
    
    def _ordinal_encode(
        self,
        data: List[Any],
        field: Optional[str],
        mapping: Dict[Any, int]
    ) -> ActionResult:
        """Ordinal encode."""
        items = data if field is None else [d.get(field) if isinstance(d, dict) else d for d in data]
        
        if not mapping:
            unique_values = sorted(set(items))
            mapping = {v: i for i, v in enumerate(unique_values)}
        
        encoded = [mapping.get(item, -1) for item in items]
        
        return ActionResult(
            success=True,
            message=f"Ordinal encoded {len(mapping)} categories",
            data={
                'encoded': encoded,
                'mapping': mapping
            }
        )
    
    def _fit_transform(self, params: Dict[str, Any]) -> ActionResult:
        """Fit and transform data (learns params and applies)."""
        return self._normalize(params)
    
    def _inverse_transform(self, params: Dict[str, Any]) -> ActionResult:
        """Inverse transform normalized data."""
        data = params.get('data', [])
        method = params.get('method', 'standard')
        scale_params = params.get('params', {})
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if method == 'standard' or method == 'zscore':
            mean = scale_params.get('mean', 0)
            std = scale_params.get('std', 1)
            original = [x * std + mean for x in data]
        elif method == 'minmax' or method == 'min_max':
            min_val = scale_params.get('min', 0)
            max_val = scale_params.get('max', 1)
            feature_range = scale_params.get('range', (0, 1))
            min_target, max_target = feature_range
            original = [min_val + (x - min_target) / (max_target - min_target) * (max_val - min_val) for x in data]
        elif method == 'robust':
            median = scale_params.get('median', 0)
            q1 = scale_params.get('q1', 0)
            q3 = scale_params.get('q3', 1)
            iqr = q3 - q1 if q3 != q1 else 1
            original = [x * iqr + median for x in data]
        else:
            return ActionResult(success=False, message=f"Cannot inverse transform for method: {method}")
        
        return ActionResult(
            success=True,
            message=f"Inverse transformed {len(data)} values",
            data={'original': original}
        )
