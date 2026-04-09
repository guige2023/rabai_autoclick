"""Data normalization action module for RabAI AutoClick.

Provides data normalization operations:
- MinMaxNormalizerAction: Min-max normalization
- ZScoreNormalizerAction: Z-score normalization
- DecimalScalerAction: Decimal scaling normalization
- RobustScalerAction: Robust scaling normalization
- UnitVectorNormalizerAction: Unit vector normalization
"""

from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MinMaxNormalizerAction(BaseAction):
    """Min-max normalization."""
    action_type = "minmax_normalizer"
    display_name = "最小最大归一化"
    description = "将数据归一化到指定范围"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            feature_range = params.get("feature_range", (0, 1))
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            min_val, max_val = feature_range
            result = self._normalize(data, fields, min_val, max_val)
            
            return ActionResult(
                success=True,
                message="Min-max normalization complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "feature_range": feature_range,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str], 
                   min_val: float, max_val: float) -> List[Dict]:
        result = []
        
        for field in fields:
            values = [item.get(field, 0) for item in data if isinstance(item, dict)]
            
            if not values:
                continue
            
            data_min = min(values)
            data_max = max(values)
            data_range = data_max - data_min
            
            if data_range == 0:
                continue
            
            scale = (max_val - min_val) / data_range
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = ((item[field] - data_min) * scale) + min_val
        
        return data


class ZScoreNormalizerAction(BaseAction):
    """Z-score normalization."""
    action_type = "zscore_normalizer"
    display_name = "Z分数归一化"
    description = "使用Z分数进行数据归一化"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result = self._normalize(data, fields)
            
            return ActionResult(
                success=True,
                message="Z-score normalization complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str]) -> List[Dict]:
        for field in fields:
            values = [item.get(field, 0) for item in data if isinstance(item, dict)]
            
            if not values:
                continue
            
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5
            
            if std == 0:
                continue
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = (item[field] - mean) / std
        
        return data


class DecimalScalerAction(BaseAction):
    """Decimal scaling normalization."""
    action_type = "decimal_scaler"
    display_name = "小数点缩放归一化"
    description = "使用小数点缩放进行归一化"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result, scaling_factors = self._normalize(data, fields)
            
            return ActionResult(
                success=True,
                message="Decimal scaling normalization complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "scaling_factors": scaling_factors,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str]) -> Tuple[List[Dict], Dict]:
        scaling_factors = {}
        
        for field in fields:
            values = [item.get(field, 0) for item in data if isinstance(item, dict)]
            
            if not values:
                continue
            
            max_abs = max(abs(v) for v in values)
            
            if max_abs == 0:
                continue
            
            j = 1
            while max_abs >= 1:
                max_abs /= 10
                j *= 10
            while max_abs < 0.1:
                max_abs *= 10
                j /= 10
            
            scaling_factors[field] = j
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = item[field] * j
        
        return data, scaling_factors


class RobustScalerAction(BaseAction):
    """Robust scaling normalization."""
    action_type = "robust_scaler"
    display_name = "鲁棒缩放归一化"
    description = "使用中位数和四分位数进行鲁棒缩放"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result = self._normalize(data, fields)
            
            return ActionResult(
                success=True,
                message="Robust scaling normalization complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str]) -> List[Dict]:
        for field in fields:
            values = sorted([item.get(field, 0) for item in data if isinstance(item, dict)])
            
            if not values:
                continue
            
            median = values[len(values) // 2]
            
            q1_idx = len(values) // 4
            q3_idx = 3 * len(values) // 4
            iqr = values[q3_idx] - values[q1_idx]
            
            if iqr == 0:
                continue
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = (item[field] - median) / iqr
        
        return data


class UnitVectorNormalizerAction(BaseAction):
    """Unit vector normalization."""
    action_type = "unit_vector_normalizer"
    display_name = "单位向量归一化"
    description = "将数据归一化为单位向量"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            norm_type = params.get("norm_type", "l2")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result = self._normalize(data, fields, norm_type)
            
            return ActionResult(
                success=True,
                message=f"Unit vector normalization (L{norm_type}) complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "norm_type": norm_type,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str], norm_type: str) -> List[Dict]:
        if norm_type == "l2":
            for item in data:
                if isinstance(item, dict):
                    values = [item.get(f, 0) for f in fields]
                    magnitude = sum(v ** 2 for v in values) ** 0.5
                    
                    if magnitude > 0:
                        for f in fields:
                            item[f] = item.get(f, 0) / magnitude
        elif norm_type == "l1":
            for item in data:
                if isinstance(item, dict):
                    values = [abs(item.get(f, 0)) for f in fields]
                    norm = sum(values)
                    
                    if norm > 0:
                        for f in fields:
                            item[f] = item.get(f, 0) / norm
        elif norm_type == "max":
            for item in data:
                if isinstance(item, dict):
                    values = [abs(item.get(f, 0)) for f in fields]
                    max_val = max(values) if values else 1
                    
                    if max_val > 0:
                        for f in fields:
                            item[f] = item.get(f, 0) / max_val
        
        return data


class LogNormalizerAction(BaseAction):
    """Log transformation normalization."""
    action_type = "log_normalizer"
    display_name = "对数归一化"
    description = "使用对数变换进行归一化"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            log_base = params.get("log_base", "natural")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result = self._normalize(data, fields, log_base)
            
            return ActionResult(
                success=True,
                message="Log normalization complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "log_base": log_base,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str], log_base: str) -> List[Dict]:
        import math
        
        for field in fields:
            for item in data:
                if isinstance(item, dict) and field in item:
                    value = item[field]
                    
                    if isinstance(value, (int, float)) and value > 0:
                        if log_base == "natural":
                            item[field] = math.log(value)
                        elif log_base == "10":
                            item[field] = math.log10(value)
                        elif log_base == "2":
                            item[field] = math.log2(value)
        
        return data


class PowerNormalizerAction(BaseAction):
    """Power transformation normalization."""
    action_type = "power_normalizer"
    display_name = "幂次归一化"
    description = "使用幂次变换进行归一化"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            power = params.get("power", 0.5)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            result = self._normalize(data, fields, power)
            
            return ActionResult(
                success=True,
                message=f"Power normalization (power={power}) complete",
                data={
                    "original_count": len(data),
                    "fields_normalized": fields,
                    "power": power,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _normalize(self, data: List[Dict], fields: List[str], power: float) -> List[Dict]:
        for field in fields:
            for item in data:
                if isinstance(item, dict) and field in item:
                    value = item[field]
                    
                    if isinstance(value, (int, float)):
                        if power == 0:
                            item[field] = 0 if value == 0 else 1
                        else:
                            item[field] = value ** power
        
        return data
