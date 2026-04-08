"""
Data Normalizer Action Module.

Normalizes and standardizes data including min-max scaling,
z-score normalization, log transforms, and categorical encoding.

Author: RabAi Team
"""

from __future__ import annotations

import math
import re
import sys
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NormalizationMethod(Enum):
    """Data normalization methods."""
    MIN_MAX = "min_max"
    ZSCORE = "zscore"
    LOG = "log"
    LOG10 = "log10"
    SQRT = "sqrt"
    ROBUST = "robust"
    DECIMAL = "decimal"
    MAX = "max"
    L1 = "l1"
    L2 = "l2"
    BOX_COX = "box_cox"


class EncodingMethod(Enum):
    """Categorical encoding methods."""
    ONE_HOT = "one_hot"
    LABEL = "label"
    ORDINAL = "ordinal"
    TARGET = "target"
    FREQUENCY = "frequency"


@dataclass
class NormalizerStats:
    """Statistics for normalization."""
    mean: Optional[float] = None
    std: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    median: Optional[float] = None
    q1: Optional[float] = None
    q3: Optional[float] = None


class DataNormalizerAction(BaseAction):
    """Data normalizer action.
    
    Normalizes and standardizes data using various methods
    with support for batch processing and inverse transforms.
    """
    action_type = "data_normalizer"
    display_name = "数据标准化"
    description = "数据归一化标准化"
    
    def __init__(self):
        super().__init__()
        self._column_stats: Dict[str, NormalizerStats] = {}
        self._label_mappings: Dict[str, Dict[str, int]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Normalize data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: Data to normalize
                - operation: normalize/denormalize/encode/decode
                - method: Normalization method
                - columns: Column names to normalize
                - feature_range: Tuple of (min, max) for min-max
                - encoding: Categorical encoding method
                - stats: Pre-computed statistics
                
        Returns:
            ActionResult with normalized data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "normalize")
        data = params.get("data", [])
        method_str = params.get("method", "min_max")
        columns = params.get("columns", [])
        feature_range = params.get("feature_range", (0, 1))
        
        try:
            method = NormalizationMethod(method_str)
        except ValueError:
            method = NormalizationMethod.MIN_MAX
        
        try:
            if operation == "normalize":
                result = self._normalize(data, method, columns, feature_range, start_time)
            elif operation == "denormalize":
                result = self._denormalize(data, method, columns, feature_range, start_time)
            elif operation == "encode":
                result = self._encode_categorical(data, columns, params, start_time)
            elif operation == "decode":
                result = self._decode_categorical(data, columns, start_time)
            elif operation == "stats":
                result = self._compute_stats(data, columns, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Normalization failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _normalize(
        self, data: List, method: NormalizationMethod,
        columns: List[str], feature_range: Tuple[float, float], start_time: float
    ) -> ActionResult:
        """Normalize data."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to normalize",
                data={"data": [], "stats": {}},
                duration=time.time() - start_time
            )
        
        if not isinstance(data[0], dict):
            return self._normalize_list(data, method, feature_range, start_time)
        
        if not columns:
            columns = list(data[0].keys())
        
        normalized = []
        stats = {}
        
        for col in columns:
            col_values = [float(r.get(col, 0)) for r in data if isinstance(r, dict) and col in r]
            if not col_values:
                continue
            
            col_stats = self._compute_column_stats(col_values)
            stats[col] = {
                "mean": col_stats.mean,
                "std": col_stats.std,
                "min": col_stats.min,
                "max": col_stats.max,
                "median": col_stats.median
            }
            
            self._column_stats[col] = col_stats
        
        for record in data:
            if not isinstance(record, dict):
                continue
            
            new_record = dict(record)
            for col in columns:
                if col not in record:
                    continue
                
                col_stats = self._column_stats.get(col)
                if col_stats is None:
                    continue
                
                value = float(record[col])
                
                if method == NormalizationMethod.MIN_MAX:
                    new_val = self._min_max_scale(value, col_stats, feature_range)
                elif method == NormalizationMethod.ZSCORE:
                    new_val = self._zscore(value, col_stats)
                elif method == NormalizationMethod.LOG:
                    new_val = math.log(value + 1e-10) if value > 0 else 0
                elif method == NormalizationMethod.LOG10:
                    new_val = math.log10(value + 1e-10) if value > 0 else 0
                elif method == NormalizationMethod.SQRT:
                    new_val = math.sqrt(value) if value >= 0 else 0
                elif method == NormalizationMethod.ROBUST:
                    new_val = self._robust_scale(value, col_stats, feature_range)
                elif method == NormalizationMethod.MAX:
                    new_val = value / (col_stats.max or 1)
                elif method == NormalizationMethod.L1:
                    total = sum(abs(float(r.get(col, 0)) or 0) for r in data if isinstance(r, dict))
                    new_val = abs(value) / (total or 1)
                elif method == NormalizationMethod.L2:
                    total = math.sqrt(sum((float(r.get(col, 0) or 0)) ** 2 for r in data if isinstance(r, dict)))
                    new_val = value / (total or 1)
                else:
                    new_val = self._min_max_scale(value, col_stats, feature_range)
                
                new_record[col] = new_val
            
            normalized.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Normalized {len(normalized)} records using {method.value}",
            data={
                "data": normalized,
                "stats": stats,
                "method": method.value
            },
            duration=time.time() - start_time
        )
    
    def _normalize_list(self, data: List, method: NormalizationMethod, feature_range: Tuple[float, float], start_time: float) -> ActionResult:
        """Normalize a plain list."""
        values = [float(v) for v in data]
        stats = self._compute_column_stats(values)
        
        normalized = []
        for v in values:
            if method == NormalizationMethod.MIN_MAX:
                normalized.append(self._min_max_scale(v, stats, feature_range))
            elif method == NormalizationMethod.ZSCORE:
                normalized.append(self._zscore(v, stats))
            elif method == NormalizationMethod.LOG:
                normalized.append(math.log(v + 1e-10) if v > 0 else 0)
            elif method == NormalizationMethod.SQRT:
                normalized.append(math.sqrt(v) if v >= 0 else 0)
            else:
                normalized.append(self._min_max_scale(v, stats, feature_range))
        
        return ActionResult(
            success=True,
            message=f"Normalized {len(normalized)} values",
            data={
                "data": normalized,
                "stats": {"mean": stats.mean, "std": stats.std, "min": stats.min, "max": stats.max},
                "method": method.value
            },
            duration=time.time() - start_time
        )
    
    def _min_max_scale(self, value: float, stats: NormalizerStats, feature_range: Tuple[float, float]) -> float:
        """Apply min-max scaling."""
        min_val, max_val = stats.min or 0, stats.max or 1
        out_min, out_max = feature_range
        
        if max_val == min_val:
            return (out_min + out_max) / 2
        
        return out_min + (value - min_val) / (max_val - min_val) * (out_max - out_min)
    
    def _zscore(self, value: float, stats: NormalizerStats) -> float:
        """Apply z-score normalization."""
        if stats.std and stats.std > 0 and stats.mean is not None:
            return (value - stats.mean) / stats.std
        return 0.0
    
    def _robust_scale(self, value: float, stats: NormalizerStats, feature_range: Tuple[float, float]) -> float:
        """Apply robust scaling using IQR."""
        q1 = stats.q1 or stats.min or 0
        q3 = stats.q3 or stats.max or 1
        iqr = q3 - q1
        
        if iqr == 0:
            return feature_range[0]
        
        out_min, out_max = feature_range
        scaled = (value - q1) / iqr
        
        return out_min + scaled * (out_max - out_min)
    
    def _compute_column_stats(self, values: List[float]) -> NormalizerStats:
        """Compute statistics for a column."""
        n = len(values)
        if n == 0:
            return NormalizerStats()
        
        sorted_values = sorted(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = math.sqrt(variance) if variance > 0 else 0
        
        return NormalizerStats(
            mean=mean,
            std=std,
            min=sorted_values[0],
            max=sorted_values[-1],
            median=sorted_values[n // 2],
            q1=sorted_values[n // 4],
            q3=sorted_values[3 * n // 4]
        )
    
    def _denormalize(
        self, data: List, method: NormalizationMethod,
        columns: List[str], feature_range: Tuple[float, float], start_time: float
    ) -> ActionResult:
        """Denormalize previously normalized data."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to denormalize",
                data={"data": []},
                duration=time.time() - start_time
            )
        
        denormalized = []
        
        for record in data:
            if not isinstance(record, dict):
                continue
            
            new_record = dict(record)
            for col in columns:
                col_stats = self._column_stats.get(col)
                if col_stats is None:
                    continue
                
                value = float(record.get(col, 0))
                in_min, in_max = feature_range
                out_min, out_max = col_stats.min or 0, col_stats.max or 1
                
                if method == NormalizationMethod.MIN_MAX:
                    normalized = (value - in_min) / (in_max - in_min) if in_max != in_min else 0
                    denorm = out_min + normalized * (out_max - out_min)
                    new_record[col] = denorm
                elif method == NormalizationMethod.ZSCORE:
                    if col_stats.std and col_stats.std > 0 and col_stats.mean is not None:
                        new_record[col] = value * col_stats.std + col_stats.mean
                else:
                    new_record[col] = value
            
            denormalized.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Denormalized {len(denormalized)} records",
            data={"data": denormalized, "method": method.value},
            duration=time.time() - start_time
        )
    
    def _encode_categorical(
        self, data: List[Dict], columns: List[str], params: Dict, start_time: float
    ) -> ActionResult:
        """Encode categorical variables."""
        encoding_str = params.get("encoding", "label")
        ordinal_map = params.get("ordinal_map", {})
        
        try:
            encoding = EncodingMethod(encoding_str)
        except ValueError:
            encoding = EncodingMethod.LABEL
        
        if not columns:
            columns = [k for k, v in data[0].items() if isinstance(v, (str, bool))] if data else []
        
        encoded = []
        mappings = {}
        
        for record in data:
            new_record = dict(record)
            
            for col in columns:
                value = record.get(col)
                if value is None:
                    continue
                
                if encoding == EncodingMethod.LABEL:
                    if col not in self._label_mappings:
                        self._label_mappings[col] = {}
                    
                    mapping = self._label_mappings[col]
                    
                    if value not in mapping:
                        mapping[value] = len(mapping)
                    
                    new_record[f"{col}_encoded"] = mapping[value]
                    mappings[col] = mapping
                
                elif encoding == EncodingMethod.ONE_HOT:
                    one_hot = {}
                    unique_vals = set(r.get(col) for r in data if isinstance(r, dict))
                    for val in unique_vals:
                        one_hot[f"{col}_{val}"] = 1 if record.get(col) == val else 0
                    new_record.update(one_hot)
                
                elif encoding == EncodingMethod.ORDINAL:
                    ordinal = ordinal_map.get(col, {})
                    new_record[f"{col}_ordinal"] = ordinal.get(value, 0)
                
                elif encoding == EncodingMethod.FREQUENCY:
                    freq = sum(1 for r in data if isinstance(r, dict) and r.get(col) == value)
                    new_record[f"{col}_freq"] = freq
                
                elif encoding == EncodingMethod.TARGET:
                    pass
            
            encoded.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Encoded {len(encoded)} records",
            data={
                "data": encoded,
                "mappings": mappings,
                "encoding": encoding.value
            },
            duration=time.time() - start_time
        )
    
    def _decode_categorical(self, data: List[Dict], columns: List[str], start_time: float) -> ActionResult:
        """Decode previously encoded categorical variables."""
        decoded = []
        
        for record in data:
            new_record = dict(record)
            
            for col in columns:
                mapping = self._label_mappings.get(col, {})
                if not mapping:
                    continue
                
                reverse_mapping = {v: k for k, v in mapping.items()}
                
                if f"{col}_encoded" in record:
                    encoded_val = int(record[f"{col}_encoded"])
                    if encoded_val in reverse_mapping:
                        new_record[col] = reverse_mapping[encoded_val]
                    del new_record[f"{col}_encoded"]
            
            decoded.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Decoded {len(decoded)} records",
            data={"data": decoded},
            duration=time.time() - start_time
        )
    
    def _compute_stats(self, data: List[Dict], columns: List[str], start_time: float) -> ActionResult:
        """Compute statistics for data columns."""
        if not columns:
            columns = list(data[0].keys()) if data else []
        
        all_stats = {}
        
        for col in columns:
            col_values = [float(r.get(col, 0)) for r in data if isinstance(r, dict) and col in r and r.get(col) is not None]
            if col_values:
                stats = self._compute_column_stats(col_values)
                all_stats[col] = {
                    "mean": stats.mean,
                    "std": stats.std,
                    "min": stats.min,
                    "max": stats.max,
                    "median": stats.median,
                    "q1": stats.q1,
                    "q3": stats.q3,
                    "count": len(col_values)
                }
        
        return ActionResult(
            success=True,
            message=f"Computed stats for {len(all_stats)} columns",
            data={"stats": all_stats},
            duration=time.time() - start_time
        )
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate normalizer parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
