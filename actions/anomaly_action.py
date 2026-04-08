"""Anomaly detection action module for RabAI AutoClick.

Provides statistical anomaly detection: z-score, IQR,
isolation forest, and threshold-based detection.
"""

import sys
import os
import math
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ZScoreAnomalyAction(BaseAction):
    """Detect anomalies using z-score method.
    
    Flag values that deviate more than n standard
    deviations from the mean.
    """
    action_type = "zscore_anomaly"
    display_name = "Z-Score异常检测"
    description = "基于Z-Score方法检测数据异常"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Detect anomalies using z-score.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of numbers
                - field: str (field name in dict records)
                - threshold: float (z-score threshold, default 3.0)
                - save_to_var: str
        
        Returns:
            ActionResult with anomaly detection result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        threshold = params.get('threshold', 3.0)
        save_to_var = params.get('save_to_var', 'anomaly_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Extract values
        values = []
        records = []
        for item in data:
            if field and isinstance(item, dict):
                val = item.get(field)
                if val is not None:
                    try:
                        values.append(float(val))
                        records.append(item)
                    except (ValueError, TypeError):
                        pass
            else:
                try:
                    values.append(float(item))
                    records.append(item)
                except (ValueError, TypeError):
                    pass

        if len(values) < 3:
            return ActionResult(success=False, message="Need at least 3 values for z-score")

        # Compute mean and std
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance)

        if std == 0:
            return ActionResult(success=False, message="Standard deviation is 0, cannot compute z-score")

        anomalies = []
        normal = []

        for i, (val, record) in enumerate(zip(values, records)):
            z = abs((val - mean) / std)
            if z > threshold:
                anomalies.append({
                    'index': i,
                    'value': val,
                    'z_score': round(z, 4),
                    'record': record,
                })
            else:
                normal.append(record)

        result = {
            'mean': mean,
            'std': std,
            'threshold': threshold,
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Found {len(anomalies)} anomalies (threshold={threshold})"
        )


class IQranomalyAction(BaseAction):
    """Detect anomalies using IQR (Interquartile Range) method.
    
    Flag values outside Q1 - 1.5*IQR and Q3 + 1.5*IQR.
    """
    action_type = "iqr_anomaly"
    display_name = "IQR异常检测"
    description = "基于四分位距(IQR)方法检测异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Detect anomalies using IQR.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of numbers
                - field: str (field name in dict records)
                - multiplier: float (IQR multiplier, default 1.5)
                - save_to_var: str
        
        Returns:
            ActionResult with anomaly detection result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        multiplier = params.get('multiplier', 1.5)
        save_to_var = params.get('save_to_var', 'iqr_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        values = []
        records = []
        for item in data:
            if field and isinstance(item, dict):
                val = item.get(field)
                if val is not None:
                    try:
                        values.append(float(val))
                        records.append(item)
                    except (ValueError, TypeError):
                        pass
            else:
                try:
                    values.append(float(item))
                    records.append(item)
                except (ValueError, TypeError):
                    pass

        if len(values) < 4:
            return ActionResult(success=False, message="Need at least 4 values for IQR")

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        anomalies = []
        for i, (val, record) in enumerate(zip(values, records)):
            if val < lower_bound or val > upper_bound:
                anomalies.append({
                    'index': i,
                    'value': val,
                    'bounds': (lower_bound, upper_bound),
                    'type': 'low' if val < lower_bound else 'high',
                    'record': record,
                })

        result = {
            'q1': q1,
            'q3': q3,
            'iqr': iqr,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Found {len(anomalies)} anomalies (IQR multiplier={multiplier})"
        )


class ThresholdAnomalyAction(BaseAction):
    """Detect anomalies using simple threshold rules.
    
    Flag values outside min/max bounds or matching
    specific patterns.
    """
    action_type = "threshold_anomaly"
    display_name = "阈值异常检测"
    description = "基于阈值规则检测异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Detect anomalies using thresholds.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - field: str (field to check)
                - min_value: float (minimum allowed)
                - max_value: float (maximum allowed)
                - exclude_values: list (values to flag as anomaly)
                - pattern: str (regex pattern to flag)
                - save_to_var: str
        
        Returns:
            ActionResult with threshold anomaly result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        min_value = params.get('min_value', None)
        max_value = params.get('max_value', None)
        exclude_values = params.get('exclude_values', [])
        pattern = params.get('pattern', '')
        save_to_var = params.get('save_to_var', 'threshold_result')

        import re
        compiled_pattern = re.compile(pattern) if pattern else None

        anomalies = []
        for i, item in enumerate(data):
            if field and isinstance(item, dict):
                val = item.get(field)
                record = item
            else:
                val = item
                record = {'_value': item}

            is_anomaly = False
            reasons = []

            # Check bounds
            if min_value is not None:
                try:
                    if float(val) < min_value:
                        is_anomaly = True
                        reasons.append(f'below_min({min_value})')
                except (ValueError, TypeError):
                    pass

            if max_value is not None:
                try:
                    if float(val) > max_value:
                        is_anomaly = True
                        reasons.append(f'above_max({max_value})')
                except (ValueError, TypeError):
                    pass

            # Check exclude values
            if val in exclude_values:
                is_anomaly = True
                reasons.append('in_exclude_list')

            # Check pattern
            if compiled_pattern:
                val_str = str(val)
                if compiled_pattern.search(val_str):
                    is_anomaly = True
                    reasons.append('pattern_match')

            if is_anomaly:
                anomalies.append({
                    'index': i,
                    'value': val,
                    'reasons': reasons,
                    'record': item if field else None,
                })

        result = {
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
            'min_value': min_value,
            'max_value': max_value,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Found {len(anomalies)} threshold violations"
        )
