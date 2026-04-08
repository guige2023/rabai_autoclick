"""Data Anomaly action module for RabAI AutoClick.

Provides anomaly detection operations:
- AnomalyZScoreAction: Z-score anomaly detection
- AnomalyIQRAction: IQR-based anomaly detection
- AnomalyIsolationAction: Isolation forest-style detection
- AnomalyThresholdAction: Threshold-based detection
"""

from __future__ import annotations

import sys
import os
import math
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AnomalyZScoreAction(BaseAction):
    """Z-score anomaly detection."""
    action_type = "anomaly_zscore"
    display_name = "Z-Score异常检测"
    description = "Z-Score异常检测"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Z-score anomaly detection."""
        data = params.get('data', [])
        field = params.get('field', '')
        threshold = params.get('threshold', 3)
        output_var = params.get('output_var', 'anomaly_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]

            if not values:
                return ActionResult(success=False, message=f"No numeric values in field '{field}'")

            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            std = math.sqrt(variance)

            anomalies = []
            for i, record in enumerate(resolved_data):
                val = record.get(field)
                if isinstance(val, (int, float)):
                    z_score = abs((val - mean) / std) if std > 0 else 0
                    if z_score > threshold:
                        anomalies.append({
                            'record': record,
                            'value': val,
                            'z_score': z_score,
                            'index': i,
                        })

            result = {
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'total_count': len(resolved_data),
                'anomaly_rate': len(anomalies) / len(resolved_data) if resolved_data else 0,
                'mean': mean,
                'std': std,
                'threshold': threshold,
                'method': 'z_score',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Z-score: {len(anomalies)} anomalies detected (threshold={threshold})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Z-score anomaly error: {e}")


class AnomalyIQRAction(BaseAction):
    """IQR-based anomaly detection."""
    action_type = "anomaly_iqr"
    display_name = "IQR异常检测"
    description = "IQR四分位距异常检测"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute IQR anomaly detection."""
        data = params.get('data', [])
        field = params.get('field', '')
        multiplier = params.get('multiplier', 1.5)
        output_var = params.get('output_var', 'anomaly_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            values = sorted([r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))])

            if len(values) < 4:
                return ActionResult(success=False, message="Need at least 4 values for IQR")

            q1_idx = len(values) // 4
            q3_idx = 3 * len(values) // 4
            q1 = values[q1_idx]
            q3 = values[q3_idx]
            iqr = q3 - q1

            lower_bound = q1 - multiplier * iqr
            upper_bound = q3 + multiplier * iqr

            anomalies = []
            for i, record in enumerate(resolved_data):
                val = record.get(field)
                if isinstance(val, (int, float)):
                    if val < lower_bound or val > upper_bound:
                        anomalies.append({
                            'record': record,
                            'value': val,
                            'bounds': (lower_bound, upper_bound),
                            'index': i,
                        })

            result = {
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'total_count': len(resolved_data),
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'multiplier': multiplier,
                'method': 'iqr',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"IQR: {len(anomalies)} anomalies detected (bounds=[{lower_bound:.2f}, {upper_bound:.2f}])"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"IQR anomaly error: {e}")


class AnomalyIsolationAction(BaseAction):
    """Isolation forest-style detection."""
    action_type = "anomaly_isolation"
    display_name = "隔离森林异常检测"
    description = "隔离森林风格异常检测"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute isolation-style anomaly detection."""
        data = params.get('data', [])
        field = params.get('field', '')
        contamination = params.get('contamination', 0.1)
        output_var = params.get('output_var', 'anomaly_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]

            if not values:
                return ActionResult(success=False, message=f"No numeric values in field '{field}'")

            sorted_values = sorted(values)
            n = len(sorted_values)

            isolation_scores = []
            for val in values:
                idx = sorted_values.index(val)
                score = (idx / n) * (1 - idx / n)
                isolation_scores.append(score)

            threshold_idx = int(n * contamination)
            threshold = sorted(isolation_scores, reverse=True)[threshold_idx] if threshold_idx < n else 0

            anomalies = []
            for i, record in enumerate(resolved_data):
                if isolation_scores[i] < threshold:
                    anomalies.append({
                        'record': record,
                        'value': record.get(field),
                        'isolation_score': isolation_scores[i],
                        'index': i,
                    })

            result = {
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'total_count': len(resolved_data),
                'contamination': contamination,
                'method': 'isolation',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Isolation: {len(anomalies)} anomalies detected (contamination={contamination})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Isolation anomaly error: {e}")


class AnomalyThresholdAction(BaseAction):
    """Threshold-based detection."""
    action_type = "anomaly_threshold"
    display_name = "阈值异常检测"
    description = "基于阈值的异常检测"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute threshold anomaly detection."""
        data = params.get('data', [])
        field = params.get('field', '')
        min_threshold = params.get('min_threshold', None)
        max_threshold = params.get('max_threshold', None)
        output_var = params.get('output_var', 'anomaly_result')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            anomalies = []
            for i, record in enumerate(resolved_data):
                val = record.get(field)
                if isinstance(val, (int, float)):
                    if min_threshold is not None and val < min_threshold:
                        anomalies.append({'record': record, 'value': val, 'reason': 'below_min', 'index': i})
                    elif max_threshold is not None and val > max_threshold:
                        anomalies.append({'record': record, 'value': val, 'reason': 'above_max', 'index': i})

            result = {
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'total_count': len(resolved_data),
                'min_threshold': min_threshold,
                'max_threshold': max_threshold,
                'method': 'threshold',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Threshold: {len(anomalies)} anomalies detected"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Threshold anomaly error: {e}")
