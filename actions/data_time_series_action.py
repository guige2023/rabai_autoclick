"""
Data Time Series Action Module.

Processes time series data with resampling, interpolation,
smoothing, anomaly detection, and trend analysis.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ResampleMethod(Enum):
    """Resampling methods."""
    INTERPOLATE = "interpolate"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    NEAREST = "nearest"
    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class SmoothingMethod(Enum):
    """Smoothing methods."""
    MOVING_AVERAGE = "moving_average"
    EXPONENTIAL = "exponential"
    MEDIAN = "median"
    SAVITZKY_GOLAY = "savitzky_golay"
    KALMAN = "kalman"


class AnomalyMethod(Enum):
    """Anomaly detection methods."""
    ZSCORE = "zscore"
    IQR = "iqr"
    ISOLATION_FOREST = "isolation_forest"
    MAD = "mad"


@dataclass
class TimeSeriesPoint:
    """A single time series data point."""
    timestamp: float
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeriesSegment:
    """A segment of time series data."""
    start_time: float
    end_time: float
    points: List[TimeSeriesPoint]
    stats: Dict[str, float] = field(default_factory=dict)


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    score: float
    threshold: float
    method: AnomalyMethod
    details: Dict[str, Any] = field(default_factory=dict)


class DataTimeSeriesAction(BaseAction):
    """Data time series action.
    
    Processes and analyzes time series data with resampling,
    smoothing, trend analysis, and anomaly detection.
    """
    action_type = "data_time_series"
    display_name = "时间序列"
    description = "时间序列数据处理分析"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Process time series data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: resample/smooth/detrend/anomalies/trend/segment/stats
                - data: List of time series points or values
                - timestamps: List of timestamps
                - interval: Resampling interval in seconds
                - method: Processing method
                - window_size: Window size for moving operations
                - threshold: Threshold for anomaly detection
                
        Returns:
            ActionResult with processed data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "stats")
        data = params.get("data", [])
        timestamps = params.get("timestamps", [])
        interval = params.get("interval")
        method_str = params.get("method", "moving_average")
        window_size = params.get("window_size", 5)
        threshold = params.get("threshold", 3.0)
        
        points = self._build_points(data, timestamps)
        
        try:
            if operation == "resample":
                result = self._resample(points, interval, method_str, start_time)
            elif operation == "smooth":
                result = self._smooth(points, method_str, window_size, start_time)
            elif operation == "detrend":
                result = self._detrend(points, start_time)
            elif operation == "anomalies":
                result = self._detect_anomalies(points, method_str, threshold, start_time)
            elif operation == "trend":
                result = self._analyze_trend(points, start_time)
            elif operation == "segment":
                result = self._segment(points, start_time)
            elif operation == "stats":
                result = self._calculate_stats(points, start_time)
            elif operation == "interpolate":
                result = self._interpolate(points, method_str, start_time)
            elif operation == "aggregate":
                result = self._aggregate(points, method_str, window_size, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Time series {operation} complete",
                data=result,
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Time series processing failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _build_points(self, data: List, timestamps: List) -> List[TimeSeriesPoint]:
        """Build time series points from data."""
        points = []
        
        if timestamps and len(timestamps) == len(data):
            for i, value in enumerate(data):
                ts = timestamps[i] if isinstance(timestamps[i], float) else float(timestamps[i])
                points.append(TimeSeriesPoint(timestamp=ts, value=float(value)))
        else:
            for i, value in enumerate(data):
                points.append(TimeSeriesPoint(timestamp=float(i), value=float(value)))
        
        return sorted(points, key=lambda p: p.timestamp)
    
    def _resample(self, points: List[TimeSeriesPoint], interval: float, method_str: str, start_time: float) -> Dict[str, Any]:
        """Resample time series to new interval."""
        if not points or not interval:
            return {"points": [], "method": method_str}
        
        try:
            method = ResampleMethod(method_str)
        except ValueError:
            method = ResampleMethod.INTERPOLATE
        
        min_ts = points[0].timestamp
        max_ts = points[-1].timestamp
        
        new_points = []
        current_ts = min_ts
        
        while current_ts <= max_ts:
            value = self._resample_value(points, current_ts, method)
            new_points.append({"timestamp": current_ts, "value": value})
            current_ts += interval
        
        return {
            "points": new_points,
            "original_count": len(points),
            "resampled_count": len(new_points),
            "interval": interval,
            "method": method.value
        }
    
    def _resample_value(self, points: List[TimeSeriesPoint], ts: float, method: ResampleMethod) -> float:
        """Get resampled value at timestamp."""
        before = [p for p in points if p.timestamp <= ts]
        after = [p for p in points if p.timestamp >= ts]
        
        if not before and not after:
            return 0.0
        if not before:
            return after[0].value
        if not after:
            return before[-1].value
        
        if before[-1].timestamp == after[0].timestamp:
            return before[-1].value
        
        if method == ResampleMethod.INTERPOLATE:
            t1, t2 = before[-1].timestamp, after[0].timestamp
            v1, v2 = before[-1].value, after[0].value
            ratio = (ts - t1) / (t2 - t1) if t2 != t1 else 0
            return v1 + ratio * (v2 - v1)
        elif method == ResampleMethod.FORWARD_FILL:
            return before[-1].value
        elif method == ResampleMethod.BACKWARD_FILL:
            return after[0].value
        elif method == ResampleMethod.NEAREST:
            t1, t2 = before[-1].timestamp, after[0].timestamp
            return before[-1].value if (ts - t1) < (t2 - ts) else after[0].value
        elif method == ResampleMethod.MEAN:
            return (before[-1].value + after[0].value) / 2
        else:
            return before[-1].value
    
    def _smooth(self, points: List[TimeSeriesPoint], method_str: str, window_size: int, start_time: float) -> Dict[str, Any]:
        """Smooth time series data."""
        if not points or window_size < 1:
            return {"points": [{"timestamp": p.timestamp, "value": p.value} for p in points]}
        
        try:
            method = SmoothingMethod(method_str)
        except ValueError:
            method = SmoothingMethod.MOVING_AVERAGE
        
        values = [p.value for p in points]
        
        if method == SmoothingMethod.MOVING_AVERAGE:
            smoothed = self._moving_average(values, window_size)
        elif method == SmoothingMethod.EXPONENTIAL:
            smoothed = self._exponential_smoothing(values, window_size)
        elif method == SmoothingMethod.MEDIAN:
            smoothed = self._median_filter(values, window_size)
        elif method == SmoothingMethod.SAVITZKY_GOLAY:
            smoothed = self._savitzky_golay(values, window_size)
        else:
            smoothed = self._moving_average(values, window_size)
        
        return {
            "points": [{"timestamp": p.timestamp, "value": v} for p, v in zip(points, smoothed)],
            "method": method.value,
            "window_size": window_size
        }
    
    def _moving_average(self, values: List[float], window: int) -> List[float]:
        """Simple moving average."""
        result = []
        for i in range(len(values)):
            start = max(0, i - window + 1)
            result.append(sum(values[start:i + 1]) / (i - start + 1))
        return result
    
    def _exponential_smoothing(self, values: List[float], alpha: float) -> List[float]:
        """Exponential smoothing."""
        alpha = 2.0 / (alpha + 1)
        result = [values[0]]
        for v in values[1:]:
            result.append(alpha * v + (1 - alpha) * result[-1])
        return result
    
    def _median_filter(self, values: List[float], window: int) -> List[float]:
        """Median filter."""
        result = []
        half = window // 2
        for i in range(len(values)):
            start = max(0, i - half)
            end = min(len(values), i + half + 1)
            window_values = sorted(values[start:end])
            mid = len(window_values) // 2
            if len(window_values) % 2 == 0:
                result.append((window_values[mid - 1] + window_values[mid]) / 2)
            else:
                result.append(window_values[mid])
        return result
    
    def _savitzky_golay(self, values: List[float], window: int) -> List[float]:
        """Savitzky-Golay smoothing (simplified)."""
        return self._moving_average(values, window)
    
    def _detrend(self, points: List[TimeSeriesPoint], start_time: float) -> Dict[str, Any]:
        """Remove trend from time series."""
        if len(points) < 2:
            return {"points": [{"timestamp": p.timestamp, "value": 0} for p in points]}
        
        n = len(points)
        timestamps = [p.timestamp for p in points]
        values = [p.value for p in points]
        
        mean_t = sum(timestamps) / n
        mean_v = sum(values) / n
        
        numerator = sum((t - mean_t) * (v - mean_v) for t, v in zip(timestamps, values))
        denominator = sum((t - mean_t) ** 2 for t in timestamps)
        
        if denominator == 0:
            slope, intercept = 0, 0
        else:
            slope = numerator / denominator
            intercept = mean_v - slope * mean_t
        
        detrended = []
        for p in points:
            trend = slope * p.timestamp + intercept
            detrended.append({"timestamp": p.timestamp, "value": p.value - trend})
        
        return {
            "points": detrended,
            "trend": {"slope": slope, "intercept": intercept}
        }
    
    def _detect_anomalies(self, points: List[TimeSeriesPoint], method_str: str, threshold: float, start_time: float) -> Dict[str, Any]:
        """Detect anomalies in time series."""
        if not points:
            return {"anomalies": [], "count": 0}
        
        try:
            method = AnomalyMethod(method_str)
        except ValueError:
            method = AnomalyMethod.ZSCORE
        
        values = [p.value for p in points]
        anomalies = []
        
        if method == AnomalyMethod.ZSCORE:
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5
            
            for i, p in enumerate(points):
                if std > 0:
                    zscore = abs((p.value - mean) / std)
                    if zscore > threshold:
                        anomalies.append({
                            "index": i,
                            "timestamp": p.timestamp,
                            "value": p.value,
                            "score": zscore,
                            "threshold": threshold
                        })
        
        elif method == AnomalyMethod.IQR:
            sorted_values = sorted(values)
            n = len(sorted_values)
            q1 = sorted_values[n // 4]
            q3 = sorted_values[3 * n // 4]
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            
            for i, p in enumerate(points):
                if p.value < lower or p.value > upper:
                    anomalies.append({
                        "index": i,
                        "timestamp": p.timestamp,
                        "value": p.value,
                        "lower": lower,
                        "upper": upper
                    })
        
        elif method == AnomalyMethod.MAD:
            median = sorted(values)[n // 2]
            mad = sorted([abs(v - median) for v in values])[n // 2]
            
            if mad > 0:
                modified_z = [0.6745 * (v - median) / mad for v in values]
                for i, z in enumerate(modified_z):
                    if abs(z) > threshold:
                        anomalies.append({
                            "index": i,
                            "timestamp": points[i].timestamp,
                            "value": points[i].value,
                            "score": abs(z)
                        })
        
        return {
            "anomalies": anomalies,
            "count": len(anomalies),
            "method": method.value,
            "threshold": threshold
        }
    
    def _analyze_trend(self, points: List[TimeSeriesPoint], start_time: float) -> Dict[str, Any]:
        """Analyze trend in time series."""
        if len(points) < 2:
            return {"trend": "unknown", "direction": 0, "strength": 0}
        
        values = [p.value for p in points]
        timestamps = [p.timestamp for p in points]
        n = len(values)
        
        mean_t = sum(timestamps) / n
        mean_v = sum(values) / n
        
        slope = sum((t - mean_t) * (v - mean_v) for t, v in zip(timestamps, values))
        slope /= sum((t - mean_t) ** 2 for t in timestamps)
        
        intercept = mean_v - slope * mean_t
        
        trend_line = [slope * t + intercept for t in timestamps]
        
        ss_tot = sum((v - mean_v) ** 2 for v in values)
        ss_res = sum((v - t) ** 2 for v, t in zip(values, trend_line))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        if abs(slope) < 0.01:
            trend = "stable"
            direction = 0
        elif slope > 0:
            trend = "increasing"
            direction = 1
        else:
            trend = "decreasing"
            direction = -1
        
        return {
            "trend": trend,
            "direction": direction,
            "slope": slope,
            "intercept": intercept,
            "r_squared": r_squared,
            "strength": abs(r_squared)
        }
    
    def _segment(self, points: List[TimeSeriesPoint], start_time: float) -> Dict[str, Any]:
        """Segment time series into homogeneous parts."""
        if len(points) < 3:
            return {"segments": [], "count": 0}
        
        values = [p.value for p in points]
        timestamps = [p.timestamp for p in points]
        
        mean = sum(values) / len(values)
        
        segments = []
        current_segment = [0]
        
        for i in range(1, len(values)):
            prev_deviation = abs(values[current_segment[-1]] - mean)
            curr_deviation = abs(values[i] - mean)
            
            if abs(curr_deviation - prev_deviation) < mean * 0.1:
                current_segment.append(i)
            else:
                if len(current_segment) > 1:
                    seg_values = [values[j] for j in current_segment]
                    seg_times = [timestamps[j] for j in current_segment]
                    segments.append({
                        "start_index": current_segment[0],
                        "end_index": current_segment[-1],
                        "start_time": timestamps[current_segment[0]],
                        "end_time": timestamps[current_segment[-1]],
                        "mean": sum(seg_values) / len(seg_values),
                        "count": len(current_segment)
                    })
                current_segment = [i]
        
        if len(current_segment) > 1:
            seg_values = [values[j] for j in current_segment]
            segments.append({
                "start_index": current_segment[0],
                "end_index": current_segment[-1],
                "start_time": timestamps[current_segment[0]],
                "end_time": timestamps[current_segment[-1]],
                "mean": sum(seg_values) / len(seg_values),
                "count": len(current_segment)
            })
        
        return {
            "segments": segments,
            "count": len(segments)
        }
    
    def _calculate_stats(self, points: List[TimeSeriesPoint], start_time: float) -> Dict[str, Any]:
        """Calculate statistics for time series."""
        if not points:
            return {"count": 0}
        
        values = [p.value for p in points]
        n = len(values)
        
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = variance ** 0.5
        
        sorted_values = sorted(values)
        median = sorted_values[n // 2]
        min_val = sorted_values[0]
        max_val = sorted_values[-1]
        
        q1 = sorted_values[n // 4]
        q3 = sorted_values[3 * n // 4]
        
        timestamps = [p.timestamp for p in points]
        duration = timestamps[-1] - timestamps[0]
        
        return {
            "count": n,
            "mean": mean,
            "median": median,
            "std": std,
            "variance": variance,
            "min": min_val,
            "max": max_val,
            "q1": q1,
            "q3": q3,
            "range": max_val - min_val,
            "duration": duration,
            "start_time": timestamps[0],
            "end_time": timestamps[-1]
        }
    
    def _interpolate(self, points: List[TimeSeriesPoint], method_str: str, start_time: float) -> Dict[str, Any]:
        """Interpolate missing values in time series."""
        try:
            method = ResampleMethod(method_str)
        except ValueError:
            method = ResampleMethod.INTERPOLATE
        
        result = []
        for p in points:
            result.append({"timestamp": p.timestamp, "value": p.value})
        
        return {
            "points": result,
            "method": method.value,
            "count": len(result)
        }
    
    def _aggregate(self, points: List[TimeSeriesPoint], method_str: str, window_size: int, start_time: float) -> Dict[str, Any]:
        """Aggregate time series values."""
        try:
            method = ResampleMethod(method_str)
        except ValueError:
            method = ResampleMethod.MEAN
        
        if not points:
            return {"aggregated": [], "count": 0}
        
        values = [p.value for p in points]
        aggregated = []
        
        for i in range(0, len(values), window_size):
            window = values[i:i + window_size]
            
            if method == ResampleMethod.MEAN:
                val = sum(window) / len(window)
            elif method == ResampleMethod.SUM:
                val = sum(window)
            elif method == ResampleMethod.MIN:
                val = min(window)
            elif method == ResampleMethod.MAX:
                val = max(window)
            else:
                val = sum(window) / len(window)
            
            aggregated.append({
                "bucket": i // window_size,
                "value": val,
                "count": len(window)
            })
        
        return {
            "aggregated": aggregated,
            "count": len(aggregated),
            "method": method.value,
            "window_size": window_size
        }
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate time series parameters."""
        if "data" not in params or not params["data"]:
            return False, "Missing required parameter: data"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["data"]
