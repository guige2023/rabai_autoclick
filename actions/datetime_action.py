"""Time series data processing action module for RabAI AutoClick.

Provides time series operations:
- TimeSeriesParseAction: Parse time series data
- TimeSeriesResampleAction: Resample time series
- TimeSeriesSmoothingAction: Smooth time series
- TimeSeriesDetectAnomaliesAction: Detect anomalies
- TimeSeriesForecastAction: Simple forecasting
- RollingWindowAction: Rolling window calculations
"""

import statistics
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeSeriesParseAction(BaseAction):
    """Parse time series data from various formats."""
    action_type = "timeseries_parse"
    display_name = "时间序列解析"
    description = "解析时间序列数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            time_column = params.get("time_column", "timestamp")
            value_column = params.get("value_column", "value")
            format_str = params.get("format", "%Y-%m-%d %H:%M:%S")
            timezone = params.get("timezone", "UTC")

            if not data:
                return ActionResult(success=False, message="data list is required")

            parsed = []
            for record in data:
                if not isinstance(record, dict):
                    continue

                ts_val = record.get(time_column)
                val_val = record.get(value_column)

                if ts_val is None:
                    continue

                if isinstance(ts_val, (int, float)):
                    if ts_val > 1e12:
                        ts = datetime.fromtimestamp(ts_val / 1000)
                    else:
                        ts = datetime.fromtimestamp(ts_val)
                elif isinstance(ts_val, str):
                    try:
                        ts = datetime.strptime(ts_val, format_str)
                    except ValueError:
                        try:
                            ts = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                        except:
                            continue
                else:
                    continue

                try:
                    value = float(val_val)
                except (TypeError, ValueError):
                    value = 0.0

                parsed.append({"timestamp": ts, "value": value, "original": record})

            parsed.sort(key=lambda x: x["timestamp"])

            return ActionResult(
                success=True,
                message=f"Parsed {len(parsed)} time series points",
                data={"timeseries": parsed, "count": len(parsed), "start": str(parsed[0]["timestamp"]) if parsed else None}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Parse error: {str(e)}")


class TimeSeriesResampleAction(BaseAction):
    """Resample time series data."""
    action_type = "timeseries_resample"
    display_name = "时间序列重采样"
    description = "重采样时间序列数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeseries = params.get("timeseries", [])
            interval = params.get("interval", "1H")
            agg_method = params.get("agg_method", "mean")

            if not timeseries:
                return ActionResult(success=False, message="timeseries data required")

            interval_map = {
                "1s": 1, "5s": 5, "10s": 10, "30s": 30,
                "1m": 60, "5m": 300, "10m": 600, "30m": 1800,
                "1H": 3600, "2H": 7200, "6H": 21600, "12H": 43200,
                "1D": 86400, "1W": 604800
            }

            if interval not in interval_map:
                return ActionResult(success=False, message=f"Unknown interval: {interval}")

            interval_seconds = interval_map[interval]

            buckets = {}
            for point in timeseries:
                ts = point.get("timestamp")
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                if not isinstance(ts, datetime):
                    continue

                ts_ts = int(ts.timestamp())
                bucket_key = (ts_ts // interval_seconds) * interval_seconds
                bucket_time = datetime.fromtimestamp(bucket_key)

                if bucket_time not in buckets:
                    buckets[bucket_time] = []
                buckets[bucket_time].append(point.get("value", 0))

            agg_methods = {
                "mean": lambda vals: sum(vals) / len(vals) if vals else 0,
                "sum": lambda vals: sum(vals) if vals else 0,
                "min": lambda vals: min(vals) if vals else 0,
                "max": lambda vals: max(vals) if vals else 0,
                "count": lambda vals: len(vals),
                "first": lambda vals: vals[0] if vals else 0,
                "last": lambda vals: vals[-1] if vals else 0
            }

            agg_fn = agg_methods.get(agg_method, agg_methods["mean"])

            resampled = []
            for bucket_time in sorted(buckets.keys()):
                resampled.append({
                    "timestamp": bucket_time,
                    "value": agg_fn(buckets[bucket_time])
                })

            return ActionResult(
                success=True,
                message=f"Resampled to {len(resampled)} points",
                data={"timeseries": resampled, "count": len(resampled), "interval": interval}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Resample error: {str(e)}")


class TimeSeriesSmoothingAction(BaseAction):
    """Smooth time series data."""
    action_type = "timeseries_smooth"
    display_name = "时间序列平滑"
    description = "平滑时间序列数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeseries = params.get("timeseries", [])
            method = params.get("method", "moving_average")
            window_size = params.get("window_size", 5)

            if not timeseries:
                return ActionResult(success=False, message="timeseries data required")

            values = [p.get("value", 0) for p in timeseries]
            smoothed = []

            if method == "moving_average":
                for i in range(len(values)):
                    start = max(0, i - window_size // 2)
                    end = min(len(values), i + window_size // 2 + 1)
                    window = values[start:end]
                    smoothed.append(sum(window) / len(window))

            elif method == "exponential":
                alpha = params.get("alpha", 0.3)
                smoothed = [values[0]] if values else []
                for i in range(1, len(values)):
                    smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[-1])

            elif method == "savitzky_golay":
                if window_size % 2 == 0:
                    window_size += 1
                if len(values) < window_size:
                    window_size = len(values) if len(values) % 2 == 1 else len(values) - 1

                if window_size >= 3:
                    half = window_size // 2
                    for i in range(len(values)):
                        start = max(0, i - half)
                        end = min(len(values), i + half + 1)
                        window = values[start:end]

                        if len(window) >= 3:
                            n = len(window)
                            x = list(range(n))
                            x_mean = sum(x) / n
                            xy = [xi * yi for xi, yi in zip(x, window)]
                            slope = (sum(xy) - n * x_mean * sum(window) / n) / (sum(xi**2 for xi in x) - n * x_mean**2)
                            intercept = sum(window) / n - slope * x_mean
                            smoothed.append(intercept + slope * half)
                        else:
                            smoothed.append(sum(window) / len(window))
                else:
                    smoothed = values[:]

            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            result = []
            for i, point in enumerate(timeseries):
                result.append({
                    "timestamp": point.get("timestamp"),
                    "value": smoothed[i],
                    "original": point.get("value")
                })

            return ActionResult(
                success=True,
                message=f"Smoothed {len(result)} points using {method}",
                data={"timeseries": result, "method": method}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Smooth error: {str(e)}")


class TimeSeriesDetectAnomaliesAction(BaseAction):
    """Detect anomalies in time series."""
    action_type = "timeseries_anomalies"
    display_name = "时间序列异常检测"
    description = "检测时间序列异常"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeseries = params.get("timeseries", [])
            method = params.get("method", "zscore")
            threshold = params.get("threshold", 3.0)
            window_size = params.get("window_size", 20)

            if not timeseries:
                return ActionResult(success=False, message="timeseries data required")

            values = [p.get("value", 0) for p in timeseries]
            anomalies = []

            if method == "zscore":
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                stdev = math.sqrt(variance) if variance > 0 else 1

                for i, point in enumerate(timeseries):
                    zscore = abs(values[i] - mean) / stdev if stdev > 0 else 0
                    if zscore > threshold:
                        anomalies.append({
                            "index": i,
                            "timestamp": point.get("timestamp"),
                            "value": values[i],
                            "zscore": zscore
                        })

            elif method == "iqr":
                sorted_values = sorted(values)
                n = len(sorted_values)
                q1 = sorted_values[n // 4]
                q3 = sorted_values[3 * n // 4]
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr

                for i, point in enumerate(timeseries):
                    if values[i] < lower or values[i] > upper:
                        anomalies.append({
                            "index": i,
                            "timestamp": point.get("timestamp"),
                            "value": values[i],
                            "bounds": (lower, upper)
                        })

            elif method == "rolling":
                for i in range(len(values)):
                    start = max(0, i - window_size)
                    end = min(len(values), i + window_size // 2 + 1)
                    if i < window_size // 2 or i >= len(values) - window_size // 2:
                        continue

                    window = values[start:i] + values[i+1:end]
                    if window:
                        w_mean = sum(window) / len(window)
                        w_var = sum((v - w_mean) ** 2 for v in window) / len(window)
                        w_stdev = math.sqrt(w_var) if w_var > 0 else 1
                        zscore = abs(values[i] - w_mean) / w_stdev if w_stdev > 0 else 0

                        if zscore > threshold:
                            anomalies.append({
                                "index": i,
                                "timestamp": timeseries[i].get("timestamp"),
                                "value": values[i],
                                "zscore": zscore
                            })

            return ActionResult(
                success=True,
                message=f"Detected {len(anomalies)} anomalies",
                data={"anomalies": anomalies, "count": len(anomalies), "method": method}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Anomaly detection error: {str(e)}")


class TimeSeriesForecastAction(BaseAction):
    """Simple time series forecasting."""
    action_type = "timeseries_forecast"
    display_name = "时间序列预测"
    description = "简单时间序列预测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeseries = params.get("timeseries", [])
            method = params.get("method", "linear")
            steps = params.get("steps", 10)
            window_size = params.get("window_size", 5)

            if not timeseries:
                return ActionResult(success=False, message="timeseries data required")

            values = [p.get("value", 0) for p in timeseries]
            last_ts = timeseries[-1].get("timestamp")

            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts)

            forecasts = []

            if method == "linear":
                n = len(values)
                if n >= 2:
                    x = list(range(n))
                    x_mean = sum(x) / n
                    y_mean = sum(values) / n
                    xy = [xi * yi for xi, yi in zip(x, values)]
                    slope = (sum(xy) - n * x_mean * y_mean) / (sum(xi**2 for xi in x) - n * x_mean**2)
                    intercept = y_mean - slope * x_mean

                    for i in range(steps):
                        next_x = n + i
                        next_val = slope * next_x + intercept
                        forecasts.append({"forecast": next_val})

            elif method == "moving_average":
                for i in range(steps):
                    start = max(0, len(values) - window_size)
                    window = values[start:]
                    next_val = sum(window) / len(window)
                    forecasts.append({"forecast": next_val})

            elif method == "exponential_smoothing":
                alpha = params.get("alpha", 0.3)
                smoothed = [values[0]]
                for i in range(1, len(values)):
                    smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[-1])

                last_smoothed = smoothed[-1]
                trend = (smoothed[-1] - smoothed[-2]) / 1 if len(smoothed) > 1 else 0

                for i in range(steps):
                    next_val = last_smoothed + (i + 1) * trend
                    forecasts.append({"forecast": next_val})

            elif method == "naive":
                last_val = values[-1]
                for _ in range(steps):
                    forecasts.append({"forecast": last_val})

            return ActionResult(
                success=True,
                message=f"Generated {len(forecasts)} forecasts",
                data={"forecasts": forecasts, "method": method, "steps": steps}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Forecast error: {str(e)}")


class RollingWindowAction(BaseAction):
    """Rolling window calculations."""
    action_type = "rolling_window"
    display_name = "滚动窗口"
    description = "滚动窗口计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeseries = params.get("timeseries", [])
            window_size = params.get("window_size", 5)
            operations = params.get("operations", ["mean"])

            if not timeseries:
                return ActionResult(success=False, message="timeseries data required")

            values = [p.get("value", 0) for p in timeseries]
            results = []

            for i in range(len(values)):
                start = max(0, i - window_size + 1)
                window = values[start:i+1]

                result = {"timestamp": timeseries[i].get("timestamp"), "original": values[i]}

                for op in operations:
                    if op == "mean":
                        result["mean"] = sum(window) / len(window)
                    elif op == "sum":
                        result["sum"] = sum(window)
                    elif op == "min":
                        result["min"] = min(window)
                    elif op == "max":
                        result["max"] = max(window)
                    elif op == "std":
                        if len(window) > 1:
                            mean = sum(window) / len(window)
                            variance = sum((v - mean) ** 2 for v in window) / len(window)
                            result["std"] = math.sqrt(variance)
                        else:
                            result["std"] = 0
                    elif op == "count":
                        result["count"] = len(window)

                results.append(result)

            return ActionResult(
                success=True,
                message=f"Applied rolling window ({window_size}) to {len(results)} points",
                data={"results": results, "window_size": window_size, "operations": operations}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rolling window error: {str(e)}")
