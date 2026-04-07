"""
Metrics tracking and experiment logging utilities.

Provides experiment tracking, metric aggregation, and
visualization helpers for ML experiments.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np


class MetricsTracker:
    """Track and aggregate training metrics."""

    def __init__(self, name: str = "experiment"):
        self.name = name
        self.metrics = defaultdict(list)
        self.metadata = {}
        self.start_time = time.time()

    def log(self, metric_name: str, value: Union[float, int], step: int = None) -> None:
        """Log a metric value."""
        entry = {"value": value, "timestamp": time.time()}
        if step is not None:
            entry["step"] = step
        self.metrics[metric_name].append(entry)

    def log_dict(self, metrics_dict: Dict[str, float], step: int = None) -> None:
        """Log multiple metrics at once."""
        for name, value in metrics_dict.items():
            self.log(name, value, step)

    def get(self, metric_name: str) -> List[float]:
        """Get all values for a metric."""
        return [e["value"] for e in self.metrics[metric_name]]

    def latest(self, metric_name: str) -> Optional[float]:
        """Get latest value for a metric."""
        values = self.get(metric_name)
        return values[-1] if values else None

    def mean(self, metric_name: str, last_n: int = None) -> Optional[float]:
        """Get mean of a metric."""
        values = self.get(metric_name)
        if not values:
            return None
        if last_n:
            values = values[-last_n:]
        return float(np.mean(values))

    def std(self, metric_name: str, last_n: int = None) -> Optional[float]:
        """Get standard deviation of a metric."""
        values = self.get(metric_name)
        if not values:
            return None
        if last_n:
            values = values[-last_n:]
        return float(np.std(values))

    def summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary statistics for all metrics."""
        summary = {}
        for name, entries in self.metrics.items():
            values = [e["value"] for e in entries]
            summary[name] = {
                "count": len(values),
                "mean": float(np.mean(values)),
                "std": float(np.std(values)),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "latest": float(values[-1]),
            }
        return summary

    def to_dict(self) -> Dict:
        """Convert tracker to dictionary."""
        return {
            "name": self.name,
            "metadata": self.metadata,
            "metrics": dict(self.metrics),
            "start_time": self.start_time,
            "duration": time.time() - self.start_time,
        }

    def save(self, filepath: str) -> None:
        """Save tracker to JSON file."""
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)

    def load(self, filepath: str) -> None:
        """Load tracker from JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        self.name = data["name"]
        self.metadata = data.get("metadata", {})
        self.start_time = data.get("start_time", time.time())
        self.metrics = defaultdict(list, data.get("metrics", {}))


class AverageMeter:
    """Compute and store running average."""

    def __init__(self, name: str = ""):
        self.name = name
        self.reset()

    def reset(self) -> None:
        """Reset meter."""
        self.val = 0.0
        self.avg = 0.0
        self.sum = 0.0
        self.count = 0

    def update(self, value: float, n: int = 1) -> None:
        """Update meter with new value."""
        self.val = value
        self.sum += value * n
        self.count += n
        self.avg = self.sum / self.count

    def __repr__(self) -> str:
        return f"{self.name}: {self.avg:.4f}"


class ProgressTracker:
    """Track training progress with ETA."""

    def __init__(
        self,
        total_epochs: int,
        steps_per_epoch: int = None,
        metrics: List[str] = None,
    ):
        self.total_epochs = total_epochs
        self.steps_per_epoch = steps_per_epoch
        self.metrics = metrics or []
        self.current_epoch = 0
        self.current_step = 0
        self.start_time = time.time()
        self.epoch_start_time = time.time()
        self.history = []

    def update_epoch(self, epoch_metrics: Dict[str, float]) -> None:
        """Update after epoch completion."""
        self.current_epoch += 1
        epoch_time = time.time() - self.epoch_start_time
        total_time = time.time() - self.start_time
        avg_epoch_time = total_time / self.current_epoch
        remaining_epochs = self.total_epochs - self.current_epoch
        eta = avg_epoch_time * remaining_epochs
        self.history.append(
            {
                "epoch": self.current_epoch,
                "metrics": epoch_metrics,
                "epoch_time": epoch_time,
                "eta": eta,
            }
        )
        self.epoch_start_time = time.time()

    def update_step(self, step_metrics: Dict[str, float] = None) -> None:
        """Update after step."""
        self.current_step += 1

    def get_progress(self) -> Dict[str, Any]:
        """Get current progress info."""
        return {
            "epoch": self.current_epoch,
            "total_epochs": self.total_epochs,
            "step": self.current_step,
            "progress_pct": 100 * self.current_epoch / self.total_epochs,
        }

    def format_eta(self, seconds: float) -> str:
        """Format ETA as human-readable string."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.1f}min"
        else:
            return f"{seconds / 3600:.1f}h"


class ComparisonTracker:
    """Track and compare multiple experiment runs."""

    def __init__(self):
        self.runs = {}

    def add_run(self, run_id: str, tracker: MetricsTracker) -> None:
        """Add a run to the comparison."""
        self.runs[run_id] = tracker

    def compare_metric(self, metric_name: str, last_n: int = None) -> Dict[str, float]:
        """Compare metric across runs."""
        comparison = {}
        for run_id, tracker in self.runs.items():
            mean_val = tracker.mean(metric_name, last_n)
            if mean_val is not None:
                comparison[run_id] = mean_val
        return comparison

    def best_run(self, metric_name: str, maximize: bool = True, last_n: int = None) -> str:
        """Find best run for a metric."""
        comparison = self.compare_metric(metric_name, last_n)
        if not comparison:
            return None
        return max(comparison.items(), key=lambda x: x[1] if maximize else -x[1])[0]

    def rank_runs(self, metric_name: str, maximize: bool = True, last_n: int = None) -> List[tuple]:
        """Rank runs by metric."""
        comparison = self.compare_metric(metric_name, last_n)
        sorted_runs = sorted(comparison.items(), key=lambda x: x[1], reverse=maximize)
        return sorted_runs


class EarlyStoppingMonitor:
    """Monitor for early stopping."""

    def __init__(
        self,
        patience: int = 10,
        min_delta: float = 0.0,
        maximize: bool = True,
        metric_name: str = "val_loss",
    ):
        self.patience = patience
        self.min_delta = min_delta
        self.maximize = maximize
        self.metric_name = metric_name
        self.best_value = float("-inf") if maximize else float("inf")
        self.counter = 0
        self.should_stop = False
        self.best_epoch = 0

    def update(self, metric_value: float, epoch: int) -> bool:
        """Check if should stop."""
        if (self.maximize and metric_value > self.best_value + self.min_delta) or (
            not self.maximize and metric_value < self.best_value - self.min_delta
        ):
            self.best_value = metric_value
            self.best_epoch = epoch
            self.counter = 0
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.should_stop = True
        return self.should_stop


class CheckpointManager:
    """Manage model checkpoints."""

    def __init__(self, save_dir: str, maximize: bool = True, metric_name: str = "val_loss"):
        self.save_dir = save_dir
        self.maximize = maximize
        self.metric_name = metric_name
        self.best_value = float("-inf") if maximize else float("inf")
        self.checkpoints = []

    def should_save(self, metric_value: float) -> bool:
        """Check if model should be saved."""
        if (self.maximize and metric_value > self.best_value) or (
            not self.maximize and metric_value < self.best_value
        ):
            self.best_value = metric_value
            return True
        return False

    def save_checkpoint(self, state: Dict, filename: str) -> None:
        """Save checkpoint."""
        import os
        filepath = os.path.join(self.save_dir, filename)
        with open(filepath, "w") as f:
            json.dump(state, f, indent=2, default=str)
        self.checkpoints.append({"path": filepath, "metric": self.best_value})

    def cleanup_old(self, keep_last: int = 3) -> None:
        """Remove old checkpoints, keeping only recent ones."""
        import os
        if len(self.checkpoints) > keep_last:
            to_remove = self.checkpoints[:-keep_last]
            for ckpt in to_remove:
                if os.path.exists(ckpt["path"]):
                    os.remove(ckpt["path"])
            self.checkpoints = self.checkpoints[-keep_last:]


def format_time(seconds: float) -> str:
    """Format seconds as human-readable time."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}min"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_number(n: float, precision: int = 2) -> str:
    """Format number with K/M/B suffixes."""
    if abs(n) >= 1e9:
        return f"{n / 1e9:.1f}B"
    elif abs(n) >= 1e6:
        return f"{n / 1e6:.1f}M"
    elif abs(n) >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{n:.{precision}f}"


class TableFormatter:
    """Format data as ASCII table."""

    def __init__(self, headers: List[str]):
        self.headers = headers
        self.rows = []

    def add_row(self, values: List[Any]) -> None:
        """Add a row."""
        self.rows.append(values)

    def format(self) -> str:
        """Format as ASCII table."""
        col_widths = [len(h) for h in self.headers]
        for row in self.rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        separator = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
        header_row = "|" + "|".join(f" {h:<{col_widths[i]}} " for i, h in enumerate(self.headers)) + "|"
        lines = [separator, header_row, separator]
        for row in self.rows:
            row_str = "|" + "|".join(f" {str(v):<{col_widths[i]}} " for i, v in enumerate(row)) + "|"
            lines.append(row_str)
        lines.append(separator)
        return "\n".join(lines)


class MetricAggregator:
    """Aggregate metrics from multiple sources."""

    def __init__(self):
        self.data = defaultdict(list)

    def add(self, metrics: Dict[str, float]) -> None:
        """Add metrics."""
        for name, value in metrics.items():
            self.data[name].append(value)

    def mean(self, metric_name: str) -> float:
        """Get mean."""
        return float(np.mean(self.data[metric_name]))

    def std(self, metric_name: str) -> float:
        """Get std."""
        return float(np.std(self.data[metric_name]))

    def min(self, metric_name: str) -> float:
        """Get min."""
        return float(np.min(self.data[metric_name]))

    def max(self, metric_name: str) -> float:
        """Get max."""
        return float(np.max(self.data[metric_name]))

    def median(self, metric_name: str) -> float:
        """Get median."""
        return float(np.median(self.data[metric_name]))

    def percentile(self, metric_name: str, p: float) -> float:
        """Get percentile."""
        return float(np.percentile(self.data[metric_name], p))

    def all_stats(self, metric_name: str) -> Dict[str, float]:
        """Get all statistics."""
        return {
            "mean": self.mean(metric_name),
            "std": self.std(metric_name),
            "min": self.min(metric_name),
            "max": self.max(metric_name),
            "median": self.median(metric_name),
            "p25": self.percentile(metric_name, 25),
            "p75": self.percentile(metric_name, 75),
        }
