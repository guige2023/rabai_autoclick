"""Data Bottleneck Action Module for RabAI AutoClick.

Identifies and measures processing bottlenecks in data
pipelines with latency profiling and throughput analysis.
"""

import time
import threading
import sys
import os
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataBottleneckAction(BaseAction):
    """Bottleneck detection for data processing pipelines.

    Measures latency and throughput at each pipeline stage to
    identify bottlenecks. Supports percentile analysis, concurrent
    processing metrics, and bottleneck recommendations.
    """
    action_type = "data_bottleneck"
    display_name = "数据瓶颈分析"
    description = "数据管道瓶颈检测，延迟分析和吞吐量分析"

    _profilers: Dict[str, Dict[str, Any]] = {}
    _measurements: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bottleneck analysis operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'start', 'end', 'measure', 'report', 'reset'
                - stage_name: str - pipeline stage identifier
                - tags: list (optional) - measurement tags
                - sample_size: int (optional) - number of samples for report

        Returns:
            ActionResult with bottleneck analysis result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'measure')

            if operation == 'start':
                return self._start_stage(params, start_time)
            elif operation == 'end':
                return self._end_stage(params, start_time)
            elif operation == 'measure':
                return self._measure_stage(params, start_time)
            elif operation == 'report':
                return self._generate_report(params, start_time)
            elif operation == 'reset':
                return self._reset_stage(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Bottleneck action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _start_stage(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Start timing a pipeline stage."""
        stage_name = params.get('stage_name', 'unnamed')
        tags = params.get('tags', [])

        with self._lock:
            stage_id = f"{stage_name}_{time.time()}"
            self._profilers[stage_id] = {
                'stage_name': stage_name,
                'start_time': time.time(),
                'tags': tags,
                'thread_id': threading.current_thread().ident
            }

        return ActionResult(
            success=True,
            message=f"Stage started: {stage_name}",
            data={
                'stage_id': stage_id,
                'stage_name': stage_name
            },
            duration=time.time() - start_time
        )

    def _end_stage(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """End timing a pipeline stage."""
        stage_id = params.get('stage_id', '')
        stage_name = params.get('stage_name', '')

        with self._lock:
            profiler = None
            profiler_key = None

            if stage_id and stage_id in self._profilers:
                profiler = self._profilers[stage_id]
                profiler_key = stage_id
            elif stage_name:
                matching = [
                    (k, v) for k, v in self._profilers.items()
                    if v['stage_name'] == stage_name and v.get('end_time') is None
                ]
                if matching:
                    profiler_key, profiler = matching[-1]

            if not profiler:
                return ActionResult(
                    success=False,
                    message=f"No active profiler found",
                    duration=time.time() - start_time
                )

            duration = time.time() - profiler['start_time']
            profiler['end_time'] = time.time()
            profiler['duration'] = duration

            self._record_measurement(profiler['stage_name'], duration)

            return ActionResult(
                success=True,
                message=f"Stage ended: {profiler['stage_name']}",
                data={
                    'stage_name': profiler['stage_name'],
                    'duration_ms': round(duration * 1000, 3)
                },
                duration=time.time() - start_time
            )

    def _measure_stage(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Directly measure a stage's execution time."""
        stage_name = params.get('stage_name', 'unnamed')
        duration = params.get('duration', 0.0)

        if duration > 0:
            self._record_measurement(stage_name, duration)

        return ActionResult(
            success=True,
            message=f"Stage measured: {stage_name}",
            data={
                'stage_name': stage_name,
                'duration_ms': round(duration * 1000, 3)
            },
            duration=time.time() - start_time
        )

    def _generate_report(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Generate bottleneck analysis report."""
        sample_size = params.get('sample_size', 100)
        stage_name = params.get('stage_name')

        stages = {}
        with self._lock:
            if stage_name:
                if stage_name in self._measurements:
                    stages[stage_name] = list(self._measurements[stage_name])
            else:
                for name, measurements in self._measurements.items():
                    if measurements:
                        stages[name] = list(measurements)[-sample_size:]

        if not stages:
            return ActionResult(
                success=True,
                message="No measurements available",
                data={'stages': {}},
                duration=time.time() - start_time
            )

        analysis = {}
        bottleneck = None
        max_latency = 0

        for name, values in stages.items():
            if not values:
                continue

            sorted_values = sorted(values)
            count = len(sorted_values)

            p50 = sorted_values[int(count * 0.50)]
            p90 = sorted_values[int(count * 0.90)]
            p99 = sorted_values[int(count * 0.99)] if count > 1 else sorted_values[0]
            avg = sum(values) / count
            max_val = max(values)

            analysis[name] = {
                'count': count,
                'avg_ms': round(avg * 1000, 3),
                'min_ms': round(min(values) * 1000, 3),
                'max_ms': round(max_val * 1000, 3),
                'p50_ms': round(p50 * 1000, 3),
                'p90_ms': round(p90 * 1000, 3),
                'p99_ms': round(p99 * 1000, 3),
                'total_ms': round(sum(values) * 1000, 1)
            }

            if max_val > max_latency:
                max_latency = max_val
                bottleneck = name

        sorted_by_latency = sorted(
            analysis.items(),
            key=lambda x: x[1]['avg_ms'],
            reverse=True
        )

        return ActionResult(
            success=True,
            message=f"Bottleneck report: {len(analysis)} stages analyzed",
            data={
                'analysis': analysis,
                'sorted_stages': [s[0] for s in sorted_by_latency],
                'bottleneck': bottleneck,
                'bottleneck_avg_ms': analysis[bottleneck]['avg_ms'] if bottleneck else None,
                'sample_size': sample_size
            },
            duration=time.time() - start_time
        )

    def _reset_stage(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset measurements for a stage."""
        stage_name = params.get('stage_name')

        with self._lock:
            if stage_name:
                if stage_name in self._measurements:
                    self._measurements[stage_name].clear()
                return ActionResult(
                    success=True,
                    message=f"Reset measurements: {stage_name}",
                    data={'stage_name': stage_name},
                    duration=time.time() - start_time
                )
            else:
                count = sum(len(m) for m in self._measurements.values())
                self._measurements.clear()
                return ActionResult(
                    success=True,
                    message=f"Reset all measurements: {count} total",
                    data={'cleared': count},
                    duration=time.time() - start_time
                )

    def _record_measurement(self, stage_name: str, duration: float) -> None:
        """Record a duration measurement."""
        with self._lock:
            self._measurements[stage_name].append(duration)
