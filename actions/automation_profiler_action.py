"""Automation Profiler Action.

Profiles automation workflow execution time with step-level timing,
flamegraph data generation, and performance bottleneck detection.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationProfilerAction(BaseAction):
    """Profile automation workflow execution performance.
    
    Tracks step execution times, identifies bottlenecks,
    generates flamegraph data, and provides optimization suggestions.
    """
    action_type = "automation_profiler"
    display_name = "自动化性能分析"
    description = "分析自动化工作流执行性能，生成火焰图数据"

    def __init__(self):
        super().__init__()
        self._timings: Dict[str, List[float]] = {}
        self._call_stack: List[Dict] = []
        self._enabled = False
        self._lock = threading.RLock()
        self._profile_start: Optional[float] = None
        self._profile_data: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Profile automation execution.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'start', 'stop', 'profile_step', 'report', 'flamegraph'.
                - step_name: Name of step being profiled.
                - threshold_ms: Threshold in ms for bottleneck reporting.
                - save_to_var: Variable name for results.
        
        Returns:
            ActionResult with profiling data.
        """
        try:
            action = params.get('action', 'start')
            save_to_var = params.get('save_to_var', 'profile_data')

            if action == 'start':
                return self._start_profiling(params, save_to_var)
            elif action == 'stop':
                return self._stop_profiling(save_to_var)
            elif action == 'profile_step':
                return self._profile_step(context, params, save_to_var)
            elif action == 'report':
                return self._generate_report(params, save_to_var)
            elif action == 'flamegraph':
                return self._generate_flamegraph(save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown profiler action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Profiler error: {e}")

    def _start_profiling(self, params: Dict, save_to_var: str) -> ActionResult:
        """Start profiling session."""
        self._enabled = True
        self._profile_start = time.time()
        self._profile_data = []
        self._timings.clear()
        self._call_stack.clear()

        status = {'started': True, 'timestamp': self._profile_start}
        return ActionResult(success=True, data=status, message="Profiling started")

    def _stop_profiling(self, save_to_var: str) -> ActionResult:
        """Stop profiling session."""
        if not self._enabled:
            return ActionResult(success=False, message="Profiler not started")

        self._enabled = False
        duration = time.time() - self._profile_start if self._profile_start else 0

        summary = {
            'duration': duration,
            'total_steps': len(self._profile_data),
            'timings': dict(self._timings)
        }

        return ActionResult(success=True, data=summary, message=f"Profiling stopped: {duration:.2f}s")

    @contextmanager
    def profile_step_context(self, step_name: str):
        """Context manager for profiling a step."""
        start = time.time()
        self._call_stack.append(step_name)
        try:
            yield
        finally:
            elapsed = (time.time() - start) * 1000  # ms
            with self._lock:
                if step_name not in self._timings:
                    self._timings[step_name] = []
                self._timings[step_name].append(elapsed)
                self._profile_data.append({
                    'step': step_name,
                    'duration_ms': elapsed,
                    'timestamp': time.time()
                })
            self._call_stack.pop()

    def _profile_step(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Profile a single step execution."""
        step_name = params.get('step_name', 'unknown')
        action = params.get('action')
        action_params = params.get('params', {})

        start = time.time()
        self._call_stack.append(step_name)

        error = None
        result = None

        try:
            if action:
                action_obj = self._get_action(action)
                if action_obj:
                    result = action_obj.execute(context, action_params)
                else:
                    error = f"Unknown action: {action}"
        except Exception as e:
            error = str(e)

        elapsed_ms = (time.time() - start) * 1000
        self._call_stack.pop()

        with self._lock:
            if step_name not in self._timings:
                self._timings[step_name] = []
            self._timings[step_name].append(elapsed_ms)
            
            self._profile_data.append({
                'step': step_name,
                'duration_ms': elapsed_ms,
                'timestamp': time.time(),
                'call_stack': self._call_stack.copy(),
                'success': error is None
            })

        timing_data = {
            'step': step_name,
            'duration_ms': elapsed_ms,
            'call_depth': len(self._call_stack) + 1
        }

        context.set_variable(save_to_var, timing_data)
        return ActionResult(success=error is None, data=timing_data, 
                          message=f"{step_name}: {elapsed_ms:.2f}ms" + (f" - {error}" if error else ""))

    def _generate_report(self, params: Dict, save_to_var: str) -> ActionResult:
        """Generate profiling report."""
        threshold_ms = params.get('threshold_ms', 100)

        with self._lock:
            step_stats = {}
            for step, timings in self._timings.items():
                if timings:
                    import statistics
                    step_stats[step] = {
                        'count': len(timings),
                        'total_ms': sum(timings),
                        'avg_ms': statistics.mean(timings),
                        'min_ms': min(timings),
                        'max_ms': max(timings),
                        'stddev_ms': statistics.stdev(timings) if len(timings) > 1 else 0
                    }

            # Find bottlenecks
            bottlenecks = [
                {'step': step, 'avg_ms': stats['avg_ms'], 'count': stats['count']}
                for step, stats in step_stats.items()
                if stats['avg_ms'] > threshold_ms
            ]
            bottlenecks.sort(key=lambda x: x['avg_ms'], reverse=True)

            # Total time
            total_time = sum(stats['total_ms'] for stats in step_stats.values())
            
            report = {
                'total_time_ms': total_time,
                'total_steps': len(self._profile_data),
                'unique_steps': len(step_stats),
                'step_stats': step_stats,
                'bottlenecks': bottlenecks[:10],
                'threshold_ms': threshold_ms
            }

        context.set_variable(save_to_var, report)
        return ActionResult(success=True, data=report, 
                          message=f"Report: {len(bottlenecks)} bottlenecks found")

    def _generate_flamegraph(self, save_to_var: str) -> ActionResult:
        """Generate flamegraph-compatible data."""
        with self._lock:
            # Build hierarchical data
            root = {'name': 'root', 'children': {}}
            
            for entry in self._profile_data:
                stack = entry.get('call_stack', [])
                if not stack:
                    stack = [entry.get('step', 'unknown')]
                
                current = root
                for name in stack:
                    if name not in current['children']:
                        current['children'][name] = {'name': name, 'children': {}, 'value': 0}
                    current = current['children'][name]
                
                current['value'] += entry.get('duration_ms', 0)

            # Flatten for flamegraph format
            flamegraph_data = []
            def flatten(node, parent=''):
                name = node['name']
                flamegraph_data.append({
                    'name': name,
                    'value': node['value'],
                    'parent': parent
                })
                for child in node['children'].values():
                    flatten(child, name)
            
            flatten(root)

        context.set_variable(save_to_var, flamegraph_data)
        return ActionResult(success=True, data=flamegraph_data, 
                          message=f"Flamegraph: {len(flamegraph_data)} frames")

    def _get_action(self, action_name: str):
        """Get action from registry."""
        from core.action_registry import ActionRegistry
        registry = ActionRegistry.get_instance()
        return registry.get_action(action_name)
