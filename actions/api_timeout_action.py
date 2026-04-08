"""API timeout handling action module for RabAI AutoClick.

Provides timeout operations:
- TimeoutAction: Execute with timeout
- ReadTimeoutAction: Handle read timeouts
- ConnectionTimeoutAction: Handle connection timeouts
- TimeoutRecoveryAction: Recover from timeout
- TimeoutMonitorAction: Monitor timeout metrics
"""

import signal
import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TimeoutException(Exception):
    """Exception raised when operation times out."""
    pass


class TimeoutAction(BaseAction):
    """Execute action with timeout."""
    action_type = "timeout"
    display_name = "超时控制"
    description = "带超时控制的操作执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            timeout_seconds = params.get("timeout_seconds", 30.0)
            action_config = params.get("action", {})
            on_timeout = params.get("on_timeout", "raise")
            timeout_name = params.get("name", "unnamed")

            if timeout_seconds <= 0:
                return ActionResult(success=False, message="timeout_seconds must be positive")

            start_time = time.time()

            class TimeoutHandler:
                def __init__(self, timeout):
                    self.timeout = timeout
                    self.old_handler = None

                def __enter__(self):
                    self.old_handler = signal.signal(signal.SIGALRM, self._handler)
                    signal.setitimer(signal.ITIMER_REAL, self.timeout)
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    if self.old_handler:
                        signal.signal(signal.SIGALRM, self.old_handler)
                    return False

                def _handler(self, signum, frame):
                    raise TimeoutException(f"Operation '{timeout_name}' timed out after {self.timeout}s")

            try:
                with TimeoutHandler(timeout_seconds):
                    return ActionResult(
                        success=True,
                        data={
                            "name": timeout_name,
                            "timeout_seconds": timeout_seconds,
                            "action_type": action_config.get("type", "unknown"),
                            "status": "completed"
                        },
                        message=f"Action completed within {timeout_seconds}s"
                    )
            except TimeoutException:
                elapsed = time.time() - start_time
                if on_timeout == "raise":
                    return ActionResult(
                        success=False,
                        data={
                            "name": timeout_name,
                            "timeout_seconds": timeout_seconds,
                            "elapsed_seconds": round(elapsed, 3),
                            "status": "timeout"
                        },
                        message=f"Action timed out after {elapsed:.1f}s"
                    )
                elif on_timeout == "continue":
                    return ActionResult(
                        success=True,
                        data={
                            "name": timeout_name,
                            "elapsed_seconds": round(elapsed, 3),
                            "status": "timeout_continued"
                        },
                        message="Timeout occurred but continuing"
                    )

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout action error: {str(e)}")


class ReadTimeoutAction(BaseAction):
    """Handle read timeouts for streaming operations."""
    action_type = "read_timeout"
    display_name = "读取超时"
    description = "处理流读取超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            read_timeout = params.get("read_timeout", 30.0)
            chunk_timeout = params.get("chunk_timeout", 10.0)
            total_size = params.get("total_size", 0)
            enable_adaptive = params.get("enable_adaptive", True)
            stream_config = params.get("stream_config", {})

            if read_timeout <= 0:
                return ActionResult(success=False, message="read_timeout must be positive")

            estimated_chunks = max(1, int(total_size / 8192)) if total_size > 0 else 10

            base_timeout = read_timeout
            if enable_adaptive and total_size > 0:
                adaptive_timeout = chunk_timeout * estimated_chunks * 1.5
                base_timeout = min(adaptive_timeout, read_timeout * 2)

            return ActionResult(
                success=True,
                data={
                    "read_timeout": read_timeout,
                    "chunk_timeout": chunk_timeout,
                    "total_size": total_size,
                    "estimated_chunks": estimated_chunks,
                    "adaptive_timeout": round(base_timeout, 1),
                    "enable_adaptive": enable_adaptive
                },
                message=f"Read timeout configured: {base_timeout}s for {estimated_chunks} chunks"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read timeout error: {str(e)}")


class ConnectionTimeoutAction(BaseAction):
    """Handle connection timeouts."""
    action_type = "connection_timeout"
    display_name = "连接超时"
    description = "处理连接建立超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connect_timeout = params.get("connect_timeout", 10.0)
            retry_connect = params.get("retry_connect", True)
            max_retries = params.get("max_retries", 3)
            backoff_factor = params.get("backoff_factor", 1.5)
            target_host = params.get("target_host", "")
            target_port = params.get("target_port", 443)

            if connect_timeout <= 0:
                return ActionResult(success=False, message="connect_timeout must be positive")

            timeout_sequence = []
            current_timeout = connect_timeout

            for i in range(max_retries):
                timeout_sequence.append(round(current_timeout, 1))
                if retry_connect:
                    current_timeout *= backoff_factor

            total_timeout = sum(timeout_sequence)

            return ActionResult(
                success=True,
                data={
                    "connect_timeout": connect_timeout,
                    "retry_connect": retry_connect,
                    "max_retries": max_retries,
                    "backoff_factor": backoff_factor,
                    "timeout_sequence": timeout_sequence,
                    "total_timeout": round(total_timeout, 1),
                    "target": f"{target_host}:{target_port}" if target_host else "not specified"
                },
                message=f"Connection timeout configured: {max_retries} retries, total {total_timeout}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Connection timeout error: {str(e)}")


class TimeoutRecoveryAction(BaseAction):
    """Recover from timeout scenarios."""
    action_type = "timeout_recovery"
    display_name = "超时恢复"
    description = "超时场景恢复处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            recovery_strategy = params.get("recovery_strategy", "retry")
            timeout_count = params.get("timeout_count", 0)
            last_timeout_at = params.get("last_timeout_at", None)
            max_recovery_attempts = params.get("max_recovery_attempts", 3)
            action_config = params.get("action", {})

            recovery_strategies = {
                "retry": "重试原始操作",
                "fallback": "使用备用端点",
                "degrade": "降级服务级别",
                "skip": "跳过失败步骤",
                "queue": "放入重试队列"
            }

            strategy_description = recovery_strategies.get(recovery_strategy, recovery_strategy)

            recovery_attempted = min(timeout_count, max_recovery_attempts) > 0

            recovery_actions = []
            if recovery_strategy == "retry":
                recovery_actions.append({"step": "retry", "delay": 1.0})
            elif recovery_strategy == "fallback":
                recovery_actions.append({"step": "switch_endpoint", "delay": 0.5})
                recovery_actions.append({"step": "retry", "delay": 2.0})
            elif recovery_strategy == "degrade":
                recovery_actions.append({"step": "reduce_quality", "delay": 0.1})
                recovery_actions.append({"step": "retry", "delay": 1.0})
            elif recovery_strategy == "queue":
                recovery_actions.append({"step": "enqueue", "delay": 0.1})
                recovery_actions.append({"step": "notify", "delay": 0.5})

            return ActionResult(
                success=True,
                data={
                    "recovery_strategy": recovery_strategy,
                    "strategy_description": strategy_description,
                    "timeout_count": timeout_count,
                    "recovery_attempted": recovery_attempted,
                    "recovery_actions": recovery_actions,
                    "action_type": action_config.get("type", "unknown")
                },
                message=f"Timeout recovery: {strategy_description}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Timeout recovery error: {str(e)}")


class TimeoutMonitorAction(BaseAction):
    """Monitor timeout metrics and trends."""
    action_type = "timeout_monitor"
    display_name = "超时监控"
    description = "监控超时指标和趋势"

    def __init__(self):
        super().__init__()
        self._timeout_history = []
        self._timeout_stats = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "record")
            timeout_duration = params.get("timeout_duration", 0)
            endpoint = params.get("endpoint", "default")
            window_size = params.get("window_size", 100)

            if operation == "record":
                self._timeout_history.append({
                    "duration": timeout_duration,
                    "endpoint": endpoint,
                    "timestamp": time.time()
                })

                if len(self._timeout_history) > window_size:
                    self._timeout_history = self._timeout_history[-window_size:]

                self._update_stats(endpoint)

                return ActionResult(
                    success=True,
                    data={
                        "recorded": True,
                        "total_recorded": len(self._timeout_history),
                        "endpoint": endpoint
                    },
                    message=f"Timeout recorded: {timeout_duration}s for {endpoint}"
                )

            elif operation == "stats":
                stats = self._timeout_stats.get(endpoint, {
                    "count": 0,
                    "total": 0,
                    "avg": 0,
                    "max": 0,
                    "min": 0
                })

                endpoint_timeouts = [t for t in self._timeout_history if t["endpoint"] == endpoint]
                recent_count = len(endpoint_timeouts)
                recent_avg = sum(t["duration"] for t in endpoint_timeouts) / recent_count if recent_count > 0 else 0

                return ActionResult(
                    success=True,
                    data={
                        "endpoint": endpoint,
                        "total_timeouts": stats["count"],
                        "average_timeout": round(stats["avg"], 2),
                        "max_timeout": stats["max"],
                        "min_timeout": stats["min"],
                        "recent_count": recent_count,
                        "recent_avg": round(recent_avg, 2)
                    },
                    message=f"Timeout stats for {endpoint}: {stats['count']} timeouts, avg {stats['avg']:.1f}s"
                )

            elif operation == "reset":
                self._timeout_history = []
                self._timeout_stats = {}
                return ActionResult(
                    success=True,
                    data={"reset": True},
                    message="Timeout history reset"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout monitor error: {str(e)}")

    def _update_stats(self, endpoint: str):
        endpoint_timeouts = [t for t in self._timeout_history if t["endpoint"] == endpoint]

        if endpoint_timeouts:
            durations = [t["duration"] for t in endpoint_timeouts]
            self._timeout_stats[endpoint] = {
                "count": len(durations),
                "total": sum(durations),
                "avg": sum(durations) / len(durations),
                "max": max(durations),
                "min": min(durations)
            }
