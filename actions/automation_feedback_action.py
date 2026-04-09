"""Automation feedback action module for RabAI AutoClick.

Provides feedback loop operations:
- FeedbackCollectorAction: Collect execution feedback
- FeedbackAnalyzerAction: Analyze feedback patterns
- FeedbackLoopAction: Implement feedback control loops
- AdaptiveTunerAction: Adaptively tune parameters based on feedback
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import threading

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class FeedbackPoint:
    """A single feedback data point."""
    metric_name: str
    value: float
    expected: Optional[float] = None
    deviation: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class FeedbackLoopState:
    """State of a feedback control loop."""
    loop_id: str
    current_value: float = 0.0
    target_value: float = 0.0
    kp: float = 1.0
    ki: float = 0.0
    kd: float = 0.0
    integral: float = 0.0
    last_error: float = 0.0
    last_update: datetime = field(default_factory=datetime.now)
    is_active: bool = False


class FeedbackStore:
    """Store for feedback data points."""

    def __init__(self, max_points: int = 1000) -> None:
        self._points: deque = deque(maxlen=max_points)
        self._lock = threading.Lock()

    def record(self, point: FeedbackPoint) -> None:
        with self._lock:
            self._points.append(point)

    def get_recent(self, metric_name: str, since_minutes: int = 60) -> List[FeedbackPoint]:
        with self._lock:
            cutoff = datetime.now() - timedelta(minutes=since_minutes)
            return [p for p in self._points if p.metric_name == metric_name and p.timestamp >= cutoff]

    def get_stats(self, metric_name: str, since_minutes: int = 60) -> Dict[str, float]:
        points = self.get_recent(metric_name, since_minutes)
        if not points:
            return {}

        values = [p.value for p in points]
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "deviation_avg": sum(abs(p.deviation) for p in points) / len(points)
        }


class PIDController:
    """PID feedback controller."""

    def __init__(self, kp: float = 1.0, ki: float = 0.0, kd: float = 0.0) -> None:
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.integral = 0.0
        self.last_error = 0.0
        self.last_time = time.time()

    def compute(self, current: float, target: float) -> float:
        now = time.time()
        dt = now - self.last_time
        error = target - current

        self.integral += error * dt
        derivative = (error - self.last_error) / dt if dt > 0 else 0.0

        output = self.kp * error + self.ki * self.integral + self.kd * derivative

        self.last_error = error
        self.last_time = now

        return output

    def reset(self) -> None:
        self.integral = 0.0
        self.last_error = 0.0
        self.last_time = time.time()


_store = FeedbackStore()
_loops: Dict[str, FeedbackLoopState] = {}


class FeedbackCollectorAction(BaseAction):
    """Collect execution feedback."""
    action_type = "automation_feedback_collector"
    display_name = "反馈收集器"
    description = "收集自动化执行的反馈数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("metric_name", "")
        value = params.get("value", 0.0)
        expected = params.get("expected")
        tags = params.get("tags", {})

        if not metric_name:
            return ActionResult(success=False, message="metric_name是必需的")

        deviation = 0.0
        if expected is not None:
            deviation = value - expected

        point = FeedbackPoint(
            metric_name=metric_name,
            value=value,
            expected=expected,
            deviation=deviation,
            tags=tags
        )
        _store.record(point)

        return ActionResult(
            success=True,
            message=f"反馈已记录: {metric_name}={value} (偏差={deviation:.4f})",
            data={"metric_name": metric_name, "value": value, "deviation": deviation}
        )


class FeedbackAnalyzerAction(BaseAction):
    """Analyze feedback patterns."""
    action_type = "automation_feedback_analyzer"
    display_name = "反馈分析器"
    description = "分析反馈数据中的模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("metric_name", "")
        since_minutes = params.get("since_minutes", 60)
        analysis_type = params.get("analysis_type", "stats")

        if not metric_name:
            return ActionResult(success=False, message="metric_name是必需的")

        if analysis_type == "stats":
            stats = _store.get_stats(metric_name, since_minutes)
            if not stats:
                return ActionResult(success=False, message=f"指标 {metric_name} 无数据")
            return ActionResult(
                success=True,
                message=f"分析完成: {metric_name}",
                data=stats
            )

        if analysis_type == "trend":
            points = _store.get_recent(metric_name, since_minutes)
            if len(points) < 2:
                return ActionResult(success=False, message="数据点不足")

            values = [p.value for p in points]
            n = len(values)
            indices = list(range(n))
            sum_xy = sum(i * v for i, v in enumerate(values))
            sum_x = sum(indices)
            sum_y = sum(values)
            sum_x2 = sum(i * i for i in indices)

            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x) if (n * sum_x2 - sum_x * sum_x) != 0 else 0

            trend = "increasing" if slope > 0.01 else "decreasing" if slope < -0.01 else "stable"

            return ActionResult(
                success=True,
                message=f"趋势分析: {trend}",
                data={"trend": trend, "slope": round(slope, 6), "samples": n}
            )

        if analysis_type == "anomalies":
            points = _store.get_recent(metric_name, since_minutes)
            if not points:
                return ActionResult(success=False, message=f"指标 {metric_name} 无数据")

            values = [p.value for p in points]
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std_dev = variance ** 0.5

            threshold = params.get("threshold", 2.0)
            anomalies = []
            for p in points:
                z_score = abs(p.value - mean) / std_dev if std_dev > 0 else 0
                if z_score > threshold:
                    anomalies.append({
                        "timestamp": p.timestamp.isoformat(),
                        "value": p.value,
                        "z_score": round(z_score, 4)
                    })

            return ActionResult(
                success=True,
                message=f"发现 {len(anomalies)} 个异常点",
                data={"anomalies": anomalies, "count": len(anomalies), "threshold": threshold}
            )

        return ActionResult(success=False, message=f"未知分析类型: {analysis_type}")


class FeedbackLoopAction(BaseAction):
    """Implement feedback control loops."""
    action_type = "automation_feedback_loop"
    display_name = "反馈控制循环"
    description = "实现反馈控制循环"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "create")
        loop_id = params.get("loop_id", "")
        kp = params.get("kp", 1.0)
        ki = params.get("ki", 0.0)
        kd = params.get("kd", 0.0)
        target = params.get("target_value", 0.0)

        global _loops

        if operation == "create":
            if not loop_id:
                return ActionResult(success=False, message="loop_id是必需的")

            _loops[loop_id] = FeedbackLoopState(
                loop_id=loop_id,
                target_value=target,
                kp=kp,
                ki=ki,
                kd=kd,
                is_active=True
            )
            return ActionResult(
                success=True,
                message=f"反馈循环 {loop_id} 已创建",
                data={"loop_id": loop_id, "kp": kp, "ki": ki, "kd": kd}
            )

        if operation == "control":
            if not loop_id or loop_id not in _loops:
                return ActionResult(success=False, message=f"循环 {loop_id} 不存在")

            current = params.get("current_value", 0.0)
            loop = _loops[loop_id]
            loop.current_value = current

            if loop.ki > 0 or loop.kd > 0:
                pid = PIDController(kp=loop.kp, ki=loop.ki, kd=loop.kd)
                output = pid.compute(current, loop.target_value)
            else:
                error = loop.target_value - current
                output = loop.kp * error

            return ActionResult(
                success=True,
                message=f"控制输出: {output:.4f}",
                data={"output": round(output, 4), "current": current, "target": loop.target_value, "error": round(loop.target_value - current, 4)}
            )

        if operation == "status":
            if not loop_id or loop_id not in _loops:
                return ActionResult(success=False, message=f"循环 {loop_id} 不存在")

            loop = _loops[loop_id]
            return ActionResult(
                success=True,
                message=f"循环 {loop_id}: active={loop.is_active}",
                data={
                    "loop_id": loop.loop_id,
                    "current": loop.current_value,
                    "target": loop.target_value,
                    "kp": loop.kp,
                    "ki": loop.ki,
                    "kd": loop.kd,
                    "is_active": loop.is_active
                }
            )

        if operation == "stop":
            if loop_id in _loops:
                _loops[loop_id].is_active = False
                return ActionResult(success=True, message=f"循环 {loop_id} 已停止")
            return ActionResult(success=False, message=f"循环 {loop_id} 不存在")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class AdaptiveTunerAction(BaseAction):
    """Adaptively tune parameters based on feedback."""
    action_type = "automation_adaptive_tuner"
    display_name = "自适应调优器"
    description = "根据反馈自适应调整参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        metric_name = params.get("metric_name", "")
        target = params.get("target", 100.0)
        tolerance = params.get("tolerance", 0.05)
        since_minutes = params.get("since_minutes", 30)

        if not metric_name:
            return ActionResult(success=False, message="metric_name是必需的")

        stats = _store.get_stats(metric_name, since_minutes)
        if not stats:
            return ActionResult(success=False, message=f"指标 {metric_name} 无数据")

        current_avg = stats.get("avg", 0)
        deviation = abs(current_avg - target) / target if target != 0 else 0

        if deviation <= tolerance:
            return ActionResult(
                success=True,
                message=f"指标在容差范围内: {deviation*100:.2f}% 偏差",
                data={
                    "status": "optimal",
                    "current": current_avg,
                    "target": target,
                    "deviation": round(deviation * 100, 2)
                }
            )

        adjustment = (target - current_avg) * 0.1

        return ActionResult(
            success=True,
            message=f"建议调整: {adjustment:.4f}",
            data={
                "status": "needs_adjustment",
                "current": round(current_avg, 4),
                "target": target,
                "deviation": round(deviation * 100, 2),
                "suggested_adjustment": round(adjustment, 4)
            }
        )
