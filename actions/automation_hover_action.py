"""Automation Hover Action Module.

Provides intelligent hover automation with configurable
dwell time, move patterns, and element targeting.

Author: RabAi Team
"""

from __future__ import annotations

import time
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HoverPattern(Enum):
    """Hover movement patterns."""
    DIRECT = "direct"
    LINEAR = "linear"
    CURVE = "curve"
    BEZIER = "bezier"
    BOUNCE = "bounce"


class DwellStrategy(Enum):
    """Dwell time calculation strategies."""
    FIXED = "fixed"
    ADAPTIVE = "adaptive"
    RANDOM = "random"


@dataclass
class HoverConfig:
    """Configuration for hover action."""
    dwell_time_ms: int = 300
    move_duration_ms: int = 200
    pattern: HoverPattern = HoverPattern.LINEAR
    dwell_strategy: DwellStrategy = DwellStrategy.FIXED
    min_dwell_ms: int = 100
    max_dwell_ms: int = 1000
    smoothing: float = 0.5


@dataclass
class HoverTarget:
    """Target for hover action."""
    x: int
    y: int
    element_id: Optional[str] = None
    selector: Optional[str] = None
    offset_x: int = 0
    offset_y: int = 0


@dataclass
class HoverStep:
    """Single step in hover movement."""
    x: int
    y: int
    timestamp: float
    pressure: float = 1.0


@dataclass
class HoverSequence:
    """Complete hover sequence."""
    steps: List[HoverStep]
    total_duration_ms: float
    pattern: HoverPattern


class PathInterpolator:
    """Interpolates paths for hover movement."""

    @staticmethod
    def linear_path(
        start: Tuple[int, int],
        end: Tuple[int, int],
        steps: int
    ) -> List[Tuple[int, int]]:
        """Generate linear path between two points."""
        xs = []
        ys = []

        for i in range(steps):
            t = i / max(steps - 1, 1)
            x = int(start[0] + (end[0] - start[0]) * t)
            y = int(start[1] + (end[1] - start[1]) * t)
            xs.append(x)
            ys.append(y)

        return list(zip(xs, ys))

    @staticmethod
    def curved_path(
        start: Tuple[int, int],
        end: Tuple[int, int],
        curvature: float,
        steps: int
    ) -> List[Tuple[int, int]]:
        """Generate curved path with control point."""
        cx = (start[0] + end[0]) / 2
        cy = (start[1] + end[1]) / 2 - curvature * (end[0] - start[0])

        xs = []
        ys = []

        for i in range(steps):
            t = i / max(steps - 1, 1)
            t1 = 1 - t

            x = int(t1 * t1 * start[0] + 2 * t1 * t * cx + t * t * end[0])
            y = int(t1 * t1 * start[1] + 2 * t1 * t * cy + t * t * end[1])

            xs.append(x)
            ys.append(y)

        return list(zip(xs, ys))

    @staticmethod
    def bezier_path(
        start: Tuple[int, int],
        control1: Tuple[int, int],
        control2: Tuple[int, int],
        end: Tuple[int, int],
        steps: int
    ) -> List[Tuple[int, int]]:
        """Generate cubic bezier path."""
        points = []

        for i in range(steps):
            t = i / max(steps - 1, 1)
            t1 = 1 - t

            x = int(
                t1 * t1 * t1 * start[0] +
                3 * t1 * t1 * t * control1[0] +
                3 * t1 * t * t * control2[0] +
                t * t * t * end[0]
            )
            y = int(
                t1 * t1 * t1 * start[1] +
                3 * t1 * t1 * t * control1[1] +
                3 * t1 * t * t * control2[1] +
                t * t * t * end[1]
            )

            points.append((x, y))

        return points

    @staticmethod
    def bounce_path(
        start: Tuple[int, int],
        end: Tuple[int, int],
        amplitude: float,
        steps: int
    ) -> List[Tuple[int, int]]:
        """Generate bouncing path."""
        xs = []
        ys = []

        for i in range(steps):
            t = i / max(steps - 1, 1)
            x = int(start[0] + (end[0] - start[0]) * t)

            bounce = amplitude * math.sin(t * math.pi * 4)
            base_y = start[1] + (end[1] - start[1]) * t
            y = int(base_y + bounce)

            xs.append(x)
            ys.append(y)

        return list(zip(xs, ys))


class HoverEngine:
    """Engine for executing hover actions."""

    def __init__(self, config: HoverConfig):
        self.config = config
        self._current_position: Tuple[int, int] = (0, 0)
        self._is_hovering: bool = False
        self._last_hover_time: float = 0.0

    def set_position(self, x: int, y: int) -> None:
        """Set current mouse position."""
        self._current_position = (x, y)

    def calculate_dwell_time(self) -> int:
        """Calculate dwell time based on strategy."""
        if self.config.dwell_strategy == DwellStrategy.FIXED:
            return self.config.dwell_time_ms

        elif self.config.dwell_strategy == DwellStrategy.ADAPTIVE:
            return self.config.dwell_time_ms

        elif self.config.dwell_strategy == DwellStrategy.RANDOM:
            import random
            return random.randint(
                self.config.min_dwell_ms,
                self.config.max_dwell_ms
            )

        return self.config.dwell_time_ms

    def generate_path(
        self,
        start: Tuple[int, int],
        end: Tuple[int, int],
        steps: int
    ) -> List[Tuple[int, int]]:
        """Generate path based on pattern."""
        if self.config.pattern == HoverPattern.DIRECT:
            return [end]

        elif self.config.pattern == HoverPattern.LINEAR:
            return PathInterpolator.linear_path(start, end, steps)

        elif self.config.pattern == HoverPattern.CURVE:
            return PathInterpolator.curved_path(start, end, 0.3, steps)

        elif self.config.pattern == HoverPattern.BEZIER:
            dx = end[0] - start[0]
            dy = end[1] - start[1]
            ctrl1 = (start[0] + dx // 3, start[1] - dy // 4)
            ctrl2 = (end[0] - dx // 3, end[1] + dy // 4)
            return PathInterpolator.bezier_path(start, ctrl1, ctrl2, end, steps)

        elif self.config.pattern == HoverPattern.BOUNCE:
            return PathInterpolator.bounce_path(start, end, 10, steps)

        return PathInterpolator.linear_path(start, end, steps)

    def execute_hover(
        self,
        target: HoverTarget,
        callback: Optional[Callable] = None
    ) -> HoverSequence:
        """Execute hover action to target."""
        target_x = target.x + target.offset_x
        target_y = target.y + target.offset_y

        start = self._current_position
        end = (target_x, target_y)

        steps_count = max(2, self.config.move_duration_ms // 10)

        path = self.generate_path(start, end, steps_count)

        steps = []
        start_time = time.time()

        for i, (x, y) in enumerate(path):
            step = HoverStep(
                x=x,
                y=y,
                timestamp=time.time() - start_time,
                pressure=1.0
            )
            steps.append(step)
            self._current_position = (x, y)

            if callback:
                callback(x, y)

        dwell_ms = self.calculate_dwell_time()
        time.sleep(dwell_ms / 1000.0)

        self._is_hovering = True
        self._last_hover_time = time.time()

        total_duration = (time.time() - start_time) * 1000

        return HoverSequence(
            steps=steps,
            total_duration_ms=total_duration,
            pattern=self.config.pattern
        )

    def move_away(self) -> None:
        """Move away from current hover position."""
        x, y = self._current_position
        self._current_position = (x + 50, y + 50)
        self._is_hovering = False

    def is_hovering(self) -> bool:
        """Check if currently hovering."""
        return self._is_hovering


class AutomationHoverAction(BaseAction):
    """Action for hover automation operations."""

    def __init__(self):
        super().__init__("automation_hover")
        self._config = HoverConfig()
        self._engine = HoverEngine(self._config)
        self._hover_history: List[Dict[str, Any]] = []

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute hover action."""
        try:
            operation = params.get("operation", "hover")

            if operation == "hover":
                return self._hover(params)
            elif operation == "configure":
                return self._configure(params)
            elif operation == "move_away":
                return self._move_away(params)
            elif operation == "get_position":
                return self._get_position(params)
            elif operation == "get_history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _hover(self, params: Dict[str, Any]) -> ActionResult:
        """Execute hover to target."""
        x = params.get("x", 0)
        y = params.get("y", 0)
        element_id = params.get("element_id")
        selector = params.get("selector")
        offset_x = params.get("offset_x", 0)
        offset_y = params.get("offset_y", 0)

        target = HoverTarget(
            x=x,
            y=y,
            element_id=element_id,
            selector=selector,
            offset_x=offset_x,
            offset_y=offset_y
        )

        sequence = self._engine.execute_hover(target)

        self._hover_history.append({
            "target": (x, y),
            "pattern": sequence.pattern.value,
            "duration_ms": sequence.total_duration_ms,
            "step_count": len(sequence.steps),
            "timestamp": time.time()
        })

        return ActionResult(
            success=True,
            data={
                "x": x,
                "y": y,
                "pattern": sequence.pattern.value,
                "duration_ms": sequence.total_duration_ms,
                "steps": len(sequence.steps)
            }
        )

    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure hover behavior."""
        self._config.dwell_time_ms = params.get("dwell_time_ms", 300)
        self._config.move_duration_ms = params.get("move_duration_ms", 200)
        self._config.pattern = HoverPattern(params.get("pattern", "linear"))
        self._config.dwell_strategy = DwellStrategy(
            params.get("dwell_strategy", "fixed")
        )
        self._config.min_dwell_ms = params.get("min_dwell_ms", 100)
        self._config.max_dwell_ms = params.get("max_dwell_ms", 1000)
        self._config.smoothing = params.get("smoothing", 0.5)

        self._engine = HoverEngine(self._config)

        return ActionResult(
            success=True,
            message="Hover configuration updated"
        )

    def _move_away(self, params: Dict[str, Any]) -> ActionResult:
        """Move away from current position."""
        self._engine.move_away()
        x, y = self._engine._current_position

        return ActionResult(
            success=True,
            data={"x": x, "y": y}
        )

    def _get_position(self, params: Dict[str, Any]) -> ActionResult:
        """Get current hover position."""
        x, y = self._engine._current_position

        return ActionResult(
            success=True,
            data={
                "x": x,
                "y": y,
                "is_hovering": self._engine.is_hovering()
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get hover history."""
        limit = params.get("limit", 100)
        history = self._hover_history[-limit:]

        return ActionResult(
            success=True,
            data={
                "history": history,
                "total": len(self._hover_history)
            }
        )
