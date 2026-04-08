"""Flow Control Action Module.

Provides flow control with backpressure handling,
flow meters, and flow shaping.
"""

import time
import threading
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FlowMeter:
    """Flow meter for rate tracking."""
    rate: float
    window_seconds: float
    hits: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


class FlowControlManager:
    """Manages flow control."""

    def __init__(self):
        self._meters: Dict[str, FlowMeter] = {}
        self._backpressure: Dict[str, bool] = {}

    def create_meter(
        self,
        name: str,
        rate: float,
        window_seconds: float = 60.0
    ) -> None:
        """Create a flow meter."""
        self._meters[name] = FlowMeter(
            rate=rate,
            window_seconds=window_seconds
        )

    def record(self, name: str, count: int = 1) -> bool:
        """Record flow and check backpressure."""
        meter = self._meters.get(name)
        if not meter:
            return True

        with meter.lock:
            now = time.time()
            cutoff = now - meter.window_seconds
            meter.hits = [t for t in meter.hits if t > cutoff]

            current_rate = len(meter.hits) / meter.window_seconds

            if current_rate >= meter.rate:
                self._backpressure[name] = True
                return False

            for _ in range(count):
                meter.hits.append(now)

            self._backpressure[name] = False
            return True

    def get_rate(self, name: str) -> float:
        """Get current rate."""
        meter = self._meters.get(name)
        if not meter:
            return 0.0

        with meter.lock:
            now = time.time()
            cutoff = now - meter.window_seconds
            meter.hits = [t for t in meter.hits if t > cutoff]
            return len(meter.hits) / meter.window_seconds

    def is_backpressure(self, name: str) -> bool:
        """Check if backpressure is active."""
        return self._backpressure.get(name, False)


class FlowControlAction(BaseAction):
    """Action for flow control operations."""

    def __init__(self):
        super().__init__("flow_control")
        self._manager = FlowControlManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute flow control action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "record":
                return self._record(params)
            elif operation == "rate":
                return self._rate(params)
            elif operation == "backpressure":
                return self._backpressure(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create meter."""
        self._manager.create_meter(
            name=params.get("name", ""),
            rate=params.get("rate", 100),
            window_seconds=params.get("window_seconds", 60)
        )
        return ActionResult(success=True)

    def _record(self, params: Dict) -> ActionResult:
        """Record flow."""
        allowed = self._manager.record(
            params.get("name", ""),
            params.get("count", 1)
        )
        return ActionResult(success=True, data={"allowed": allowed})

    def _rate(self, params: Dict) -> ActionResult:
        """Get current rate."""
        rate = self._manager.get_rate(params.get("name", ""))
        return ActionResult(success=True, data={"rate": rate})

    def _backpressure(self, params: Dict) -> ActionResult:
        """Check backpressure."""
        active = self._manager.is_backpressure(params.get("name", ""))
        return ActionResult(success=True, data={"backpressure": active})
