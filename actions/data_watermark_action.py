"""Data Watermark Action Module.

Implements watermarking for data streams with configurable thresholds,
alerting on threshold breaches, and watermark progression tracking.
"""

import time
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class WatermarkThreshold:
    name: str
    value: float
    severity: str = "warning"
    callback: Optional[Callable] = None


@dataclass
class WatermarkEvent:
    timestamp: float
    watermark_value: float
    threshold_name: str
    breach: bool
    data_hash: str


class DataWatermarkAction:
    """Watermark tracking for data streams with threshold alerting."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._thresholds: List[WatermarkThreshold] = []
        self._current_watermark: float = 0.0
        self._low_watermark: float = float("inf")
        self._high_watermark: float = float("-inf")
        self._events: deque = deque(maxlen=10000)
        self._stats = {
            "total_updates": 0,
            "breach_count": 0,
            "last_update": None,
            "last_breach": None,
        }
        self._listeners: Dict[str, List[Callable]] = {
            "watermark_update": [],
            "breach": [],
        }

    def add_threshold(
        self,
        name: str,
        value: float,
        severity: str = "warning",
        callback: Optional[Callable[[WatermarkEvent], None]] = None,
    ) -> None:
        self._thresholds.append(
            WatermarkThreshold(
                name=name,
                value=value,
                severity=severity,
                callback=callback,
            )
        )

    def remove_threshold(self, name: str) -> bool:
        for i, t in enumerate(self._thresholds):
            if t.name == name:
                self._thresholds.pop(i)
                return True
        return False

    def update(
        self,
        value: float,
        data: Any = None,
        emit_events: bool = True,
    ) -> List[WatermarkEvent]:
        self._current_watermark = value
        self._low_watermark = min(self._low_watermark, value)
        self._high_watermark = max(self._high_watermark, value)
        self._stats["total_updates"] += 1
        self._stats["last_update"] = time.time()
        events = []
        if emit_events:
            events = self._check_thresholds(value, data)
        return events

    def _check_thresholds(
        self,
        value: float,
        data: Any,
    ) -> List[WatermarkEvent]:
        events = []
        data_str = str(data) if data else ""
        data_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        for threshold in self._thresholds:
            breach = False
            if threshold.severity in ("critical", "error"):
                breach = value >= threshold.value
            elif threshold.severity == "warning":
                breach = value >= threshold.value * 0.9
            elif threshold.severity == "info":
                breach = value >= threshold.value * 0.75
            event = WatermarkEvent(
                timestamp=time.time(),
                watermark_value=value,
                threshold_name=threshold.name,
                breach=breach,
                data_hash=data_hash,
            )
            self._events.append(event)
            if breach:
                self._stats["breach_count"] += 1
                self._stats["last_breach"] = time.time()
                if threshold.callback:
                    try:
                        threshold.callback(event)
                    except Exception as e:
                        logger.error(f"Threshold callback failed: {e}")
                self._notify("breach", event)
            events.append(event)
        return events

    def get_watermark(self) -> float:
        return self._current_watermark

    def get_range(self) -> Dict[str, float]:
        return {
            "low": self._low_watermark,
            "high": self._high_watermark,
            "current": self._current_watermark,
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "current_watermark": self._current_watermark,
            "low_watermark": self._low_watermark,
            "high_watermark": self._high_watermark,
            "threshold_count": len(self._thresholds),
            "event_count": len(self._events),
        }

    def get_recent_events(
        self,
        count: int = 100,
        breach_only: bool = False,
    ) -> List[Dict[str, Any]]:
        events = list(self._events)
        if breach_only:
            events = [e for e in events if e.breach]
        return [
            {
                "timestamp": e.timestamp,
                "watermark_value": e.watermark_value,
                "threshold_name": e.threshold_name,
                "breach": e.breach,
                "data_hash": e.data_hash,
            }
            for e in events[-count:]
        ]

    def reset(self) -> None:
        self._current_watermark = 0.0
        self._low_watermark = float("inf")
        self._high_watermark = float("-inf")
        self._events.clear()
        self._stats = {
            "total_updates": 0,
            "breach_count": 0,
            "last_update": None,
            "last_breach": None,
        }

    def add_listener(self, event: str, callback: Callable) -> None:
        if event in self._listeners:
            self._listeners[event].append(callback)

    def _notify(self, event: str, data: Any) -> None:
        for cb in self._listeners.get(event, []):
            try:
                cb(data)
            except Exception as e:
                logger.error(f"Watermark listener error: {e}")
