"""Data watermark action module for RabAI AutoClick.

Provides watermark tracking for data streams:
- WatermarkTracker: Track watermarks in data streams
- EventTimeWatermark: Event-time watermark processing
- ProcessingTimeWatermark: Processing-time watermark
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WatermarkStrategy(Enum):
    """Watermark strategies."""
    EVENT_TIME = "event_time"
    PROCESSING_TIME = "processing_time"
    PERIODIC = "periodic"
    PER_PARTITION = "per_partition"


@dataclass
class WatermarkConfig:
    """Configuration for watermarks."""
    strategy: WatermarkStrategy = WatermarkStrategy.EVENT_TIME
    watermark_delay: float = 5.0
    max_out_of_order: float = 60.0
    update_interval: float = 1.0


class WatermarkTracker:
    """Track watermarks in data streams."""
    
    def __init__(self, name: str, config: WatermarkConfig):
        self.name = name
        self.config = config
        self._watermarks: Dict[str, float] = defaultdict(lambda: 0.0)
        self._pending: Dict[str, List[Tuple[float, Any]]] = defaultdict(list)
        self._lock = threading.RLock()
        self._stats = {"total_events": 0, "watermarks_advanced": 0, "events_processed": 0}
    
    def update_watermark(self, partition: str, timestamp: float) -> bool:
        """Update watermark for partition."""
        with self._lock:
            old_watermark = self._watermarks[partition]
            new_watermark = timestamp - self.config.watermark_delay
            
            if new_watermark > old_watermark:
                self._watermarks[partition] = new_watermark
                self._stats["watermarks_advanced"] += 1
                return True
            return False
    
    def get_watermark(self, partition: str = "default") -> float:
        """Get current watermark for partition."""
        with self._lock:
            return self._watermarks.get(partition, 0.0)
    
    def get_min_watermark(self) -> float:
        """Get minimum watermark across all partitions."""
        with self._lock:
            if not self._watermarks:
                return 0.0
            return min(self._watermarks.values())
    
    def add_event(self, partition: str, timestamp: float, event: Any):
        """Add event to pending buffer."""
        with self._lock:
            self._pending[partition].append((timestamp, event))
            self._stats["total_events"] += 1
    
    def get_ready_events(self, partition: str = "default") -> List[Any]:
        """Get events that are ready based on watermark."""
        with self._lock:
            watermark = self._watermarks.get(partition, 0.0)
            pending = self._pending.get(partition, [])
            
            ready = []
            not_ready = []
            
            for ts, event in pending:
                if ts <= watermark:
                    ready.append(event)
                    self._stats["events_processed"] += 1
                else:
                    not_ready.append((ts, event))
            
            self._pending[partition] = not_ready
            return ready
    
    def get_stats(self) -> Dict[str, Any]:
        """Get watermark statistics."""
        with self._lock:
            return {
                "name": self.name,
                "tracked_partitions": len(self._watermarks),
                "min_watermark": self.get_min_watermark(),
                "pending_events": sum(len(v) for v in self._pending.values()),
                **{k: v for k, v in self._stats.items()},
            }


class DataWatermarkAction(BaseAction):
    """Data watermark action."""
    action_type = "data_watermark"
    display_name = "数据水印"
    description = "数据流时间水印跟踪"
    
    def __init__(self):
        super().__init__()
        self._trackers: Dict[str, WatermarkTracker] = {}
        self._lock = threading.Lock()
    
    def _get_tracker(self, name: str, config: WatermarkConfig) -> WatermarkTracker:
        """Get or create watermark tracker."""
        with self._lock:
            if name not in self._trackers:
                self._trackers[name] = WatermarkTracker(name, config)
            return self._trackers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute watermark operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "update")
            
            config = WatermarkConfig(
                strategy=WatermarkStrategy[params.get("strategy", "event_time").upper()],
                watermark_delay=params.get("watermark_delay", 5.0),
            )
            
            tracker = self._get_tracker(name, config)
            
            if command == "update":
                partition = params.get("partition", "default")
                timestamp = params.get("timestamp", time.time())
                advanced = tracker.update_watermark(partition, timestamp)
                return ActionResult(success=True, data={"advanced": advanced, "watermark": tracker.get_watermark(partition)})
            
            elif command == "get":
                partition = params.get("partition", "default")
                watermark = tracker.get_watermark(partition)
                return ActionResult(success=True, data={"watermark": watermark})
            
            elif command == "min":
                min_wm = tracker.get_min_watermark()
                return ActionResult(success=True, data={"min_watermark": min_wm})
            
            elif command == "add_event":
                partition = params.get("partition", "default")
                timestamp = params.get("timestamp", time.time())
                event = params.get("event")
                tracker.add_event(partition, timestamp, event)
                return ActionResult(success=True)
            
            elif command == "get_ready":
                partition = params.get("partition", "default")
                ready = tracker.get_ready_events(partition)
                return ActionResult(success=True, data={"events": ready, "count": len(ready)})
            
            elif command == "stats":
                stats = tracker.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataWatermarkAction error: {str(e)}")
