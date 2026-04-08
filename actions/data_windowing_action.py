"""
Data Windowing Action Module

Provides time-windowed data processing, window aggregation, and late data handling.
"""
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import asyncio


class WindowType(Enum):
    """Window types."""
    TUMBLING = "tumbling"
    HOPPING = "hopping"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


@dataclass
class WindowConfig:
    """Configuration for windowing."""
    window_type: WindowType
    size_seconds: float
    slide_seconds: Optional[float] = None
    max_late_seconds: float = 0.0
    watermark_field: str = "timestamp"
    allowed_lateness: float = 0.0


@dataclass
class Window:
    """A time window."""
    window_id: str
    start_time: datetime
    end_time: datetime
    window_type: WindowType
    is_final: bool = False


@dataclass
class WindowResult:
    """Result of window processing."""
    window: Window
    data: list[dict]
    aggregations: dict[str, Any]
    late_data: list[dict] = field(default_factory=list)
    dropped_count: int = 0


T = TypeVar('T')


class WindowAssigner:
    """Assigns elements to windows."""
    
    def __init__(self, config: WindowConfig):
        self.config = config
    
    def assign_windows(self, element: dict[str, Any]) -> list[Window]:
        """Assign one or more windows to an element."""
        timestamp = self._get_timestamp(element)
        
        if self.config.window_type == WindowType.TUMBLING:
            return [self._tumbling_window(timestamp)]
        elif self.config.window_type == WindowType.HOPPING:
            return self._hopping_windows(timestamp)
        elif self.config.window_type == WindowType.SLIDING:
            return self._sliding_windows(timestamp)
        elif self.config.window_type == WindowType.SESSION:
            return [self._session_window(timestamp)]
        else:
            return [self._global_window()]
    
    def _get_timestamp(self, element: dict[str, Any]) -> datetime:
        """Extract timestamp from element."""
        field_name = self.config.watermark_field
        value = element.get(field_name)
        
        if isinstance(value, datetime):
            return value
        elif isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        elif isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except:
                return datetime.now()
        
        return datetime.now()
    
    def _tumbling_window(self, timestamp: datetime) -> Window:
        """Create a tumbling (fixed) window."""
        window_size = timedelta(seconds=self.config.size_seconds)
        
        # Align to window boundaries
        window_start = timestamp.replace(
            microsecond=0, second=0, minute=0, hour=0
        )
        
        # Adjust based on window size
        if self.config.size_seconds >= 86400:  # Day
            pass  # Already aligned
        elif self.config.size_seconds >= 3600:  # Hour
            window_start = window_start.replace(minute=0)
        elif self.config.size_seconds >= 60:  # Minute
            window_start = window_start.replace(second=0)
        
        window_end = window_start + window_size
        
        window_id = f"tumbling_{window_start.isoformat()}"
        
        return Window(
            window_id=window_id,
            start_time=window_start,
            end_time=window_end,
            window_type=WindowType.TUMBLING
        )
    
    def _hopping_windows(self, timestamp: datetime) -> list[Window]:
        """Create hopping (fixed-size, sliding) windows."""
        windows = []
        slide = self.config.slide_seconds or self.config.size_seconds
        
        window_size = timedelta(seconds=self.config.size_seconds)
        slide_size = timedelta(seconds=slide)
        
        # Calculate window start positions
        base = timestamp.replace(microsecond=0, second=0, minute=0, hour=0)
        
        num_windows = int(self.config.size_seconds / slide)
        
        for i in range(num_windows):
            offset = timedelta(seconds=slide * i)
            window_start = base + offset
            window_end = window_start + window_size
            
            if timestamp <= window_end:
                window_id = f"hopping_{i}_{window_start.isoformat()}"
                windows.append(Window(
                    window_id=window_id,
                    start_time=window_start,
                    end_time=window_end,
                    window_type=WindowType.HOPPING
                ))
        
        return windows
    
    def _sliding_windows(self, timestamp: datetime) -> list[Window]:
        """Create sliding windows."""
        slide = self.config.slide_seconds or self.config.size_seconds
        window_size = timedelta(seconds=self.config.size_seconds)
        slide_size = timedelta(seconds=slide)
        
        windows = []
        base = timestamp - window_size
        
        # Create windows centered around timestamp
        for i in range(3):
            offset = timedelta(seconds=slide * i)
            window_start = base + offset
            window_end = window_start + window_size
            
            if window_start <= timestamp <= window_end:
                window_id = f"sliding_{i}_{window_start.isoformat()}"
                windows.append(Window(
                    window_id=window_id,
                    start_time=window_start,
                    end_time=window_end,
                    window_type=WindowType.SLIDING
                ))
        
        return windows
    
    def _session_window(self, timestamp: datetime) -> Window:
        """Create a session window (simplified)."""
        gap = timedelta(seconds=self.config.slide_seconds or 30)
        window_end = timestamp + gap
        
        window_id = f"session_{timestamp.isoformat()}"
        
        return Window(
            window_id=window_id,
            start_time=timestamp,
            end_time=window_end,
            window_type=WindowType.SESSION
        )
    
    def _global_window(self) -> Window:
        """Create a global window containing all elements."""
        return Window(
            window_id="global",
            start_time=datetime.min,
            end_time=datetime.max,
            window_type=WindowType.GLOBAL
        )


class DataWindowingAction:
    """Main data windowing action handler."""
    
    def __init__(self, default_config: Optional[WindowConfig] = None):
        self.default_config = default_config or WindowConfig(
            window_type=WindowType.TUMBLING,
            size_seconds=60.0
        )
        self._window_buffers: dict[str, list[dict]] = defaultdict(list)
        self._window_state: dict[str, dict] = {}
        self._late_data_buffers: dict[str, list[dict]] = defaultdict(list)
        self._stats: dict[str, Any] = defaultdict(int)
    
    async def process_element(
        self,
        element: dict[str, Any],
        config: Optional[WindowConfig] = None,
        aggregators: Optional[dict[str, Callable[[list], Any]]] = None
    ) -> Optional[WindowResult]:
        """
        Process an element through windowing.
        
        Args:
            element: Element to process
            config: Window configuration
            aggregators: Dictionary of aggregation functions
            
        Returns:
            WindowResult when window completes, None otherwise
        """
        cfg = config or self.default_config
        assigner = WindowAssigner(cfg)
        
        windows = assigner.assign_windows(element)
        
        results = []
        
        for window in windows:
            window_key = window.window_id
            
            # Add element to window buffer
            self._window_buffers[window_key].append(element)
            
            # Initialize window state
            if window_key not in self._window_state:
                self._window_state[window_key] = {
                    "window": window,
                    "count": 0,
                    "first_time": element.get(cfg.watermark_field),
                    "last_time": element.get(cfg.watermark_field)
                }
            
            state = self._window_state[window_key]
            state["count"] += 1
            
            # Update timestamps
            elem_time = element.get(cfg.watermark_field)
            if elem_time and elem_time > state["last_time"]:
                state["last_time"] = elem_time
            
            self._stats["elements_processed"] += 1
            
            # Check if window should emit
            should_emit = await self._should_emit(window, cfg)
            
            if should_emit:
                result = await self._emit_window(
                    window_key,
                    aggregators
                )
                results.append(result)
        
        return results[0] if len(results) == 1 else results if results else None
    
    async def _should_emit(self, window: Window, config: WindowConfig) -> bool:
        """Determine if window should emit results."""
        now = datetime.now()
        
        if config.window_type == WindowType.GLOBAL:
            return False  # Global window never auto-emits
        
        # Emit if window end time has passed
        if window.end_time <= now:
            return True
        
        return False
    
    async def _emit_window(
        self,
        window_key: str,
        aggregators: Optional[dict[str, Callable[[list], Any]]]
    ) -> WindowResult:
        """Emit results for a completed window."""
        window_data = self._window_buffers.pop(window_key, [])
        state = self._window_state.pop(window_key, {})
        
        window = state.get("window")
        if not window:
            window = Window(
                window_id=window_key,
                start_time=datetime.min,
                end_time=datetime.max,
                window_type=WindowType.TUMBLING
            )
        
        # Apply aggregations
        aggregations = {}
        if aggregators:
            for agg_name, agg_func in aggregators.items():
                try:
                    aggregations[agg_name] = agg_func(window_data)
                except Exception:
                    aggregations[agg_name] = None
        
        # Default aggregations
        if window_data:
            aggregations["count"] = len(window_data)
            
            # Sum numeric fields
            numeric_fields = set()
            for record in window_data:
                for key, value in record.items():
                    if isinstance(value, (int, float)):
                        numeric_fields.add(key)
            
            for field in numeric_fields:
                values = [r.get(field, 0) for r in window_data if isinstance(r.get(field), (int, float))]
                aggregations[f"{field}_sum"] = sum(values)
                aggregations[f"{field}_avg"] = sum(values) / len(values) if values else 0
        
        # Handle late data
        late_data = self._late_data_buffers.get(window_key, [])
        
        result = WindowResult(
            window=window,
            data=window_data,
            aggregations=aggregations,
            late_data=late_data,
            dropped_count=len(late_data)
        )
        
        self._stats["windows_emitted"] += 1
        self._stats["elements_emitted"] += len(window_data)
        
        # Clear late data buffer
        if window_key in self._late_data_buffers:
            del self._late_data_buffers[window_key]
        
        return result
    
    async def process_late_element(
        self,
        element: dict[str, Any],
        window_id: str,
        config: Optional[WindowConfig] = None
    ) -> bool:
        """
        Process a late element outside of normal window.
        
        Returns True if element was added to late data buffer.
        """
        cfg = config or self.default_config
        
        if cfg.max_late_seconds > 0:
            self._late_data_buffers[window_id].append(element)
            self._stats["late_elements"] += 1
            return True
        
        self._stats["dropped_late_elements"] += 1
        return False
    
    async def get_window_state(self, window_id: str) -> Optional[dict[str, Any]]:
        """Get current state of a window."""
        if window_id in self._window_state:
            state = self._window_state[window_id]
            return {
                "window_id": window_id,
                "window": {
                    "start_time": state["window"].start_time.isoformat(),
                    "end_time": state["window"].end_time.isoformat(),
                    "type": state["window"].window_type.value
                },
                "buffer_size": len(self._window_buffers.get(window_id, [])),
                "count": state["count"]
            }
        return None
    
    async def trigger_early_windows(self) -> list[WindowResult]:
        """Trigger emission of all currently complete windows."""
        results = []
        now = datetime.now()
        windows_to_emit = []
        
        for window_key, state in list(self._window_state.items()):
            window = state["window"]
            if window.end_time <= now:
                windows_to_emit.append(window_key)
        
        for window_key in windows_to_emit:
            result = await self._emit_window(window_key, None)
            results.append(result)
        
        return results
    
    def get_stats(self) -> dict[str, Any]:
        """Get windowing statistics."""
        return {
            **dict(self._stats),
            "active_windows": len(self._window_state),
            "buffered_elements": sum(len(b) for b in self._window_buffers.values()),
            "late_data_buffers": len(self._late_data_buffers)
        }
