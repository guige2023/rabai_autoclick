"""
Data Window Action - Windowing functions for data streams.

This module provides windowing capabilities for time-series
and streaming data including tumbling, sliding, and session windows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class WindowType(Enum):
    """Types of windows."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    """A data window."""
    window_id: str
    start_time: float
    end_time: float
    data: list[Any] = field(default_factory=list)


@dataclass
class WindowResult:
    """Result of windowing operation."""
    windows: list[Window]
    window_count: int
    total_records: int


class WindowProcessor:
    """Processes data into windows."""
    
    def __init__(self) -> None:
        pass
    
    def tumbling_window(
        self,
        data: list[dict[str, Any]],
        time_field: str,
        window_size: float,
    ) -> list[Window]:
        """Create tumbling (fixed-size non-overlapping) windows."""
        windows: list[Window] = []
        
        if not data:
            return windows
        
        sorted_data = sorted(data, key=lambda x: x.get(time_field, 0))
        
        if not sorted_data:
            return windows
        
        start_time = sorted_data[0].get(time_field, 0)
        
        current_window: list[Any] = []
        current_start = start_time
        
        for record in sorted_data:
            record_time = record.get(time_field, 0)
            
            if record_time >= current_start + window_size:
                if current_window:
                    windows.append(Window(
                        window_id=f"w_{len(windows)}",
                        start_time=current_start,
                        end_time=current_start + window_size,
                        data=current_window,
                    ))
                current_window = [record]
                current_start = record_time
            else:
                current_window.append(record)
        
        if current_window:
            windows.append(Window(
                window_id=f"w_{len(windows)}",
                start_time=current_start,
                end_time=current_start + window_size,
                data=current_window,
            ))
        
        return windows
    
    def sliding_window(
        self,
        data: list[dict[str, Any]],
        time_field: str,
        window_size: float,
        slide_interval: float,
    ) -> list[Window]:
        """Create sliding (overlapping) windows."""
        windows: list[Window] = []
        
        if not data:
            return windows
        
        sorted_data = sorted(data, key=lambda x: x.get(time_field, 0))
        
        if not sorted_data:
            return windows
        
        start_time = sorted_data[0].get(time_field, 0)
        end_time = sorted_data[-1].get(time_field, 0)
        
        window_start = start_time
        window_id = 0
        
        while window_start + window_size <= end_time:
            window_end = window_start + window_size
            window_data = [
                r for r in sorted_data
                if window_start <= r.get(time_field, 0) < window_end
            ]
            
            windows.append(Window(
                window_id=f"w_{window_id}",
                start_time=window_start,
                end_time=window_end,
                data=window_data,
            ))
            
            window_start += slide_interval
            window_id += 1
        
        return windows


class DataWindowAction:
    """Data window action for automation workflows."""
    
    def __init__(self) -> None:
        self.processor = WindowProcessor()
    
    async def window(
        self,
        data: list[dict[str, Any]],
        time_field: str,
        window_size: float,
        window_type: str = "tumbling",
        slide_interval: float | None = None,
    ) -> WindowResult:
        """Apply windowing to data."""
        if window_type == "tumbling":
            windows = self.processor.tumbling_window(data, time_field, window_size)
        elif window_type == "sliding":
            windows = self.processor.sliding_window(
                data, time_field, window_size, slide_interval or window_size / 2
            )
        else:
            windows = self.processor.tumbling_window(data, time_field, window_size)
        
        return WindowResult(
            windows=windows,
            window_count=len(windows),
            total_records=len(data),
        )


__all__ = ["WindowType", "Window", "WindowResult", "WindowProcessor", "DataWindowAction"]
