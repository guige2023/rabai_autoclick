"""
Performance Profiling Utilities for UI Automation.

This module provides utilities for profiling automation performance,
measuring execution time, and identifying bottlenecks.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable, Any
from collections import defaultdict
import statistics


class ProfileEventType(Enum):
    """Types of profiling events."""
    START = auto()
    END = auto()
    MARK = auto()
    COUNTER = auto()


@dataclass
class ProfileEvent:
    """
    A profiling event.
    
    Attributes:
        event_id: Unique event identifier
        event_type: Type of event
        name: Event name
        timestamp: Event timestamp
        duration_ms: Event duration (for START/END pairs)
        metadata: Additional event data
    """
    event_id: str
    event_type: ProfileEventType
    name: str
    timestamp: float
    duration_ms: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProfileSection:
    """
    A profiled code section with statistics.
    
    Attributes:
        name: Section name
        call_count: Number of times section was executed
        total_duration_ms: Total time spent in section
        min_duration_ms: Minimum execution time
        max_duration_ms: Maximum execution time
        mean_duration_ms: Mean execution time
        std_dev_ms: Standard deviation of execution times
    """
    name: str
    call_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    mean_duration_ms: float = 0.0
    std_dev_ms: float = 0.0
    _durations: list[float] = field(default_factory=list, repr=False)
    
    def add_sample(self, duration_ms: float) -> None:
        """Add a duration sample."""
        self._durations.append(duration_ms)
        self.call_count += 1
        self.total_duration_ms += duration_ms
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)
        
        # Update statistics
        self.mean_duration_ms = self.total_duration_ms / self.call_count
        if len(self._durations) > 1:
            self.std_dev_ms = statistics.stdev(self._durations)
    
    @property
    def median_duration_ms(self) -> float:
        """Get median execution time."""
        if not self._durations:
            return 0.0
        return statistics.median(self._durations)
    
    @property
    def throughput(self) -> float:
        """Get throughput (calls per second)."""
        if self.total_duration_ms > 0:
            return self.call_count / (self.total_duration_ms / 1000)
        return 0.0


class ProfilerContext:
    """Context manager for profiling code blocks."""
    
    def __init__(self, profiler: 'Profiler', name: str):
        self.profiler = profiler
        self.name = name
        self.start_time: Optional[float] = None
    
    def __enter__(self) -> 'ProfilerContext':
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        self.profiler.record_duration(self.name, duration_ms)


class Profiler:
    """
    Performance profiler for automation workflows.
    
    Example:
        profiler = Profiler()
        
        with profiler.profile("login"):
            perform_login()
        
        results = profiler.get_results()
    """
    
    def __init__(self):
        self._sections: dict[str, ProfileSection] = {}
        self._events: list[ProfileEvent] = []
        self._enabled = True
        self._start_time: Optional[float] = None
        self._markers: dict[str, float] = {}
    
    def profile(self, name: str) -> ProfilerContext:
        """Create a profiling context."""
        return ProfilerContext(self, name)
    
    def record_duration(self, name: str, duration_ms: float) -> None:
        """
        Record a duration for a named section.
        
        Args:
            name: Section name
            duration_ms: Duration in milliseconds
        """
        if not self._enabled:
            return
        
        if name not in self._sections:
            self._sections[name] = ProfileSection(name=name)
        
        self._sections[name].add_sample(duration_ms)
    
    def start(self) -> None:
        """Start the profiler."""
        self._start_time = time.time()
        self._markers.clear()
    
    def stop(self) -> float:
        """
        Stop the profiler.
        
        Returns:
            Total elapsed time in milliseconds
        """
        if self._start_time is None:
            return 0.0
        
        elapsed_ms = (time.time() - self._start_time) * 1000
        self._start_time = None
        return elapsed_ms
    
    def mark(self, name: str) -> float:
        """
        Place a named mark.
        
        Args:
            name: Marker name
            
        Returns:
            Time since profiler start in milliseconds
        """
        if self._start_time is None:
            self._start_time = time.time()
        
        elapsed_ms = (time.time() - self._start_time) * 1000
        self._markers[name] = elapsed_ms
        
        self._events.append(ProfileEvent(
            event_id=str(uuid.uuid4()),
            event_type=ProfileEventType.MARK,
            name=name,
            timestamp=time.time(),
            duration_ms=elapsed_ms
        ))
        
        return elapsed_ms
    
    def get_elapsed_ms(self, marker_name: Optional[str] = None) -> float:
        """
        Get elapsed time.
        
        Args:
            marker_name: Optional marker name to get time since
            
        Returns:
            Elapsed time in milliseconds
        """
        if self._start_time is None:
            return 0.0
        
        if marker_name and marker_name in self._markers:
            base_time = self._markers[marker_name]
        else:
            base_time = self._start_time
        
        return (time.time() - base_time) * 1000
    
    def get_results(self) -> dict[str, ProfileSection]:
        """Get profiling results."""
        return dict(self._sections)
    
    def get_section(self, name: str) -> Optional[ProfileSection]:
        """Get a specific section's profile."""
        return self._sections.get(name)
    
    def get_events(self) -> list[ProfileEvent]:
        """Get all profiling events."""
        return list(self._events)
    
    def get_total_duration_ms(self) -> float:
        """Get total duration across all sections."""
        return sum(s.total_duration_ms for s in self._sections.values())
    
    def get_slowest_sections(self, limit: int = 10) -> list[tuple[str, ProfileSection]]:
        """
        Get the slowest sections.
        
        Args:
            limit: Maximum number of sections to return
            
        Returns:
            List of (name, section) tuples sorted by total duration
        """
        sorted_sections = sorted(
            self._sections.items(),
            key=lambda x: x[1].total_duration_ms,
            reverse=True
        )
        return sorted_sections[:limit]
    
    def reset(self) -> None:
        """Reset all profiling data."""
        self._sections.clear()
        self._events.clear()
        self._markers.clear()
        self._start_time = None
    
    def enable(self) -> None:
        """Enable profiling."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable profiling."""
        self._enabled = False
    
    def generate_report(self) -> str:
        """
        Generate a text report of profiling results.
        
        Returns:
            Formatted report string
        """
        lines = ["Performance Profile Report", "=" * 50]
        
        if not self._sections:
            lines.append("No profiling data collected.")
            return "\n".join(lines)
        
        lines.append(f"\nTotal sections: {len(self._sections)}")
        lines.append(f"Total duration: {self.get_total_duration_ms():.2f}ms\n")
        
        lines.append("Slowest sections:")
        lines.append("-" * 50)
        
        for name, section in self.get_slowest_sections():
            lines.append(f"\n{name}:")
            lines.append(f"  Calls: {section.call_count}")
            lines.append(f"  Total: {section.total_duration_ms:.2f}ms")
            lines.append(f"  Mean: {section.mean_duration_ms:.2f}ms")
            lines.append(f"  Min: {section.min_duration_ms:.2f}ms")
            lines.append(f"  Max: {section.max_duration_ms:.2f}ms")
            if section.call_count > 1:
                lines.append(f"  StdDev: {section.std_dev_ms:.2f}ms")
        
        return "\n".join(lines)


# Global default profiler
_default_profiler: Optional[Profiler] = None


def get_profiler() -> Profiler:
    """Get the global default profiler."""
    global _default_profiler
    if _default_profiler is None:
        _default_profiler = Profiler()
    return _default_profiler


def profile(name: str) -> ProfilerContext:
    """Convenience function for global profiler."""
    return get_profiler().profile(name)
