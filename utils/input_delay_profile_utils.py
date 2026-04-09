"""
Input delay profiling utilities for performance analysis.

Track and analyze input latency patterns in UI automation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional
from collections import deque


@dataclass
class DelaySample:
    """A single input delay measurement."""
    timestamp: float
    delay_ms: float
    input_type: str
    success: bool


@dataclass
class DelayProfile:
    """Statistical profile of input delays."""
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    std_dev: float
    sample_count: int


class InputDelayProfiler:
    """Profile input delays over time."""
    
    def __init__(self, max_samples: int = 1000):
        self._samples: deque[DelaySample] = deque(maxlen=max_samples)
        self._pending_start: Optional[tuple[str, float]] = None
    
    def start_timer(self, input_type: str) -> None:
        """Start timing an input operation."""
        self._pending_start = (input_type, time.perf_counter())
    
    def end_timer(self, success: bool = True) -> Optional[float]:
        """End timing and record the delay."""
        if self._pending_start is None:
            return None
        
        input_type, start_time = self._pending_start
        end_time = time.perf_counter()
        delay_ms = (end_time - start_time) * 1000
        
        sample = DelaySample(
            timestamp=start_time,
            delay_ms=delay_ms,
            input_type=input_type,
            success=success
        )
        self._samples.append(sample)
        self._pending_start = None
        return delay_ms
    
    def get_profile(self, input_type: Optional[str] = None) -> DelayProfile:
        """Get statistical profile of delays."""
        if not self._samples:
            return DelayProfile(0, 0, 0, 0, 0, 0, 0, 0)
        
        if input_type:
            delays = [s.delay_ms for s in self._samples if s.input_type == input_type]
        else:
            delays = [s.delay_ms for s in self._samples]
        
        if not delays:
            return DelayProfile(0, 0, 0, 0, 0, 0, 0, 0)
        
        delays.sort()
        n = len(delays)
        mean = sum(delays) / n
        median = delays[n // 2]
        p95 = delays[int(n * 0.95)]
        p99 = delays[int(n * 0.99)]
        min_val = delays[0]
        max_val = delays[-1]
        
        variance = sum((d - mean) ** 2 for d in delays) / n
        std_dev = variance ** 0.5
        
        return DelayProfile(
            mean_ms=mean,
            median_ms=median,
            p95_ms=p95,
            p99_ms=p99,
            min_ms=min_val,
            max_ms=max_val,
            std_dev=std_dev,
            sample_count=n
        )
    
    def get_recent_samples(self, count: int = 100) -> list[DelaySample]:
        """Get the most recent delay samples."""
        return list(self._samples)[-count:]
    
    def clear(self) -> None:
        """Clear all samples."""
        self._samples.clear()
        self._pending_start = None


class MultiInputProfiler:
    """Profile multiple input channels simultaneously."""
    
    def __init__(self):
        self._profilers: dict[str, InputDelayProfiler] = {}
    
    def get_profiler(self, channel: str) -> InputDelayProfiler:
        """Get or create profiler for a channel."""
        if channel not in self._profilers:
            self._profilers[channel] = InputDelayProfiler()
        return self._profilers[channel]
    
    def get_combined_profile(self) -> DelayProfile:
        """Get combined profile across all channels."""
        all_samples: list[float] = []
        
        for profiler in self._profilers.values():
            samples = profiler.get_recent_samples(10000)
            all_samples.extend(s.delay_ms for s in samples)
        
        if not all_samples:
            return DelayProfile(0, 0, 0, 0, 0, 0, 0, 0)
        
        all_samples.sort()
        n = len(all_samples)
        mean = sum(all_samples) / n
        median = all_samples[n // 2]
        p95 = all_samples[int(n * 0.95)]
        p99 = all_samples[int(n * 0.99)]
        
        variance = sum((d - mean) ** 2 for d in all_samples) / n
        std_dev = variance ** 0.5
        
        return DelayProfile(
            mean_ms=mean,
            median_ms=median,
            p95_ms=p95,
            p99_ms=p99,
            min_ms=all_samples[0],
            max_ms=all_samples[-1],
            std_dev=std_dev,
            sample_count=n
        )
