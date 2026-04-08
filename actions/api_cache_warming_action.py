"""API cache warming action module for RabAI AutoClick.

Provides cache warming for API operations:
- ApiCacheWarmer: Warm API response caches proactively
- PredictiveCacheWarmer: Use patterns to predict and warm caches
- ApiCachePreheat: Preheat cache for known access patterns
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WarmingStrategy(Enum):
    """Cache warming strategies."""
    EAGER = "eager"
    LAZY = "lazy"
    PREDICTIVE = "predictive"
    BACKGROUND = "background"
    SCHEDULED = "scheduled"


@dataclass
class CacheWarmingConfig:
    """Configuration for cache warming."""
    strategy: WarmingStrategy = WarmingStrategy.LAZY
    warm_on_startup: bool = True
    prefetch_related: bool = True
    ttl_buffer: float = 0.2
    max_warm_items: int = 100
    concurrent_warming: int = 5
    warm_interval: float = 300.0
    confidence_threshold: float = 0.7
    access_pattern_window: float = 3600.0


class AccessPattern:
    """Track access patterns for predictive warming."""
    
    def __init__(self, key: str):
        self.key = key
        self.access_times: deque = deque(maxlen=1000)
        self.access_count = 0
        self._lock = threading.RLock()
    
    def record_access(self, timestamp: Optional[float] = None):
        """Record an access."""
        with self._lock:
            ts = timestamp or time.time()
            self.access_times.append(ts)
            self.access_count += 1
    
    def get_periodicity(self) -> Optional[float]:
        """Detect access periodicity in seconds."""
        with self._lock:
            if len(self.access_times) < 10:
                return None
            
            times = list(self.access_times)
            intervals = [times[i+1] - times[i] for i in range(len(times)-1)]
            
            if not intervals:
                return None
            
            avg_interval = sum(intervals) / len(intervals)
            
            variance = sum((i - avg_interval) ** 2 for i in intervals) / len(intervals)
            std_dev = variance ** 0.5
            
            if std_dev / avg_interval > 0.3:
                return None
            
            return avg_interval
    
    def predict_next_access(self) -> Optional[float]:
        """Predict next access time."""
        periodicity = self.get_periodicity()
        if not periodicity:
            return None
        
        with self._lock:
            if not self.access_times:
                return None
            last_access = self.access_times[-1]
        
        return last_access + periodicity
    
    def should_warm(self, ttl: float, buffer: float = 0.2) -> bool:
        """Check if cache should be warmed."""
        next_access = self.predict_next_access()
        if not next_access:
            return True
        
        time_until_access = next_access - time.time()
        return time_until_access < (ttl * buffer)


class ApiCacheWarmer:
    """Warm API response caches."""
    
    def __init__(self, name: str, config: Optional[CacheWarmingConfig] = None):
        self.name = name
        self.config = config or CacheWarmingConfig()
        self._patterns: Dict[str, AccessPattern] = {}
        self._warmed_keys: Set[str] = set()
        self._warm_Queue: deque = deque(maxlen=1000)
        self._active_warms: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"total_warm_requests": 0, "cache_hits": 0, "cache_misses": 0, "warm_operations": 0}
        self._fetcher: Optional[Callable] = None
        self._cache_getter: Optional[Callable] = None
        self._cache_setter: Optional[Callable] = None
    
    def set_cache_functions(self, getter: Callable, setter: Callable):
        """Set cache getter and setter functions."""
        self._cache_getter = getter
        self._cache_setter = setter
    
    def set_fetcher(self, fetcher: Callable):
        """Set data fetcher function."""
        self._fetcher = fetcher
    
    def _get_pattern(self, key: str) -> AccessPattern:
        """Get or create access pattern."""
        with self._lock:
            if key not in self._patterns:
                self._patterns[key] = AccessPattern(key)
            return self._patterns[key]
    
    def record_access(self, key: str, timestamp: Optional[float] = None):
        """Record cache access."""
        self._get_pattern(key).record_access(timestamp)
        
        with self._lock:
            self._stats["total_warm_requests"] += 1
        
        if self._cache_getter:
            value = self._cache_getter(key)
            if value is not None:
                with self._lock:
                    self._stats["cache_hits"] += 1
                return
        
        with self._lock:
            self._stats["cache_misses"] += 1
        
        if self.config.strategy in [WarmingStrategy.EAGER, WarmingStrategy.PREDICTIVE]:
            self._warm_queue_add(key)
    
    def _warm_queue_add(self, key: str):
        """Add key to warm queue."""
        with self._lock:
            if key not in self._warmed_keys:
                self._warm_Queue.append(key)
    
    def warm_key(self, key: str, ttl: float = 3600.0) -> bool:
        """Warm cache for specific key."""
        with self._lock:
            if key in self._active_warms:
                return False
        
        if self._fetcher is None:
            return False
        
        event = threading.Event()
        
        with self._lock:
            self._active_warms[key] = event
        
        def worker():
            try:
                data = self._fetcher(key)
                if data is not None and self._cache_setter:
                    self._cache_setter(key, data, ttl)
                
                with self._lock:
                    self._warmed_keys.add(key)
                    self._stats["warm_operations"] += 1
            finally:
                with self._lock:
                    self._active_warms.pop(key, None)
                event.set()
        
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        return True
    
    def warm_batch(self, keys: List[str], ttl: float = 3600.0) -> int:
        """Warm cache for multiple keys."""
        warmed = 0
        for key in keys[:self.config.max_warm_items]:
            if self.warm_key(key, ttl):
                warmed += 1
        return warmed
    
    def warm_related(self, key: str, related_keys: List[str], ttl: float = 3600.0) -> int:
        """Warm related keys based on prefetch relationships."""
        return self.warm_batch(related_keys, ttl)
    
    def get_warm_status(self, key: str) -> Dict[str, Any]:
        """Get warming status for key."""
        with self._lock:
            pattern = self._patterns.get(key)
            
            if not pattern:
                return {"key": key, "warmed": False, "pattern_known": False}
            
            next_access = pattern.predict_next_access()
            
            return {
                "key": key,
                "warmed": key in self._warmed_keys,
                "pattern_known": True,
                "access_count": pattern.access_count,
                "periodicity": pattern.get_periodicity(),
                "next_access_predicted": next_access,
                "should_warm": pattern.should_warm(3600.0, self.config.ttl_buffer) if pattern else False,
                "in_progress": key in self._active_warms,
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get warming statistics."""
        with self._lock:
            return {
                "name": self.name,
                "tracked_keys": len(self._patterns),
                "warmed_keys": len(self._warmed_keys),
                "queue_size": len(self._warm_Queue),
                **{k: v for k, v in self._stats.items()},
            }


class ApiCacheWarmingAction(BaseAction):
    """API cache warming action."""
    action_type = "api_cache_warming"
    display_name = "API缓存预热"
    description = "API缓存主动预热"
    
    def __init__(self):
        super().__init__()
        self._warmers: Dict[str, ApiCacheWarmer] = {}
        self._lock = threading.Lock()
    
    def _get_warmer(self, name: str, config: Optional[CacheWarmingConfig] = None) -> ApiCacheWarmer:
        """Get or create cache warmer."""
        with self._lock:
            if name not in self._warmers:
                self._warmers[name] = ApiCacheWarmer(name, config)
            return self._warmers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache warming operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "record")
            
            config = CacheWarmingConfig(
                strategy=WarmingStrategy[params.get("strategy", "lazy").upper()],
                ttl_buffer=params.get("ttl_buffer", 0.2),
                max_warm_items=params.get("max_warm_items", 100),
            )
            
            warmer = self._get_warmer(name, config)
            
            if command == "record":
                key = params.get("key")
                if key:
                    warmer.record_access(key)
                return ActionResult(success=True)
            
            elif command == "warm":
                key = params.get("key")
                ttl = params.get("ttl", 3600.0)
                if key:
                    success = warmer.warm_key(key, ttl)
                    return ActionResult(success=success)
                return ActionResult(success=False, message="key required")
            
            elif command == "warm_batch":
                keys = params.get("keys", [])
                ttl = params.get("ttl", 3600.0)
                warmed = warmer.warm_batch(keys, ttl)
                return ActionResult(success=True, data={"warmed": warmed, "total": len(keys)})
            
            elif command == "set_functions":
                getter = params.get("cache_getter")
                setter = params.get("cache_setter")
                fetcher = params.get("fetcher")
                if getter:
                    warmer.set_cache_functions(getter, setter)
                if fetcher:
                    warmer.set_fetcher(fetcher)
                return ActionResult(success=True)
            
            elif command == "status":
                key = params.get("key")
                if key:
                    status = warmer.get_warm_status(key)
                    return ActionResult(success=True, data={"status": status})
                return ActionResult(success=True, data={"stats": warmer.get_stats()})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"ApiCacheWarmingAction error: {str(e)}")
