"""API Prefetch Action Module.

Provides predictive API request prefetching based on access patterns,
learning from historical data to preload likely needed resources.

Example:
    >>> from actions.api.api_prefetch_action import APIPrefetchAction
    >>> action = APIPrefetchAction()
    >>> await action.prefetch_likely_requests(ctx)
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading


class PrefetchStrategy(Enum):
    """Prefetching strategy types."""
    CONSERVATIVE = "conservative"
    AGGRESSIVE = "aggressive"
    ADAPTIVE = "adaptive"
    PREDICTIVE = "predictive"


class PredictionConfidence(Enum):
    """Confidence level for predictions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class RequestPattern:
    """Represents a learned request pattern.
    
    Attributes:
        pattern_id: Unique pattern identifier
        sequence: Ordered list of request keys in the pattern
        frequency: How often this pattern occurs
        last_access: Last time pattern was observed
        avg_interval: Average time between pattern occurrences
        confidence: Prediction confidence level
    """
    pattern_id: str
    sequence: List[str] = field(default_factory=list)
    frequency: int = 0
    last_access: Optional[datetime] = None
    avg_interval: float = 0.0
    confidence: PredictionConfidence = PredictionConfidence.LOW
    success_rate: float = 0.0


@dataclass
class PrefetchRequest:
    """A prefetch request candidate.
    
    Attributes:
        request_key: Key identifying the request
        url: Full URL to prefetch
        priority: Priority (higher = more likely to be prefetched)
        confidence: Prediction confidence
        estimated_size: Estimated response size in bytes
    """
    request_key: str
    url: str
    priority: int = 0
    confidence: PredictionConfidence = PredictionConfidence.LOW
    estimated_size: int = 0
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class PrefetchConfig:
    """Configuration for prefetching behavior.
    
    Attributes:
        strategy: Prefetching strategy to use
        max_prefetch: Maximum concurrent prefetch requests
        prefetch_window: Seconds ahead to predict
        min_confidence: Minimum confidence to trigger prefetch
        cache_ttl: Cache TTL for prefetched responses
        enable_learning: Whether to learn from patterns
        pattern_window: Time window for pattern analysis
    """
    strategy: PrefetchStrategy = PrefetchStrategy.ADAPTIVE
    max_prefetch: int = 5
    prefetch_window: float = 30.0
    min_confidence: PredictionConfidence = PredictionConfidence.MEDIUM
    cache_ttl: float = 300.0
    enable_learning: bool = True
    pattern_window: float = 3600.0


class APIPrefetchAction:
    """Handles predictive API request prefetching.
    
    Learns from access patterns to prefetch likely-needed resources
    before they are explicitly requested, reducing latency.
    
    Attributes:
        config: Current prefetch configuration
    
    Example:
        >>> action = APIPrefetchAction()
        >>> await action.prefetch_likely_requests(ctx)
    """
    
    def __init__(self, config: Optional[PrefetchConfig] = None):
        """Initialize the prefetch action.
        
        Args:
            config: Prefetch configuration. Uses defaults if not provided.
        """
        self.config = config or PrefetchConfig()
        self._patterns: Dict[str, RequestPattern] = {}
        self._sequence_buffer: List[str] = []
        self._prefetch_cache: Dict[str, Tuple[Any, float]] = {}
        self._active_prefetches: Set[str] = set()
        self._request_history: List[Tuple[str, datetime]] = []
        self._lock = threading.RLock()
        self._pattern_counter = 0
        self._fetch_fn: Optional[Callable] = None
    
    def set_fetch_function(self, fetch_fn: Callable[[str, Dict], Any]) -> None:
        """Set the function used to fetch API resources.
        
        Args:
            fetch_fn: Async function(url, headers) that returns response data
        """
        self._fetch_fn = fetch_fn
    
    def record_request(self, request_key: str, url: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record an API request for pattern learning.
        
        Args:
            request_key: Unique key identifying this request
            url: Full URL of the request
            metadata: Optional metadata about the request
        """
        now = datetime.now()
        
        with self._lock:
            self._request_history.append((request_key, now))
            
            # Update sequence buffer
            self._sequence_buffer.append(request_key)
            
            # Keep buffer bounded
            if len(self._sequence_buffer) > 100:
                self._sequence_buffer = self._sequence_buffer[-50:]
            
            # Learn patterns if enabled
            if self.config.enable_learning:
                self._learn_pattern(request_key, now)
            
            # Clean old history
            cutoff = now - timedelta(seconds=self.config.pattern_window)
            self._request_history = [
                (k, t) for k, t in self._request_history if t > cutoff
            ]
    
    def _learn_pattern(self, request_key: str, now: datetime) -> None:
        """Learn patterns from request sequences.
        
        Args:
            request_key: The request key just seen
            now: Current timestamp
        """
        if len(self._sequence_buffer) < 2:
            return
        
        # Look for repeating sequences
        sequence_len = min(3, len(self._sequence_buffer) - 1)
        
        for length in range(1, sequence_len + 1):
            # Check last `length` items
            subsequence = tuple(self._sequence_buffer[-(length + 1):-1])
            next_item = self._sequence_buffer[-1]
            
            if not subsequence:
                continue
            
            pattern_key = f"{'->'.join(subsequence)}->{next_item}"
            
            if pattern_key in self._patterns:
                pattern = self._patterns[pattern_key]
                pattern.frequency += 1
                pattern.last_access = now
                
                # Update interval
                if pattern.sequence:
                    last_time = getattr(pattern, '_last_time', now)
                    interval = (now - last_time).total_seconds()
                    pattern.avg_interval = (pattern.avg_interval * 0.7 + interval * 0.3)
                    pattern._last_time = now
                
                # Update confidence
                if pattern.frequency >= 10:
                    pattern.confidence = PredictionConfidence.HIGH
                elif pattern.frequency >= 5:
                    pattern.confidence = PredictionConfidence.MEDIUM
                
            else:
                self._pattern_counter += 1
                pattern = RequestPattern(
                    pattern_id=f"pattern_{self._pattern_counter}",
                    sequence=list(subsequence) + [next_item],
                    frequency=1,
                    last_access=now,
                    avg_interval=0.0,
                    confidence=PredictionConfidence.LOW
                )
                pattern._last_time = now
                self._patterns[pattern_key] = pattern
    
    async def predict_next_requests(self, context: Dict[str, Any], count: int = 5) -> List[PrefetchRequest]:
        """Predict which requests are likely to be needed next.
        
        Args:
            context: Current execution context
            count: Maximum number of predictions to return
        
        Returns:
            List of predicted prefetch requests sorted by priority
        """
        predictions: List[PrefetchRequest] = []
        
        with self._lock:
            current_sequence = tuple(self._sequence_buffer[-3:] if self._sequence_buffer else [])
        
        if not current_sequence:
            return predictions
        
        # Find patterns that match current sequence
        pattern_matches: List[Tuple[RequestPattern, float]] = []
        
        for pattern_key, pattern in self._patterns.items():
            prefix = pattern_key.rsplit("->", 1)[0]
            prefix_tuple = tuple(prefix.split("->")) if prefix else ()
            
            if len(prefix_tuple) > 0 and current_sequence[-len(prefix_tuple):] == prefix_tuple:
                score = pattern.frequency * (1.0 if pattern.confidence == PredictionConfidence.HIGH else 0.5)
                pattern_matches.append((pattern, score))
        
        # Sort by score and extract next requests
        pattern_matches.sort(key=lambda x: x[1], reverse=True)
        
        seen_next = set()
        for pattern, _ in pattern_matches[:count * 2]:
            if len(predictions) >= count:
                break
            
            next_key = pattern.sequence[-1]
            if next_key in seen_next:
                continue
            
            seen_next.add(next_key)
            predictions.append(PrefetchRequest(
                request_key=next_key,
                url=next_key,  # Caller should map to actual URL
                priority=int(pattern.frequency * (1 if pattern.confidence == PredictionConfidence.HIGH else 0.5)),
                confidence=pattern.confidence
            ))
        
        return predictions
    
    async def prefetch_likely_requests(
        self,
        context: Dict[str, Any],
        url_resolver: Callable[[str], Optional[str]] = None
    ) -> List[str]:
        """Prefetch likely-needed API requests based on predictions.
        
        Args:
            context: Current execution context
            url_resolver: Optional function to resolve request_key to URL
        
        Returns:
            List of successfully prefetched request keys
        """
        predictions = await self.predict_next_requests(context, self.config.max_prefetch)
        
        # Filter by minimum confidence
        min_order = {
            PredictionConfidence.LOW: 0,
            PredictionConfidence.MEDIUM: 1,
            PredictionConfidence.HIGH: 2
        }
        
        filtered = [
            p for p in predictions
            if min_order[p.confidence] >= min_order[self.config.min_confidence]
        ]
        
        results: List[str] = []
        
        for prefetch_req in filtered[:self.config.max_prefetch]:
            request_key = prefetch_req.request_key
            
            # Skip if already cached or prefetching
            with self._lock:
                if request_key in self._prefetch_cache or request_key in self._active_prefetches:
                    continue
                self._active_prefetches.add(request_key)
            
            try:
                # Resolve URL
                url = prefetch_req.url
                if url_resolver:
                    resolved = url_resolver(request_key)
                    if resolved:
                        url = resolved
                
                # Perform prefetch
                if self._fetch_fn:
                    result = await self._fetch_fn(url, prefetch_req.headers)
                    
                    with self._lock:
                        self._prefetch_cache[request_key] = (
                            result,
                            time.time() + self.config.cache_ttl
                        )
                    
                    results.append(request_key)
                else:
                    # No fetch function - just cache a placeholder
                    with self._lock:
                        self._prefetch_cache[request_key] = (
                            None,
                            time.time() + self.config.cache_ttl
                        )
                    results.append(request_key)
                    
            except Exception as e:
                pass  # Prefetch failures are non-critical
            finally:
                with self._lock:
                    self._active_prefetches.discard(request_key)
        
        return results
    
    def get_prefetched(self, request_key: str) -> Optional[Any]:
        """Get a prefetched result if available and not expired.
        
        Args:
            request_key: Request key to look up
        
        Returns:
            Prefetched result or None
        """
        with self._lock:
            if request_key not in self._prefetch_cache:
                return None
            
            result, expiry = self._prefetch_cache[request_key]
            
            if time.time() > expiry:
                del self._prefetch_cache[request_key]
                return None
            
            return result
    
    def invalidate_prefetch(self, request_key: str) -> None:
        """Invalidate a prefetched result.
        
        Args:
            request_key: Request key to invalidate
        """
        with self._lock:
            self._prefetch_cache.pop(request_key, None)
    
    def clear_cache(self) -> int:
        """Clear all prefetch cache entries.
        
        Returns:
            Number of entries cleared
        """
        with self._lock:
            count = len(self._prefetch_cache)
            self._prefetch_cache.clear()
            return count
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get prefetching statistics.
        
        Returns:
            Dictionary of statistics
        """
        with self._lock:
            return {
                "active_patterns": len(self._patterns),
                "cached_prefetches": len(self._prefetch_cache),
                "active_prefetches": len(self._active_prefetches),
                "recorded_requests": len(self._request_history),
                "buffer_size": len(self._sequence_buffer)
            }
    
    def get_top_patterns(self, limit: int = 10) -> List[RequestPattern]:
        """Get the most frequent request patterns.
        
        Args:
            limit: Maximum number of patterns to return
        
        Returns:
            List of top patterns sorted by frequency
        """
        with self._lock:
            sorted_patterns = sorted(
                self._patterns.values(),
                key=lambda p: p.frequency,
                reverse=True
            )
            return sorted_patterns[:limit]
