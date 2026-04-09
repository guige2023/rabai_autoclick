"""
API Throttling Policy Action Module

Implements configurable throttling policies for API request control.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import threading
import asyncio


class ThrottleType(Enum):
    """Throttling type."""
    HARD_LIMIT = "hard_limit"  # Reject requests over limit
    SOFT_LIMIT = "soft_limit"  # Add latency to requests
    QUEUE = "queue"  # Queue requests for later


class Priority(Enum):
    """Request priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5


@dataclass
class ThrottlePolicy:
    """Single throttling policy rule."""
    name: str
    max_requests: int
    window_seconds: int
    throttle_type: ThrottleType = ThrottleType.HARD_LIMIT
    added_latency_ms: int = 0
    max_queue_size: int = 0
    priority_boost: int = 0  # Priority adjustment


@dataclass
class ThrottleDecision:
    """Decision from throttling policy."""
    allowed: bool
    priority: Priority
    latency_ms: int
    queue_position: Optional[int] = None
    reason: str = ""


@dataclass
class PriorityQueueEntry:
    """Entry in priority queue."""
    request_id: str
    priority: Priority
    arrival_time: float
    payload: Any = None


class MinHeapPriorityQueue:
    """Min-heap priority queue implementation."""

    def __init__(self):
        self.heap: List[PriorityQueueEntry] = []
        self.lock = threading.Lock()
        self.counter = 0  # Tiebreaker for same priority

    def push(self, entry: PriorityQueueEntry) -> int:
        """Push entry into queue. Returns position."""
        with self.lock:
            self.counter += 1
            entry.payload = (entry.priority.value, self.counter, entry.arrival_time)
            # Convert to sortable tuple
            self.heap.append(entry)
            self._sift_up(len(self.heap) - 1)
            return len(self.heap)

    def pop(self) -> Optional[PriorityQueueEntry]:
        """Pop highest priority entry."""
        with self.lock:
            if not self.heap:
                return None
            result = self.heap[0]
            last = self.heap.pop()
            if self.heap:
                self.heap[0] = last
                self._sift_down(0)
            return result

    def peek(self) -> Optional[PriorityQueueEntry]:
        """Peek at highest priority without removing."""
        with self.lock:
            return self.heap[0] if self.heap else None

    def remove(self, request_id: str) -> bool:
        """Remove specific entry by request_id."""
        with self.lock:
            for i, entry in enumerate(self.heap):
                if entry.request_id == request_id:
                    del self.heap[i]
                    return True
            return False

    def __len__(self) -> int:
        return len(self.heap)

    def _sift_up(self, index: int) -> None:
        while index > 0:
            parent = (index - 1) // 2
            if self._less(index, parent):
                self.heap[index], self.heap[parent] = self.heap[parent], self.heap[index]
                index = parent
            else:
                break

    def _sift_down(self, index: int) -> None:
        n = len(self.heap)
        while True:
            smallest = index
            left = 2 * index + 1
            right = 2 * index + 2
            if left < n and self._less(left, smallest):
                smallest = left
            if right < n and self._less(right, smallest):
                smallest = right
            if smallest != index:
                self.heap[index], self.heap[smallest] = self.heap[smallest], self.heap[index]
                index = smallest
            else:
                break

    def _less(self, i: int, j: int) -> bool:
        """Compare two entries by priority and arrival time."""
        a = self.heap[i].payload
        b = self.heap[j].payload
        if not isinstance(a, tuple):
            a = (self.heap[i].priority.value, 0, self.heap[i].arrival_time)
            b = (self.heap[j].priority.value, 0, self.heap[j].arrival_time)
        if a[0] != b[0]:
            return a[0] < b[0]  # Lower priority value = higher priority
        if a[1] != b[1]:
            return a[1] < b[1]  # Earlier counter = higher priority
        return a[2] < b[2]  # Earlier arrival = higher priority


class SlidingWindowThrottler:
    """Sliding window rate throttler."""

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []
        self.lock = threading.Lock()

    def try_acquire(self) -> bool:
        """Try to acquire a request slot."""
        with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def get_utilization(self) -> float:
        """Get current utilization (0.0 to 1.0)."""
        with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            return len(self.requests) / self.max_requests


class ApiThrottlingPolicyAction:
    """
    Implements configurable throttling policies for API request control.

    Supports hard limits (reject), soft limits (add latency), and queue-based
    throttling with priority-based request ordering.

    Example:
        throttler = ApiThrottlingPolicyAction()
        throttler.add_policy(ThrottlePolicy(
            name="default",
            max_requests=100,
            window_seconds=60
        ))
        decision = throttler.evaluate("req_123", Priority.NORMAL)
    """

    def __init__(self):
        """Initialize throttling policy manager."""
        self.policies: Dict[str, ThrottlePolicy] = {}
        self.default_policy_name: Optional[str] = None
        self.throttlers: Dict[str, SlidingWindowThrottler] = {}
        self.priority_queues: Dict[str, MinHeapPriorityQueue] = {}
        self.lock = threading.RLock()
        self.request_counter = 0
        self._request_tracking: Dict[str, float] = {}

    def add_policy(self, policy: ThrottlePolicy) -> None:
        """
        Add a throttling policy.

        Args:
            policy: ThrottlePolicy configuration
        """
        with self.lock:
            self.policies[policy.name] = policy
            self.throttlers[policy.name] = SlidingWindowThrottler(
                policy.max_requests, policy.window_seconds
            )
            if self.default_policy_name is None:
                self.default_policy_name = policy.name

    def set_default_policy(self, name: str) -> None:
        """Set the default policy name."""
        if name in self.policies:
            self.default_policy_name = name

    def evaluate(
        self,
        request_id: str,
        priority: Priority,
        policy_name: Optional[str] = None
    ) -> ThrottleDecision:
        """
        Evaluate a request against throttling policies.

        Args:
            request_id: Unique request identifier
            priority: Request priority level
            policy_name: Optional specific policy to use

        Returns:
            ThrottleDecision with the result
        """
        with self.lock:
            policy = self.policies.get(policy_name or self.default_policy_name)
            if not policy:
                return ThrottleDecision(
                    allowed=True,
                    priority=priority,
                    latency_ms=0,
                    reason="No policy found"
                )

            throttler = self.throttlers.get(policy.name)
            if not throttler:
                return ThrottleDecision(
                    allowed=True,
                    priority=priority,
                    latency_ms=0,
                    reason="Throttler not initialized"
                )

            # Apply priority boost
            effective_priority = Priority(
                max(1, min(5, priority.value + policy.priority_boost))
            )

            if policy.throttle_type == ThrottleType.HARD_LIMIT:
                if throttler.try_acquire():
                    return ThrottleDecision(
                        allowed=True,
                        priority=effective_priority,
                        latency_ms=0,
                        reason="Allowed"
                    )
                else:
                    return ThrottleDecision(
                        allowed=False,
                        priority=effective_priority,
                        latency_ms=0,
                        reason="Hard limit exceeded"
                    )

            elif policy.throttle_type == ThrottleType.SOFT_LIMIT:
                throttler.try_acquire()  # Always allow, but add latency based on utilization
                utilization = throttler.get_utilization()
                added_latency = int(policy.added_latency_ms * utilization)
                return ThrottleDecision(
                    allowed=True,
                    priority=effective_priority,
                    latency_ms=added_latency,
                    reason=f"Soft limit, utilization={utilization:.2%}"
                )

            elif policy.throttle_type == ThrottleType.QUEUE:
                if policy.max_queue_size > 0:
                    if len(self.priority_queues.get(policy.name, MinHeapPriorityQueue())) >= policy.max_queue_size:
                        return ThrottleDecision(
                            allowed=False,
                            priority=effective_priority,
                            latency_ms=0,
                            reason="Queue full"
                        )

                if policy.name not in self.priority_queues:
                    self.priority_queues[policy.name] = MinHeapPriorityQueue()

                entry = PriorityQueueEntry(
                    request_id=request_id,
                    priority=effective_priority,
                    arrival_time=time.time()
                )
                position = self.priority_queues[policy.name].push(entry)
                return ThrottleDecision(
                    allowed=True,
                    priority=effective_priority,
                    latency_ms=0,
                    queue_position=position,
                    reason="Queued for processing"
                )

            return ThrottleDecision(
                allowed=True,
                priority=priority,
                latency_ms=0,
                reason="Unknown throttle type"
            )

    def dequeue(self, policy_name: Optional[str] = None) -> Optional[PriorityQueueEntry]:
        """Dequeue the next request from the queue."""
        with self.lock:
            policy = self.policies.get(policy_name or self.default_policy_name)
            if not policy or policy.name not in self.priority_queues:
                return None
            return self.priority_queues[policy.name].pop()

    def get_queue_stats(self, policy_name: Optional[str] = None) -> Dict[str, Any]:
        """Get queue statistics."""
        with self.lock:
            stats = {}
            policies_to_check = [policy_name] if policy_name else list(self.policies.keys())

            for name in policies_to_check:
                if name in self.priority_queues:
                    queue = self.priority_queues[name]
                    stats[name] = {
                        "queue_length": len(queue),
                        "oldest_request_age_ms": 0
                    }
                    if len(queue) > 0:
                        oldest = queue.peek()
                        if oldest:
                            stats[name]["oldest_request_age_ms"] = (
                                time.time() - oldest.arrival_time
                            ) * 1000

            return stats

    def get_throttle_stats(self, policy_name: Optional[str] = None) -> Dict[str, Any]:
        """Get throttle utilization statistics."""
        with self.lock:
            stats = {}
            policies_to_check = [policy_name] if policy_name else list(self.policies.keys())

            for name in policies_to_check:
                if name in self.throttlers:
                    throttler = self.throttlers[name]
                    policy = self.policies.get(name)
                    stats[name] = {
                        "utilization": throttler.get_utilization(),
                        "max_requests": policy.max_requests if policy else 0,
                        "window_seconds": policy.window_seconds if policy else 0
                    }

            return stats

    def reset(self, policy_name: Optional[str] = None) -> None:
        """Reset throttling state."""
        with self.lock:
            if policy_name:
                if policy_name in self.throttlers:
                    self.throttlers[policy_name] = SlidingWindowThrottler(
                        self.policies[policy_name].max_requests,
                        self.policies[policy_name].window_seconds
                    )
                if policy_name in self.priority_queues:
                    self.priority_queues[policy_name] = MinHeapPriorityQueue()
            else:
                # Reset all
                for name, policy in self.policies.items():
                    self.throttlers[name] = SlidingWindowThrottler(
                        policy.max_requests, policy.window_seconds
                    )
                self.priority_queues.clear()
