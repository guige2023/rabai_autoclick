"""Data priority action module for RabAI AutoClick.

Provides priority-based data processing:
- PriorityQueue: Priority queue for data items
- WeightedDataProcessor: Process data by priority
- PriorityScheduler: Schedule data operations by priority
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Type
import time
import threading
import logging
import heapq
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PriorityLevel(Enum):
    """Priority levels for data items."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class PriorityItem:
    """Priority queue item."""
    priority: int
    item: Any
    data: Any
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other: "PriorityItem") -> bool:
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at


@dataclass
class PriorityConfig:
    """Configuration for priority processing."""
    default_priority: int = 2
    max_queue_size: int = 10000
    min_priority: int = 0
    max_priority: int = 10
    priority_buckets: int = 5
    aging_enabled: bool = True
    aging_factor: float = 0.1
    aging_interval: float = 60.0


class PriorityQueue:
    """Thread-safe priority queue with priority levels."""
    
    def __init__(self, config: Optional[PriorityConfig] = None):
        self.config = config or PriorityConfig()
        self._heap: List[PriorityItem] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._item_count = 0
        self._stats = {"total_enqueued": 0, "total_dequeued": 0, "total_requeued": 0, "priority_violations": 0}
        self._aging_enabled = self.config.aging_enabled
    
    def enqueue(self, item: Any, data: Any, priority: Optional[int] = None, 
                metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Enqueue item with priority."""
        with self._lock:
            if len(self._heap) >= self.config.max_queue_size:
                return False
            
            p = priority if priority is not None else self.config.default_priority
            p = max(self.config.min_priority, min(p, self.config.max_priority))
            
            priority_item = PriorityItem(priority=p, item=item, data=data, metadata=metadata or {})
            heapq.heappush(self._heap, priority_item)
            self._item_count += 1
            self._stats["total_enqueued"] += 1
            self._not_empty.notify()
            
            return True
    
    def dequeue(self, timeout: Optional[float] = None) -> Optional[PriorityItem]:
        """Dequeue highest priority item."""
        with self._not_empty:
            if timeout is None:
                while not self._heap:
                    self._not_empty.wait()
            else:
                end_time = time.time() + timeout
                while not self._heap:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return None
                    self._not_empty.wait(remaining)
            
            if self._heap:
                item = heapq.heappop(self._heap)
                self._item_count -= 1
                self._stats["total_dequeued"] += 1
                return item
        
        return None
    
    def peek(self) -> Optional[PriorityItem]:
        """Peek at highest priority item without removing."""
        with self._lock:
            if self._heap:
                return self._heap[0]
        return None
    
    def requeue(self, item: PriorityItem, new_priority: Optional[int] = None) -> bool:
        """Requeue item with possibly new priority."""
        with self._lock:
            if len(self._heap) >= self.config.max_queue_size:
                return False
            
            p = new_priority if new_priority is not None else item.priority
            new_item = PriorityItem(priority=p, item=item.item, data=item.data, 
                                    metadata=item.metadata.copy())
            heapq.heappush(self._heap, new_item)
            self._stats["total_requeued"] += 1
            return True
    
    def remove(self, item: Any) -> bool:
        """Remove specific item from queue."""
        with self._lock:
            for i, pi in enumerate(self._heap):
                if pi.item == item:
                    self._heap.pop(i)
                    heapq.heapify(self._heap)
                    self._item_count -= 1
                    return True
        return False
    
    def clear(self):
        """Clear all items."""
        with self._lock:
            self._heap.clear()
            self._item_count = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        with self._lock:
            priority_counts = defaultdict(int)
            for item in self._heap:
                priority_counts[item.priority] += 1
            
            return {
                "size": len(self._heap),
                "item_count": self._item_count,
                "priority_distribution": dict(priority_counts),
                **{k: v for k, v in self._stats.items()},
            }


class WeightedDataProcessor:
    """Process data items by priority with weighted allocation."""
    
    def __init__(self, queue: PriorityQueue, processor: Callable, config: Optional[PriorityConfig] = None):
        self.queue = queue
        self.processor = processor
        self.config = config or PriorityConfig()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._processed_count = 0
    
    def start(self):
        """Start processing loop."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._process_loop, daemon=True)
            self._thread.start()
    
    def stop(self):
        """Stop processing."""
        with self._lock:
            self._running = False
            if self._thread:
                self._thread.join(timeout=5.0)
    
    def _process_loop(self):
        """Main processing loop."""
        while self._running:
            item = self.queue.dequeue(timeout=1.0)
            if item is None:
                continue
            
            try:
                self.processor(item.data)
                self._processed_count += 1
            except Exception as e:
                logging.error(f"WeightedDataProcessor error: {e}")


class DataPriorityAction(BaseAction):
    """Data priority action."""
    action_type = "data_priority"
    display_name = "数据优先级"
    description = "基于优先级的数据处理"
    
    def __init__(self):
        super().__init__()
        self._queue: Optional[PriorityQueue] = None
        self._config: Optional[PriorityConfig] = None
        self._lock = threading.Lock()
    
    def _get_queue(self, params: Dict[str, Any]) -> PriorityQueue:
        """Get or create priority queue."""
        with self._lock:
            if self._queue is None:
                self._config = PriorityConfig(
                    default_priority=params.get("default_priority", 2),
                    max_queue_size=params.get("max_queue_size", 10000),
                    aging_enabled=params.get("aging_enabled", True),
                )
                self._queue = PriorityQueue(self._config)
            return self._queue
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute priority operation."""
        try:
            command = params.get("command", "enqueue")
            queue = self._get_queue(params)
            
            if command == "enqueue":
                item = params.get("item")
                data = params.get("data")
                priority = params.get("priority")
                
                if item is None:
                    return ActionResult(success=False, message="item required")
                
                success = queue.enqueue(item, data, priority)
                return ActionResult(success=success, message="Enqueued" if success else "Queue full")
            
            elif command == "dequeue":
                timeout = params.get("timeout")
                item = queue.dequeue(timeout)
                
                if item:
                    return ActionResult(success=True, data={"item": item.item, "data": item.data, "priority": item.priority})
                return ActionResult(success=False, message="Queue empty")
            
            elif command == "peek":
                item = queue.peek()
                if item:
                    return ActionResult(success=True, data={"item": item.item, "priority": item.priority})
                return ActionResult(success=False, message="Queue empty")
            
            elif command == "size":
                stats = queue.get_stats()
                return ActionResult(success=True, data={"size": stats["size"]})
            
            elif command == "stats":
                stats = queue.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "clear":
                queue.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataPriorityAction error: {str(e)}")
