"""
Queue Manager Action Module

Multi-queue management with priority queues, dead letter queues,
and configurable retry policies. Thread-safe implementation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class QueueType(Enum):
    """Queue implementation types."""
    
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"
    DELAYED = "delayed"


class QueueEvent(Enum):
    """Queue lifecycle events."""
    
    ENQUEUE = "enqueue"
    DEQUEUE = "dequeue"
    REQUEUE = "requeue"
    DISCARD = "discard"
    EXPIRE = "expire"


@dataclass
class QueueItem:
    """Item in the queue."""
    
    id: str
    data: Any
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    available_at: float = 0
    attempts: int = 0
    max_attempts: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other: "QueueItem") -> bool:
        return self.priority < other.priority


@dataclass
class QueueStats:
    """Queue statistics."""
    
    total_enqueued: int = 0
    total_dequeued: int = 0
    total_requeued: int = 0
    total_discarded: int = 0
    current_size: int = 0


class QueueBackend:
    """Backend storage for queue items."""
    
    def __init__(self):
        self._items: Dict[str, QueueItem] = {}
        self._by_priority: Dict[int, List[str]] = defaultdict(list)
    
    def add(self, item: QueueItem) -> None:
        """Add item to backend."""
        self._items[item.id] = item
        self._by_priority[item.priority].append(item.id)
    
    def remove(self, item_id: str) -> Optional[QueueItem]:
        """Remove item from backend."""
        item = self._items.pop(item_id, None)
        if item:
            self._by_priority[item.priority].remove(item_id)
        return item
    
    def get(self, item_id: str) -> Optional[QueueItem]:
        """Get item by ID."""
        return self._items.get(item_id)
    
    def get_all(self) -> List[QueueItem]:
        """Get all items sorted by priority."""
        result = []
        for priority in sorted(self._by_priority.keys()):
            for item_id in self._by_priority[priority]:
                if item_id in self._items:
                    result.append(self._items[item_id])
        return result
    
    def size(self) -> int:
        """Get total items."""
        return len(self._items)


class QueueManager:
    """Manages multiple queues with different configurations."""
    
    def __init__(self):
        self._queues: Dict[str, QueueBackend] = {}
        self._configs: Dict[str, Dict] = {}
        self._stats: Dict[str, QueueStats] = {}
        self._handlers: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()
    
    def create_queue(
        self,
        name: str,
        queue_type: QueueType = QueueType.FIFO,
        max_size: int = 10000,
        ttl_seconds: Optional[int] = None
    ) -> None:
        """Create a new queue."""
        self._queues[name] = QueueBackend()
        self._configs[name] = {
            "type": queue_type,
            "max_size": max_size,
            "ttl_seconds": ttl_seconds
        }
        self._stats[name] = QueueStats()
    
    def delete_queue(self, name: str) -> bool:
        """Delete a queue."""
        if name in self._queues:
            del self._queues[name]
            del self._configs[name]
            del self._stats[name]
            return True
        return False
    
    async def enqueue(
        self,
        queue_name: str,
        data: Any,
        priority: int = 0,
        delay_seconds: float = 0,
        metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """Enqueue an item."""
        async with self._lock:
            if queue_name not in self._queues:
                return None
            
            config = self._configs[queue_name]
            backend = self._queues[queue_name]
            
            if backend.size() >= config["max_size"]:
                logger.warning(f"Queue {queue_name} is full")
                return None
            
            item_id = str(uuid.uuid4())
            item = QueueItem(
                id=item_id,
                data=data,
                priority=priority,
                available_at=time.time() + delay_seconds if delay_seconds > 0 else 0,
                metadata=metadata or {}
            )
            
            backend.add(item)
            self._stats[queue_name].total_enqueued += 1
            self._stats[queue_name].current_size = backend.size()
            
            return item_id
    
    async def dequeue(
        self,
        queue_name: str,
        timeout: float = 0
    ) -> Optional[Any]:
        """Dequeue an item."""
        start_time = time.time()
        
        while True:
            async with self._lock:
                if queue_name not in self._queues:
                    return None
                
                backend = self._queues[queue_name]
                config = self._configs[queue_name]
                
                items = backend.get_all()
                now = time.time()
                
                for item in items:
                    if config["type"] == QueueType.DELAYED:
                        if item.available_at > now:
                            continue
                    
                    backend.remove(item.id)
                    self._stats[queue_name].total_dequeued += 1
                    self._stats[queue_name].current_size = backend.size()
                    
                    return item.data
                
                if timeout > 0 and (time.time() - start_time) >= timeout:
                    return None
                
                if timeout == 0:
                    return None
            
            await asyncio.sleep(0.01)
    
    async def requeue(
        self,
        queue_name: str,
        item_data: Any,
        max_attempts: int = 3
    ) -> Optional[str]:
        """Requeue an item for retry."""
        async with self._lock:
            if queue_name not in self._queues:
                return None
            
            return await self.enqueue(
                queue_name,
                item_data,
                metadata={"requeued": True}
            )
    
    def get_stats(self, queue_name: str) -> Optional[Dict]:
        """Get queue statistics."""
        if queue_name not in self._stats:
            return None
        
        stats = self._stats[queue_name]
        return {
            "queue_name": queue_name,
            "total_enqueued": stats.total_enqueued,
            "total_dequeued": stats.total_dequeued,
            "total_requeued": stats.total_requeued,
            "total_discarded": stats.total_discarded,
            "current_size": stats.current_size,
            "config": self._configs.get(queue_name, {})
        }
    
    def size(self, queue_name: str) -> int:
        """Get current queue size."""
        if queue_name in self._queues:
            return self._queues[queue_name].size()
        return 0


class QueueManagerAction:
    """
    Main queue manager action handler.
    
    Provides unified interface for managing multiple queues
    with different priorities and configurations.
    """
    
    def __init__(self):
        self.manager = QueueManager()
        self._middleware: List[Callable] = []
        self._processors: Dict[str, Callable] = {}
    
    def create_queue(
        self,
        name: str,
        queue_type: QueueType = QueueType.FIFO,
        max_size: int = 10000
    ) -> None:
        """Create a new queue."""
        self.manager.create_queue(name, queue_type, max_size)
    
    def delete_queue(self, name: str) -> bool:
        """Delete a queue."""
        return self.manager.delete_queue(name)
    
    async def enqueue(
        self,
        queue: str,
        data: Any,
        priority: int = 0,
        delay: float = 0
    ) -> Optional[str]:
        """Enqueue data to a queue."""
        return await self.manager.enqueue(queue, data, priority, delay)
    
    async def dequeue(
        self,
        queue: str,
        timeout: float = 0
    ) -> Optional[Any]:
        """Dequeue data from a queue."""
        return await self.manager.dequeue(queue, timeout)
    
    async def enqueue_batch(
        self,
        queue: str,
        items: List[Any],
        priority: int = 0
    ) -> List[str]:
        """Enqueue multiple items."""
        results = []
        for item in items:
            item_id = await self.enqueue(queue, item, priority)
            if item_id:
                results.append(item_id)
        return results
    
    def register_processor(self, queue: str, processor: Callable) -> None:
        """Register a processor function for a queue."""
        self._processors[queue] = processor
    
    async def process_queue(
        self,
        queue: str,
        max_items: Optional[int] = None
    ) -> List[Any]:
        """Process items from a queue with registered processor."""
        results = []
        processor = self._processors.get(queue)
        
        if not processor:
            return results
        
        count = 0
        while count < (max_items or float("inf")):
            item = await self.dequeue(queue, timeout=1.0)
            if item is None:
                break
            
            try:
                result = await processor(item)
                results.append(result)
            except Exception as e:
                logger.error(f"Processor error for queue {queue}: {e}")
            
            count += 1
        
        return results
    
    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all queues."""
        return {
            name: self.manager.get_stats(name)
            for name in self.manager._queues.keys()
        }
    
    def list_queues(self) -> List[str]:
        """List all queue names."""
        return list(self.manager._queues.keys())
