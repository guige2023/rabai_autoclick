"""Automation Dispatcher Action Module.

Provides task dispatching and routing for automation workflows
including routing rules, load balancing, and worker management.

Example:
    >>> from actions.automation.automation_dispatcher_action import AutomationDispatcherAction
    >>> action = AutomationDispatcherAction()
    >>> await action.dispatch(task_data, route="compute")
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import threading
import time
import uuid


class DispatchStrategy(Enum):
    """Task dispatch strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    RANDOM = "random"
    PRIORITY = "priority"
    AFFINITY = "affinity"


class WorkerStatus(Enum):
    """Worker status."""
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"
    DRAINING = "draining"


@dataclass
class Worker:
    """Worker definition.
    
    Attributes:
        worker_id: Unique worker identifier
        name: Worker name
        status: Current status
        current_load: Current task load
        max_load: Maximum task capacity
        tags: Worker tags for routing
        last_seen: Last activity timestamp
    """
    worker_id: str
    name: str
    status: WorkerStatus = WorkerStatus.IDLE
    current_load: int = 0
    max_load: int = 10
    tags: Set[str] = field(default_factory=set)
    last_seen: float = field(default_factory=time.time)
    capabilities: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Route:
    """Route definition.
    
    Attributes:
        route_id: Unique route identifier
        name: Route name
        required_tags: Required worker tags
        required_capabilities: Required capabilities
        max_workers: Maximum workers for this route
        strategy: Dispatch strategy
    """
    route_id: str
    name: str
    required_tags: Set[str] = field(default_factory=set)
    required_capabilities: Dict[str, Any] = field(default_factory=dict)
    max_workers: int = 100
    strategy: DispatchStrategy = DispatchStrategy.ROUND_ROBIN


@dataclass
class DispatchConfig:
    """Configuration for dispatcher.
    
    Attributes:
        default_strategy: Default dispatch strategy
        health_check_interval: Worker health check interval
        max_retries: Maximum dispatch retries
        retry_delay: Delay between retries
    """
    default_strategy: DispatchStrategy = DispatchStrategy.ROUND_ROBIN
    health_check_interval: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class DispatchResult:
    """Result of dispatch operation.
    
    Attributes:
        success: Whether dispatch succeeded
        worker_id: Dispatched worker ID
        task_id: Task identifier
        error: Error message if failed
        duration: Dispatch duration
    """
    success: bool
    worker_id: Optional[str] = None
    task_id: Optional[str] = None
    error: Optional[str] = None
    duration: float = 0.0


@dataclass
class DispatcherStats:
    """Dispatcher statistics.
    
    Attributes:
        total_dispatched: Total tasks dispatched
        total_failed: Total dispatch failures
        avg_dispatch_time: Average dispatch time
        active_workers: Number of active workers
    """
    total_dispatched: int
    total_failed: int
    avg_dispatch_time: float
    active_workers: int
    idle_workers: int


class AutomationDispatcherAction:
    """Task dispatcher for automation workflows.
    
    Manages task routing, worker selection, and load
    balancing across automation workers.
    
    Attributes:
        config: Dispatcher configuration
        _workers: Registered workers
        _routes: Registered routes
        _tasks: Active tasks
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[DispatchConfig] = None,
    ) -> None:
        """Initialize dispatcher action.
        
        Args:
            config: Dispatcher configuration
        """
        self.config = config or DispatchConfig()
        self._workers: Dict[str, Worker] = {}
        self._routes: Dict[str, Route] = {}
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._worker_index: Dict[str, int] = {}
        self._lock = threading.RLock()
        self._total_dispatched = 0
        self._total_failed = 0
        self._dispatch_times: List[float] = []
    
    def register_worker(
        self,
        name: str,
        tags: Optional[Set[str]] = None,
        max_load: int = 10,
        capabilities: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a worker.
        
        Args:
            name: Worker name
            tags: Worker tags
            max_load: Maximum task capacity
            capabilities: Worker capabilities
        
        Returns:
            Worker ID
        """
        worker_id = str(uuid.uuid4())
        
        worker = Worker(
            worker_id=worker_id,
            name=name,
            tags=tags or set(),
            max_load=max_load,
            capabilities=capabilities or {},
        )
        
        with self._lock:
            self._workers[worker_id] = worker
            self._worker_index[worker_id] = 0
        
        return worker_id
    
    def unregister_worker(self, worker_id: str) -> bool:
        """Unregister a worker.
        
        Args:
            worker_id: Worker identifier
        
        Returns:
            True if unregistered
        """
        with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].status = WorkerStatus.OFFLINE
                del self._workers[worker_id]
                return True
            return False
    
    def register_route(
        self,
        name: str,
        required_tags: Optional[Set[str]] = None,
        strategy: DispatchStrategy = DispatchStrategy.ROUND_ROBIN,
    ) -> str:
        """Register a route.
        
        Args:
            name: Route name
            required_tags: Required worker tags
            strategy: Dispatch strategy
        
        Returns:
            Route ID
        """
        route_id = str(uuid.uuid4())
        
        route = Route(
            route_id=route_id,
            name=name,
            required_tags=required_tags or set(),
            strategy=strategy,
        )
        
        with self._lock:
            self._routes[name] = route
        
        return route_id
    
    async def dispatch(
        self,
        task_data: Dict[str, Any],
        route: Optional[str] = None,
        strategy: Optional[DispatchStrategy] = None,
        worker_id: Optional[str] = None,
    ) -> DispatchResult:
        """Dispatch task to worker.
        
        Args:
            task_data: Task data
            route: Route name
            strategy: Override dispatch strategy
            worker_id: Specific worker ID
        
        Returns:
            DispatchResult
        """
        import time
        start_time = time.time()
        
        target_worker = None
        
        if worker_id:
            target_worker = await self._get_worker_by_id(worker_id)
        elif route:
            route_obj = self._routes.get(route)
            strat = strategy or (route_obj.strategy if route_obj else self.config.default_strategy)
            target_worker = await self._select_worker(route, strat)
        else:
            target_worker = await self._select_worker(None, strategy or self.config.default_strategy)
        
        if not target_worker:
            self._total_failed += 1
            return DispatchResult(
                success=False,
                error="No available worker",
                duration=time.time() - start_time,
            )
        
        task_id = str(uuid.uuid4())
        
        with self._lock:
            self._tasks[task_id] = {
                "task_id": task_id,
                "worker_id": target_worker.worker_id,
                "data": task_data,
                "dispatched_at": time.time(),
            }
            target_worker.current_load += 1
            target_worker.status = WorkerStatus.BUSY
        
        self._total_dispatched += 1
        self._dispatch_times.append(time.time() - start_time)
        
        return DispatchResult(
            success=True,
            worker_id=target_worker.worker_id,
            task_id=task_id,
            duration=time.time() - start_time,
        )
    
    async def _get_worker_by_id(self, worker_id: str) -> Optional[Worker]:
        """Get worker by ID.
        
        Args:
            worker_id: Worker identifier
        
        Returns:
            Worker if found and available
        """
        with self._lock:
            worker = self._workers.get(worker_id)
            
            if worker and worker.status != WorkerStatus.OFFLINE:
                if worker.current_load < worker.max_load:
                    return worker
            
            return None
    
    async def _select_worker(
        self,
        route: Optional[str],
        strategy: DispatchStrategy,
    ) -> Optional[Worker]:
        """Select worker based on strategy.
        
        Args:
            route: Route name
            strategy: Dispatch strategy
        
        Returns:
            Selected worker
        """
        with self._lock:
            available = [
                w for w in self._workers.values()
                if w.status != WorkerStatus.OFFLINE
                and w.current_load < w.max_load
            ]
            
            if route:
                route_obj = self._routes.get(route)
                if route_obj:
                    available = [
                        w for w in available
                        if route_obj.required_tags.issubset(w.tags)
                    ]
            
            if not available:
                return None
        
        if strategy == DispatchStrategy.ROUND_ROBIN:
            return self._select_round_robin(available)
        elif strategy == DispatchStrategy.LEAST_LOADED:
            return min(available, key=lambda w: w.current_load)
        elif strategy == DispatchStrategy.RANDOM:
            import random
            return random.choice(available) if available else None
        elif strategy == DispatchStrategy.PRIORITY:
            return self._select_priority(available)
        else:
            return available[0] if available else None
    
    def _select_round_robin(self, workers: List[Worker]) -> Optional[Worker]:
        """Select worker using round-robin.
        
        Args:
            workers: Available workers
        
        Returns:
            Selected worker
        """
        if not workers:
            return None
        
        for worker in workers:
            idx = self._worker_index.get(worker.worker_id, 0)
            self._worker_index[worker.worker_id] = (idx + 1) % len(workers)
            if worker.worker_id == workers[idx % len(workers)].worker_id:
                return workers[idx % len(workers)]
        
        return workers[0]
    
    def _select_priority(self, workers: List[Worker]) -> Optional[Worker]:
        """Select worker using priority (least loaded).
        
        Args:
            workers: Available workers
        
        Returns:
            Selected worker
        """
        return min(workers, key=lambda w: w.current_load) if workers else None
    
    async def complete_task(self, task_id: str) -> bool:
        """Mark task as complete.
        
        Args:
            task_id: Task identifier
        
        Returns:
            True if completed
        """
        with self._lock:
            if task_id not in self._tasks:
                return False
            
            task = self._tasks[task_id]
            worker_id = task.get("worker_id")
            
            if worker_id and worker_id in self._workers:
                worker = self._workers[worker_id]
                worker.current_load = max(0, worker.current_load - 1)
                
                if worker.current_load == 0:
                    worker.status = WorkerStatus.IDLE
            
            del self._tasks[task_id]
            return True
    
    def get_stats(self) -> DispatcherStats:
        """Get dispatcher statistics.
        
        Returns:
            DispatcherStats
        """
        with self._lock:
            active = [w for w in self._workers.values() if w.status == WorkerStatus.BUSY]
            idle = [w for w in self._workers.values() if w.status == WorkerStatus.IDLE]
            
            avg_time = (
                sum(self._dispatch_times) / len(self._dispatch_times)
                if self._dispatch_times else 0.0
            )
            
            return DispatcherStats(
                total_dispatched=self._total_dispatched,
                total_failed=self._total_failed,
                avg_dispatch_time=avg_time,
                active_workers=len(active),
                idle_workers=len(idle),
            )
