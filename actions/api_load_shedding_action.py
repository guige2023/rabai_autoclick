"""API Load Shedding Action Module.

Provides load shedding capabilities for protecting APIs from
overload by rejecting requests based on priority and system load.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class PriorityLevel(Enum):
    """Request priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class SheddingStrategy(Enum):
    """Load shedding strategies."""
    REJECT_ALL = "reject_all"
    PRIORITY_BASED = "priority_based"
    RANDOM = "random"
    THROTTLE = "throttle"
    QUEUE = "queue"


@dataclass
class LoadMetrics:
    """Current system load metrics."""
    timestamp: float
    active_requests: int = 0
    queued_requests: int = 0
    avg_response_time_ms: float = 0.0
    error_rate: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0


@dataclass
class RequestTicket:
    """A request in the shedding system."""
    request_id: str
    priority: PriorityLevel
    arrived_at: float
    estimated_cost: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SheddingConfig:
    """Load shedding configuration."""
    max_concurrent: int = 1000
    max_queue_depth: int = 5000
    queue_timeout_sec: float = 30.0
    shedding_strategy: SheddingStrategy = SheddingStrategy.PRIORITY_BASED
    shed_threshold: float = 0.8  # Start shedding at 80% capacity
    priority_weights: Dict[PriorityLevel, float] = field(default_factory=lambda: {
        PriorityLevel.CRITICAL: 1.0,
        PriorityLevel.HIGH: 0.8,
        PriorityLevel.NORMAL: 0.6,
        PriorityLevel.LOW: 0.4,
        PriorityLevel.BACKGROUND: 0.1,
    })


class APILoadSheddingAction(BaseAction):
    """API load shedding action.

    Protects APIs from overload by shedding requests when
    system load exceeds thresholds.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (admit, complete, abandon, get_status, configure)
            - request_id: Request identifier
            - priority: Priority level (critical, high, normal, low, background)
            - config: Shedding configuration dict
            - dataset_id: Identifier for the shedder instance
    """
    action_type = "api_load_shedding"
    display_name = "API负载丢弃"
    description = "API过载保护与请求丢弃策略"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "request_id": None,
            "priority": "normal",
            "estimated_cost": 1.0,
            "config": None,
            "dataset_id": "default",
            "metadata": {},
        }

    def __init__(self) -> None:
        super().__init__()
        self._configs: Dict[str, SheddingConfig] = {}
        self._active_requests: Dict[str, RequestTicket] = {}
        self._request_queue: deque = deque()
        self._request_history: deque = deque(maxlen=10000)
        self._metrics_history: Dict[str, deque] = {}

    def _get_config(self, dataset_id: str) -> SheddingConfig:
        """Get or create shedding config."""
        if dataset_id not in self._configs:
            self._configs[dataset_id] = SheddingConfig()
        return self._configs[dataset_id]

    def _parse_priority(self, priority_str: str) -> PriorityLevel:
        """Parse priority string to PriorityLevel."""
        try:
            return PriorityLevel[priority_str.upper()]
        except KeyError:
            return PriorityLevel.NORMAL

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute load shedding operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        request_id = params.get("request_id")
        priority_str = params.get("priority", "normal")
        estimated_cost = params.get("estimated_cost", 1.0)
        config = params.get("config")
        dataset_id = params.get("dataset_id", "default")
        metadata = params.get("metadata", {})

        cfg = self._get_config(dataset_id)

        if config:
            self._apply_config(cfg, config)

        priority = self._parse_priority(priority_str)

        if operation == "admit":
            return self._admit_request(request_id, priority, estimated_cost, metadata, cfg, dataset_id, start_time)
        elif operation == "complete":
            return self._complete_request(request_id, cfg, dataset_id, start_time)
        elif operation == "abandon":
            return self._abandon_request(request_id, cfg, dataset_id, start_time)
        elif operation == "get_status":
            return self._get_shedder_status(cfg, dataset_id, start_time)
        elif operation == "configure":
            return self._configure_shedder(cfg, config, start_time)
        elif operation == "get_metrics":
            return self._get_load_metrics(cfg, dataset_id, start_time)
        elif operation == "should_shed":
            return self._should_shed(cfg, priority, dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _admit_request(
        self,
        request_id: Optional[str],
        priority: PriorityLevel,
        estimated_cost: float,
        metadata: Dict[str, Any],
        cfg: SheddingConfig,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Attempt to admit a request."""
        if not request_id:
            request_id = f"req_{int(time.time() * 1000)}"

        current_load = len(self._active_requests) / cfg.max_concurrent if cfg.max_concurrent > 0 else 1.0

        # Check if we should shed
        if current_load >= cfg.shed_threshold:
            if cfg.shedding_strategy == SheddingStrategy.REJECT_ALL:
                return ActionResult(
                    success=False,
                    message=f"Request '{request_id}' REJECTED - system overloaded",
                    data={
                        "request_id": request_id,
                        "admitted": False,
                        "reason": "overload",
                        "current_load": current_load,
                        "shed_threshold": cfg.shed_threshold,
                        "active_requests": len(self._active_requests),
                    },
                    duration=time.time() - start_time
                )
            elif cfg.shedding_strategy == SheddingStrategy.PRIORITY_BASED:
                # Check if lower priority should be shed
                if priority.value >= PriorityLevel.NORMAL.value and current_load >= cfg.shed_threshold:
                    # Only shed lower than critical
                    if priority != PriorityLevel.CRITICAL:
                        return ActionResult(
                            success=False,
                            message=f"Request '{request_id}' REJECTED - priority {priority.name} shed at high load",
                            data={
                                "request_id": request_id,
                                "admitted": False,
                                "reason": "priority_shedding",
                                "priority": priority.name,
                                "current_load": current_load,
                            },
                            duration=time.time() - start_time
                        )

        # Admit request
        ticket = RequestTicket(
            request_id=request_id,
            priority=priority,
            arrived_at=time.time(),
            estimated_cost=estimated_cost,
            metadata=metadata,
        )
        self._active_requests[request_id] = ticket
        self._request_history.append(ticket)

        return ActionResult(
            success=True,
            message=f"Request '{request_id}' ADMITTED at priority {priority.name}",
            data={
                "request_id": request_id,
                "admitted": True,
                "priority": priority.name,
                "estimated_cost": estimated_cost,
                "active_requests": len(self._active_requests),
                "capacity_remaining": cfg.max_concurrent - len(self._active_requests),
            },
            duration=time.time() - start_time
        )

    def _complete_request(
        self,
        request_id: Optional[str],
        cfg: SheddingConfig,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Mark a request as complete."""
        if not request_id:
            return ActionResult(success=False, message="request_id required", duration=time.time() - start_time)

        if request_id in self._active_requests:
            ticket = self._active_requests[request_id]
            wait_time = time.time() - ticket.arrived_at
            del self._active_requests[request_id]

            return ActionResult(
                success=True,
                message=f"Request '{request_id}' completed",
                data={
                    "request_id": request_id,
                    "priority": ticket.priority.name,
                    "wait_time_sec": wait_time,
                    "active_requests": len(self._active_requests),
                },
                duration=time.time() - start_time
            )

        return ActionResult(
            success=False,
            message=f"Request '{request_id}' not found",
            duration=time.time() - start_time
        )

    def _abandon_request(
        self,
        request_id: Optional[str],
        cfg: SheddingConfig,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Abandon a request (e.g., timeout)."""
        if not request_id:
            return ActionResult(success=False, message="request_id required", duration=time.time() - start_time)

        if request_id in self._active_requests:
            del self._active_requests[request_id]

        return ActionResult(
            success=True,
            message=f"Request '{request_id}' abandoned",
            data={"request_id": request_id, "active_requests": len(self._active_requests)},
            duration=time.time() - start_time
        )

    def _should_shed(
        self,
        cfg: SheddingConfig,
        priority: PriorityLevel,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Check if a request should be shed."""
        current_load = len(self._active_requests) / cfg.max_concurrent if cfg.max_concurrent > 0 else 1.0
        should_shed = current_load >= cfg.shed_threshold and priority.value >= PriorityLevel.NORMAL.value

        return ActionResult(
            success=True,
            message=f"Load: {current_load:.2%}, Should shed: {should_shed}",
            data={
                "current_load": current_load,
                "shed_threshold": cfg.shed_threshold,
                "should_shed": should_shed,
                "active_requests": len(self._active_requests),
                "max_concurrent": cfg.max_concurrent,
            },
            duration=time.time() - start_time
        )

    def _get_shedder_status(
        self,
        cfg: SheddingConfig,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get load shedder status."""
        current_load = len(self._active_requests) / cfg.max_concurrent if cfg.max_concurrent > 0 else 0.0
        priority_counts = {p.name: 0 for p in PriorityLevel}
        for t in self._active_requests.values():
            priority_counts[t.priority.name] += 1

        return ActionResult(
            success=True,
            message=f"Load shedder '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "strategy": cfg.shedding_strategy.value,
                "active_requests": len(self._active_requests),
                "max_concurrent": cfg.max_concurrent,
                "current_load_pct": current_load * 100,
                "shed_threshold": cfg.shed_threshold * 100,
                "priority_distribution": priority_counts,
                "queued_requests": len(self._request_queue),
                "max_queue_depth": cfg.max_queue_depth,
            },
            duration=time.time() - start_time
        )

    def _configure_shedder(
        self,
        cfg: SheddingConfig,
        config: Optional[Dict],
        start_time: float
    ) -> ActionResult:
        """Configure the load shedder."""
        if not config:
            return ActionResult(success=False, message="config required", duration=time.time() - start_time)

        self._apply_config(cfg, config)

        return ActionResult(
            success=True,
            message="Load shedder configured",
            data={
                "max_concurrent": cfg.max_concurrent,
                "strategy": cfg.shedding_strategy.value,
                "shed_threshold": cfg.shed_threshold,
            },
            duration=time.time() - start_time
        )

    def _apply_config(self, cfg: SheddingConfig, config: Dict) -> None:
        """Apply configuration to shedder."""
        if "max_concurrent" in config:
            cfg.max_concurrent = int(config["max_concurrent"])
        if "max_queue_depth" in config:
            cfg.max_queue_depth = int(config["max_queue_depth"])
        if "queue_timeout_sec" in config:
            cfg.queue_timeout_sec = float(config["queue_timeout_sec"])
        if "shed_threshold" in config:
            cfg.shed_threshold = float(config["shed_threshold"])
        if "strategy" in config:
            try:
                cfg.shedding_strategy = SheddingStrategy(config["strategy"])
            except ValueError:
                pass

    def _get_load_metrics(
        self,
        cfg: SheddingConfig,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get current load metrics."""
        current_load = len(self._active_requests) / cfg.max_concurrent if cfg.max_concurrent > 0 else 0.0
        recent = [t for t in self._request_history if time.time() - t.arrived_at < 60]

        return ActionResult(
            success=True,
            message="Load metrics retrieved",
            data={
                "dataset_id": dataset_id,
                "current_load_pct": current_load * 100,
                "active_requests": len(self._active_requests),
                "requests_last_minute": len(recent),
                "capacity_remaining": max(0, cfg.max_concurrent - len(self._active_requests)),
            },
            duration=time.time() - start_time
        )
