"""
Automation Guard Rail Module.

Provides safety monitoring, threshold detection, circuit breakers,
and automatic safeguards for automation workflows.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from collections import deque
import logging

logger = logging.getLogger(__name__)


class GuardType(Enum):
    """Type of guard rail."""
    RATE = "rate"
    THRESHOLD = "threshold"
    TIMEOUT = "timeout"
    CIRCUIT_BREAKER = "circuit_breaker"
    RESOURCE = "resource"
    CUSTOM = "custom"


class GuardState(Enum):
    """State of a guard."""
    NORMAL = "normal"
    WARNING = "warning"
    TRIGGERED = "triggered"
    DISABLED = "disabled"


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Blocking requests
    HALF_OPEN = "half_open"  # Testing


@dataclass
class GuardConfig:
    """Configuration for a guard rail."""
    name: str
    guard_type: GuardType
    threshold: float = 100.0
    window_size: float = 60.0  # seconds
    cooldown: float = 60.0  # seconds after triggered
    enabled: bool = True
    action: str = "log"  # log, block, rollback, callback
    callback: Optional[Callable[..., Any]] = None
    strict: bool = True  # If True, blocks on violation


@dataclass
class GuardEvent:
    """Event recorded by a guard."""
    guard_name: str
    event_type: str
    value: float
    threshold: float
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0  # Seconds before trying half-open
    half_open_max_calls: int = 3


class GuardRail:
    """
    Safety guard rail for automation workflows.
    
    Example:
        guard = GuardRail()
        
        # Add rate limit guard
        guard.add_guard(GuardConfig(
            name="api_rate",
            guard_type=GuardType.RATE,
            threshold=100,
            window_size=60,
            action="block"
        ))
        
        # Add threshold guard
        guard.add_guard(GuardConfig(
            name="error_rate",
            guard_type=GuardType.THRESHOLD,
            threshold=0.1,  # 10%
            action="rollback"
        ))
        
        # Check before action
        if await guard.check("api_rate", current_value=50):
            await execute_action()
    """
    
    def __init__(self) -> None:
        """Initialize the guard rail system."""
        self._guards: Dict[str, GuardConfig] = {}
        self._states: Dict[str, GuardState] = {}
        self._circuits: Dict[str, CircuitBreakerConfig] = {}
        self._circuit_states: Dict[str, CircuitState] = {}
        self._metrics: Dict[str, deque] = {}  # time-bounded metrics
        self._events: deque = deque(maxlen=10000)
        self._last_triggered: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        
    def add_guard(
        self,
        config: GuardConfig,
        circuit_config: Optional[CircuitBreakerConfig] = None,
    ) -> None:
        """
        Add a guard rail.
        
        Args:
            config: Guard configuration.
            circuit_config: Optional circuit breaker config.
        """
        self._guards[config.name] = config
        self._states[config.name] = GuardState.NORMAL
        
        if config.guard_type == GuardType.CIRCUIT_BREAKER and circuit_config:
            self._circuits[config.name] = circuit_config
            self._circuit_states[config.name] = CircuitState.CLOSED
            
        self._metrics[config.name] = deque()
        
        logger.info(f"Added guard rail: {config.name} (type={config.guard_type.value})")
        
    def remove_guard(self, name: str) -> bool:
        """Remove a guard rail."""
        if name in self._guards:
            del self._guards[name]
            del self._states[name]
            if name in self._circuits:
                del self._circuits[name]
                del self._circuit_states[name]
            del self._metrics[name]
            return True
        return False
        
    async def check(
        self,
        guard_name: str,
        current_value: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Check a guard condition.
        
        Args:
            guard_name: Name of the guard to check.
            current_value: Current measured value.
            metadata: Optional metadata for the check.
            
        Returns:
            True if check passes, False if blocked.
        """
        if guard_name not in self._guards:
            logger.warning(f"Unknown guard: {guard_name}")
            return True
            
        guard = self._guards[guard_name]
        
        if not guard.enabled:
            return True
            
        # Check cooldown
        if guard_name in self._last_triggered:
            cooldown_end = self._last_triggered[guard_name] + guard.cooldown
            if time.time() < cooldown_end:
                return True
                
        # Check circuit breaker first
        if guard.guard_type == GuardType.CIRCUIT_BREAKER:
            if not await self._check_circuit(guard_name):
                return False
                
        # Record metric
        self._record_metric(guard_name, current_value)
        
        # Check threshold
        violated = False
        state = GuardState.NORMAL
        
        if guard.guard_type == GuardType.RATE:
            rate = self._calculate_rate(guard_name)
            violated = rate >= guard.threshold
        elif guard.guard_type == GuardType.THRESHOLD:
            violated = current_value >= guard.threshold
        elif guard.guard_type == GuardType.TIMEOUT:
            violated = current_value >= guard.threshold
        elif guard.guard_type == GuardType.RESOURCE:
            violated = current_value >= guard.threshold
            
        if violated:
            state = GuardState.TRIGGERED
            self._last_triggered[guard_name] = time.time()
            
            event = GuardEvent(
                guard_name=guard_name,
                event_type="violation",
                value=current_value,
                threshold=guard.threshold,
                timestamp=time.time(),
                metadata=metadata or {},
            )
            self._events.append(event)
            
            logger.warning(
                f"Guard triggered: {guard_name} "
                f"(value={current_value:.2f}, threshold={guard.threshold:.2f})"
            )
            
            # Execute action
            await self._execute_action(guard, current_value, metadata)
            
        elif self._states[guard_name] == GuardState.TRIGGERED:
            # Check if we should exit triggered state
            if guard.guard_type in (GuardType.RATE, GuardType.THRESHOLD):
                rate = self._calculate_rate(guard_name)
                if rate < guard.threshold * 0.8:  # 80% of threshold
                    state = GuardState.NORMAL
                    
        self._states[guard_name] = state
        return state != GuardState.TRIGGERED or not guard.strict
        
    async def _check_circuit(self, guard_name: str) -> bool:
        """Check circuit breaker state."""
        if guard_name not in self._circuits:
            return True
            
        state = self._circuit_states[guard_name]
        config = self._circuits[guard_name]
        
        if state == CircuitState.CLOSED:
            return True
            
        if state == CircuitState.OPEN:
            # Check if timeout has passed
            last_failure = self._last_triggered.get(guard_name, 0)
            if time.time() - last_failure >= config.timeout:
                self._circuit_states[guard_name] = CircuitState.HALF_OPEN
                logger.info(f"Circuit {guard_name}: OPEN -> HALF_OPEN")
                return True
            return False
            
        # HALF_OPEN - allow limited calls
        return True
        
    async def record_success(self, guard_name: str) -> None:
        """Record a successful operation for circuit breaker."""
        if guard_name not in self._circuits:
            return
            
        state = self._circuit_states[guard_name]
        config = self._circuits[guard_name]
        
        if state == CircuitState.HALF_OPEN:
            # Increment success counter
            success_count = self._metrics.get(f"{guard_name}_success", deque()).count
            if success_count >= config.success_threshold:
                self._circuit_states[guard_name] = CircuitState.CLOSED
                logger.info(f"Circuit {guard_name}: HALF_OPEN -> CLOSED")
                
    async def record_failure(self, guard_name: str) -> None:
        """Record a failed operation for circuit breaker."""
        if guard_name not in self._circuits:
            return
            
        config = self._circuits[guard_name]
        state = self._circuit_states[guard_name]
        
        # Record failure
        self._last_triggered[guard_name] = time.time()
        
        if state == CircuitState.HALF_OPEN:
            self._circuit_states[guard_name] = CircuitState.OPEN
            logger.warning(f"Circuit {guard_name}: HALF_OPEN -> OPEN (failure)")
        else:
            # Check failure count
            failures = len([
                e for e in self._events
                if e.guard_name == guard_name and e.event_type == "failure"
            ])
            
            if failures >= config.failure_threshold:
                self._circuit_states[guard_name] = CircuitState.OPEN
                logger.warning(f"Circuit {guard_name}: CLOSED -> OPEN ({failures} failures)")
                
        event = GuardEvent(
            guard_name=guard_name,
            event_type="failure",
            value=0,
            threshold=config.failure_threshold,
            timestamp=time.time(),
        )
        self._events.append(event)
        
    def _record_metric(self, guard_name: str, value: float) -> None:
        """Record a metric value with timestamp."""
        if guard_name not in self._metrics:
            self._metrics[guard_name] = deque()
            
        guard = self._guards[guard_name]
        now = time.time()
        
        # Add new value
        self._metrics[guard_name].append((now, value))
        
        # Remove old values outside window
        cutoff = now - guard.window_size
        while self._metrics[guard_name] and self._metrics[guard_name][0][0] < cutoff:
            self._metrics[guard_name].popleft()
            
    def _calculate_rate(self, guard_name: str) -> float:
        """Calculate rate within the window."""
        if guard_name not in self._metrics or not self._metrics[guard_name]:
            return 0.0
            
        guard = self._guards[guard_name]
        now = time.time()
        
        # Count events in window
        cutoff = now - guard.window_size
        count = sum(1 for t, _ in self._metrics[guard_name] if t >= cutoff)
        
        return count / guard.window_size if guard.window_size > 0 else 0
        
    async def _execute_action(
        self,
        guard: GuardConfig,
        value: float,
        metadata: Optional[Dict[str, Any]],
    ) -> None:
        """Execute guard action."""
        if guard.action == "log":
            pass  # Already logged
        elif guard.action == "callback" and guard.callback:
            try:
                if asyncio.iscoroutinefunction(guard.callback):
                    await guard.callback(guard.name, value, metadata)
                else:
                    guard.callback(guard.name, value, metadata)
            except Exception as e:
                logger.error(f"Guard callback error: {e}")
        elif guard.action == "block":
            logger.warning(f"Guard {guard.name} blocking operation")
        elif guard.action == "rollback":
            logger.warning(f"Guard {guard.name} triggering rollback")
            
    def get_state(self, guard_name: str) -> Optional[GuardState]:
        """Get current state of a guard."""
        return self._states.get(guard_name)
        
    def get_circuit_state(self, guard_name: str) -> Optional[CircuitState]:
        """Get circuit breaker state."""
        return self._circuit_states.get(guard_name)
        
    def get_metrics(self, guard_name: str) -> List[float]:
        """Get current metric values."""
        if guard_name not in self._metrics:
            return []
        return [v for _, v in self._metrics[guard_name]]
        
    def get_events(
        self,
        guard_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[GuardEvent]:
        """Get recent guard events."""
        events = list(self._events)
        if guard_name:
            events = [e for e in events if e.guard_name == guard_name]
        return events[-limit:]
        
    def get_stats(self) -> Dict[str, Any]:
        """Get guard rail statistics."""
        return {
            "total_guards": len(self._guards),
            "enabled": sum(1 for g in self._guards.values() if g.enabled),
            "triggered": sum(1 for s in self._states.values() if s == GuardState.TRIGGERED),
            "circuits": {
                name: state.value
                for name, state in self._circuit_states.items()
            },
            "events_count": len(self._events),
        }
        
    def enable(self, guard_name: str) -> bool:
        """Enable a guard."""
        if guard_name in self._guards:
            self._guards[guard_name].enabled = True
            self._states[guard_name] = GuardState.NORMAL
            return True
        return False
        
    def disable(self, guard_name: str) -> bool:
        """Disable a guard."""
        if guard_name in self._guards:
            self._guards[guard_name].enabled = False
            self._states[guard_name] = GuardState.DISABLED
            return True
        return False
