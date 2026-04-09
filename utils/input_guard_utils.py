"""
Input guard utilities for safe automation.

Guard against unintended input during automation runs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional
from enum import Enum, auto


class GuardState(Enum):
    """State of an input guard."""
    INACTIVE = auto()
    ARMED = auto()
    TRIGGERED = auto()
    DISARMED = auto()


@dataclass
class GuardEvent:
    """Event from an input guard."""
    timestamp: float
    guard_id: str
    state: GuardState
    details: dict = field(default_factory=dict)


class InputGuard:
    """Guard against specific input patterns."""
    
    def __init__(self, guard_id: str):
        self.guard_id = guard_id
        self._state = GuardState.INACTIVE
        self._triggered_callbacks: list[Callable[[GuardEvent], None]] = []
        self._armed_time: float = 0
    
    def arm(self) -> None:
        """Arm the guard."""
        self._state = GuardState.ARMED
        self._armed_time = time.time()
    
    def disarm(self) -> None:
        """Disarm the guard."""
        self._state = GuardState.DISARMED
    
    def trigger(self, details: Optional[dict] = None) -> None:
        """Trigger the guard."""
        self._state = GuardState.TRIGGERED
        
        event = GuardEvent(
            timestamp=time.time(),
            guard_id=self.guard_id,
            state=self._state,
            details=details or {}
        )
        
        for callback in self._triggered_callbacks:
            callback(event)
    
    def on_triggered(self, callback: Callable[[GuardEvent], None]) -> None:
        """Register callback for trigger events."""
        self._triggered_callbacks.append(callback)
    
    def get_state(self) -> GuardState:
        """Get current guard state."""
        return self._state
    
    def is_armed(self) -> bool:
        """Check if guard is armed."""
        return self._state == GuardState.ARMED
    
    def get_armed_duration(self) -> float:
        """Get time since guard was armed."""
        if self._state != GuardState.ARMED:
            return 0
        return time.time() - self._armed_time


class RegionGuard(InputGuard):
    """Guard that triggers when input occurs in a region."""
    
    def __init__(self, guard_id: str, x: int, y: int, width: int, height: int):
        super().__init__(guard_id)
        self.x = x
        self.y = y
        self.width = width
        self.height = height
    
    def check_input(self, x: float, y: float) -> bool:
        """Check if input is within guarded region."""
        if not self.is_armed():
            return False
        
        if self.x <= x <= self.x + self.width and self.y <= y <= self.y + self.height:
            self.trigger({"x": x, "y": y})
            return True
        
        return False


class TimeoutGuard(InputGuard):
    """Guard that triggers after a timeout."""
    
    def __init__(self, guard_id: str, timeout_seconds: float):
        super().__init__(guard_id)
        self.timeout_seconds = timeout_seconds
    
    def check(self) -> bool:
        """Check if timeout has been reached."""
        if not self.is_armed():
            return False
        
        if self.get_armed_duration() >= self.timeout_seconds:
            self.trigger({"duration": self.timeout_seconds})
            return True
        
        return False


class InputGuardManager:
    """Manage multiple input guards."""
    
    def __init__(self):
        self._guards: dict[str, InputGuard] = {}
        self._global_callbacks: list[Callable[[GuardEvent], None]] = []
    
    def register_guard(self, guard: InputGuard) -> None:
        """Register a guard."""
        self._guards[guard.guard_id] = guard
        guard.on_triggered(self._on_guard_triggered)
    
    def unregister_guard(self, guard_id: str) -> None:
        """Unregister a guard."""
        if guard_id in self._guards:
            del self._guards[guard_id]
    
    def get_guard(self, guard_id: str) -> Optional[InputGuard]:
        """Get a guard by ID."""
        return self._guards.get(guard_id)
    
    def arm_all(self) -> None:
        """Arm all registered guards."""
        for guard in self._guards.values():
            guard.arm()
    
    def disarm_all(self) -> None:
        """Disarm all registered guards."""
        for guard in self._guards.values():
            guard.disarm()
    
    def check_all(self) -> list[GuardEvent]:
        """Check all guards and return triggered events."""
        triggered = []
        
        for guard in self._guards.values():
            if isinstance(guard, TimeoutGuard):
                if guard.check():
                    triggered.append(GuardEvent(
                        timestamp=time.time(),
                        guard_id=guard.guard_id,
                        state=GuardState.TRIGGERED
                    ))
        
        return triggered
    
    def on_global_trigger(self, callback: Callable[[GuardEvent], None]) -> None:
        """Register global callback for any guard trigger."""
        self._global_callbacks.append(callback)
    
    def _on_guard_triggered(self, event: GuardEvent) -> None:
        """Handle guard trigger event."""
        for callback in self._global_callbacks:
            callback(event)
