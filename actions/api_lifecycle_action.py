"""API Lifecycle Management Action Module.

Manages the full lifecycle of API resources: creation, warmup,
active serving, cool down, and retirement/shutdown.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import logging

logger = logging.getLogger(__name__)


class LifecyclePhase(Enum):
    """Phases of an API resource lifecycle."""
    INITIAL = "initial"
    WARMUP = "warmup"
    ACTIVE = "active"
    COOLDOWN = "cooldown"
    RETIRED = "retired"
    ERROR = "error"


@dataclass
class LifecycleTransition:
    """Record of a lifecycle phase transition."""
    from_phase: LifecyclePhase
    to_phase: LifecyclePhase
    timestamp: float
    reason: Optional[str] = None


class APILifecycleAction:
    """Manages lifecycle transitions for API resources.
    
    Tracks current phase, enforces valid transitions, and invokes
    callbacks registered for each phase entry/exit.
    """

    def __init__(self, resource_id: str) -> None:
        self.resource_id = resource_id
        self._phase: LifecyclePhase = LifecyclePhase.INITIAL
        self._history: List[LifecycleTransition] = []
        self._callbacks: Dict[LifecyclePhase, List[Callable[..., None]]] = {
            phase: [] for phase in LifecyclePhase
        }
        self._metadata: Dict[str, Any] = {}
        self._created_at = time.time()
        self._last_update = time.time()

    @property
    def phase(self) -> LifecyclePhase:
        """Current lifecycle phase."""
        return self._phase

    def transition(
        self,
        to_phase: LifecyclePhase,
        reason: Optional[str] = None,
    ) -> bool:
        """Transition to a new lifecycle phase.
        
        Args:
            to_phase: Target phase.
            reason: Optional reason for the transition.
        
        Returns:
            True if transition was valid and executed.
        """
        valid = self._is_valid_transition(self._phase, to_phase)
        if not valid:
            logger.warning(
                "Invalid lifecycle transition for %s: %s -> %s",
                self.resource_id, self._phase.value, to_phase.value,
            )
            return False

        prev = self._phase
        self._phase = to_phase
        self._last_update = time.time()
        self._history.append(LifecycleTransition(
            from_phase=prev, to_phase=to_phase,
            timestamp=time.time(), reason=reason,
        ))

        logger.info(
            "Lifecycle %s: %s -> %s (%s)",
            self.resource_id, prev.value, to_phase.value, reason or "normal",
        )
        self._run_callbacks(to_phase)
        return True

    def _is_valid_transition(
        self,
        from_phase: LifecyclePhase,
        to_phase: LifecyclePhase,
    ) -> bool:
        valid_map: Dict[LifecyclePhase, List[LifecyclePhase]] = {
            LifecyclePhase.INITIAL: [LifecyclePhase.WARMUP, LifecyclePhase.ERROR],
            LifecyclePhase.WARMUP: [LifecyclePhase.ACTIVE, LifecyclePhase.ERROR],
            LifecyclePhase.ACTIVE: [LifecyclePhase.COOLDOWN, LifecyclePhase.RETIRED, LifecyclePhase.ERROR],
            LifecyclePhase.COOLDOWN: [LifecyclePhase.ACTIVE, LifecyclePhase.RETIRED, LifecyclePhase.ERROR],
            LifecyclePhase.RETIRED: [],
            LifecyclePhase.ERROR: [LifecyclePhase.WARMUP, LifecyclePhase.RETIRED],
        }
        return to_phase in valid_map.get(from_phase, [])

    def on_enter(self, phase: LifecyclePhase, callback: Callable[..., None]) -> None:
        """Register a callback to run when entering a phase.
        
        Args:
            phase: Target lifecycle phase.
            callback: Callable invoked with no arguments.
        """
        self._callbacks[phase].append(callback)

    def _run_callbacks(self, phase: LifecyclePhase) -> None:
        for cb in self._callbacks.get(phase, []):
            try:
                cb()
            except Exception as exc:  # pragma: no cover
                logger.error("Lifecycle callback error: %s", exc)

    def warmup(self, warmup_fn: Optional[Callable[..., None]] = None) -> bool:
        """Transition to warmup phase.
        
        Args:
            warmup_fn: Optional warmup function to execute.
        
        Returns:
            True if transition succeeded.
        """
        if warmup_fn:
            try:
                warmup_fn()
            except Exception as exc:
                logger.error("Warmup failed for %s: %s", self.resource_id, exc)
                return self.transition(LifecyclePhase.ERROR, reason=str(exc))
        return self.transition(LifecyclePhase.WARMUP)

    def activate(self) -> bool:
        """Transition to active serving phase."""
        return self.transition(LifecyclePhase.ACTIVE)

    def cooldown(self) -> bool:
        """Transition to cooldown phase."""
        return self.transition(LifecyclePhase.COOLDOWN)

    def retire(self, reason: Optional[str] = None) -> bool:
        """Transition to retired phase.
        
        Args:
            reason: Optional retirement reason.
        """
        return self.transition(LifecyclePhase.RETIRED, reason=reason)

    def set_metadata(self, key: str, value: Any) -> None:
        """Store arbitrary metadata."""
        self._metadata[key] = value

    def get_info(self) -> Dict[str, Any]:
        """Get lifecycle state and metadata."""
        return {
            "resource_id": self.resource_id,
            "phase": self._phase.value,
            "created_at": self._created_at,
            "last_update": self._last_update,
            "transition_count": len(self._history),
            "metadata": dict(self._metadata),
        }
