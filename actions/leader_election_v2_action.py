"""Leader Election V2 Action Module.

Provides leader election with
lease-based locking.
"""

import time
import threading
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LeaderLease:
    """Leader lease information."""
    leader_id: str
    term: int
    acquired_at: float
    expires_at: float


class LeaderElectionManager:
    """Manages leader election."""

    def __init__(self):
        self._leader: Optional[LeaderLease] = None
        self._lock = threading.RLock()

    def try_acquire_leadership(
        self,
        candidate_id: str,
        lease_duration: float = 10.0
    ) -> bool:
        """Try to become leader."""
        with self._lock:
            now = time.time()

            if self._leader:
                if now < self._leader.expires_at:
                    return False

            term = (self._leader.term + 1) if self._leader else 1

            self._leader = LeaderLease(
                leader_id=candidate_id,
                term=term,
                acquired_at=now,
                expires_at=now + lease_duration
            )
            return True

    def renew_lease(
        self,
        candidate_id: str,
        lease_duration: float = 10.0
    ) -> bool:
        """Renew leadership lease."""
        with self._lock:
            if not self._leader:
                return False

            if self._leader.leader_id != candidate_id:
                return False

            now = time.time()
            self._leader.expires_at = now + lease_duration
            return True

    def get_leader(self) -> Optional[Dict]:
        """Get current leader."""
        with self._lock:
            if not self._leader:
                return None

            return {
                "leader_id": self._leader.leader_id,
                "term": self._leader.term,
                "acquired_at": self._leader.acquired_at,
                "expires_at": self._leader.expires_at,
                "is_active": time.time() < self._leader.expires_at
            }

    def release_leadership(self, candidate_id: str) -> bool:
        """Release leadership."""
        with self._lock:
            if not self._leader:
                return False

            if self._leader.leader_id != candidate_id:
                return False

            self._leader = None
            return True


class LeaderElectionV2Action(BaseAction):
    """Action for leader election operations."""

    def __init__(self):
        super().__init__("leader_election_v2")
        self._manager = LeaderElectionManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute leader election action."""
        try:
            operation = params.get("operation", "acquire")

            if operation == "acquire":
                return self._acquire(params)
            elif operation == "renew":
                return self._renew(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "release":
                return self._release(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _acquire(self, params: Dict) -> ActionResult:
        """Acquire leadership."""
        success = self._manager.try_acquire_leadership(
            candidate_id=params.get("candidate_id", ""),
            lease_duration=params.get("lease_duration", 10)
        )
        return ActionResult(success=success)

    def _renew(self, params: Dict) -> ActionResult:
        """Renew lease."""
        success = self._manager.renew_lease(
            candidate_id=params.get("candidate_id", ""),
            lease_duration=params.get("lease_duration", 10)
        )
        return ActionResult(success=success)

    def _get(self, params: Dict) -> ActionResult:
        """Get leader."""
        leader = self._manager.get_leader()
        if not leader:
            return ActionResult(success=False, message="No leader")
        return ActionResult(success=True, data=leader)

    def _release(self, params: Dict) -> ActionResult:
        """Release leadership."""
        success = self._manager.release_leadership(params.get("candidate_id", ""))
        return ActionResult(success=success)
