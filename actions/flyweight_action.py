"""Flyweight Pattern Action Module.

Provides flyweight pattern for memory
optimization through sharing.
"""

import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Flyweight:
    """Shared flyweight object."""
    flyweight_id: str
    intrinsic_state: Any
    reference_count: int = 0


class FlyweightFactory:
    """Factory for flyweight objects."""

    def __init__(self):
        self._flyweights: Dict[str, Flyweight] = {}

    def get_flyweight(
        self,
        key: str,
        intrinsic_state: Any
    ) -> Flyweight:
        """Get or create flyweight."""
        if key in self._flyweights:
            fw = self._flyweights[key]
            fw.reference_count += 1
            return fw

        fw = Flyweight(
            flyweight_id=key,
            intrinsic_state=intrinsic_state,
            reference_count=1
        )

        self._flyweights[key] = fw
        return fw

    def release_flyweight(self, key: str) -> bool:
        """Release flyweight reference."""
        if key not in self._flyweights:
            return False

        fw = self._flyweights[key]
        fw.reference_count -= 1

        if fw.reference_count <= 0:
            del self._flyweights[key]

        return True

    def get_stats(self) -> Dict:
        """Get factory statistics."""
        total_refs = sum(fw.reference_count for fw in self._flyweights.values())
        return {
            "flyweight_count": len(self._flyweights),
            "total_references": total_refs
        }


class FlyweightPatternAction(BaseAction):
    """Action for flyweight pattern operations."""

    def __init__(self):
        super().__init__("flyweight")
        self._factory = FlyweightFactory()

    def execute(self, params: Dict) -> ActionResult:
        """Execute flyweight action."""
        try:
            operation = params.get("operation", "get")

            if operation == "get":
                return self._get(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get(self, params: Dict) -> ActionResult:
        """Get flyweight."""
        fw = self._factory.get_flyweight(
            params.get("key", ""),
            params.get("intrinsic_state")
        )
        return ActionResult(success=True, data={
            "flyweight_id": fw.flyweight_id,
            "reference_count": fw.reference_count
        })

    def _release(self, params: Dict) -> ActionResult:
        """Release flyweight."""
        success = self._factory.release_flyweight(params.get("key", ""))
        return ActionResult(success=success)

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        return ActionResult(success=True, data=self._factory.get_stats())
