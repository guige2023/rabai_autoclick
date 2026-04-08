"""API Throttle Action Module.

Implements API throttling with adaptive rate limits
and burst handling.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ThrottleConfig:
    """Throttle configuration."""
    requests_per_second: float = 10.0
    burst_size: int = 20
    adaptive: bool = False


class APIThrottleAction(BaseAction):
    """
    API throttling with adaptive rate limits.

    Implements throttling with burst handling
    and adaptive rate adjustment.

    Example:
        throttle = APIThrottleAction()
        result = throttle.execute(ctx, {"action": "check", "identity": "user-123"})
    """
    action_type = "api_throttle"
    display_name = "API节流"
    description = "API自适应节流和突发处理"

    def __init__(self) -> None:
        super().__init__()
        self._tokens: Dict[str, float] = {}
        self._last_refill: Dict[str, float] = {}
        self._config = ThrottleConfig()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "check":
                return self._check_throttle(params)
            elif action == "set_config":
                return self._set_config(params)
            elif action == "reset":
                return self._reset(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle error: {str(e)}")

    def _check_throttle(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity", "default")
        cost = params.get("cost", 1.0)

        allowed, remaining = self._consume_tokens(identity, cost)

        if allowed:
            return ActionResult(success=True, message="Request allowed", data={"allowed": True, "remaining": remaining})
        else:
            return ActionResult(success=False, message="Throttled", data={"allowed": False, "retry_after_ms": 100})

    def _consume_tokens(self, identity: str, cost: float) -> tuple[bool, float]:
        now = time.time()

        if identity not in self._tokens:
            self._tokens[identity] = self._config.burst_size
            self._last_refill[identity] = now

        tokens = self._tokens[identity]
        last_refill = self._last_refill[identity]

        elapsed = now - last_refill
        refill_amount = elapsed * self._config.requests_per_second
        tokens = min(self._config.burst_size, tokens + refill_amount)

        if tokens >= cost:
            self._tokens[identity] = tokens - cost
            self._last_refill[identity] = now
            return True, self._tokens[identity]
        else:
            self._tokens[identity] = tokens
            self._last_refill[identity] = now
            return False, 0.0

    def _set_config(self, params: Dict[str, Any]) -> ActionResult:
        rps = params.get("requests_per_second", 10.0)
        burst = params.get("burst_size", 20)
        adaptive = params.get("adaptive", False)

        self._config = ThrottleConfig(requests_per_second=rps, burst_size=burst, adaptive=adaptive)

        return ActionResult(success=True, message="Throttle config updated")

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        identity = params.get("identity")
        if identity:
            if identity in self._tokens:
                del self._tokens[identity]
            return ActionResult(success=True, message=f"Reset throttle for {identity}")
        else:
            self._tokens.clear()
            self._last_refill.clear()
            return ActionResult(success=True, message="All throttles reset")
