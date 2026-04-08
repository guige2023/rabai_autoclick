"""Rate limit action module for RabAI AutoClick.

Provides rate limiting with token bucket, sliding window,
leaky bucket, and fixed window algorithms.
"""

import sys
import os
import time
import threading
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Configuration for a rate limiter."""
    capacity: int = 10
    refill_rate: float = 1.0
    refill_period: float = 1.0
    timeout: float = 0.0


class RateLimitAction(BaseAction):
    """Rate limiting for actions and API calls.
    
    Supports token bucket, sliding window, leaky bucket,
    and fixed window rate limiting algorithms.
    """
    action_type = "rate_limit"
    display_name = "限流控制"
    description = "限流控制：令牌桶/滑动窗口/漏桶/固定窗口"

    _limiters: Dict[str, Any] = {}
    _locks: Dict[str, threading.Lock] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check or consume rate limit tokens.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (check/consume/reset/get_status)
                - limiter_name: str, name of the rate limiter
                - algorithm: str (token_bucket/sliding_window/leaky_bucket/fixed_window)
                - capacity: int, max tokens/requests
                - refill_rate: float, tokens/requests per refill_period
                - refill_period: float, refill period in seconds
                - tokens: int, tokens to consume (default 1)
                - timeout: float, max time to wait for token (0 = no wait)
                - save_to_var: str
        
        Returns:
            ActionResult with rate limit check result.
        """
        operation = params.get('operation', 'check')
        limiter_name = params.get('limiter_name', 'default')
        algorithm = params.get('algorithm', 'token_bucket')
        capacity = params.get('capacity', 10)
        refill_rate = params.get('refill_rate', 1.0)
        refill_period = params.get('refill_period', 1.0)
        tokens = params.get('tokens', 1)
        timeout = params.get('timeout', 0.0)
        save_to_var = params.get('save_to_var', None)

        self._ensure_limiter(limiter_name, algorithm, capacity, refill_rate, refill_period)

        if operation == 'check':
            return self._check_limit(limiter_name, tokens, timeout, save_to_var)
        elif operation == 'consume':
            return self._consume(limiter_name, tokens, timeout, save_to_var)
        elif operation == 'reset':
            return self._reset_limiter(limiter_name, save_to_var)
        elif operation == 'get_status':
            return self._get_status(limiter_name, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _ensure_limiter(
        self, name: str, algorithm: str, capacity: int,
        refill_rate: float, refill_period: float
    ) -> None:
        """Ensure limiter exists."""
        if name not in self._limiters:
            with threading.Lock():
                if name not in self._limiters:
                    self._limiters[name] = {
                        'algorithm': algorithm,
                        'capacity': capacity,
                        'refill_rate': refill_rate,
                        'refill_period': refill_period,
                        'state': self._create_state(algorithm, capacity),
                        'created_at': time.time(),
                    }
                    self._locks[name] = threading.Lock()

    def _create_state(self, algorithm: str, capacity: int) -> Dict:
        """Create initial state for algorithm."""
        if algorithm == 'token_bucket':
            return {'tokens': float(capacity), 'last_refill': time.time()}
        elif algorithm == 'sliding_window':
            return {'requests': deque(), 'window_size': 60}
        elif algorithm == 'leaky_bucket':
            return {'level': 0.0, 'last_update': time.time(), 'leak_rate': 1.0}
        elif algorithm == 'fixed_window':
            return {'count': 0, 'window_start': time.time(), 'window_size': 60}
        return {}

    def _check_limit(
        self, name: str, tokens: int, timeout: float, save_to_var: Optional[str]
    ) -> ActionResult:
        """Check if request would be allowed."""
        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter '{name}' not found")

        deadline = time.time() + timeout if timeout > 0 else None

        while True:
            allowed, reason = self._is_allowed(limiter, tokens)
            if allowed:
                break
            if timeout <= 0 or (deadline and time.time() >= deadline):
                return ActionResult(
                    success=False,
                    message=f"Rate limited: {reason}",
                    data={'allowed': False, 'reason': reason}
                )
            time.sleep(0.05)

        status = self._get_limiter_status(limiter)
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = status

        return ActionResult(
            success=True,
            message="Rate limit check passed",
            data={'allowed': True, **status}
        )

    def _consume(
        self, name: str, tokens: int, timeout: float, save_to_var: Optional[str]
    ) -> ActionResult:
        """Consume tokens from the limiter."""
        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter '{name}' not found")

        deadline = time.time() + timeout if timeout > 0 else None

        while True:
            allowed, reason = self._try_consume(limiter, tokens)
            if allowed:
                break
            if timeout <= 0 or (deadline and time.time() >= deadline):
                return ActionResult(
                    success=False,
                    message=f"Rate limited: {reason}",
                    data={'allowed': False, 'reason': reason}
                )
            time.sleep(0.05)

        status = self._get_limiter_status(limiter)
        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = status

        return ActionResult(
            success=True,
            message=f"Consumed {tokens} token(s)",
            data={'allowed': True, **status}
        )

    def _is_allowed(self, limiter: Dict, tokens: int) -> Tuple[bool, str]:
        """Check if tokens would be allowed."""
        algorithm = limiter['algorithm']
        state = limiter['state']

        if algorithm == 'token_bucket':
            self._refill_token_bucket(limiter)
            available = state['tokens']
            return available >= tokens, f"Only {available:.1f} tokens available"

        elif algorithm == 'sliding_window':
            now = time.time()
            window_size = state['window_size']
            cutoff = now - window_size
            while state['requests'] and state['requests'][0] <= cutoff:
                state['requests'].popleft()
            count = len(state['requests'])
            capacity = limiter['capacity']
            return count + tokens <= capacity, f"Window has {count}/{capacity} requests"

        elif algorithm == 'leaky_bucket':
            self._leak_leaky_bucket(limiter)
            level = state['level']
            capacity = limiter['capacity']
            return level + tokens <= capacity, f"Leaky bucket level {level:.1f}/{capacity}"

        elif algorithm == 'fixed_window':
            now = time.time()
            window_size = limiter['refill_period']
            if now - state['window_start'] >= window_size:
                state['count'] = 0
                state['window_start'] = now
            count = state['count']
            capacity = limiter['capacity']
            return count + tokens <= capacity, f"Window has {count}/{capacity} requests"

        return True, "ok"

    def _try_consume(self, limiter: Dict, tokens: int) -> Tuple[bool, str]:
        """Try to consume tokens."""
        allowed, reason = self._is_allowed(limiter, tokens)
        if not allowed:
            return False, reason

        state = limiter['state']
        algorithm = limiter['algorithm']

        if algorithm == 'token_bucket':
            state['tokens'] -= tokens
        elif algorithm == 'sliding_window':
            for _ in range(tokens):
                state['requests'].append(time.time())
        elif algorithm == 'leaky_bucket':
            state['level'] += tokens
        elif algorithm == 'fixed_window':
            state['count'] += tokens

        return True, "ok"

    def _refill_token_bucket(self, limiter: Dict) -> None:
        """Refill token bucket based on elapsed time."""
        state = limiter['state']
        now = time.time()
        elapsed = now - state['last_refill']
        refill_amount = elapsed * limiter['refill_rate']
        state['tokens'] = min(limiter['capacity'], state['tokens'] + refill_amount)
        state['last_refill'] = now

    def _leak_leaky_bucket(self, limiter: Dict) -> None:
        """Leak from leaky bucket based on elapsed time."""
        state = limiter['state']
        now = time.time()
        elapsed = now - state['last_update']
        leak_amount = elapsed * state['leak_rate']
        state['level'] = max(0.0, state['level'] - leak_amount)
        state['last_update'] = now

    def _get_limiter_status(self, limiter: Dict) -> Dict[str, Any]:
        """Get current limiter status."""
        algorithm = limiter['algorithm']
        state = limiter['state']

        if algorithm == 'token_bucket':
            self._refill_token_bucket(limiter)
            return {
                'tokens': round(state['tokens'], 2),
                'capacity': limiter['capacity'],
                'algorithm': algorithm
            }
        elif algorithm == 'sliding_window':
            cutoff = time.time() - state['window_size']
            while state['requests'] and state['requests'][0] <= cutoff:
                state['requests'].popleft()
            return {
                'requests_in_window': len(state['requests']),
                'capacity': limiter['capacity'],
                'algorithm': algorithm
            }
        elif algorithm == 'leaky_bucket':
            self._leak_leaky_bucket(limiter)
            return {
                'level': round(state['level'], 2),
                'capacity': limiter['capacity'],
                'algorithm': algorithm
            }
        elif algorithm == 'fixed_window':
            return {
                'count': state['count'],
                'capacity': limiter['capacity'],
                'window_start': state['window_start'],
                'algorithm': algorithm
            }
        return {}

    def _reset_limiter(self, name: str, save_to_var: Optional[str]) -> ActionResult:
        """Reset a limiter to initial state."""
        if name not in self._limiters:
            return ActionResult(success=False, message=f"Limiter '{name}' not found")

        limiter = self._limiters[name]
        limiter['state'] = self._create_state(limiter['algorithm'], limiter['capacity'])

        return ActionResult(success=True, message=f"Reset limiter '{name}'")

    def _get_status(self, name: str, save_to_var: Optional[str]) -> ActionResult:
        """Get full status of a limiter."""
        limiter = self._limiters.get(name)
        if not limiter:
            return ActionResult(success=False, message=f"Limiter '{name}' not found")

        status = self._get_limiter_status(limiter)
        status['name'] = name
        status['created_at'] = limiter['created_at']

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = status

        return ActionResult(success=True, message=f"Status for '{name}'", data=status)

    def get_required_params(self) -> List[str]:
        return ['operation', 'limiter_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'algorithm': 'token_bucket',
            'capacity': 10,
            'refill_rate': 1.0,
            'refill_period': 1.0,
            'tokens': 1,
            'timeout': 0.0,
            'save_to_var': None,
        }
