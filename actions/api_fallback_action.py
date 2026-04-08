"""
API Fallback Action Module.

Provides cascading fallback mechanisms for API requests with automatic
 failover to backup endpoints and cached responses.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FallbackStrategy(Enum):
    """Strategy for selecting fallback targets."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class FallbackTarget:
    """A single fallback target endpoint."""
    name: str
    url: str
    priority: int = 1
    timeout: float = 5.0
    enabled: bool = True


@dataclass
class FallbackResult:
    """Result of a fallback operation."""
    success: bool
    data: Optional[Any] = None
    target_used: Optional[str] = None
    attempts: int = 0
    latency_ms: float = 0.0
    error: Optional[str] = None


class APIFallbackAction:
    """
    Cascading API fallback handler.

    Automatically tries multiple endpoints in priority order,
    falling back to cached responses when all endpoints fail.

    Example:
        handler = APIFallbackAction([
            FallbackTarget("primary", "https://api.example.com"),
            FallbackTarget("backup", "https://backup.example.com"),
        ])
        result = await handler.execute("/data", {"key": "value"})
    """

    def __init__(
        self,
        targets: list[FallbackTarget],
        cache: Optional[dict[str, Any]] = None,
        strategy: FallbackStrategy = FallbackStrategy.SEQUENTIAL,
        max_attempts: int = 3,
    ) -> None:
        self.targets = sorted(targets, key=lambda t: t.priority)
        self.cache = cache or {}
        self.strategy = strategy
        self.max_attempts = max_attempts
        self._circuit_state: dict[str, bool] = {}

    async def execute(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        method: str = "GET",
        headers: Optional[dict[str, str]] = None,
    ) -> FallbackResult:
        """Execute request with automatic fallback."""
        start_time = time.monotonic()
        attempts = 0
        last_error = None

        for target in self.targets:
            if not target.enabled:
                continue
            if self._circuit_state.get(target.name, False):
                logger.debug(f"Circuit open for {target.name}, skipping")
                continue

            for attempt in range(self.max_attempts):
                attempts += 1
                try:
                    result = await self._try_target(
                        target, path, params, method, headers
                    )
                    if result.success:
                        return result
                    last_error = result.error
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"Attempt {attempt} failed for {target.name}: {e}")

            self._circuit_state[target.name] = True

        cache_key = f"{method}:{path}"
        if cache_key in self.cache:
            logger.info(f"All targets failed, returning cached response for {path}")
            return FallbackResult(
                success=True,
                data=self.cache[cache_key],
                target_used="cache",
                attempts=attempts,
                latency_ms=(time.monotonic() - start_time) * 1000,
            )

        return FallbackResult(
            success=False,
            attempts=attempts,
            latency_ms=(time.monotonic() - start_time) * 1000,
            error=last_error or "All fallback targets exhausted",
        )

    async def _try_target(
        self,
        target: FallbackTarget,
        path: str,
        params: Optional[dict[str, Any]],
        method: str,
        headers: Optional[dict[str, str]],
    ) -> FallbackResult:
        """Try a single target with timeout."""
        start = time.monotonic()
        url = f"{target.url.rstrip('/')}/{path.lstrip('/')}"

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                kwargs: dict[str, Any] = {"timeout": aiohttp.ClientTimeout(total=target.timeout)}
                if params and method in ("POST", "PUT", "PATCH"):
                    kwargs["json"] = params
                elif params:
                    kwargs["params"] = params
                if headers:
                    kwargs["headers"] = headers

                async with session.request(method, url, **kwargs) as resp:
                    data = await resp.json()
                    return FallbackResult(
                        success=resp.status < 400,
                        data=data,
                        target_used=target.name,
                        attempts=1,
                        latency_ms=(time.monotonic() - start) * 1000,
                    )
        except asyncio.TimeoutError:
            return FallbackResult(
                success=False,
                target_used=target.name,
                attempts=1,
                latency_ms=(time.monotonic() - start) * 1000,
                error=f"Timeout after {target.timeout}s",
            )
        except Exception as e:
            return FallbackResult(
                success=False,
                target_used=target.name,
                attempts=1,
                latency_ms=(time.monotonic() - start) * 1000,
                error=str(e),
            )

    def cache_response(self, method: str, path: str, data: Any) -> None:
        """Cache a response for fallback use."""
        key = f"{method}:{path}"
        self.cache[key] = data

    def reset_circuit(self, target_name: Optional[str] = None) -> None:
        """Reset circuit breaker for a target or all targets."""
        if target_name:
            self._circuit_state[target_name] = False
        else:
            self._circuit_state.clear()
