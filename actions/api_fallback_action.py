"""API Fallback Action Module.

Provides cascading fallback chains, circuit breaker,
and graceful degradation for API clients.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class FallbackConfig:
    """Fallback chain configuration."""
    name: str
    func: Callable[[], T]
    timeout: float = 5.0
    retry_count: int = 0
    retry_delay: float = 0.5


class APIFallbackAction:
    """Cascading fallback handler.

    Example:
        fallback = APIFallbackAction()

        fallback.add_fallback(FallbackConfig(
            name="primary",
            func=lambda: api.get_primary_data()
        ))

        fallback.add_fallback(FallbackConfig(
            name="cache",
            func=lambda: cache.get("data")
        ))

        fallback.add_fallback(FallbackConfig(
            name="default",
            func=lambda: {"data": []}
        ))

        result = await fallback.execute()
    """

    def __init__(self) -> None:
        self._fallbacks: List[FallbackConfig] = []
        self._default_result: Optional[Any] = None

    def add_fallback(self, config: FallbackConfig) -> "APIFallbackAction":
        """Add fallback to chain. Returns self for chaining."""
        self._fallbacks.append(config)
        return self

    def set_default_result(self, result: Any) -> "APIFallbackAction":
        """Set default result when all fallbacks fail."""
        self._default_result = result
        return self

    async def execute(self) -> Any:
        """Execute fallback chain.

        Returns:
            Result from first successful fallback or default
        """
        last_error: Optional[Exception] = None

        for fallback in self._fallbacks:
            try:
                result = await asyncio.wait_for(
                    self._call_func(fallback.func),
                    timeout=fallback.timeout
                )

                if result is not None:
                    logger.info(f"Fallback '{fallback.name}' succeeded")
                    return result

                last_error = ValueError(f"Fallback '{fallback.name}' returned None")

            except asyncio.TimeoutError:
                logger.warning(f"Fallback '{fallback.name}' timed out")
                last_error = asyncio.TimeoutError(f"Fallback '{fallback.name}' timeout")

            except Exception as e:
                logger.warning(f"Fallback '{fallback.name}' failed: {e}")
                last_error = e

            await asyncio.sleep(fallback.retry_delay * (fallback.retry_count + 1))

        if self._default_result is not None:
            logger.info("All fallbacks failed, returning default")
            return self._default_result

        raise last_error or ValueError("All fallbacks failed")

    async def _call_func(self, func: Callable) -> Any:
        """Call function (async or sync)."""
        if asyncio.iscoroutinefunction(func):
            return await func()
        return func()

    def clear(self) -> None:
        """Clear all fallbacks."""
        self._fallbacks.clear()
        self._default_result = None
