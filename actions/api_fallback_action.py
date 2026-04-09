"""
API Fallback Action Module.

Fallback chain for API requests with degradation handling.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine, Dict, List, Optional, TypeVar


T = TypeVar("T")


class FallbackError(Exception):
    """All fallbacks failed."""
    pass


@dataclass
class FallbackConfig:
    """Configuration for fallback behavior."""
    timeout_seconds: float = 5.0
    max_retries: int = 2
    retry_delay_seconds: float = 1.0


class ApiFallbackAction:
    """
    Fallback chain for API requests.

    Tries handlers in sequence until one succeeds.
    """

    def __init__(
        self,
        config: Optional[FallbackConfig] = None,
    ) -> None:
        self.config = config or FallbackConfig()
        self._handlers: List[Callable[..., Coroutine[Any, Any, T]]] = []
        self._stats: Dict[str, int] = {}

    def add_handler(
        self,
        handler: Callable[..., Coroutine[Any, Any, T]],
    ) -> "ApiFallbackAction":
        """
        Add a handler to the fallback chain.

        Args:
            handler: Async function to try

        Returns:
            Self for chaining
        """
        self._handlers.append(handler)
        return self

    async def execute(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute fallback chain.

        Tries each handler in order until one succeeds.

        Args:
            *args: Positional arguments for handlers
            **kwargs: Keyword arguments for handlers

        Returns:
            Result from first successful handler

        Raises:
            FallbackError: If all handlers fail
        """
        errors = []

        for i, handler in enumerate(self._handlers):
            handler_name = f"{handler.__name__}_{i}"

            for attempt in range(self.config.max_retries + 1):
                try:
                    result = await asyncio.wait_for(
                        handler(*args, **kwargs),
                        timeout=self.config.timeout_seconds,
                    )
                    self._record_success(handler_name)
                    return result

                except asyncio.TimeoutError:
                    error = f"Timeout on {handler_name}"
                    errors.append(error)
                    self._record_failure(handler_name)

                except Exception as e:
                    error = f"{type(e).__name__}: {e}"
                    errors.append(f"{handler_name}: {error}")
                    self._record_failure(handler_name)

                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay_seconds)

        raise FallbackError(f"All {len(self._handlers)} handlers failed: {errors}")

    def _record_success(self, handler_name: str) -> None:
        """Record successful call."""
        self._stats[f"{handler_name}_success"] = self._stats.get(f"{handler_name}_success", 0) + 1

    def _record_failure(self, handler_name: str) -> None:
        """Record failed call."""
        self._stats[f"{handler_name}_failure"] = self._stats.get(f"{handler_name}_failure", 0) + 1

    def get_stats(self) -> Dict[str, Any]:
        """Get fallback statistics."""
        return {
            "handlers_count": len(self._handlers),
            "stats": self._stats,
        }

    def clear_handlers(self) -> None:
        """Clear all handlers."""
        self._handlers.clear()

    def clear_stats(self) -> None:
        """Clear statistics."""
        self._stats.clear()


@dataclass
class DegradationLevel:
    """A degradation level configuration."""
    name: str
    enabled_handlers: int
    description: str = ""


class ApiDegradationAction:
    """
    Progressive API degradation.

    Starts with full feature set, degrades gracefully under load.
    """

    def __init__(self) -> None:
        self._levels: List[DegradationLevel] = []
        self._current_level: int = 0
        self._fallback = ApiFallbackAction()

    def add_level(
        self,
        name: str,
        enabled_handlers: int,
        description: str = "",
    ) -> "ApiDegradationAction":
        """
        Add a degradation level.

        Args:
            name: Level name
            enabled_handlers: Number of handlers to enable
            description: Human-readable description

        Returns:
            Self for chaining
        """
        self._levels.append(DegradationLevel(
            name=name,
            enabled_handlers=enabled_handlers,
            description=description,
        ))
        return self

    def set_level(self, level: int) -> bool:
        """
        Set current degradation level.

        Args:
            level: Level index

        Returns:
            True if level was set
        """
        if 0 <= level < len(self._levels):
            self._current_level = level
            return True
        return False

    def get_current_level(self) -> Optional[DegradationLevel]:
        """Get current degradation level."""
        if self._levels:
            return self._levels[self._current_level]
        return None

    def degrade(self) -> bool:
        """
        Increase degradation by one level.

        Returns:
            True if degraded, False if at max
        """
        if self._current_level < len(self._levels) - 1:
            self._current_level += 1
            return True
        return False

    def recover(self) -> bool:
        """
        Decrease degradation by one level.

        Returns:
            True if recovered, False if at min
        """
        if self._current_level > 0:
            self._current_level -= 1
            return True
        return False
