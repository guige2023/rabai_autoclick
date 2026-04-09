"""
API Fallback Action Module.

Provides degradation handling with fallback strategies,
circuit breaking, and graceful degradation.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class FallbackStrategy(Enum):
    """Fallback strategies."""
    RETURN_DEFAULT = "return_default"
    CALL_ANOTHER = "call_another"
    CACHE_STALE = "cache_stale"
    NULL = "null"
    EXCEPTION = "exception"


@dataclass
class FallbackConfig:
    """Fallback configuration."""
    strategy: FallbackStrategy = FallbackStrategy.RETURN_DEFAULT
    default_value: Any = None
    fallback_func: Optional[Callable] = None
    cache_func: Optional[Callable] = None
    timeout: float = 5.0


@dataclass
class FallbackResult:
    """Result with fallback tracking."""
    success: bool
    value: Any = None
    source: str = "primary"
    error: Optional[Exception] = None
    fallback_used: bool = False


class APIFallbackAction:
    """
    API fallback with graceful degradation.

    Example:
        fallback = APIFallbackAction(
            strategy=FallbackStrategy.CACHE_STALE,
            cache_func=get_cached_response
        )

        result = await fallback.execute(
            primary_func=lambda: api.get(url),
            fallback_func=lambda: backup_api.get(url)
        )
    """

    def __init__(
        self,
        strategy: FallbackStrategy = FallbackStrategy.RETURN_DEFAULT,
        default_value: Any = None
    ):
        self.config = FallbackConfig(
            strategy=strategy,
            default_value=default_value
        )

    async def execute(
        self,
        primary_func: Callable[[], T],
        fallback_func: Optional[Callable[[], T]] = None,
        cache_func: Optional[Callable[[], Any]] = None,
        default_value: Optional[Any] = None
    ) -> FallbackResult:
        """Execute with fallback."""
        result = FallbackResult(success=False)

        try:
            if asyncio.iscoroutinefunction(primary_func):
                value = await asyncio.wait_for(
                    primary_func(),
                    timeout=self.config.timeout
                )
            else:
                value = await asyncio.wait_for(
                    asyncio.to_thread(primary_func),
                    timeout=self.config.timeout
                )

            result.success = True
            result.value = value
            result.source = "primary"
            return result

        except asyncio.TimeoutError:
            result.error = TimeoutError("Primary function timed out")

        except Exception as e:
            result.error = e

        if fallback_func:
            result.fallback_used = True
            try:
                if asyncio.iscoroutinefunction(fallback_func):
                    value = await asyncio.wait_for(
                        fallback_func(),
                        timeout=self.config.timeout
                    )
                else:
                    value = await asyncio.wait_for(
                        asyncio.to_thread(fallback_func),
                        timeout=self.config.timeout
                    )

                result.success = True
                result.value = value
                result.source = "fallback"
                return result

            except Exception as e:
                result.error = e

        if cache_func:
            result.fallback_used = True
            try:
                cached = await asyncio.wait_for(
                    asyncio.to_thread(cache_func),
                    timeout=self.config.timeout
                )
                if cached is not None:
                    result.success = True
                    result.value = cached
                    result.source = "cache"
                    return result
            except Exception:
                pass

        if self.config.strategy == FallbackStrategy.RETURN_DEFAULT:
            result.success = True
            result.value = default_value or self.config.default_value
            result.source = "default"

        elif self.config.strategy == FallbackStrategy.NULL:
            result.success = True
            result.value = None
            result.source = "null"

        elif self.config.strategy == FallbackStrategy.EXCEPTION:
            result.success = False

        return result

    def set_fallback_func(self, func: Callable) -> None:
        """Set default fallback function."""
        self.config.fallback_func = func

    def set_cache_func(self, func: Callable) -> None:
        """Set cache retrieval function."""
        self.config.cache_func = func
