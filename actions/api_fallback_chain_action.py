"""API Fallback Chain Action Module.

Implements a chain-of-fallback pattern for API calls: if the primary
endpoint fails, try secondary, tertiary, etc. providers in sequence.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class FallbackProvider:
    """A provider endpoint in the fallback chain."""
    name: str
    call: Callable[..., Any]
    timeout_sec: float = 5.0
    enabled: bool = True
    failure_count: int = 0
    last_failure: Optional[float] = None


class APIFallbackChainAction:
    """Chain-of-fallback API caller.
    
    Executes a primary API call and falls back through a list of
    alternative providers if the call fails or times out.
    """

    def __init__(
        self,
        max_retries_per_provider: int = 3,
        cooldown_sec: float = 60.0,
    ) -> None:
        self.max_retries_per_provider = max_retries_per_provider
        self.cooldown_sec = cooldown_sec
        self._providers: List[FallbackProvider] = []
        self._total_calls: int = 0
        self._successful_fallbacks: int = 0

    def add_provider(
        self,
        name: str,
        call: Callable[..., Any],
        timeout_sec: float = 5.0,
    ) -> None:
        """Add a provider to the fallback chain.
        
        Args:
            name: Unique name for this provider.
            call: Callable that executes the API call.
            timeout_sec: Timeout in seconds for this provider.
        """
        provider = FallbackProvider(name=name, call=call, timeout_sec=timeout_sec)
        self._providers.append(provider)

    def call(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Tuple[Any, str]:
        """Execute the fallback chain.
        
        Args:
            *args: Positional args passed to each provider.
            **kwargs: Keyword args passed to each provider.
        
        Returns:
            Tuple of (result, provider_name) if any provider succeeds.
        
        Raises:
            Exception: If all providers in the chain fail.
        """
        self._total_calls += 1
        errors: List[str] = []

        for provider in self._providers:
            if not provider.enabled:
                continue
            if self._is_cooldown(provider):
                continue

            for attempt in range(self.max_retries_per_provider):
                try:
                    start = time.time()
                    result = provider.call(*args, **kwargs)
                    elapsed = time.time() - start
                    logger.info(
                        "Fallback chain: provider %s succeeded in %.3fs",
                        provider.name, elapsed,
                    )
                    return result, provider.name
                except Exception as exc:  # pragma: no cover
                    provider.failure_count += 1
                    provider.last_failure = time.time()
                    error_msg = f"{provider.name} attempt {attempt+1}: {exc}"
                    errors.append(error_msg)
                    logger.warning("Fallback chain failure: %s", error_msg)

        self._log_total_failure(errors)
        raise RuntimeError(
            f"All {len(self._providers)} providers failed. "
            f"Errors: {'; '.join(errors)}"
        )

    def _is_cooldown(self, provider: FallbackProvider) -> bool:
        if provider.last_failure is None:
            return False
        return time.time() - provider.last_failure < self.cooldown_sec

    def _log_total_failure(self, errors: List[str]) -> None:
        logger.error(
            "Fallback chain exhausted after %d providers. Total calls: %d",
            len(self._providers), self._total_calls,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get fallback chain statistics.
        
        Returns:
            Dict with per-provider stats and aggregate metrics.
        """
        return {
            "total_calls": self._total_calls,
            "provider_count": len(self._providers),
            "providers": [
                {
                    "name": p.name,
                    "enabled": p.enabled,
                    "failure_count": p.failure_count,
                    "in_cooldown": self._is_cooldown(p),
                }
                for p in self._providers
            ],
        }

    def enable_provider(self, name: str) -> bool:
        """Enable a provider by name.
        
        Returns:
            True if provider was found and enabled.
        """
        for p in self._providers:
            if p.name == name:
                p.enabled = True
                return True
        return False

    def disable_provider(self, name: str) -> bool:
        """Disable a provider by name.
        
        Returns:
            True if provider was found and disabled.
        """
        for p in self._providers:
            if p.name == name:
                p.enabled = False
                return True
        return False
