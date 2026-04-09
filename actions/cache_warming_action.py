"""Cache warming action module.

Provides cache warming functionality to pre-populate
caches with frequently accessed data.
"""

from __future__ import annotations

import time
import logging
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WarmingStrategy(Enum):
    """Cache warming strategies."""
    EAGER = "eager"
    LAZY = "lazy"
    BACKGROUND = "background"
    SCHEDULED = "scheduled"


@dataclass
class WarmingTask:
    """Cache warming task."""
    key: str
    loader: Callable[[], Any]
    priority: int = 0
    ttl: Optional[float] = None
    dependencies: list[str] = field(default_factory=list)


@dataclass
class WarmingResult:
    """Result of warming operation."""
    key: str
    success: bool
    value: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class WarmingConfig:
    """Cache warming configuration."""
    strategy: WarmingStrategy = WarmingStrategy.EAGER
    max_workers: int = 4
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0


class CacheWarmingStrategy:
    """Base class for warming strategies."""

    def __init__(self, config: WarmingConfig):
        """Initialize strategy.

        Args:
            config: Warming configuration
        """
        self.config = config

    def warm(self, tasks: list[WarmingTask], cache: Any) -> list[WarmingResult]:
        """Execute warming.

        Args:
            tasks: List of warming tasks
            cache: Cache client

        Returns:
            List of warming results
        """
        raise NotImplementedError


class EagerWarming(CacheWarmingStrategy):
    """Eager warming - load all data immediately."""

    def warm(self, tasks: list[WarmingTask], cache: Any) -> list[WarmingResult]:
        """Execute eager warming."""
        results = []
        for task in tasks:
            result = self._load_task(task)
            results.append(result)
        return results

    def _load_task(self, task: WarmingTask) -> WarmingResult:
        """Load single task."""
        start = time.time()
        try:
            value = task.loader()
            duration = (time.time() - start) * 1000
            return WarmingResult(
                key=task.key,
                success=True,
                value=value,
                duration_ms=duration,
            )
        except Exception as e:
            duration = (time.time() - start) * 1000
            return WarmingResult(
                key=task.key,
                success=False,
                error=str(e),
                duration_ms=duration,
            )


class BackgroundWarming(CacheWarmingStrategy):
    """Background warming - load data in background threads."""

    def warm(self, tasks: list[WarmingTask], cache: Any) -> list[WarmingResult]:
        """Execute background warming."""
        results = []

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self._load_task, task): task for task in tasks}
            for future in futures:
                results.append(future.result())

        return results

    def _load_task(self, task: WarmingTask) -> WarmingResult:
        """Load single task."""
        start = time.time()
        try:
            value = task.loader()
            duration = (time.time() - start) * 1000
            return WarmingResult(
                key=task.key,
                success=True,
                value=value,
                duration_ms=duration,
            )
        except Exception as e:
            duration = (time.time() - start) * 1000
            return WarmingResult(
                key=task.key,
                success=False,
                error=str(e),
                duration_ms=duration,
            )


class LazyWarming(CacheWarmingStrategy):
    """Lazy warming - load on first access."""

    def warm(self, tasks: list[WarmingTask], cache: Any) -> list[WarmingResult]:
        """Execute lazy warming - returns empty results."""
        return []


class CacheWarmer:
    """Cache warming manager."""

    def __init__(self, cache_client: Any, config: Optional[WarmingConfig] = None):
        """Initialize cache warmer.

        Args:
            cache_client: Cache client instance
            config: Warming configuration
        """
        self.cache_client = cache_client
        self.config = config or WarmingConfig()
        self._tasks: list[WarmingTask] = []
        self._lock = threading.Lock()

    def add_task(self, task: WarmingTask) -> None:
        """Add warming task.

        Args:
            task: Warming task to add
        """
        with self._lock:
            self._tasks.append(task)

    def add_tasks(self, tasks: list[WarmingTask]) -> None:
        """Add multiple warming tasks.

        Args:
            tasks: List of warming tasks
        """
        with self._lock:
            self._tasks.extend(tasks)

    def register_loader(
        self,
        key: str,
        loader: Callable[[], Any],
        priority: int = 0,
        ttl: Optional[float] = None,
    ) -> None:
        """Register a cache loader.

        Args:
            key: Cache key
            loader: Function to load data
            priority: Loading priority
            ttl: Optional TTL
        """
        task = WarmingTask(
            key=key,
            loader=loader,
            priority=priority,
            ttl=ttl,
        )
        self.add_task(task)

    def warm(self) -> list[WarmingResult]:
        """Execute warming.

        Returns:
            List of warming results
        """
        with self._lock:
            tasks_to_warm = self._tasks.copy()
            tasks_to_warm.sort(key=lambda t: t.priority, reverse=True)

        if not tasks_to_warm:
            return []

        strategy = self._get_strategy()
        return strategy.warm(tasks_to_warm, self.cache_client)

    def warm_keys(self, keys: list[str]) -> list[WarmingResult]:
        """Warm specific keys.

        Args:
            keys: List of cache keys to warm

        Returns:
            List of warming results
        """
        with self._lock:
            tasks_to_warm = [t for t in self._tasks if t.key in keys]

        if not tasks_to_warm:
            return []

        strategy = self._get_strategy()
        return strategy.warm(tasks_to_warm, self.cache_client)

    def _get_strategy(self) -> CacheWarmingStrategy:
        """Get warming strategy instance."""
        if self.config.strategy == WarmingStrategy.EAGER:
            return EagerWarming(self.config)
        elif self.config.strategy == WarmingStrategy.LAZY:
            return LazyWarming(self.config)
        elif self.config.strategy == WarmingStrategy.BACKGROUND:
            return BackgroundWarming(self.config)
        else:
            return EagerWarming(self.config)

    def clear_tasks(self) -> None:
        """Clear all warming tasks."""
        with self._lock:
            self._tasks.clear()

    def get_task_count(self) -> int:
        """Get number of registered tasks."""
        with self._lock:
            return len(self._tasks)


class PriorityWarmingScheduler:
    """Priority-based warming scheduler."""

    def __init__(self, warmer: CacheWarmer):
        """Initialize scheduler.

        Args:
            warmer: CacheWarmer instance
        """
        self.warmer = warmer
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self, interval: float = 3600.0) -> None:
        """Start scheduled warming.

        Args:
            interval: Interval between warming runs (seconds)
        """
        with self._lock:
            if self._running:
                return
            self._running = True

        def run():
            while self._running:
                try:
                    logger.info("Starting scheduled cache warming")
                    results = self.warmer.warm()
                    success_count = sum(1 for r in results if r.success)
                    logger.info(f"Cache warming completed: {success_count}/{len(results)} successful")
                except Exception as e:
                    logger.error(f"Cache warming failed: {e}")

                time.sleep(interval)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        logger.info(f"Started cache warming scheduler (interval={interval}s)")

    def stop(self) -> None:
        """Stop scheduled warming."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Stopped cache warming scheduler")


def create_cache_warmer(cache_client: Any, strategy: str = "eager") -> CacheWarmer:
    """Create cache warmer.

    Args:
        cache_client: Cache client instance
        strategy: Warming strategy name

    Returns:
        CacheWarmer instance
    """
    config = WarmingConfig(strategy=WarmingStrategy(strategy))
    return CacheWarmer(cache_client, config)
