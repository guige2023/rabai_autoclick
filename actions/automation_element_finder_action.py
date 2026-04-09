"""Automation Element Finder Action Module.

Locates UI elements using multiple strategies: XPath, CSS selector,
accessibility attributes, image recognition, and fallback chains.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FinderStrategy(Enum):
    """Element finding strategies."""
    XPATH = "xpath"
    CSS = "css"
    ACCESSIBILITY = "accessibility"
    TEXT = "text"
    IMAGE = "image"
    COORDINATE = "coordinate"


@dataclass
class ElementQuery:
    """A query to locate a UI element."""
    strategy: FinderStrategy
    value: str
    timeout_sec: float = 5.0
    index: int = 0  # For multiple matches


@dataclass
class ElementLocation:
    """Result of locating an element."""
    found: bool
    query: ElementQuery
    element: Optional[Any] = None
    position: Optional[Tuple[int, int]] = None
    size: Optional[Tuple[int, int]] = None
    error: Optional[str] = None


class AutomationElementFinderAction:
    """Finds UI elements using multiple strategies.
    
    Tries element queries in order, with fallback chains for
    resilient element location across different UI frameworks.
    """

    def __init__(self) -> None:
        self._providers: Dict[FinderStrategy, Callable[..., Optional[Any]]] = {}
        self._fallback_chain: List[FinderStrategy] = [
            FinderStrategy.XPATH,
            FinderStrategy.CSS,
            FinderStrategy.ACCESSIBILITY,
            FinderStrategy.TEXT,
        ]
        self._cache: Dict[str, ElementLocation] = {}
        self._stats: Dict[str, int] = {"hits": 0, "misses": 0}

    def register_provider(
        self,
        strategy: FinderStrategy,
        provider: Callable[..., Optional[Any]],
    ) -> None:
        """Register a provider function for a strategy.
        
        Args:
            strategy: The strategy this provider handles.
            provider: Callable that takes (value, index) and returns element or None.
        """
        self._providers[strategy] = provider

    def find(
        self,
        query: ElementQuery,
        use_cache: bool = True,
    ) -> ElementLocation:
        """Find an element using the specified strategy.
        
        Args:
            query: ElementQuery with strategy and value.
            use_cache: Use cached result if available.
        
        Returns:
            ElementLocation with found status and element reference.
        """
        cache_key = f"{query.strategy.value}:{query.value}:{query.index}"
        if use_cache and cache_key in self._cache:
            self._stats["hits"] += 1
            return self._cache[cache_key]

        provider = self._providers.get(query.strategy)
        if not provider:
            result = ElementLocation(
                found=False,
                query=query,
                error=f"No provider registered for {query.strategy.value}",
            )
            self._stats["misses"] += 1
            return result

        try:
            element = provider(query.value, query.index)
            result = ElementLocation(found=element is not None, query=query, element=element)
            if element is None:
                result.error = "Element not found"
                self._stats["misses"] += 1
            else:
                self._stats["hits"] += 1
        except Exception as exc:
            result = ElementLocation(
                found=False, query=query, error=str(exc)
            )
            self._stats["misses"] += 1

        self._cache[cache_key] = result
        return result

    def find_with_fallback(
        self,
        queries: List[ElementQuery],
    ) -> ElementLocation:
        """Try multiple queries in order until one succeeds.
        
        Args:
            queries: List of ElementQuery in fallback order.
        
        Returns:
            ElementLocation from the first successful query.
        """
        for query in queries:
            result = self.find(query)
            if result.found:
                logger.debug("Element found with strategy %s", query.strategy.value)
                return result
        return ElementLocation(
            found=False,
            query=queries[0] if queries else ElementQuery(strategy=FinderStrategy.XPATH, value=""),
            error="All fallback strategies failed",
        )

    def invalidate_cache(self, pattern: Optional[str] = None) -> None:
        """Clear the element cache.
        
        Args:
            pattern: If provided, only clear entries matching this pattern.
        """
        if pattern is None:
            self._cache.clear()
        else:
            keys_to_remove = [k for k in self._cache if pattern in k]
            for k in keys_to_remove:
                del self._cache[k]

    def set_fallback_chain(self, strategies: List[FinderStrategy]) -> None:
        """Set the default fallback chain order.
        
        Args:
            strategies: Ordered list of strategies to try.
        """
        self._fallback_chain = strategies

    def get_stats(self) -> Dict[str, Any]:
        """Get finder statistics."""
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": round(self._stats["hits"] / total, 4) if total > 0 else 0.0,
            "cache_size": len(self._cache),
        }
