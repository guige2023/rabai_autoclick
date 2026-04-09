"""UI element guard for safe element access with existence checks."""
from typing import Optional, Callable, Any, List, Dict, Protocol
from dataclasses import dataclass
import time


class ElementFinder(Protocol):
    """Protocol for element finding functions."""
    def find(self, selector: str) -> Any: ...


@dataclass
class ElementGuardConfig:
    """Configuration for element guard behavior."""
    wait_before_check: float = 0.0
    poll_interval: float = 0.1
    max_poll_attempts: int = 10
    validate_on_access: bool = True
    cache_results: bool = True


class UIElementGuard:
    """Guards UI element access with existence validation and caching.
    
    Provides safe access to UI elements with automatic waiting,
    validation, and optional caching to prevent redundant queries.
    
    Example:
        guard = UIElementGuard(finder=app.find_element, config=ElementGuardConfig())
        
        with guard.element("submit_button") as elem:
            elem.click()
        
        # Or with validation
        if guard.exists("loading_indicator"):
            guard.wait_until_gone("loading_indicator")
    """

    def __init__(
        self,
        finder: Optional[ElementFinder] = None,
        config: Optional[ElementGuardConfig] = None,
    ) -> None:
        """Initialize the element guard.
        
        Args:
            finder: Element finder function compatible with ElementFinder protocol.
            config: Guard configuration.
        """
        self._finder = finder
        self._config = config or ElementGuardConfig()
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_ttl: float = 5.0

    def set_finder(self, finder: ElementFinder) -> None:
        """Set the element finder function.
        
        Args:
            finder: Function to find elements.
        """
        self._finder = finder

    def element(
        self,
        selector: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """Get an element with optional waiting.
        
        Args:
            selector: Element selector string.
            wait: Whether to wait for element to appear.
            timeout: Maximum wait time in seconds.
            
        Returns:
            Element object or None if not found.
        """
        if self._config.wait_before_check > 0:
            time.sleep(self._config.wait_before_check)
        
        if wait:
            return self._wait_for_element(selector, timeout)
        return self._find_element(selector)

    def exists(self, selector: str, use_cache: bool = True) -> bool:
        """Check if an element exists.
        
        Args:
            selector: Element selector string.
            use_cache: Whether to use cached result if available.
            
        Returns:
            True if element exists.
        """
        if use_cache and self._config.cache_results and selector in self._cache:
            if time.time() - self._cache_timestamps.get(selector, 0) < self._cache_ttl:
                return True
        
        elem = self._find_element(selector)
        if elem is not None and self._config.cache_results:
            self._cache[selector] = elem
            self._cache_timestamps[selector] = time.time()
        return elem is not None

    def wait_until_gone(
        self,
        selector: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait until an element disappears.
        
        Args:
            selector: Element selector string.
            timeout: Maximum wait time in seconds.
            
        Returns:
            True if element disappeared within timeout.
        """
        timeout_val = timeout or (self._config.poll_interval * self._config.max_poll_attempts)
        start = time.time()
        
        while time.time() - start < timeout_val:
            if not self._find_element(selector):
                return True
            time.sleep(self._config.poll_interval)
        
        return False

    def wait_until_visible(
        self,
        selector: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait until an element is visible.
        
        Args:
            selector: Element selector string.
            timeout: Maximum wait time in seconds.
            
        Returns:
            True if element became visible within timeout.
        """
        timeout_val = timeout or (self._config.poll_interval * self._config.max_poll_attempts)
        start = time.time()
        
        while time.time() - start < timeout_val:
            elem = self._find_element(selector)
            if elem and self._is_visible(elem):
                return True
            time.sleep(self._config.poll_interval)
        
        return False

    def wait_until_clickable(
        self,
        selector: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait until an element is visible and enabled.
        
        Args:
            selector: Element selector string.
            timeout: Maximum wait time in seconds.
            
        Returns:
            True if element became clickable within timeout.
        """
        timeout_val = timeout or (self._config.poll_interval * self._config.max_poll_attempts)
        start = time.time()
        
        while time.time() - start < timeout_val:
            elem = self._find_element(selector)
            if elem and self._is_visible(elem) and self._is_enabled(elem):
                return True
            time.sleep(self._config.poll_interval)
        
        return False

    def invalidate_cache(self, selector: Optional[str] = None) -> None:
        """Invalidate cached element results.
        
        Args:
            selector: Specific selector to invalidate, or None for all.
        """
        if selector:
            self._cache.pop(selector, None)
            self._cache_timestamps.pop(selector, None)
        else:
            self._cache.clear()
            self._cache_timestamps.clear()

    def _find_element(self, selector: str) -> Optional[Any]:
        """Find element using configured finder.
        
        Args:
            selector: Element selector string.
            
        Returns:
            Element or None.
        """
        if self._finder is None:
            raise RuntimeError("No element finder configured")
        
        try:
            return self._finder.find(selector)
        except Exception:
            return None

    def _wait_for_element(
        self,
        selector: str,
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """Wait for element to appear.
        
        Args:
            selector: Element selector string.
            timeout: Maximum wait time in seconds.
            
        Returns:
            Element or None if not found.
        """
        timeout_val = timeout or (self._config.poll_interval * self._config.max_poll_attempts)
        start = time.time()
        
        while time.time() - start < timeout_val:
            elem = self._find_element(selector)
            if elem:
                if self._config.validate_on_access and self._config.cache_results:
                    self._cache[selector] = elem
                    self._cache_timestamps[selector] = time.time()
                return elem
            time.sleep(self._config.poll_interval)
        
        return None

    def _is_visible(self, elem: Any) -> bool:
        """Check if element is visible.
        
        Args:
            elem: Element object.
            
        Returns:
            True if element is visible.
        """
        try:
            return getattr(elem, "is_visible", lambda: True)()
        except Exception:
            return True

    def _is_enabled(self, elem: Any) -> bool:
        """Check if element is enabled.
        
        Args:
            elem: Element object.
            
        Returns:
            True if element is enabled.
        """
        try:
            return getattr(elem, "is_enabled", lambda: True)()
        except Exception:
            return True
