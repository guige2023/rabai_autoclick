"""
Element Locator Utilities for UI Automation.

This module provides utilities for finding and matching UI elements
using various strategies including XPath, CSS selectors, and visual matching.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class LocatorType(Enum):
    """Types of element locators supported."""
    XPATH = auto()
    CSS_SELECTOR = auto()
    ID = auto()
    NAME = auto()
    CLASS_NAME = auto()
    TAG_NAME = auto()
    LINK_TEXT = auto()
    PARTIAL_LINK_TEXT = auto()
    TEXT = auto()
    VISUAL = auto()
    ACCESSIBILITY = auto()


@dataclass
class Locator:
    """
    Represents a locator strategy for finding UI elements.
    
    Attributes:
        locator_type: Type of locator
        value: Locator value (XPath expression, CSS selector, etc.)
        timeout: Maximum wait time in seconds
        parent: Optional parent locator for nested searches
    """
    locator_type: LocatorType
    value: str
    timeout: float = 10.0
    parent: Optional[Locator] = None
    index: int = 0  # For selecting among multiple matches
    
    def to_dict(self) -> dict[str, Any]:
        """Convert locator to dictionary format."""
        return {
            "type": self.locator_type.name.lower(),
            "value": self.value,
            "timeout": self.timeout,
            "index": self.index
        }


@dataclass
class LocatorResult:
    """Result of a locator search operation."""
    found: bool
    element: Optional[Any] = None
    matched_count: int = 0
    search_time_ms: float = 0.0
    error: Optional[str] = None


class ElementLocator:
    """
    Main element locator class supporting multiple strategies.
    
    Example:
        locator = ElementLocator()
        element = locator.find(
            Locator(LocatorType.XPATH, "//button[@id='submit']")
        )
    """
    
    def __init__(self):
        self._registry: dict[str, Callable] = {}
        self._cache_enabled = True
        self._cache: dict[str, Any] = {}
    
    def register_strategy(
        self, 
        name: str, 
        func: Callable[[Locator], Any]
    ) -> None:
        """Register a custom locator strategy."""
        self._registry[name] = func
    
    def find(self, locator: Locator, context: Optional[Any] = None) -> LocatorResult:
        """
        Find an element using the given locator.
        
        Args:
            locator: The locator to use
            context: Optional context (driver/page) to search within
            
        Returns:
            LocatorResult with found status and element
        """
        import time
        start_time = time.time()
        
        try:
            if self._cache_enabled:
                cache_key = self._get_cache_key(locator)
                if cache_key in self._cache:
                    return self._cache[cache_key]
            
            strategy_func = self._get_strategy_function(locator.locator_type)
            element = strategy_func(locator, context)
            matched_count = self._count_matches(element)
            
            result = LocatorResult(
                found=element is not None,
                element=element,
                matched_count=matched_count,
                search_time_ms=(time.time() - start_time) * 1000
            )
            
            if self._cache_enabled:
                self._cache[self._get_cache_key(locator)] = result
                
            return result
            
        except Exception as e:
            return LocatorResult(
                found=False,
                error=str(e),
                search_time_ms=(time.time() - start_time) * 1000
            )
    
    def find_all(self, locator: Locator, context: Optional[Any] = None) -> list[Any]:
        """Find all elements matching the locator."""
        try:
            strategy_func = self._get_strategy_function(locator.locator_type)
            elements = strategy_func(locator, context)
            return elements if isinstance(elements, list) else [elements]
        except Exception:
            return []
    
    def _get_strategy_function(self, locator_type: LocatorType) -> Callable:
        """Get the strategy function for the locator type."""
        strategies = {
            LocatorType.XPATH: self._xpath_strategy,
            LocatorType.CSS_SELECTOR: self._css_strategy,
            LocatorType.ID: self._id_strategy,
            LocatorType.NAME: self._name_strategy,
            LocatorType.CLASS_NAME: self._class_name_strategy,
            LocatorType.TAG_NAME: self._tag_name_strategy,
            LocatorType.LINK_TEXT: self._link_text_strategy,
            LocatorType.PARTIAL_LINK_TEXT: self._partial_link_text_strategy,
            LocatorType.TEXT: self._text_strategy,
            LocatorType.ACCESSIBILITY: self._accessibility_strategy,
        }
        return strategies.get(locator_type, self._xpath_strategy)
    
    def _xpath_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """XPath locator strategy."""
        # Placeholder - actual implementation would use WebDriver
        return None
    
    def _css_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """CSS selector locator strategy."""
        return None
    
    def _id_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """ID-based locator strategy."""
        return None
    
    def _name_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Name-based locator strategy."""
        return None
    
    def _class_name_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Class name locator strategy."""
        return None
    
    def _tag_name_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Tag name locator strategy."""
        return None
    
    def _link_text_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Link text locator strategy."""
        return None
    
    def _partial_link_text_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Partial link text locator strategy."""
        return None
    
    def _text_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Text content locator strategy."""
        return None
    
    def _accessibility_strategy(self, locator: Locator, context: Optional[Any]) -> Any:
        """Accessibility-based locator strategy."""
        return None
    
    def _count_matches(self, element: Any) -> int:
        """Count how many elements match."""
        if element is None:
            return 0
        try:
            return len(element) if hasattr(element, '__len__') else 1
        except Exception:
            return 1
    
    def _get_cache_key(self, locator: Locator) -> str:
        """Generate a cache key for the locator."""
        return f"{locator.locator_type.name}:{locator.value}:{locator.index}"
    
    def clear_cache(self) -> None:
        """Clear the locator cache."""
        self._cache.clear()


def xpath(expr: str, timeout: float = 10.0) -> Locator:
    """Create an XPath locator."""
    return Locator(LocatorType.XPATH, expr, timeout)


def css(selector: str, timeout: float = 10.0) -> Locator:
    """Create a CSS selector locator."""
    return Locator(LocatorType.CSS_SELECTOR, selector, timeout)


def id_(value: str, timeout: float = 10.0) -> Locator:
    """Create an ID-based locator."""
    return Locator(LocatorType.ID, value, timeout)


def name(value: str, timeout: float = 10.0) -> Locator:
    """Create a name-based locator."""
    return Locator(LocatorType.NAME, value, timeout)


def class_name(value: str, timeout: float = 10.0) -> Locator:
    """Create a class name locator."""
    return Locator(LocatorType.CLASS_NAME, value, timeout)


def tag_name(value: str, timeout: float = 10.0) -> Locator:
    """Create a tag name locator."""
    return Locator(LocatorType.TAG_NAME, value, timeout)


def link_text(text: str, timeout: float = 10.0) -> Locator:
    """Create a link text locator."""
    return Locator(LocatorType.LINK_TEXT, text, timeout)


def partial_link_text(text: str, timeout: float = 10.0) -> Locator:
    """Create a partial link text locator."""
    return Locator(LocatorType.PARTIAL_LINK_TEXT, text, timeout)


def text(content: str, timeout: float = 10.0) -> Locator:
    """Create a text-based locator."""
    return Locator(LocatorType.TEXT, content, timeout)


class LocatorChain:
    """Chain multiple locators together for complex element finding."""
    
    def __init__(self):
        self._locators: list[Locator] = []
    
    def add(self, locator: Locator) -> 'LocatorChain':
        """Add a locator to the chain."""
        self._locators.append(locator)
        return self
    
    def find(self, context: Any) -> LocatorResult:
        """Execute the locator chain."""
        if not self._locators:
            return LocatorResult(found=False, error="No locators in chain")
        
        element = context
        for locator in self._locators:
            result = ElementLocator().find(locator, element)
            if not result.found:
                return result
            element = result.element
            
        return LocatorResult(found=True, element=element)


def smart_locator(text: str, element_type: Optional[str] = None) -> Locator:
    """
    Create a smart locator that tries multiple strategies.
    
    Args:
        text: Text to search for
        element_type: Optional element type hint (button, input, etc.)
        
    Returns:
        Locator with multiple strategies
    """
    if element_type:
        return xpath(f"//{element_type}[contains(text(),'{text}')]")
    return xpath(f"//*[contains(text(),'{text}')]")
