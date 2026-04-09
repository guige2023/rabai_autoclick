"""
Element Locator Action Module

Provides intelligent element finding, fuzzy matching, and
accessibility-based element detection for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class LocatorStrategy(Enum):
    """Supported element locator strategies."""

    ID = "id"
    NAME = "name"
    XPATH = "xpath"
    CSS_SELECTOR = "css"
    ACCESSIBILITY = "accessibility"
    TEXT = "text"
    PARTIAL_TEXT = "partial_text"
    LABEL = "label"
    IMAGE = "image"
    COORDINATE = "coordinate"


@dataclass
class ElementCriteria:
    """Criteria for element matching."""

    tag_name: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = None
    class_name: Optional[str] = None
    text: Optional[str] = None
    partial_text: Optional[str] = None
    accessibility_label: Optional[str] = None
    accessibility_role: Optional[str] = None
    enabled: Optional[bool] = None
    visible: Optional[bool] = None
    attributes: Optional[Dict[str, str]] = None


@dataclass
class LocatedElement:
    """Represents a located UI element."""

    handle: Any
    locator_used: LocatorStrategy
    locator_value: str
    criteria: ElementCriteria
    bounding_box: Optional[Tuple[int, int, int, int]] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0


@dataclass
class LocatorConfig:
    """Configuration for element locator."""

    default_timeout: float = 10.0
    polling_interval: float = 0.5
    case_sensitive: bool = False
    fuzzy_threshold: float = 0.8
    prefer_visible: bool = True
    prefer_enabled: bool = True


class ElementLocator:
    """
    Locates UI elements using multiple strategies.

    Supports ID, name, XPath, CSS selector, accessibility attributes,
    text matching, and image-based location with fuzzy matching.
    """

    def __init__(
        self,
        config: Optional[LocatorConfig] = None,
        driver: Optional[Any] = None,
    ):
        self.config = config or LocatorConfig()
        self.driver = driver
        self._cache: Dict[str, LocatedElement] = {}
        self._last_query: Optional[str] = None

    def set_driver(self, driver: Any) -> None:
        """Set the underlying driver for element queries."""
        self.driver = driver

    def find_element(
        self,
        strategy: LocatorStrategy,
        value: str,
        timeout: Optional[float] = None,
    ) -> Optional[LocatedElement]:
        """
        Find a single element using the specified strategy.

        Args:
            strategy: Locator strategy to use
            value: Locator value (selector, xpath, etc.)
            timeout: Optional timeout override

        Returns:
            LocatedElement or None if not found
        """
        timeout = timeout or self.config.default_timeout
        deadline = time.time() + timeout

        while time.time() < deadline:
            element = self._find_single(strategy, value)
            if element:
                return element
            time.sleep(self.config.polling_interval)

        logger.warning(f"Element not found: {strategy.value}={value}")
        return None

    def find_elements(
        self,
        strategy: LocatorStrategy,
        value: str,
    ) -> List[LocatedElement]:
        """
        Find all elements matching the locator.

        Args:
            strategy: Locator strategy to use
            value: Locator value

        Returns:
            List of LocatedElement (may be empty)
        """
        return self._find_multiple(strategy, value)

    def _find_single(
        self,
        strategy: LocatorStrategy,
        value: str,
    ) -> Optional[LocatedElement]:
        """Internal single element finder."""
        try:
            if strategy == LocatorStrategy.ID:
                return self._find_by_id(value)
            elif strategy == LocatorStrategy.NAME:
                return self._find_by_name(value)
            elif strategy == LocatorStrategy.XPATH:
                return self._find_by_xpath(value)
            elif strategy == LocatorStrategy.CSS_SELECTOR:
                return self._find_by_css(value)
            elif strategy == LocatorStrategy.TEXT:
                return self._find_by_text(value)
            elif strategy == LocatorStrategy.PARTIAL_TEXT:
                return self._find_by_partial_text(value)
            elif strategy == LocatorStrategy.ACCESSIBILITY:
                return self._find_by_accessibility(value)
            else:
                logger.warning(f"Unsupported strategy: {strategy}")
                return None
        except Exception as e:
            logger.error(f"Find element failed: {e}")
            return None

    def _find_multiple(
        self,
        strategy: LocatorStrategy,
        value: str,
    ) -> List[LocatedElement]:
        """Internal multiple element finder."""
        try:
            if strategy == LocatorStrategy.XPATH:
                return self._find_all_by_xpath(value)
            elif strategy == LocatorStrategy.CSS_SELECTOR:
                return self._find_all_by_css(value)
            elif strategy == LocatorStrategy.NAME:
                return self._find_all_by_name(value)
            elif strategy == LocatorStrategy.ACCESSIBILITY:
                return self._find_all_by_accessibility(value)
            else:
                element = self._find_single(strategy, value)
                return [element] if element else []
        except Exception as e:
            logger.error(f"Find elements failed: {e}")
            return []

    def _find_by_id(self, id_value: str) -> Optional[LocatedElement]:
        """Find element by ID."""
        if not self.driver:
            return None
        try:
            element = self.driver.find_element_by_id(id_value)
            return self._wrap_element(element, LocatorStrategy.ID, id_value)
        except Exception:
            return None

    def _find_by_name(self, name: str) -> Optional[LocatedElement]:
        """Find element by name attribute."""
        if not self.driver:
            return None
        try:
            element = self.driver.find_element_by_name(name)
            return self._wrap_element(element, LocatorStrategy.NAME, name)
        except Exception:
            return None

    def _find_by_xpath(self, xpath: str) -> Optional[LocatedElement]:
        """Find element by XPath."""
        if not self.driver:
            return None
        try:
            element = self.driver.find_element_by_xpath(xpath)
            return self._wrap_element(element, LocatorStrategy.XPATH, xpath)
        except Exception:
            return None

    def _find_all_by_xpath(self, xpath: str) -> List[LocatedElement]:
        """Find all elements by XPath."""
        if not self.driver:
            return []
        try:
            elements = self.driver.find_elements_by_xpath(xpath)
            return [
                self._wrap_element(el, LocatorStrategy.XPATH, xpath)
                for el in elements
            ]
        except Exception:
            return []

    def _find_by_css(self, css: str) -> Optional[LocatedElement]:
        """Find element by CSS selector."""
        if not self.driver:
            return None
        try:
            element = self.driver.find_element_by_css_selector(css)
            return self._wrap_element(element, LocatorStrategy.CSS_SELECTOR, css)
        except Exception:
            return None

    def _find_all_by_css(self, css: str) -> List[LocatedElement]:
        """Find all elements by CSS selector."""
        if not self.driver:
            return []
        try:
            elements = self.driver.find_elements_by_css_selector(css)
            return [
                self._wrap_element(el, LocatorStrategy.CSS_SELECTOR, css)
                for el in elements
            ]
        except Exception:
            return []

    def _find_by_text(self, text: str) -> Optional[LocatedElement]:
        """Find element by exact text match."""
        if not self.driver:
            return None
        escaped = text.replace('"', '\\"')
        xpath = f'//*[text()="{escaped}"]'
        return self._find_by_xpath(xpath)

    def _find_by_partial_text(self, text: str) -> Optional[LocatedElement]:
        """Find element by partial text match."""
        if not self.driver:
            return None
        escaped = text.replace('"', '\\"')
        xpath = f'//*[contains(text(),"{escaped}")]'
        return self._find_by_xpath(xpath)

    def _find_by_accessibility(self, label: str) -> Optional[LocatedElement]:
        """Find element by accessibility label."""
        if not self.driver:
            return None
        try:
            element = self.driver.find_element_by_accessibility_id(label)
            return self._wrap_element(element, LocatorStrategy.ACCESSIBILITY, label)
        except Exception:
            return None

    def _find_all_by_name(self, name: str) -> List[LocatedElement]:
        """Find all elements by name attribute."""
        if not self.driver:
            return []
        try:
            elements = self.driver.find_elements_by_name(name)
            return [
                self._wrap_element(el, LocatorStrategy.NAME, name)
                for el in elements
            ]
        except Exception:
            return []

    def _find_all_by_accessibility(self, label: str) -> List[LocatedElement]:
        """Find all elements by accessibility label."""
        if not self.driver:
            return []
        try:
            elements = self.driver.find_elements_by_accessibility_id(label)
            return [
                self._wrap_element(el, LocatorStrategy.ACCESSIBILITY, label)
                for el in elements
            ]
        except Exception:
            return []

    def _wrap_element(
        self,
        element: Any,
        strategy: LocatorStrategy,
        value: str,
    ) -> LocatedElement:
        """Wrap a raw element in a LocatedElement."""
        bounding_box = None
        attributes = {}

        try:
            if hasattr(element, "rect"):
                rect = element.rect
                bounding_box = (rect["x"], rect["y"], rect["width"], rect["height"])
            if hasattr(element, "get_attribute"):
                for attr in ["id", "name", "class", "text", "tag_name"]:
                    try:
                        attributes[attr] = element.get_attribute(attr)
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"Could not extract element details: {e}")

        return LocatedElement(
            handle=element,
            locator_used=strategy,
            locator_value=value,
            criteria=ElementCriteria(),
            bounding_box=bounding_box,
            attributes=attributes,
        )

    def find_by_criteria(
        self,
        criteria: ElementCriteria,
        timeout: Optional[float] = None,
    ) -> Optional[LocatedElement]:
        """
        Find element using structured criteria.

        Args:
            criteria: Element matching criteria
            timeout: Optional timeout

        Returns:
            First matching element or None
        """
        timeout = timeout or self.config.default_timeout

        if criteria.id:
            return self.find_element(LocatorStrategy.ID, criteria.id, timeout)

        if criteria.accessibility_label:
            return self.find_element(LocatorStrategy.ACCESSIBILITY, criteria.accessibility_label, timeout)

        if criteria.text:
            return self.find_element(LocatorStrategy.TEXT, criteria.text, timeout)

        if criteria.partial_text:
            return self.find_element(LocatorStrategy.PARTIAL_TEXT, criteria.partial_text, timeout)

        if criteria.name:
            return self.find_element(LocatorStrategy.NAME, criteria.name, timeout)

        logger.warning("No usable criteria provided")
        return None

    def wait_for_element(
        self,
        strategy: LocatorStrategy,
        value: str,
        timeout: Optional[float] = None,
        disappear: bool = False,
    ) -> bool:
        """
        Wait for element to appear or disappear.

        Args:
            strategy: Locator strategy
            value: Locator value
            timeout: Wait timeout
            disappear: Wait for element to disappear instead

        Returns:
            True if condition met
        """
        timeout = timeout or self.config.default_timeout
        deadline = time.time() + timeout

        while time.time() < deadline:
            element = self._find_single(strategy, value)
            if disappear:
                if not element:
                    return True
            else:
                if element:
                    return True
            time.sleep(self.config.polling_interval)

        return False

    def clear_cache(self) -> None:
        """Clear the element cache."""
        self._cache.clear()
        self._last_query = None


import time


def create_element_locator(
    config: Optional[LocatorConfig] = None,
    driver: Optional[Any] = None,
) -> ElementLocator:
    """Factory function to create an ElementLocator."""
    return ElementLocator(config=config, driver=driver)
