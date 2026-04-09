"""
Element Locator Utilities for UI Automation

Provides flexible element location strategies including
CSS selectors, XPath, and accessibility tree traversal.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional


class LocatorStrategy(Enum):
    """Element location strategies."""
    ID = auto()
    CSS_SELECTOR = auto()
    XPATH = auto()
    ACCESSIBILITY = auto()
    IMAGE = auto()
    TEXT = auto()


from enum import Enum, auto


@dataclass
class Locator:
    """Element locator definition."""
    strategy: LocatorStrategy
    value: str
    timeout: float = 10.0
    index: int = 0
    parent: Optional[Locator] = None


@dataclass
class LocateResult:
    """Result of an element location attempt."""
    found: bool
    element: Optional[dict] = None
    confidence: float = 0.0
    metadata: dict = None


class ElementLocator:
    """
    Locates UI elements using multiple strategies.

    Supports CSS selectors, XPath, accessibility attributes,
    image-based matching, and text content.
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict] = {}
        self._cache_enabled: bool = True
        self._cache_ttl: float = 60.0

    def by_id(self, element_id: str) -> Locator:
        """
        Create a locator by element ID.

        Args:
            element_id: HTML element ID

        Returns:
            Locator object
        """
        return Locator(
            strategy=LocatorStrategy.ID,
            value=element_id,
        )

    def by_css(self, selector: str, timeout: float = 10.0) -> Locator:
        """
        Create a locator by CSS selector.

        Args:
            selector: CSS selector string
            timeout: Search timeout in seconds

        Returns:
            Locator object
        """
        return Locator(
            strategy=LocatorStrategy.CSS_SELECTOR,
            value=selector,
            timeout=timeout,
        )

    def by_xpath(self, xpath: str, timeout: float = 10.0) -> Locator:
        """
        Create a locator by XPath expression.

        Args:
            xpath: XPath expression
            timeout: Search timeout in seconds

        Returns:
            Locator object
        """
        return Locator(
            strategy=LocatorStrategy.XPATH,
            value=xpath,
            timeout=timeout,
        )

    def by_accessibility(
        self,
        role: Optional[str] = None,
        name: Optional[str] = None,
        **attributes,
    ) -> Locator:
        """
        Create a locator by accessibility attributes.

        Args:
            role: Accessibility role (e.g., 'button', 'link')
            name: Accessibility name/label
            **attributes: Additional accessibility attributes

        Returns:
            Locator object
        """
        value = self._build_accessibility_query(role, name, **attributes)
        return Locator(
            strategy=LocatorStrategy.ACCESSIBILITY,
            value=value,
        )

    def by_text(self, text: str, exact: bool = False) -> Locator:
        """
        Create a locator by text content.

        Args:
            text: Text to search for
            exact: Whether to match exactly

        Returns:
            Locator object
        """
        prefix = "exact:" if exact else "partial:"
        return Locator(
            strategy=LocatorStrategy.TEXT,
            value=f"{prefix}{text}",
        )

    def by_image(self, image_path: str, confidence: float = 0.8) -> Locator:
        """
        Create a locator by template image.

        Args:
            image_path: Path to template image file
            confidence: Minimum confidence threshold

        Returns:
            Locator object
        """
        return Locator(
            strategy=LocatorStrategy.IMAGE,
            value=image_path,
        )

    def with_parent(self, locator: Locator, parent: Locator) -> Locator:
        """
        Create a locator that is a child of another locator.

        Args:
            locator: Child locator
            parent: Parent locator

        Returns:
            New locator with parent set
        """
        locator.parent = parent
        return locator

    def _build_accessibility_query(
        self,
        role: Optional[str] = None,
        name: Optional[str] = None,
        **attributes,
    ) -> str:
        """Build accessibility query string from parameters."""
        parts = []
        if role:
            parts.append(f"role={role}")
        if name:
            parts.append(f"name={name}")
        for key, value in attributes.items():
            parts.append(f"{key}={value}")
        return ";".join(parts)

    def locate(self, locator: Locator) -> LocateResult:
        """
        Locate an element using the given locator.

        Args:
            locator: Locator to use

        Returns:
            LocateResult with found status and element info
        """
        cache_key = f"{locator.strategy.name}:{locator.value}"

        if self._cache_enabled and cache_key in self._cache:
            cached = self._cache[cache_key]
            return LocateResult(
                found=True,
                element=cached,
                confidence=1.0,
            )

        if locator.strategy == LocatorStrategy.ID:
            return self._locate_by_id(locator)
        elif locator.strategy == LocatorStrategy.CSS_SELECTOR:
            return self._locate_by_css(locator)
        elif locator.strategy == LocatorStrategy.XPATH:
            return self._locate_by_xpath(locator)
        elif locator.strategy == LocatorStrategy.ACCESSIBILITY:
            return self._locate_by_accessibility(locator)
        elif locator.strategy == LocatorStrategy.TEXT:
            return self._locate_by_text(locator)
        elif locator.strategy == LocatorStrategy.IMAGE:
            return self._locate_by_image(locator)

        return LocateResult(found=False)

    def _locate_by_id(self, locator: Locator) -> LocateResult:
        """Locate element by ID."""
        # Placeholder - would integrate with actual UI framework
        return LocateResult(found=False)

    def _locate_by_css(self, locator: Locator) -> LocateResult:
        """Locate element by CSS selector."""
        return LocateResult(found=False)

    def _locate_by_xpath(self, locator: Locator) -> LocateResult:
        """Locate element by XPath."""
        return LocateResult(found=False)

    def _locate_by_accessibility(self, locator: Locator) -> LocateResult:
        """Locate element by accessibility attributes."""
        return LocateResult(found=False)

    def _locate_by_text(self, locator: Locator) -> LocateResult:
        """Locate element by text content."""
        return LocateResult(found=False)

    def _locate_by_image(self, locator: Locator) -> LocateResult:
        """Locate element by template image."""
        return LocateResult(found=False)

    def clear_cache(self) -> None:
        """Clear the locator cache."""
        self._cache = {}

    def enable_cache(self, enabled: bool) -> None:
        """Enable or disable caching."""
        self._cache_enabled = enabled


def parse_css_selector(selector: str) -> dict[str, list[str]]:
    """
    Parse a CSS selector into its component parts.

    Args:
        selector: CSS selector string

    Returns:
        Dictionary with selector components
    """
    result: dict[str, list[str]] = {
        "tag": [],
        "class": [],
        "id": [],
        "attribute": [],
    }

    # Simple parser for common patterns
    tag_match = re.match(r"^([a-zA-Z0-9]+)", selector)
    if tag_match:
        result["tag"].append(tag_match.group(1))

    # Extract classes
    class_matches = re.findall(r"\.([a-zA-Z_-][a-zA-Z0-9_-]*)", selector)
    result["class"].extend(class_matches)

    # Extract ID
    id_match = re.search(r"#([a-zA-Z_-][a-zA-Z0-9_-]*)", selector)
    if id_match:
        result["id"].append(id_match.group(1))

    # Extract attribute selectors
    attr_matches = re.findall(r"\[([^\]]+)\]", selector)
    result["attribute"].extend(attr_matches)

    return result


def xpath_contains_text(element: str, text: str, exact: bool = False) -> str:
    """
    Generate XPath for element containing specific text.

    Args:
        element: Element tag name
        text: Text to search for
        exact: Whether to match exactly

    Returns:
        XPath expression string
    """
    if exact:
        return f"//{element}[text()='{text}']"
    else:
        return f"//{element}[contains(text(),'{text}')]"


def xpath_with_attribute(
    element: str,
    attribute: str,
    value: str,
    operator: str = "=",
) -> str:
    """
    Generate XPath for element with specific attribute value.

    Args:
        element: Element tag name
        attribute: Attribute name
        value: Attribute value
        operator: Comparison operator (=, !=, contains, starts-with, etc.)

    Returns:
        XPath expression string
    """
    if operator == "=":
        return f"//{element}[@{attribute}='{value}']"
    elif operator == "contains":
        return f"//{element}[contains(@{attribute},'{value}')]"
    elif operator == "starts-with":
        return f"//{element}[starts-with(@{attribute},'{value}')]"
    else:
        return f"//{element}[@{attribute}{operator}'{value}']"
