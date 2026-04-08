"""Element Path and Locator Utilities.

Builds and manages element locators using various strategies.
Supports XPath, CSS selectors, accessibility paths, and coordinate-based locators.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class LocatorStrategy(Enum):
    """Strategies for element location."""

    XPATH = auto()
    CSS_SELECTOR = auto()
    ACCESSIBILITY_ID = auto()
    ACCESSIBILITY_ROLE = auto()
    IMAGE = auto()
    COORDINATE = auto()
    TEXT = auto()
    COMPOUND = auto()


@dataclass
class Locator:
    """A locator for finding UI elements.

    Attributes:
        strategy: The locator strategy to use.
        value: The locator value string.
        confidence: Confidence score (0.0 to 1.0).
        timeout_ms: Maximum time to wait for element.
        index: Index for multiple matches (0 = first).
    """

    strategy: LocatorStrategy
    value: str
    confidence: float = 1.0
    timeout_ms: int = 5000
    index: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "strategy": self.strategy.name,
            "value": self.value,
            "confidence": self.confidence,
            "timeout_ms": self.timeout_ms,
            "index": self.index,
        }

    def __str__(self) -> str:
        """String representation of the locator."""
        return f"{self.strategy.name}:{self.value}"


@dataclass
class ElementLocator:
    """Complete locator information for an element.

    Attributes:
        element_id: Unique element identifier.
        locators: List of possible locators for the element.
        alternative_locators: Locators with lower confidence.
    """

    element_id: str
    locators: list[Locator] = field(default_factory=list)
    alternative_locators: list[Locator] = field(default_factory=list)


class XPathBuilder:
    """Builds XPath locators for UI elements.

    Example:
        builder = XPathBuilder()
        xpath = builder.by_role("button").with_text("Submit").build()
    """

    def __init__(self):
        """Initialize the XPath builder."""
        self._conditions: list[str] = []

    def by_role(self, role: str) -> "XPathBuilder":
        """Add role condition.

        Args:
            role: Element role (e.g., 'button', 'link').

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'@role="{role}"')
        return self

    def by_id(self, element_id: str) -> "XPathBuilder":
        """Add ID condition.

        Args:
            element_id: Element ID.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'@id="{element_id}"')
        return self

    def by_name(self, name: str) -> "XPathBuilder":
        """Add name condition.

        Args:
            name: Element name attribute.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'@name="{name}"')
        return self

    def by_label(self, label: str) -> "XPathBuilder":
        """Add label condition.

        Args:
            label: Label text.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'@aria-label="{label}"')
        return self

    def with_text(self, text: str) -> "XPathBuilder":
        """Add text content condition.

        Args:
            text: Text content to match.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'contains(text(), "{text}")')
        return self

    def by_class(self, class_name: str) -> "XPathBuilder":
        """Add class condition.

        Args:
            class_name: CSS class name.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f'contains(@class, "{class_name}")')
        return self

    def at_position(self, index: int) -> "XPathBuilder":
        """Add position condition.

        Args:
            index: 1-based index.

        Returns:
            Self for chaining.
        """
        self._conditions.append(f"position()={index}")
        return self

    def build(self) -> str:
        """Build the final XPath.

        Returns:
            Complete XPath string.
        """
        if not self._conditions:
            return "//*"
        return "//*" + "".join(f"[{cond}]" for cond in self._conditions)


class CSSSelectorBuilder:
    """Builds CSS selectors for UI elements.

    Example:
        builder = CSSSelectorBuilder()
        selector = builder.tag("button").class_name("primary").build()
    """

    def __init__(self):
        """Initialize the CSS selector builder."""
        self._parts: list[str] = []

    def tag(self, tag_name: str) -> "CSSSelectorBuilder":
        """Set element tag.

        Args:
            tag_name: HTML tag name.

        Returns:
            Self for chaining.
        """
        self._parts.append(tag_name)
        return self

    def class_name(self, class_name: str) -> "CSSSelectorBuilder":
        """Add class selector.

        Args:
            class_name: CSS class name.

        Returns:
            Self for chaining.
        """
        self._parts.append(f".{class_name}")
        return self

    def id(self, element_id: str) -> "CSSSelectorBuilder":
        """Add ID selector.

        Args:
            element_id: Element ID.

        Returns:
            Self for chaining.
        """
        self._parts.append(f"#{element_id}")
        return self

    def attribute(self, name: str, value: str) -> "CSSSelectorBuilder":
        """Add attribute selector.

        Args:
            name: Attribute name.
            value: Attribute value.

        Returns:
            Self for chaining.
        """
        self._parts.append(f'[{name}="{value}"]')
        return self

    def attribute_contains(self, name: str, value: str) -> "CSSSelectorBuilder":
        """Add attribute contains selector.

        Args:
            name: Attribute name.
            value: Partial attribute value.

        Returns:
            Self for chaining.
        """
        self._parts.append(f'[{name}*="{value}"]')
        return self

    def nth_child(self, index: int) -> "CSSSelectorBuilder":
        """Add nth-child selector.

        Args:
            index: 1-based index.

        Returns:
            Self for chaining.
        """
        self._parts.append(f":nth-child({index})")
        return self

    def build(self) -> str:
        """Build the final CSS selector.

        Returns:
            Complete CSS selector string.
        """
        return "".join(self._parts)


class LocatorOptimizer:
    """Optimizes locators for reliability and performance.

    Example:
        optimizer = LocatorOptimizer()
        optimized = optimizer.optimize(locator, element_data)
    """

    def __init__(self):
        """Initialize the optimizer."""
        pass

    def optimize(
        self,
        locator: Locator,
        element_data: dict,
    ) -> Locator:
        """Optimize a locator for the given element.

        Args:
            locator: Original locator.
            element_data: Element properties for context.

        Returns:
            Optimized locator.
        """
        optimized = Locator(
            strategy=locator.strategy,
            value=locator.value,
            confidence=locator.confidence,
            timeout_ms=locator.timeout_ms,
            index=locator.index,
        )

        # Boost confidence for locators with unique identifiers
        if locator.strategy == LocatorStrategy.ACCESSIBILITY_ID:
            if element_data.get("has_unique_id"):
                optimized.confidence = min(1.0, locator.confidence * 1.2)

        # Adjust timeout based on expected load time
        if element_data.get("is_loading_indicator"):
            optimized.timeout_ms = max(locator.timeout_ms, 10000)

        return optimized

    def make_unique(
        self,
        locator: Locator,
        siblings: list[dict],
    ) -> Locator:
        """Make a locator unique among sibling elements.

        Args:
            locator: Original locator.
            siblings: List of sibling element data.

        Returns:
            Locator with added uniqueness.
        """
        if len(siblings) <= 1:
            return locator

        unique_attrs = self._find_unique_attributes(siblings)
        if not unique_attrs:
            # Use index as fallback
            return Locator(
                strategy=locator.strategy,
                value=locator.value,
                confidence=locator.confidence * 0.8,
                timeout_ms=locator.timeout_ms,
                index=0,
            )

        # Add unique attribute to locator
        if locator.strategy == LocatorStrategy.XPATH:
            for attr, value in unique_attrs.items():
                xpath_builder = XPathBuilder()
                xpath_builder._conditions = [f"@{attr}=\"{value}\""]
                extra = xpath_builder.build()
                return Locator(
                    strategy=locator.strategy,
                    value=locator.value + extra,
                    confidence=locator.confidence * 1.1,
                    timeout_ms=locator.timeout_ms,
                    index=locator.index,
                )

        return locator

    def _find_unique_attributes(
        self,
        elements: list[dict],
    ) -> dict[str, str]:
        """Find attributes that uniquely identify an element.

        Args:
            elements: List of element data dictionaries.

        Returns:
            Dictionary of unique attribute name to value.
        """
        candidates = ["id", "name", "aria-label", "data-testid"]

        for attr in candidates:
            values = [e.get(attr) for e in elements]
            if len(values) == len(set(values)) and all(values):
                return {attr: elements[0].get(attr, "")}

        return {}


class LocatorFactory:
    """Factory for creating element locators.

    Example:
        factory = LocatorFactory()
        locators = factory.create_locators(element_data)
    """

    def __init__(self):
        """Initialize the factory."""
        self.xpath_builder = XPathBuilder()
        self.css_builder = CSSSelectorBuilder()
        self.optimizer = LocatorOptimizer()

    def create_locators(
        self,
        element_data: dict,
    ) -> ElementLocator:
        """Create multiple locator strategies for an element.

        Args:
            element_data: Element properties.

        Returns:
            ElementLocator with all possible locators.
        """
        element_id = element_data.get("id", "unknown")
        locators: list[Locator] = []
        alt_locators: list[Locator] = []

        role = element_data.get("role", "")
        name = element_data.get("name", "")
        label = element_data.get("label", element_data.get("aria-label", ""))
        text = element_data.get("text", "")
        class_name = element_data.get("class", "")

        # Accessibility ID (highest priority)
        if element_id:
            locators.append(
                Locator(
                    strategy=LocatorStrategy.ACCESSIBILITY_ID,
                    value=element_id,
                    confidence=0.95,
                )
            )

        # XPath by role and label
        if role and label:
            xpath = (
                XPathBuilder()
                .by_role(role)
                .by_label(label)
                .build()
            )
            locators.append(
                Locator(
                    strategy=LocatorStrategy.XPATH,
                    value=xpath,
                    confidence=0.85,
                )
            )

        # XPath by role and text
        if role and text:
            xpath = (
                XPathBuilder()
                .by_role(role)
                .with_text(text)
                .build()
            )
            locators.append(
                Locator(
                    strategy=LocatorStrategy.XPATH,
                    value=xpath,
                    confidence=0.8,
                )
            )

        # CSS by class
        if class_name:
            css = CSSSelectorBuilder().tag("*").class_name(class_name.split()[0]).build()
            alt_locators.append(
                Locator(
                    strategy=LocatorStrategy.CSS_SELECTOR,
                    value=css,
                    confidence=0.6,
                )
            )

        # Coordinate fallback
        bounds = element_data.get("bounds")
        if bounds:
            cx = bounds[0] + bounds[2] / 2
            cy = bounds[1] + bounds[3] / 2
            locators.append(
                Locator(
                    strategy=LocatorStrategy.COORDINATE,
                    value=f"{cx},{cy}",
                    confidence=0.5,
                )
            )

        return ElementLocator(
            element_id=element_id,
            locators=locators,
            alternative_locators=alt_locators,
        )
