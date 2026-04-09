"""
Element Finder Action Module.

Finds UI elements using various strategies: XPath, CSS selector,
text content, and visual heuristics.
"""

import re
from typing import Any, Callable, Optional


class ElementCriteria:
    """Criteria for element matching."""

    def __init__(
        self,
        tag: Optional[str] = None,
        text: Optional[str] = None,
        text_contains: Optional[str] = None,
        class_name: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        href: Optional[str] = None,
        placeholder: Optional[str] = None,
        role: Optional[str] = None,
        visible: Optional[bool] = None,
        enabled: Optional[bool] = None,
    ):
        """
        Initialize element criteria.

        Args:
            tag: HTML/XML tag name.
            text: Exact text match.
            text_contains: Text must contain this substring.
            class_name: CSS class name (partial match).
            id: Element ID.
            name: Name attribute.
            href: Href attribute (for anchors).
            placeholder: Placeholder attribute.
            role: ARIA role attribute.
            visible: Element visibility requirement.
            enabled: Element enabled state requirement.
        """
        self.tag = tag
        self.text = text
        self.text_contains = text_contains
        self.class_name = class_name
        self.id = id
        self.name = name
        self.href = href
        self.placeholder = placeholder
        self.role = role
        self.visible = visible
        self.enabled = enabled

    def matches(self, element: dict) -> bool:
        """
        Check if an element matches all criteria.

        Args:
            element: Element dictionary with attributes.

        Returns:
            True if matches, False otherwise.
        """
        if self.tag:
            if element.get("tag", "").lower() != self.tag.lower():
                return False

        if self.id:
            if element.get("id", "") != self.id:
                return False

        if self.name:
            if element.get("name", "") != self.name:
                return False

        if self.class_name:
            class_attr = element.get("class", "")
            if self.class_name not in class_attr:
                return False

        if self.role:
            if element.get("role", "") != self.role:
                return False

        if self.href:
            if self.href not in element.get("href", ""):
                return False

        if self.placeholder:
            if element.get("placeholder", "") != self.placeholder:
                return False

        if self.text:
            elem_text = element.get("text", "").strip()
            if elem_text != self.text:
                return False

        if self.text_contains:
            elem_text = element.get("text", "").lower()
            if self.text_contains.lower() not in elem_text:
                return False

        if self.visible is not None:
            if element.get("visible", True) != self.visible:
                return False

        if self.enabled is not None:
            if element.get("enabled", True) != self.enabled:
                return False

        return True


class ElementFinder:
    """Finds elements in a DOM-like structure."""

    def __init__(self):
        """Initialize element finder."""
        self._xpath_engine = XPathEngine()

    def find_all(
        self,
        dom: list[dict],
        criteria: ElementCriteria,
    ) -> list[dict]:
        """
        Find all elements matching criteria.

        Args:
            dom: List of element dictionaries (DOM tree).
            criteria: Element matching criteria.

        Returns:
            List of matching element dictionaries.
        """
        results = []
        self._search(dom, criteria, results)
        return results

    def find_first(
        self,
        dom: list[dict],
        criteria: ElementCriteria,
    ) -> Optional[dict]:
        """
        Find first element matching criteria.

        Args:
            dom: List of element dictionaries.
            criteria: Element matching criteria.

        Returns:
            First matching element or None.
        """
        for element in self._flatten(dom):
            if criteria.matches(element):
                return element
        return None

    def find_by_xpath(self, dom: list[dict], xpath: str) -> list[dict]:
        """
        Find elements using XPath expression.

        Args:
            dom: DOM structure.
            xpath: XPath expression.

        Returns:
            List of matching elements.
        """
        return self._xpath_engine.find(dom, xpath)

    def _search(
        self,
        elements: list[dict],
        criteria: ElementCriteria,
        results: list[dict],
    ) -> None:
        """Recursively search DOM for matching elements."""
        for elem in elements:
            if criteria.matches(elem):
                results.append(elem)
            children = elem.get("children", [])
            if children:
                self._search(children, criteria, results)

    def _flatten(self, elements: list[dict]) -> list[dict]:
        """Flatten DOM tree into list."""
        result = []
        for elem in elements:
            result.append(elem)
            children = elem.get("children", [])
            if children:
                result.extend(self._flatten(children))
        return result


class XPathEngine:
    """Simple XPath expression engine for element finding."""

    def find(self, dom: list[dict], xpath: str) -> list[dict]:
        """
        Execute XPath query on DOM.

        Args:
            dom: DOM structure.
            xpath: XPath expression.

        Returns:
            List of matching elements.
        """
        if xpath.startswith("//"):
            tag = xpath[2:]
            return self._descendant_or_self(dom, tag)
        elif xpath.startswith("/"):
            return self._descendant_or_self(dom, xpath[1:])
        return []

    def _descendant_or_self(
        self, elements: list[dict], tag: str
    ) -> list[dict]:
        """Find all descendants matching tag."""
        results = []
        for elem in elements:
            if elem.get("tag", "").lower() == tag.lower():
                results.append(elem)
            children = elem.get("children", [])
            if children:
                results.extend(self._descendant_or_self(children, tag))
        return results


def find_element(
    dom: list[dict],
    tag: Optional[str] = None,
    text: Optional[str] = None,
    text_contains: Optional[str] = None,
    class_name: Optional[str] = None,
    id: Optional[str] = None,
    role: Optional[str] = None,
) -> Optional[dict]:
    """
    Convenience function to find a single element.

    Args:
        dom: DOM structure.
        tag: HTML tag name.
        text: Exact text match.
        text_contains: Text contains substring.
        class_name: Class name.
        id: Element ID.
        role: ARIA role.

    Returns:
        First matching element or None.
    """
    criteria = ElementCriteria(
        tag=tag,
        text=text,
        text_contains=text_contains,
        class_name=class_name,
        id=id,
        role=role,
    )
    finder = ElementFinder()
    return finder.find_first(dom, criteria)
