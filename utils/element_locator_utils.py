"""
Element Locator Utilities

Provides utilities for locating and addressing
UI elements in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ElementLocator:
    """Locator for addressing a UI element."""
    selector: str
    locator_type: str = "css"
    timeout_ms: int = 5000
    retry_count: int = 3


class LocatorRegistry:
    """
    Registry for element locators.
    
    Manages locator configurations and provides
    lookup and resolution services.
    """

    def __init__(self) -> None:
        self._locators: dict[str, ElementLocator] = {}

    def register(
        self,
        name: str,
        selector: str,
        locator_type: str = "css",
        timeout_ms: int = 5000,
    ) -> None:
        """Register a named locator."""
        self._locators[name] = ElementLocator(
            selector=selector,
            locator_type=locator_type,
            timeout_ms=timeout_ms,
        )

    def get(self, name: str) -> ElementLocator | None:
        """Get a registered locator by name."""
        return self._locators.get(name)

    def unregister(self, name: str) -> bool:
        """Unregister a locator."""
        if name in self._locators:
            del self._locators[name]
            return True
        return False

    def list_locators(self) -> list[str]:
        """List all registered locator names."""
        return list(self._locators.keys())


def build_locator(
    selector: str,
    locator_type: str = "css",
) -> ElementLocator:
    """Build an element locator."""
    return ElementLocator(selector=selector, locator_type=locator_type)
