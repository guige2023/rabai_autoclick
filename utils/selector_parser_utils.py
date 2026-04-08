"""
Selector Parser Utilities

Provides utilities for parsing element selectors
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SelectorPart:
    """Part of a selector."""
    type: str
    value: str


class SelectorParser:
    """
    Parses element selectors into components.
    
    Supports CSS, XPath, and accessibility
    selector formats.
    """

    def __init__(self) -> None:
        self._selector_types = ["id", "class", "tag", "xpath", "css"]

    def parse(self, selector: str) -> list[SelectorPart]:
        """
        Parse a selector string.
        
        Args:
            selector: Selector string.
            
        Returns:
            List of SelectorPart components.
        """
        parts = []

        if selector.startswith("#"):
            parts.append(SelectorPart(type="id", value=selector[1:]))
        elif selector.startswith("."):
            parts.append(SelectorPart(type="class", value=selector[1:]))
        elif selector.startswith("//") or selector.startswith("/"):
            parts.append(SelectorPart(type="xpath", value=selector))
        elif "=" in selector:
            key, value = selector.split("=", 1)
            parts.append(SelectorPart(type=key.strip(), value=value.strip()))
        else:
            parts.append(SelectorPart(type="tag", value=selector))

        return parts

    def to_xpath(self, selector: str) -> str | None:
        """Convert selector to XPath."""
        parts = self.parse(selector)
        if not parts:
            return None
        xpath = "//"
        for part in parts:
            if part.type == "tag":
                xpath += part.value
            elif part.type == "id":
                xpath += f"[@id='{part.value}']"
            elif part.type == "class":
                xpath += f"[contains(@class,'{part.value}')]"
        return xpath
