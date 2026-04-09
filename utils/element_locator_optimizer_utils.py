"""
Element locator optimization utilities.

Optimize element locators for reliability and performance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class LocatorScore:
    """Score for a locator strategy."""
    strategy: str
    locator: str
    reliability: float
    performance: float
    uniqueness: float
    overall: float


class LocatorOptimizer:
    """Optimize element locators."""
    
    def __init__(self):
        self._strategy_weights = {
            "accessibility_id": {"reliability": 0.9, "performance": 0.95, "uniqueness": 0.9},
            "resource_id": {"reliability": 0.85, "performance": 0.9, "uniqueness": 0.85},
            "class_name": {"reliability": 0.7, "performance": 0.8, "uniqueness": 0.5},
            "text": {"reliability": 0.75, "performance": 0.75, "uniqueness": 0.7},
            "xpath": {"reliability": 0.6, "performance": 0.5, "uniqueness": 0.8},
            "css": {"reliability": 0.65, "performance": 0.7, "uniqueness": 0.75}
        }
    
    def optimize_locator(self, locator: str) -> list[LocatorScore]:
        """Optimize a locator and return scored alternatives."""
        strategy = self._detect_strategy(locator)
        scores = []
        
        scores.append(LocatorScore(
            strategy=strategy,
            locator=locator,
            reliability=0.7,
            performance=0.7,
            uniqueness=0.7,
            overall=0.7
        ))
        
        weights = self._strategy_weights.get(strategy, {"reliability": 0.7, "performance": 0.7, "uniqueness": 0.7})
        scores[0].reliability = weights["reliability"]
        scores[0].performance = weights["performance"]
        scores[0].uniqueness = weights["uniqueness"]
        scores[0].overall = (
            weights["reliability"] * 0.4 +
            weights["performance"] * 0.3 +
            weights["uniqueness"] * 0.3
        )
        
        return sorted(scores, key=lambda s: s.overall, reverse=True)
    
    def _detect_strategy(self, locator: str) -> str:
        """Detect locator strategy from string."""
        if locator.startswith("accessibility_id:"):
            return "accessibility_id"
        elif locator.startswith("resource-id:") or locator.startswith("resource_id:"):
            return "resource_id"
        elif locator.startswith("class:"):
            return "class_name"
        elif locator.startswith("text:") or locator.startswith("//*["):
            return "text"
        elif locator.startswith("//") or locator.startswith("(//"):
            return "xpath"
        elif locator.startswith("#") or locator.startswith("."):
            return "css"
        return "unknown"
    
    def simplify_xpath(self, xpath: str) -> str:
        """Simplify XPath locator."""
        xpath = xpath.strip()
        
        xpath = re.sub(r'//\*\[@[^]]+="[^"]+"\]\[@[^]]+="[^"]+"\]', '//*', xpath)
        
        xpath = re.sub(r'//div\[@[^]]+\]//', '//', xpath)
        
        return xpath
    
    def make_unique_locator(
        self,
        base_locator: str,
        index: Optional[int] = None
    ) -> str:
        """Make a locator unique by adding index."""
        if index is not None:
            if base_locator.startswith("//"):
                return f"({base_locator})[{index + 1}]"
            elif "[" in base_locator:
                return f"{base_locator}[{index + 1}]"
        
        return base_locator


class LocatorValidator:
    """Validate locator strings."""
    
    @staticmethod
    def is_valid_xpath(xpath: str) -> bool:
        """Check if XPath is valid."""
        try:
            if not (xpath.startswith("//") or xpath.startswith("(//")):
                return False
            return True
        except Exception:
            return False
    
    @staticmethod
    def is_valid_css(css: str) -> bool:
        """Check if CSS selector is valid."""
        try:
            if css.startswith("#") or css.startswith(".") or css.startswith("["):
                return True
            return False
        except Exception:
            return False
    
    @staticmethod
    def get_locator_stability(locator: str) -> str:
        """Assess locator stability."""
        if "index" in locator.lower():
            return "unstable"
        if "relative" in locator.lower():
            return "moderate"
        if "accessibility" in locator.lower():
            return "stable"
        if "resource-id" in locator.lower():
            return "stable"
        return "moderate"
