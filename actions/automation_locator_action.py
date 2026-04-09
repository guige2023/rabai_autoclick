"""Automation Locator Action Module.

Provides intelligent element location strategies with fallback
mechanisms for UI automation reliability.

Author: RabAi Team
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LocatorType(Enum):
    """Types of element locators."""
    ID = "id"
    NAME = "name"
    CSS_SELECTOR = "css"
    XPATH = "xpath"
    TEXT = "text"
    PARTIAL_TEXT = "partial_text"
    CLASS = "class"
    TAG = "tag"
    ATTRIBUTE = "attribute"
    IMAGE = "image"
    COORDINATE = "coordinate"


class ConfidenceLevel(Enum):
    """Confidence levels for locator matches."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


@dataclass
class LocatorStrategy:
    """Strategy for locating an element."""
    locator_type: LocatorType
    value: str
    timeout_ms: int = 5000
    retry_count: int = 3
    confidence_threshold: float = 0.8


@dataclass
class LocatorResult:
    """Result of element location attempt."""
    found: bool
    element: Optional[Any] = None
    confidence: float = 0.0
    confidence_level: ConfidenceLevel = ConfidenceLevel.NONE
    attempts: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class ElementMatch:
    """Matched element with confidence scoring."""
    element: Any
    confidence: float
    locator_used: LocatorStrategy
    attributes: Dict[str, Any] = field(default_factory=dict)


class LocatorParser:
    """Parses and validates locator strings."""

    PATTERNS = {
        LocatorType.ID: re.compile(r'^#[\w\-\.]+$'),
        LocatorType.CLASS: re.compile(r'^\.[\w\-\.\s]+$'),
        LocatorType.CSS_SELECTOR: re.compile(r'^[^\'"]+:[^"\']+$'),
        LocatorType.XPATH: re.compile(r'^//|/'),
        LocatorType.ATTRIBUTE: re.compile(r'^\[.+\]$'),
    }

    @classmethod
    def detect_locator_type(cls, locator: str) -> Optional[LocatorType]:
        """Auto-detect locator type from string."""
        locator = locator.strip()

        for loc_type, pattern in cls.PATTERNS.items():
            if pattern.match(locator):
                return loc_type

        if locator.startswith("//") or locator.startswith("/"):
            return LocatorType.XPATH
        elif locator.startswith("#"):
            return LocatorType.ID
        elif locator.startswith("."):
            return LocatorType.CLASS

        return LocatorType.TEXT

    @classmethod
    def validate_locator(
        cls,
        locator_type: LocatorType,
        value: str
    ) -> Tuple[bool, Optional[str]]:
        """Validate locator format."""
        if not value or not value.strip():
            return False, "Locator value cannot be empty"

        if locator_type == LocatorType.XPATH:
            if not (value.startswith("//") or value.startswith("/")):
                return False, "XPath must start with // or /"

        if locator_type == LocatorType.CSS_SELECTOR:
            if ":" not in value and not value.strip():
                return False, "CSS selector appears invalid"

        return True, None


class AutomationLocator:
    """Intelligent element locator with fallback strategies."""

    def __init__(self):
        self.locator_cache: Dict[str, List[LocatorResult]] = {}
        self.strategy_history: Dict[str, List[LocatorStrategy]] = {}
        self.success_rates: Dict[LocatorType, float] = {}

        for lt in LocatorType:
            self.success_rates[lt] = 0.5

    def locate(
        self,
        locator: str,
        locator_type: Optional[LocatorType] = None,
        timeout_ms: int = 5000
    ) -> LocatorResult:
        """Locate element with given locator."""
        start_time = time.time()

        if locator_type is None:
            locator_type = LocatorParser.detect_locator_type(locator)

        if locator_type is None:
            return LocatorResult(
                found=False,
                error="Could not detect locator type",
                duration_ms=(time.time() - start_time) * 1000
            )

        valid, error = LocatorParser.validate_locator(locator_type, locator)
        if not valid:
            return LocatorResult(
                found=False,
                error=error,
                duration_ms=(time.time() - start_time) * 1000
            )

        result = LocatorResult(
            found=False,
            attempts=1,
            duration_ms=0.0
        )

        attempts = 0
        end_time = time.time() + (timeout_ms / 1000.0)

        while attempts < 10 and time.time() < end_time:
            attempts += 1

            found = self._try_locate(locator_type, locator)
            result.attempts = attempts

            if found:
                result.found = True
                result.confidence = 0.9
                result.confidence_level = ConfidenceLevel.HIGH
                self._record_success(locator_type)
                break

            time.sleep(0.1)

        result.duration_ms = (time.time() - start_time) * 1000
        return result

    def _try_locate(self, locator_type: LocatorType, value: str) -> bool:
        """Attempt to locate element (mock - real implementation would use browser automation)."""
        return False

    def _record_success(self, locator_type: LocatorType) -> None:
        """Record successful location for statistics."""
        current = self.success_rates[locator_type]
        self.success_rates[locator_type] = current * 0.9 + 0.1

    def _record_failure(self, locator_type: LocatorType) -> None:
        """Record failed location for statistics."""
        current = self.success_rates[locator_type]
        self.success_rates[locator_type] = current * 0.9

    def get_fallback_strategies(
        self,
        element_id: str
    ) -> List[LocatorStrategy]:
        """Generate fallback strategies for element."""
        strategies = []

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.ID,
            value=f"#{element_id}"
        ))

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.NAME,
            value=element_id
        ))

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.CSS_SELECTOR,
            value=f"[data-testid='{element_id}']"
        ))

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.XPATH,
            value=f"//*[@id='{element_id}']"
        ))

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.XPATH,
            value=f"//*[contains(@class, '{element_id}')]"
        ))

        strategies.append(LocatorStrategy(
            locator_type=LocatorType.TEXT,
            value=element_id
        ))

        return strategies

    def rank_strategies(
        self,
        strategies: List[LocatorStrategy]
    ) -> List[LocatorStrategy]:
        """Rank strategies by historical success rate."""
        def strategy_score(s: LocatorStrategy) -> float:
            base_score = self.success_rates.get(s.locator_type, 0.5)

            if s.locator_type == LocatorType.ID:
                base_score *= 1.2
            elif s.locator_type == LocatorType.DATA_ATTRIBUTE:
                base_score *= 1.1

            return base_score

        return sorted(strategies, key=strategy_score, reverse=True)

    def smart_locate(
        self,
        element_descriptor: Dict[str, Any],
        timeout_ms: int = 5000
    ) -> LocatorResult:
        """Smart locate with multiple fallbacks."""
        start_time = time.time()

        element_id = element_descriptor.get("id", "")
        alternatives = element_descriptor.get("alternatives", [])
        priority = element_descriptor.get("priority", [])

        strategies = []

        if element_id:
            strategies.extend(self.get_fallback_strategies(element_id))

        for alt in alternatives:
            if isinstance(alt, str):
                strategies.append(LocatorStrategy(
                    locator_type=LocatorParser.detect_locator_type(alt) or LocatorType.TEXT,
                    value=alt
                ))

        ranked = self.rank_strategies(strategies)

        for strategy in ranked[:5]:
            result = self.locate(
                strategy.value,
                strategy.locator_type,
                timeout_ms // 5
            )

            if result.found:
                result.duration_ms = (time.time() - start_time) * 1000
                return result

        return LocatorResult(
            found=False,
            error="All fallback strategies failed",
            duration_ms=(time.time() - start_time) * 1000
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Get locator statistics."""
        return {
            "success_rates": {
                lt.value: rate
                for lt, rate in self.success_rates.items()
            },
            "cached_locators": len(self.locator_cache),
            "strategy_history_count": len(self.strategy_history)
        }


class AutomationLocatorAction(BaseAction):
    """Action for element locator operations."""

    def __init__(self):
        super().__init__("automation_locator")
        self._locator = AutomationLocator()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute locator action."""
        try:
            operation = params.get("operation", "locate")

            if operation == "locate":
                return self._locate(params)
            elif operation == "detect":
                return self._detect(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "fallbacks":
                return self._get_fallbacks(params)
            elif operation == "smart_locate":
                return self._smart_locate(params)
            elif operation == "stats":
                return self._get_stats(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _locate(self, params: Dict[str, Any]) -> ActionResult:
        """Locate element by locator."""
        locator = params.get("locator", "")
        locator_type_str = params.get("type")
        timeout_ms = params.get("timeout_ms", 5000)

        locator_type = None
        if locator_type_str:
            try:
                locator_type = LocatorType(locator_type_str)
            except ValueError:
                return ActionResult(
                    success=False,
                    message=f"Invalid locator type: {locator_type_str}"
                )

        result = self._locator.locate(locator, locator_type, timeout_ms)

        return ActionResult(
            success=result.found,
            message="Element found" if result.found else "Element not found",
            data={
                "found": result.found,
                "confidence": result.confidence,
                "confidence_level": result.confidence_level.value,
                "attempts": result.attempts,
                "duration_ms": result.duration_ms,
                "error": result.error
            }
        )

    def _detect(self, params: Dict[str, Any]) -> ActionResult:
        """Auto-detect locator type."""
        locator = params.get("locator", "")

        detected = LocatorParser.detect_locator_type(locator)

        return ActionResult(
            success=True,
            data={
                "locator": locator,
                "detected_type": detected.value if detected else None
            }
        )

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate locator format."""
        locator = params.get("locator", "")
        locator_type_str = params.get("type")

        if not locator_type_str:
            detected = LocatorParser.detect_locator_type(locator)
            locator_type = detected or LocatorType.TEXT
        else:
            locator_type = LocatorType(locator_type_str)

        valid, error = LocatorParser.validate_locator(locator_type, locator)

        return ActionResult(
            success=valid,
            message=error or "Locator is valid",
            data={
                "valid": valid,
                "error": error,
                "detected_type": locator_type.value
            }
        )

    def _get_fallbacks(self, params: Dict[str, Any]) -> ActionResult:
        """Get fallback strategies for element."""
        element_id = params.get("element_id", "")

        if not element_id:
            return ActionResult(
                success=False,
                message="element_id is required"
            )

        strategies = self._locator.get_fallback_strategies(element_id)

        return ActionResult(
            success=True,
            data={
                "strategies": [
                    {"type": s.locator_type.value, "value": s.value}
                    for s in strategies
                ]
            }
        )

    def _smart_locate(self, params: Dict[str, Any]) -> ActionResult:
        """Smart locate with fallbacks."""
        descriptor = params.get("descriptor", {})
        timeout_ms = params.get("timeout_ms", 5000)

        if not descriptor:
            return ActionResult(
                success=False,
                message="descriptor is required"
            )

        result = self._locator.smart_locate(descriptor, timeout_ms)

        return ActionResult(
            success=result.found,
            message="Element found" if result.found else "Element not found",
            data={
                "found": result.found,
                "confidence": result.confidence,
                "attempts": result.attempts,
                "duration_ms": result.duration_ms,
                "error": result.error
            }
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get locator statistics."""
        stats = self._locator.get_statistics()
        return ActionResult(success=True, data=stats)
