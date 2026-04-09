"""Automation Accessibility Audit Action Module.

Provides comprehensive accessibility auditing for UI automation including
WCAG compliance checking, screen reader simulation, keyboard navigation
analysis, and automated remediation suggestions.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class WCAGLevel(Enum):
    """WCAG compliance levels."""
    A = "A"
    AA = "AA"
    AAA = "AAA"


class Severity(Enum):
    """Issue severity levels."""
    CRITICAL = "critical"
    MAJOR = "major"
    MODERATE = "moderate"
    MINOR = "minor"
    INFO = "info"


@dataclass
class AccessibilityIssue:
    """An accessibility issue found during audit."""
    issue_id: str
    severity: Severity
    wcag_criterion: Optional[str] = None
    wcag_level: Optional[WCAGLevel] = None
    element_selector: Optional[str] = None
    element_description: Optional[str] = None
    issue_description: str = ""
    suggested_fix: Optional[str] = None
    impact: str = ""
    tags: List[str] = field(default_factory=list)


@dataclass
class AuditResult:
    """Result of an accessibility audit."""
    page_url: str
    timestamp: datetime
    total_elements: int = 0
    issues: List[AccessibilityIssue] = field(default_factory=list)
    wcag_compliance: Dict[WCAGLevel, float] = field(default_factory=dict)
    critical_count: int = 0
    major_count: int = 0
    moderate_count: int = 0
    minor_count: int = 0
    score: float = 0.0


@dataclass
class AccessibilityConfig:
    """Configuration for accessibility auditing."""
    wcag_level: WCAGLevel = WCAGLevel.AA
    check_color_contrast: bool = True
    check_keyboard_navigation: bool = True
    check_screen_reader: bool = True
    check_alt_text: bool = True
    check_heading_order: bool = True
    check_form_labels: bool = True
    check_focus_visibility: bool = True
    min_color_contrast_ratio: float = 4.5
    generate_remediation: bool = True


class WCAGChecker:
    """Check WCAG compliance criteria."""

    def __init__(self, level: WCAGLevel = WCAGLevel.AA):
        self._level = level
        self._criteria_map = self._build_criteria_map()

    def _build_criteria_map(self) -> Dict[str, Dict[str, Any]]:
        """Build WCAG criteria mapping."""
        return {
            "1.1.1": {
                "name": "Non-text Content",
                "level": WCAGLevel.A,
                "description": "All non-text content has text alternative"
            },
            "1.3.1": {
                "name": "Info and Relationships",
                "level": WCAGLevel.A,
                "description": "Information structure is programmatically determinable"
            },
            "1.4.3": {
                "name": "Contrast (Minimum)",
                "level": WCAGLevel.AA,
                "description": "Text has contrast ratio of at least 4.5:1"
            },
            "2.1.1": {
                "name": "Keyboard",
                "level": WCAGLevel.A,
                "description": "All functionality available by keyboard"
            },
            "2.4.1": {
                "name": "Bypass Blocks",
                "level": WCAGLevel.A,
                "description": "Mechanisms to bypass navigation blocks"
            },
            "2.4.2": {
                "name": "Page Titled",
                "level": WCAGLevel.A,
                "description": "Pages have descriptive titles"
            },
            "2.4.4": {
                "name": "Link Purpose",
                "level": WCAGLevel.A,
                "description": "Link purpose is determinable from link text"
            },
            "2.4.6": {
                "name": "Headings and Labels",
                "level": WCAGLevel.AA,
                "description": "Headings and labels describe topic or purpose"
            },
            "2.4.7": {
                "name": "Focus Visible",
                "level": WCAGLevel.AA,
                "description": "Keyboard focus indicator is visible"
            },
            "3.1.1": {
                "name": "Language of Page",
                "level": WCAGLevel.A,
                "description": "Page language is programmatically determinable"
            },
            "3.3.1": {
                "name": "Error Identification",
                "level": WCAGLevel.A,
                "description": "Errors are identified with text descriptions"
            },
            "4.1.1": {
                "name": "Parsing",
                "level": WCAGLevel.A,
                "description": "Markup is valid"
            },
            "4.1.2": {
                "name": "Name, Role, Value",
                "level": WCAGLevel.A,
                "description": "UI components have accessible names and states"
            }
        }

    def get_criteria_for_level(self) -> List[str]:
        """Get all criteria IDs for the configured level."""
        criteria_ids = []
        level_order = [WCAGLevel.A, WCAGLevel.AA, WCAGLevel.AAAA]
        min_idx = level_order.index(self._level)

        for criterion_id, criterion in self._criteria_map.items():
            if level_order.index(criterion["level"]) <= min_idx:
                criteria_ids.append(criterion_id)

        return criteria_ids


class ColorContrastAnalyzer:
    """Analyze color contrast ratios."""

    @staticmethod
    def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
        """Convert hex color to RGB tuple."""
        hex_color = hex_color.lstrip("#")
        if len(hex_color) == 3:
            hex_color = "".join(c * 2 for c in hex_color)
        return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))

    @staticmethod
    def get_relative_luminance(r: int, g: int, b: int) -> float:
        """Calculate relative luminance."""
        def adjust(c):
            c = c / 255.0
            return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

        r_lum = adjust(r)
        g_lum = adjust(g)
        b_lum = adjust(b)
        return 0.2126 * r_lum + 0.7152 * g_lum + 0.0722 * b_lum

    @staticmethod
    def calculate_contrast_ratio(color1: str, color2: str) -> float:
        """Calculate contrast ratio between two colors."""
        try:
            rgb1 = ColorContrastAnalyzer.hex_to_rgb(color1)
            rgb2 = ColorContrastAnalyzer.hex_to_rgb(color2)

            lum1 = ColorContrastAnalyzer.get_relative_luminance(*rgb1)
            lum2 = ColorContrastAnalyzer.get_relative_luminance(*rgb2)

            lighter = max(lum1, lum2)
            darker = min(lum1, lum2)
            return (lighter + 0.05) / (darker + 0.05)
        except Exception:
            return 0.0

    @staticmethod
    def meets_aa_requirement(ratio: float, large_text: bool = False) -> bool:
        """Check if contrast ratio meets WCAG AA requirement."""
        if large_text:
            return ratio >= 3.0
        return ratio >= 4.5


class KeyboardNavigationAnalyzer:
    """Analyze keyboard navigation patterns."""

    @staticmethod
    def check_focus_order(elements: List[Dict[str, Any]]) -> List[AccessibilityIssue]:
        """Check if focus order is logical."""
        issues = []

        focusable_elements = [
            (i, el) for i, el in enumerate(elements)
            if el.get("tabindex", 0) >= 0
        ]

        for i in range(len(focusable_elements) - 1):
            curr_idx, curr_el = focusable_elements[i]
            next_idx, next_el = focusable_elements[i + 1]

            if curr_el.get("tabindex", 1) > next_el.get("tabindex", 0):
                issues.append(AccessibilityIssue(
                    issue_id=f"focus-order-{curr_idx}",
                    severity=Severity.MODERATE,
                    wcag_criterion="2.4.3",
                    wcag_level=WCAGLevel.AA,
                    element_selector=curr_el.get("selector", ""),
                    issue_description="Focus order may not be logical",
                    suggested_fix="Ensure interactive elements are in DOM order"
                ))

        return issues

    @staticmethod
    def check_skip_links(elements: List[Dict[str, Any]]) -> List[AccessibilityIssue]:
        """Check for skip link presence."""
        issues = []
        has_skip_link = any(
            el.get("role") == "link" and "skip" in el.get("text", "").lower()
            for el in elements
        )

        if not has_skip_link:
            issues.append(AccessibilityIssue(
                issue_id="skip-link-missing",
                severity=Severity.MODERATE,
                wcag_criterion="2.4.1",
                wcag_level=WCAGLevel.A,
                issue_description="No skip link found to bypass navigation",
                suggested_fix="Add a skip link as the first focusable element"
            ))

        return issues


class AutomationAccessibilityAuditAction(BaseAction):
    """Action for accessibility auditing."""

    def __init__(self):
        super().__init__(name="automation_accessibility_audit")
        self._config = AccessibilityConfig()
        self._wcag_checker = WCAGChecker(level=self._config.wcag_level)
        self._contrast_analyzer = ColorContrastAnalyzer()
        self._audit_history: List[AuditResult] = []

    def configure(self, config: AccessibilityConfig):
        """Configure accessibility audit settings."""
        self._config = config
        self._wcag_checker = WCAGChecker(level=config.wcag_level)

    def audit(
        self,
        page_url: str,
        elements: List[Dict[str, Any]],
        color_contrast_data: Optional[List[Dict[str, str]]] = None
    ) -> AuditResult:
        """Perform accessibility audit on page elements."""
        issues: List[AccessibilityIssue] = []

        elements_data = elements if elements else []

        if self._config.check_alt_text:
            issues.extend(self._check_alt_text(elements_data))

        if self._config.check_heading_order:
            issues.extend(self._check_heading_order(elements_data))

        if self._config.check_form_labels:
            issues.extend(self._check_form_labels(elements_data))

        if self._config.check_keyboard_navigation:
            issues.extend(KeyboardNavigationAnalyzer.check_focus_order(elements_data))
            issues.extend(KeyboardNavigationAnalyzer.check_skip_links(elements_data))

        if self._config.check_color_contrast and color_contrast_data:
            issues.extend(self._check_color_contrast(color_contrast_data))

        critical = sum(1 for i in issues if i.severity == Severity.CRITICAL)
        major = sum(1 for i in issues if i.severity == Severity.MAJOR)
        moderate = sum(1 for i in issues if i.severity == Severity.MODERATE)
        minor = sum(1 for i in issues if i.severity == Severity.MINOR)

        total_issues = len(issues)
        score = max(0.0, 100.0 - (critical * 15 + major * 10 + moderate * 5 + minor * 2))

        wcag_compliance = self._calculate_wcag_compliance(issues)

        result = AuditResult(
            page_url=page_url,
            timestamp=datetime.now(),
            total_elements=len(elements_data),
            issues=issues,
            wcag_compliance=wcag_compliance,
            critical_count=critical,
            major_count=major,
            moderate_count=moderate,
            minor_count=minor,
            score=score
        )

        self._audit_history.append(result)
        return result

    def _check_alt_text(self, elements: List[Dict[str, Any]]) -> List[AccessibilityIssue]:
        """Check for missing or inadequate alt text."""
        issues = []
        for el in elements:
            tag = el.get("tag", "").lower()
            if tag in ("img", "input", "area"):
                alt = el.get("alt", "")
                if alt is None or alt == "":
                    issues.append(AccessibilityIssue(
                        issue_id=f"missing-alt-{el.get('id', id(el))}",
                        severity=Severity.CRITICAL,
                        wcag_criterion="1.1.1",
                        wcag_level=WCAGLevel.A,
                        element_selector=el.get("selector", ""),
                        element_description=el.get("src", el.get("type", "")),
                        issue_description="Image missing alt text",
                        suggested_fix='Add alt attribute with descriptive text'
                    ))

        return issues

    def _check_heading_order(self, elements: List[Dict[str, Any]]) -> List[AccessibilityIssue]:
        """Check heading hierarchy."""
        issues = []
        headings = [
            (el.get("level", 0), el)
            for el in elements
            if el.get("tag", "").lower() in ("h1", "h2", "h3", "h4", "h5", "h6")
            for level in [int(el.get("tag")[1])]
        ]

        prev_level = 0
        for level, el in headings:
            if level > prev_level + 1:
                issues.append(AccessibilityIssue(
                    issue_id=f"heading-skip-{el.get('id', id(el))}",
                    severity=Severity.MODERATE,
                    wcag_criterion="1.3.1",
                    wcag_level=WCAGLevel.AA,
                    element_selector=el.get("selector", ""),
                    issue_description=f"Heading level skipped from h{prev_level} to h{level}",
                    suggested_fix=f"Use h{prev_level + 1} instead of h{level}"
                ))
            prev_level = level

        return issues

    def _check_form_labels(self, elements: List[Dict[str, Any]]) -> List[AccessibilityIssue]:
        """Check form elements for labels."""
        issues = []
        form_tags = ("input", "select", "textarea")

        for el in elements:
            tag = el.get("tag", "").lower()
            if tag not in form_tags:
                continue

            input_type = el.get("type", "text").lower()
            if input_type in ("hidden", "submit", "button", "image"):
                continue

            has_label = el.get("aria-label") or el.get("aria-labelledby") or el.get("placeholder")
            if not has_label:
                issues.append(AccessibilityIssue(
                    issue_id=f"missing-label-{el.get('id', id(el))}",
                    severity=Severity.MAJOR,
                    wcag_criterion="1.3.1",
                    wcag_level=WCAGLevel.A,
                    element_selector=el.get("selector", ""),
                    issue_description="Form input missing accessible label",
                    suggested_fix='Add label, aria-label, or aria-labelledby attribute'
                ))

        return issues

    def _check_color_contrast(
        self,
        contrast_data: List[Dict[str, str]]
    ) -> List[AccessibilityIssue]:
        """Check color contrast ratios."""
        issues = []

        for item in contrast_data:
            fg = item.get("foreground", "")
            bg = item.get("background", "")
            is_large_text = item.get("large_text", False)

            ratio = self._contrast_analyzer.calculate_contrast_ratio(fg, bg)
            required = 3.0 if is_large_text else 4.5

            if ratio < required:
                issues.append(AccessibilityIssue(
                    issue_id=f"contrast-{item.get('element_id', id(item))}",
                    severity=Severity.MAJOR if ratio < required - 1 else Severity.MODERATE,
                    wcag_criterion="1.4.3",
                    wcag_level=WCAGLevel.AA,
                    element_selector=item.get("selector", ""),
                    issue_description=f"Contrast ratio {ratio:.2f}:1 is below minimum {required}:1",
                    suggested_fix=f"Increase contrast between {fg} and {bg}"
                ))

        return issues

    def _calculate_wcag_compliance(
        self,
        issues: List[AccessibilityIssue]
    ) -> Dict[WCAGLevel, float]:
        """Calculate WCAG compliance percentage."""
        compliance = {}

        for level in WCAGLevel:
            criteria = self._wcag_checker.get_criteria_for_level()
            if level == self._wcag_checker._level:
                total_criteria = len(criteria)
                failed_criteria = set(
                    i.wcag_criterion for i in issues
                    if i.wcag_level == level and i.wcag_criterion
                )
                passed = total_criteria - len(failed_criteria)
                compliance[level] = (passed / max(total_criteria, 1)) * 100
            else:
                compliance[level] = 100.0

        return compliance

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute accessibility audit action."""
        try:
            page_url = params.get("page_url", "")
            elements = params.get("elements", [])
            contrast_data = params.get("color_contrast_data")

            result = self.audit(page_url, elements, contrast_data)

            return ActionResult(
                success=result.score >= 80,
                data={
                    "score": result.score,
                    "total_issues": len(result.issues),
                    "critical": result.critical_count,
                    "major": result.major_count,
                    "moderate": result.moderate_count,
                    "minor": result.minor_count,
                    "wcag_compliance": {
                        k.value: v for k, v in result.wcag_compliance.items()
                    },
                    "issues": [
                        {
                            "id": i.issue_id,
                            "severity": i.severity.value,
                            "criterion": i.wcag_criterion,
                            "description": i.issue_description,
                            "fix": i.suggested_fix
                        }
                        for i in result.issues[:20]
                    ]
                }
            )
        except Exception as e:
            logger.exception("Accessibility audit failed")
            return ActionResult(success=False, error=str(e))

    def get_history(self) -> List[AuditResult]:
        """Get audit history."""
        return self._audit_history.copy()
