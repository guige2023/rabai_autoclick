"""Accessibility Audit Utilities.

Tools for auditing accessibility compliance of UI elements.
Checks contrast ratios, label presence, focus order, and ARIA attributes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AuditSeverity(Enum):
    """Severity levels for accessibility violations."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class AuditViolation:
    """Represents a single accessibility violation."""

    severity: AuditSeverity
    rule_id: str
    message: str
    element_id: Optional[str] = None
    element_role: Optional[str] = None
    suggestion: Optional[str] = None


@dataclass
class AuditResult:
    """Result of an accessibility audit on an element."""

    element_id: str
    role: str
    label: Optional[str] = None
    description: Optional[str] = None
    violations: list[AuditViolation] = field(default_factory=list)
    checks_passed: list[str] = field(default_factory=list)


class ContrastChecker:
    """Checks color contrast ratios for accessibility compliance.

    Implements WCAG 2.1 contrast requirements.
    """

    # WCAG 2.1 contrast thresholds
    WCAG_AA_NORMAL_TEXT = 4.5
    WCAG_AA_LARGE_TEXT = 3.0
    WCAG_AAA_NORMAL_TEXT = 7.0
    WCAG_AAA_LARGE_TEXT = 4.5

    @staticmethod
    def luminance(r: int, g: int, b: int) -> float:
        """Calculate relative luminance of an RGB color.

        Args:
            r: Red component (0-255).
            g: Green component (0-255).
            b: Blue component (0-255).

        Returns:
            Relative luminance value (0.0 to 1.0).
        """
        def adjust(c: int) -> float:
            s = c / 255.0
            return s / 12.92 if s <= 0.03928 else ((s + 0.055) / 1.055) ** 2.4

        return 0.2126 * adjust(r) + 0.7152 * adjust(g) + 0.0722 * adjust(b)

    @staticmethod
    def contrast_ratio(color1: tuple[int, int, int], color2: tuple[int, int, int]) -> float:
        """Calculate contrast ratio between two colors.

        Args:
            color1: First RGB color tuple (r, g, b).
            color2: Second RGB color tuple (r, g, b).

        Returns:
            Contrast ratio value.
        """
        l1 = ContrastChecker.luminance(*color1)
        l2 = ContrastChecker.luminance(*color2)
        lighter = max(l1, l2)
        darker = min(l1, l2)
        return (lighter + 0.05) / (darker + 0.05)

    def check_aa_compliance(
        self,
        foreground: tuple[int, int, int],
        background: tuple[int, int, int],
        is_large_text: bool = False,
    ) -> tuple[bool, float]:
        """Check WCAG AA compliance.

        Args:
            foreground: RGB tuple for text color.
            background: RGB tuple for background color.
            is_large_text: Whether this is large text (18pt+ or 14pt bold).

        Returns:
            Tuple of (is_compliant, contrast_ratio).
        """
        ratio = self.contrast_ratio(foreground, background)
        threshold = self.WCAG_AA_LARGE_TEXT if is_large_text else self.WCAG_AA_NORMAL_TEXT
        return ratio >= threshold, ratio

    def check_aaa_compliance(
        self,
        foreground: tuple[int, int, int],
        background: tuple[int, int, int],
        is_large_text: bool = False,
    ) -> tuple[bool, float]:
        """Check WCAG AAA compliance.

        Args:
            foreground: RGB tuple for text color.
            background: RGB tuple for background color.
            is_large_text: Whether this is large text.

        Returns:
            Tuple of (is_compliant, contrast_ratio).
        """
        ratio = self.contrast_ratio(foreground, background)
        threshold = self.WCAG_AAA_LARGE_TEXT if is_large_text else self.WCAG_AAA_NORMAL_TEXT
        return ratio >= threshold, ratio


class LabelPresenceChecker:
    """Checks for proper labeling of UI elements."""

    def check_has_label(self, element: dict) -> list[AuditViolation]:
        """Check if element has proper labeling.

        Args:
            element: Element data dictionary with attributes.

        Returns:
            List of violations found.
        """
        violations = []
        role = element.get("role", "")
        has_aria_label = bool(element.get("aria-label"))
        has_aria_labelledby = bool(element.get("aria-labelledby"))
        has_title = bool(element.get("title"))
        has_text_content = bool(element.get("text_content", "").strip())
        has_description = bool(element.get("description"))

        has_label = has_aria_label or has_aria_labelledby or has_title or has_text_content

        interactive_roles = {"button", "link", "checkbox", "radio", "textbox", "slider"}
        if role.lower() in interactive_roles and not has_label:
            violations.append(
                AuditViolation(
                    severity=AuditSeverity.ERROR,
                    rule_id="label-presence",
                    message=f"Interactive element with role '{role}' lacks accessible label",
                    element_id=element.get("id"),
                    element_role=role,
                    suggestion="Add aria-label, aria-labelledby, title, or visible text",
                )
            )

        return violations


class FocusOrderChecker:
    """Validates logical focus order for keyboard navigation."""

    def check_focus_order(self, elements: list[dict]) -> list[AuditViolation]:
        """Check if focus order is logical and tabbable elements are reachable.

        Args:
            elements: List of element data dictionaries.

        Returns:
            List of violations found.
        """
        violations = []
        tabbable_elements = [
            e for e in elements if e.get("focusable") or e.get("tabbable")
        ]
        tab_index_elements = [e for e in tabbable_elements if e.get("tabindex") is not None]

        # Check for positive tabindex
        positive_tabindex = [e for e in tab_index_elements if int(e.get("tabindex", 0)) > 0]
        if positive_tabindex:
            violations.append(
                AuditViolation(
                    severity=AuditSeverity.WARNING,
                    rule_id="positive-tabindex",
                    message="Elements with positive tabindex disrupt natural focus order",
                    suggestion="Use tabindex=0 or -1 instead",
                )
            )

        # Check for elements with tabindex but not focusable
        non_focusable_with_tabindex = [
            e for e in tab_index_elements
            if not e.get("focusable") and not e.get("tabbable")
        ]
        if non_focusable_with_tabindex:
            violations.append(
                AuditViolation(
                    severity=AuditSeverity.WARNING,
                    rule_id="unreachable-tabindex",
                    message="Elements have tabindex but are not keyboard focusable",
                    suggestion="Ensure element is interactive or remove tabindex",
                )
            )

        return violations


class AccessibilityAuditor:
    """Main accessibility auditor that combines all checks.

    Example:
        auditor = AccessibilityAuditor()
        elements = [
            {"id": "btn1", "role": "button", "aria-label": "Submit"},
            {"id": "txt1", "role": "textbox", "text_content": "Name"},
        ]
        results = auditor.audit_elements(elements)
    """

    def __init__(self):
        """Initialize the auditor with all checkers."""
        self.contrast_checker = ContrastChecker()
        self.label_checker = LabelPresenceChecker()
        self.focus_checker = FocusOrderChecker()

    def audit_element(self, element: dict) -> AuditResult:
        """Audit a single element for accessibility.

        Args:
            element: Element data dictionary.

        Returns:
            AuditResult with all findings.
        """
        result = AuditResult(
            element_id=element.get("id", "unknown"),
            role=element.get("role", "unknown"),
            label=element.get("aria-label"),
            description=element.get("description"),
        )

        # Check labeling
        label_violations = self.label_checker.check_has_label(element)
        result.violations.extend(label_violations)

        if not label_violations:
            result.checks_passed.append("has_accessible_label")

        # Check contrast if colors are available
        fg = element.get("foreground_color")
        bg = element.get("background_color")
        if fg and bg:
            is_large = element.get("font_size", 0) >= 18
            is_aa_compliant, ratio = self.contrast_checker.check_aa_compliance(
                tuple(fg), tuple(bg), is_large
            )
            if not is_aa_compliant:
                result.violations.append(
                    AuditViolation(
                        severity=AuditSeverity.ERROR,
                        rule_id="color-contrast",
                        message=f"Contrast ratio {ratio:.2f} fails WCAG AA",
                        element_id=element.get("id"),
                        suggestion=f"Need ratio of {4.5 if not is_large else 3.0}",
                    )
                )
            else:
                result.checks_passed.append("color_contrast_aa")

        return result

    def audit_elements(self, elements: list[dict]) -> list[AuditResult]:
        """Audit multiple elements.

        Args:
            elements: List of element data dictionaries.

        Returns:
            List of AuditResult for each element.
        """
        return [self.audit_element(el) for el in elements]
