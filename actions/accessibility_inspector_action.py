"""
Accessibility Inspector Action Module.

Inspects UI elements for accessibility properties including
ARIA roles, keyboard navigation, focus management, and
screen reader compatibility checks.
"""

from typing import Any, Optional


class AccessibilityChecker:
    """Checks elements for accessibility compliance."""

    INTERACTIVE_ROLES = {
        "button", "link", "checkbox", "radio", "menuitem",
        "tab", "slider", "spinbutton", "textbox", "searchbox",
        "combobox", "listbox", "tree", "menu", "toolbar",
    }

    LANDMARK_ROLES = {
        "banner", "navigation", "main", "complementary",
        "contentinfo", "form", "search", "region",
    }

    def __init__(self):
        """Initialize accessibility checker."""
        self._issues: list[dict] = []

    def check_element(self, element: dict) -> list[dict]:
        """
        Check an element for accessibility issues.

        Args:
            element: Element dictionary with attributes.

        Returns:
            List of accessibility issues found.
        """
        issues = []
        tag = element.get("tag", "").lower()
        role = element.get("role", "").lower()
        text = element.get("text", "").strip()
        aria_label = element.get("aria-label", "")
        aria_labelledby = element.get("aria-labelledby", "")
        has_alt = "alt" in element
        has_title = "title" in element

        if role in self.INTERACTIVE_ROLES:
            if not text and not aria_label and not aria_labelledby and not has_title:
                issues.append({
                    "severity": "error",
                    "code": "interactive-no-label",
                    "message": f"Interactive element <{tag}> has no accessible label",
                    "element": tag,
                })

            if role == "button" and text.lower() in {"submit", "click", "button", ""}:
                issues.append({
                    "severity": "warning",
                    "code": "generic-button-text",
                    "message": f"Button has generic text: '{text}'",
                    "element": tag,
                })

        if tag in {"img", "image"} and not has_alt:
            issues.append({
                "severity": "error",
                "code": "img-missing-alt",
                "message": "Image element missing alt attribute",
                "element": tag,
            })

        if tag == "a" and not text and not aria_label:
            issues.append({
                "severity": "error",
                "code": "link-no-text",
                "message": "Link has no text content",
                "element": tag,
            })

        if element.get("tabindex", "0") == "-1" and role not in self.INTERACTIVE_ROLES:
            issues.append({
                "severity": "warning",
                "code": "non-interactive-tabindex",
                "message": f"Non-interactive element has tabindex=-1",
                "element": tag,
            })

        if role == "img" and not has_alt and not has_title:
            issues.append({
                "severity": "warning",
                "code": "img-no-alt-or-title",
                "message": "Image role element has no alt or title",
                "element": tag,
            })

        return issues

    def check_page(self, dom: list[dict]) -> dict[str, Any]:
        """
        Check entire page for accessibility issues.

        Args:
            dom: DOM tree of elements.

        Returns:
            Summary of all accessibility issues.
        """
        all_issues = []
        for elem in self._flatten(dom):
            issues = self.check_element(elem)
            all_issues.extend(issues)

        return {
            "total_issues": len(all_issues),
            "errors": len([i for i in all_issues if i["severity"] == "error"]),
            "warnings": len([i for i in all_issues if i["severity"] == "warning"]),
            "issues_by_code": self._group_by_code(all_issues),
            "issues": all_issues,
        }

    def _flatten(self, elements: list[dict]) -> list[dict]:
        """Flatten DOM tree."""
        result = []
        for elem in elements:
            result.append(elem)
            result.extend(self._flatten(elem.get("children", [])))
        return result

    @staticmethod
    def _group_by_code(issues: list[dict]) -> dict[str, int]:
        """Group issues by error code."""
        grouped = {}
        for issue in issues:
            code = issue["code"]
            grouped[code] = grouped.get(code, 0) + 1
        return grouped


class ARIAPropertyValidator:
    """Validates ARIA properties on elements."""

    @staticmethod
    def validate_aria_attributes(element: dict) -> list[dict]:
        """
        Validate ARIA attributes on an element.

        Args:
            element: Element dictionary.

        Returns:
            List of validation issues.
        """
        issues = []
        role = element.get("role", "")
        aria_hidden = element.get("aria-hidden", False)
        aria_disabled = element.get("aria-disabled", False)
        aria_expanded = element.get("aria-expanded")

        if aria_hidden and role:
            issues.append({
                "code": "aria-hidden-with-role",
                "message": f"Element with role='{role}' has aria-hidden='true'",
            })

        if aria_disabled and not element.get("disabled", False):
            issues.append({
                "code": "aria-disabled-no-disabled",
                "message": "aria-disabled is set but disabled attribute is not",
            })

        if role == "button" and aria_expanded is None:
            issues.append({
                "code": "button-missing-aria-expanded",
                "message": "Button role should have aria-expanded attribute",
            })

        return issues
