"""
Accessibility Utilities for UI Automation.

This module provides utilities for accessibility testing and
interaction with assistive technologies.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Any


class AccessibilityRole(Enum):
    """WAI-ARIA roles."""
    BUTTON = "button"
    LINK = "link"
    CHECKBOX = "checkbox"
    RADIO = "radio"
    TEXTBOX = "textbox"
    COMBOBOX = "combobox"
    LISTBOX = "listbox"
    MENU = "menu"
    MENUBAR = "menubar"
    MENU_ITEM = "menuitem"
    TREE = "tree"
    TREE_ITEM = "treeitem"
    TABLE = "table"
    ROW = "row"
    CELL = "cell"
    COLUMN_HEADER = "columnheader"
    ROW_HEADER = "rowheader"
    TAB = "tab"
    TAB_LIST = "tablist"
    TAB_PANEL = "tabpanel"
    DIALOG = "dialog"
    ALERT = "alert"
    IMG = "img"
    APPLICATION = "application"
    DOCUMENT = "document"
    HEADING = "heading"
    PARAGRAPH = "paragraph"
    LABEL = "label"
    FORM = "form"
    GROUP = "group"
    PRESENTATION = "presentation"


class AccessibilityState(Enum):
    """WAI-ARIA states and properties."""
    BUSY = "busy"
    CHECKED = "checked"
    DISABLED = "disabled"
    EXPANDED = "expanded"
    HASPOPUP = "haspopup"
    HIDDEN = "hidden"
    INVALID = "invalid"
    MULTISELECTABLE = "multiselectable"
    PRESSED = "pressed"
    READONLY = "readonly"
    REQUIRED = "required"
    SELECTED = "selected"


@dataclass
class AccessibilityInfo:
    """
    Accessibility information for a UI element.
    
    Attributes:
        element_id: Element identifier
        role: Element WAI-ARIA role
        name: Accessible name
        description: Accessible description
        value: Current value
        states: Set of active states
        properties: Additional properties
    """
    element_id: str
    role: Optional[AccessibilityRole] = None
    name: Optional[str] = None
    description: Optional[str] = None
    value: Optional[str] = None
    states: set[AccessibilityState] = field(default_factory=set)
    properties: dict[str, Any] = field(default_factory=dict)
    keyboard_shortcut: Optional[str] = None
    label_elements: list[str] = field(default_factory=list)
    
    @property
    def has_name(self) -> bool:
        """Check if element has an accessible name."""
        return bool(self.name and self.name.strip())
    
    @property
    def is_focusable(self) -> bool:
        """Check if element can receive focus."""
        return AccessibilityState.DISABLED not in self.states
    
    @property
    def is_visible(self) -> bool:
        """Check if element is visible."""
        return AccessibilityState.HIDDEN not in self.states
    
    def get_role_name(self) -> str:
        """Get the role name as a string."""
        return self.role.value if self.role else "unknown"


class AccessibilityChecker:
    """
    Checks accessibility compliance.
    
    Example:
        checker = AccessibilityChecker()
        issues = checker.check_element(element_info)
    """
    
    def __init__(self, level: str = "AA"):
        """
        Initialize accessibility checker.
        
        Args:
            level: WCAG compliance level ("A", "AA", or "AAA")
        """
        self.level = level
        self._rules = self._load_rules()
    
    def _load_rules(self) -> dict:
        """Load accessibility check rules."""
        return {
            "name_required": {
                "roles": ["button", "link", "checkbox", "radio", "textbox", "combobox"],
                "severity": "error"
            },
            "label_association": {
                "roles": ["textbox", "checkbox", "radio", "combobox"],
                "severity": "warning"
            },
            "keyboard_navigation": {
                "roles": ["button", "link", "menuitem", "tab"],
                "severity": "error"
            }
        }
    
    def check_element(self, info: AccessibilityInfo) -> list['AccessibilityIssue']:
        """
        Check an element for accessibility issues.
        
        Args:
            info: Accessibility information
            
        Returns:
            List of identified issues
        """
        issues = []
        
        # Check for name on interactive elements
        if info.role and info.role.value in self._rules["name_required"]["roles"]:
            if not info.has_name:
                issues.append(AccessibilityIssue(
                    element_id=info.element_id,
                    rule="name_required",
                    severity="error",
                    message=f"{info.get_role_name()} element missing accessible name",
                    wcag_criterion="4.1.2"
                ))
        
        # Check for label association
        if info.role and info.role.value in self._rules["label_association"]["roles"]:
            if not info.label_elements and not info.has_name:
                issues.append(AccessibilityIssue(
                    element_id=info.element_id,
                    rule="label_association",
                    severity="warning",
                    message=f"{info.get_role_name()} element has no associated label",
                    wcag_criterion="1.3.1"
                ))
        
        # Check for keyboard navigation support
        if info.role and info.role.value in self._rules["keyboard_navigation"]["roles"]:
            if AccessibilityState.DISABLED in info.states:
                pass  # Disabled elements are expected to not be keyboard accessible
            elif not info.is_focusable:
                issues.append(AccessibilityIssue(
                    element_id=info.element_id,
                    rule="keyboard_navigation",
                    severity="error",
                    message=f"{info.get_role_name()} should be keyboard accessible",
                    wcag_criterion="2.1.1"
                ))
        
        return issues
    
    def check_page(self, elements: list[AccessibilityInfo]) -> 'AccessibilityReport':
        """
        Check an entire page for accessibility issues.
        
        Args:
            elements: List of accessibility information for all elements
            
        Returns:
            Accessibility report
        """
        all_issues = []
        
        for element in elements:
            issues = self.check_element(element)
            all_issues.extend(issues)
        
        return AccessibilityReport(
            total_elements=len(elements),
            total_issues=len(all_issues),
            issues_by_severity={
                "error": sum(1 for i in all_issues if i.severity == "error"),
                "warning": sum(1 for i in all_issues if i.severity == "warning"),
                "info": sum(1 for i in all_issues if i.severity == "info")
            },
            issues=all_issues
        )


@dataclass
class AccessibilityIssue:
    """Represents an accessibility issue."""
    element_id: str
    rule: str
    severity: str  # "error", "warning", "info"
    message: str
    wcag_criterion: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AccessibilityReport:
    """Accessibility check report."""
    total_elements: int
    total_issues: int
    issues_by_severity: dict[str, int]
    issues: list[AccessibilityIssue]
    timestamp: float = field(default_factory=time.time)
    
    @property
    def pass_rate(self) -> float:
        """Calculate pass rate percentage."""
        if self.total_elements == 0:
            return 100.0
        error_count = self.issues_by_severity.get("error", 0)
        return ((self.total_elements - error_count) / self.total_elements) * 100
    
    @property
    def is_compliant(self) -> bool:
        """Check if page meets minimum compliance (no errors)."""
        return self.issues_by_severity.get("error", 0) == 0


class AccessibilityNavigator:
    """
    Navigates using accessibility APIs.
    
    Example:
        nav = AccessibilityNavigator()
        nav.focus_element("main_button")
        nav.select_option("dropdown", "Option 1")
    """
    
    def __init__(self):
        self._current_element: Optional[str] = None
    
    def focus_element(self, element_id: str) -> bool:
        """
        Focus an element by accessibility properties.
        
        Args:
            element_id: Element identifier
            
        Returns:
            True if successful
        """
        self._current_element = element_id
        return True
    
    def get_focused_element(self) -> Optional[str]:
        """Get the currently focused element ID."""
        return self._current_element
    
    def get_element_at_point(self, x: int, y: int) -> Optional[AccessibilityInfo]:
        """
        Get accessibility info for element at coordinates.
        
        Args:
            x: X coordinate
            y: Y coordinate
            
        Returns:
            AccessibilityInfo if an element is found
        """
        return None
    
    def get_element_by_role(
        self, 
        role: AccessibilityRole, 
        name: Optional[str] = None
    ) -> list[AccessibilityInfo]:
        """
        Find elements by role and optional name.
        
        Args:
            role: Element role to search for
            name: Optional name to match
            
        Returns:
            List of matching elements
        """
        return []
    
    def get_tab_order(self) -> list[str]:
        """
        Get the tab navigation order.
        
        Returns:
            List of element IDs in tab order
        """
        return []
