"""
UI Semantic Utilities - Semantic UI element classification and grouping.

This module provides utilities for classifying UI elements based on
their semantic meaning (buttons, inputs, containers, etc.) and organizing
them into semantic groups for automation workflows.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


SemanticRole = str

ROLENAME_BUTTON = "button"
ROLENAME_INPUT = "input"
ROLENAME_CONTAINER = "container"
ROLENAME_TEXT = "text"
ROLENAME_IMAGE = "image"
ROLENAME_LINK = "link"
ROLENAME_LIST = "list"
ROLENAME_LISTITEM = "listitem"
ROLENAME_CHECKBOX = "checkbox"
ROLENAME_RADIO = "radio"
ROLENAME_TAB = "tab"
ROLENAME_MENU = "menu"
ROLENAME_MENUITEM = "menuitem"
ROLENAME_TABLE = "table"
ROLENAME_ROW = "row"
ROLENAME_CELL = "cell"
ROLENAME_HEADER = "header"
ROLENAME_FOOTER = "footer"
ROLENAME_NAVIGATION = "navigation"
ROLENAME_DIALOG = "dialog"
ROLENAME_ALERT = "alert"
ROLENAME_TOOLBAR = "toolbar"
ROLENAME_SLIDER = "slider"
ROLENAME_SWITCH = "switch"
ROLENAME_PROGRESS = "progressbar"


@dataclass
class SemanticClassifier:
    """Classifier configuration for semantic role detection.
    
    Attributes:
        role: The semantic role this classifier detects.
        keywords: Keywords that indicate this role.
        patterns: Regex patterns that indicate this role.
        validator: Optional validation function.
        priority: Priority when multiple classifiers match.
    """
    role: SemanticRole
    keywords: tuple[str, ...] = ()
    patterns: tuple[str, ...] = ()
    validator: Optional[Callable[[object], bool]] = None
    priority: int = 0


@dataclass
class ClassifiedElement:
    """A UI element with semantic classification.
    
    Attributes:
        id: Unique identifier for this element.
        element: The underlying UI element.
        role: Assigned semantic role.
        label: Element label or accessible name.
        confidence: Classification confidence (0.0 to 1.0).
        properties: Additional semantic properties.
        groups: Semantic groups this element belongs to.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    element: Optional[object] = None
    role: Optional[SemanticRole] = None
    label: Optional[str] = None
    confidence: float = 0.0
    properties: dict = field(default_factory=dict)
    groups: set[str] = field(default_factory=set)
    
    def has_role(self, role: SemanticRole) -> bool:
        """Check if element has a specific role.
        
        Args:
            role: Role to check.
            
        Returns:
            True if element has the role.
        """
        return self.role == role
    
    def is_interactive(self) -> bool:
        """Check if element is interactive.
        
        Returns:
            True if element can be interacted with.
        """
        interactive_roles = {
            ROLENAME_BUTTON, ROLENAME_INPUT, ROLENAME_LINK,
            ROLENAME_CHECKBOX, ROLENAME_RADIO, ROLENAME_TAB,
            ROLENAME_MENUITEM, ROLENAME_SLIDER, ROLENAME_SWITCH
        }
        return self.role in interactive_roles if self.role else False
    
    def is_container(self) -> bool:
        """Check if element is a container.
        
        Returns:
            True if element can contain other elements.
        """
        container_roles = {
            ROLENAME_CONTAINER, ROLENAME_LIST, ROLENAME_MENU,
            ROLENAME_TABLE, ROLENAME_DIALOG, ROLENAME_NAVIGATION
        }
        return self.role in container_roles if self.role else False


class SemanticClassifierRegistry:
    """Registry of classifiers for semantic role detection.
    
    Provides a centralized collection of classifiers that can be
    used to determine the semantic role of UI elements.
    
    Example:
        >>> registry = SemanticClassifierRegistry()
        >>> registry.register_default_classifiers()
        >>> role = registry.classify(element)
    """
    
    def __init__(self) -> None:
        """Initialize an empty classifier registry."""
        self._classifiers: list[SemanticClassifier] = []
    
    def register(self, classifier: SemanticClassifier) -> None:
        """Register a classifier.
        
        Args:
            classifier: Classifier to register.
        """
        self._classifiers.append(classifier)
        self._classifiers.sort(key=lambda c: c.priority, reverse=True)
    
    def register_default_classifiers(self) -> None:
        """Register the default set of classifiers."""
        self.register(SemanticClassifier(
            role=ROLENAME_BUTTON,
            keywords=("button", "btn", "submit", "cancel", "ok", "save", "delete"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_INPUT,
            keywords=("input", "textfield", "text", "field", "search", "enter"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_LINK,
            keywords=("link", "anchor", "href"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_CHECKBOX,
            keywords=("checkbox", "check", "toggle"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_RADIO,
            keywords=("radio", "option", "choice"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_SWITCH,
            keywords=("switch", "toggle", "on", "off"),
            priority=10
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_IMAGE,
            keywords=("image", "img", "icon", "picture", "photo"),
            priority=5
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_TEXT,
            keywords=("text", "label", "span", "paragraph"),
            priority=5
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_LIST,
            keywords=("list", "ul", "ol", "menu"),
            priority=8
        ))
        self.register(SemanticClassifier(
            role=ROLENAME_CONTAINER,
            keywords=("container", "div", "panel", "section", "group"),
            priority=1
        ))
    
    def classify(
        self,
        element: object,
        label: Optional[str] = None,
        attributes: Optional[dict] = None
    ) -> Optional[SemanticRole]:
        """Classify an element to determine its semantic role.
        
        Args:
            element: UI element to classify.
            label: Optional element label or name.
            attributes: Optional element attributes.
            
        Returns:
            Detected semantic role, or None if unclassified.
        """
        attributes = attributes or {}
        label = label or ""
        label_lower = label.lower()
        
        for classifier in self._classifiers:
            if classifier.validator and not classifier.validator(element):
                continue
            
            for keyword in classifier.keywords:
                if keyword in label_lower:
                    return classifier.role
            
            for pattern in classifier.patterns:
                import re
                if re.search(pattern, label, re.IGNORECASE):
                    return classifier.role
        
        return None


class SemanticGroupManager:
    """Manages semantic groups of UI elements.
    
    Groups provide a way to organize related elements by their
    semantic purpose (navigation, forms, dialogs, etc.).
    
    Example:
        >>> manager = SemanticGroupManager()
        >>> manager.add_to_group("navigation", element1)
        >>> manager.add_to_group("navigation", element2)
        >>> nav_elements = manager.get_group("navigation")
    """
    
    def __init__(self) -> None:
        """Initialize an empty group manager."""
        self._groups: dict[str, set[str]] = {}
        self._elements: dict[str, ClassifiedElement] = {}
    
    def add_element(self, classified: ClassifiedElement) -> str:
        """Add a classified element to the manager.
        
        Args:
            classified: Classified element to add.
            
        Returns:
            Element ID.
        """
        self._elements[classified.id] = classified
        return classified.id
    
    def add_to_group(self, group_name: str, element_id: str) -> bool:
        """Add an element to a semantic group.
        
        Args:
            group_name: Name of the group.
            element_id: ID of the element to add.
            
        Returns:
            True if element was added.
        """
        if element_id not in self._elements:
            return False
        
        if group_name not in self._groups:
            self._groups[group_name] = set()
        self._groups[group_name].add(element_id)
        self._elements[element_id].groups.add(group_name)
        return True
    
    def remove_from_group(self, group_name: str, element_id: str) -> bool:
        """Remove an element from a group.
        
        Args:
            group_name: Name of the group.
            element_id: ID of the element to remove.
            
        Returns:
            True if element was removed.
        """
        if group_name in self._groups:
            removed = element_id in self._groups[group_name]
            if removed:
                self._groups[group_name].discard(element_id)
                self._elements.get(element_id, ClassifiedElement()).groups.discard(group_name)
            return removed
        return False
    
    def get_group(self, group_name: str) -> list[ClassifiedElement]:
        """Get all elements in a group.
        
        Args:
            group_name: Name of the group.
            
        Returns:
            List of elements in the group.
        """
        if group_name not in self._groups:
            return []
        return [
            self._elements[eid]
            for eid in self._groups[group_name]
            if eid in self._elements
        ]
    
    def get_element(self, element_id: str) -> Optional[ClassifiedElement]:
        """Get an element by ID.
        
        Args:
            element_id: Element ID.
            
        Returns:
            ClassifiedElement if found.
        """
        return self._elements.get(element_id)
    
    def get_elements_by_role(
        self,
        role: SemanticRole
    ) -> list[ClassifiedElement]:
        """Get all elements with a specific role.
        
        Args:
            role: Semantic role to filter by.
            
        Returns:
            List of matching elements.
        """
        return [e for e in self._elements.values() if e.role == role]
    
    def get_interactive_elements(self) -> list[ClassifiedElement]:
        """Get all interactive elements.
        
        Returns:
            List of interactive elements.
        """
        return [e for e in self._elements.values() if e.is_interactive()]
    
    def get_containers(self) -> list[ClassifiedElement]:
        """Get all container elements.
        
        Returns:
            List of container elements.
        """
        return [e for e in self._elements.values() if e.is_container()]
    
    def filter_elements(
        self,
        predicate: Callable[[ClassifiedElement], bool]
    ) -> list[ClassifiedElement]:
        """Filter elements by a predicate.
        
        Args:
            predicate: Function returning True for elements to keep.
            
        Returns:
            List of matching elements.
        """
        return [e for e in self._elements.values() if predicate(e)]
    
    def get_groups_for_element(self, element_id: str) -> set[str]:
        """Get all groups an element belongs to.
        
        Args:
            element_id: Element ID.
            
        Returns:
            Set of group names.
        """
        element = self._elements.get(element_id)
        return element.groups.copy() if element else set()
    
    def iterate_elements(self) -> Iterator[ClassifiedElement]:
        """Iterate over all elements.
        
        Yields:
            Each ClassifiedElement.
        """
        yield from self._elements.values()
    
    def clear_group(self, group_name: str) -> None:
        """Remove all elements from a group.
        
        Args:
            group_name: Name of the group to clear.
        """
        if group_name in self._groups:
            for element_id in self._groups[group_name]:
                element = self._elements.get(element_id)
                if element:
                    element.groups.discard(group_name)
            self._groups[group_name].clear()
    
    def remove_element(self, element_id: str) -> bool:
        """Remove an element from the manager.
        
        Args:
            element_id: ID of element to remove.
            
        Returns:
            True if element was removed.
        """
        if element_id not in self._elements:
            return False
        
        element = self._elements[element_id]
        for group_name in element.groups:
            self._groups.get(group_name, set()).discard(element_id)
        
        del self._elements[element_id]
        return True


def create_semantic_classifier(
    role: SemanticRole,
    keywords: Sequence[str],
    priority: int = 0
) -> SemanticClassifier:
    """Create a semantic classifier with common parameters.
    
    Args:
        role: Semantic role to classify.
        keywords: Keywords that indicate this role.
        priority: Classifier priority.
        
    Returns:
        Configured SemanticClassifier.
    """
    return SemanticClassifier(
        role=role,
        keywords=tuple(keywords),
        priority=priority
    )


def classify_element_batch(
    elements: Sequence[tuple[object, Optional[str], Optional[dict]]],
    registry: SemanticClassifierRegistry
) -> list[ClassifiedElement]:
    """Classify a batch of elements.
    
    Args:
        elements: Sequence of (element, label, attributes) tuples.
        registry: Classifier registry to use.
        
    Returns:
        List of ClassifiedElements.
    """
    results = []
    for element, label, attributes in elements:
        role = registry.classify(element, label, attributes)
        classified = ClassifiedElement(
            element=element,
            role=role,
            label=label,
            confidence=1.0 if role else 0.0
        )
        results.append(classified)
    return results
