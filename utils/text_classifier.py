"""
Text Classifier Utility

Classifies UI text elements by semantic type (label, button, heading, etc.).
Useful for understanding element purpose in accessibility trees.

Example:
    >>> classifier = TextClassifier()
    >>> category = classifier.classify("Submit", role="button")
    >>> print(category)  # 'action_element'
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class TextCategory(Enum):
    """Semantic categories for text elements."""
    ACTION_ELEMENT = "action_element"  # Buttons, links, menu items
    HEADING = "heading"
    LABEL = "label"
    VALUE = "value"  # Editable text, input fields
    HEADING_1 = "heading_1"
    HEADING_2 = "heading_2"
    HEADING_3 = "heading_3"
    PARAGRAPH = "paragraph"
    CAPTION = "caption"
    LIST_ITEM = "list_item"
    TABLE_HEADER = "table_header"
    TABLE_CELL = "table_cell"
    STATUS = "status"
    ERROR = "error"
    NAVIGATION = "navigation"
    UNKNOWN = "unknown"


@dataclass
class ClassificationResult:
    """Result of text classification."""
    category: TextCategory
    confidence: float  # 0.0 to 1.0
    keywords: list[str]
    reasoning: str


class TextClassifier:
    """
    Classifies UI text by semantic type.

    Uses keyword matching, position heuristics, and role information.
    """

    def __init__(self) -> None:
        self._action_keywords = {
            "submit", "save", "cancel", "delete", "edit", "create", "add",
            "remove", "close", "open", "quit", "exit", "start", "stop",
            "play", "pause", "next", "previous", "back", "forward", "send",
            "receive", "upload", "download", "share", "copy", "paste", "cut",
            "undo", "redo", "refresh", "reload", "print", "login", "logout",
            "signin", "signout", "register", "join", "leave", "search",
            "find", "filter", "sort", "select", "choose", "confirm", "ok",
            "apply", "reset", "clear", "set", "get", "fetch", "update",
            "view", "show", "hide", "expand", "collapse", "toggle", "switch",
        }

        self._heading_patterns = [
            re.compile(r"^(h[1-6]|heading|title)$", re.I),
            re.compile(r"^(menu|nav|header|footer|sidebar)$", re.I),
        ]

        self._status_keywords = {
            "loading", "processing", "working", "pending", "waiting",
            "complete", "done", "success", "warning", "info", "ready",
        }

        self._error_keywords = {
            "error", "fail", "exception", "invalid", "wrong", "incorrect",
            "missing", "required", "denied", "forbidden", "unauthorized",
        }

        self._navigation_keywords = {
            "home", "back", "forward", "menu", "nav", "link", "tab",
            "page", "section", "breadcrumb",
        }

    def classify(
        self,
        text: str,
        role: Optional[str] = None,
        bounds: Optional[tuple[int, int, int, int]] = None,
        is_focused: bool = False,
    ) -> ClassificationResult:
        """
        Classify a text element.

        Args:
            text: The text content.
            role: Accessibility role if known.
            bounds: Element bounds (x, y, w, h).
            is_focused: Whether element has focus.

        Returns:
            ClassificationResult with category and confidence.
        """
        text = (text or "").strip()
        if not text:
            return ClassificationResult(
                TextCategory.UNKNOWN, 0.0, [], "Empty text"
            )

        text_lower = text.lower()
        keywords: list[str] = []
        confidence = 0.5
        reasoning = ""

        # Role-based classification
        if role:
            if role in ("button", "link", "menuitem", "menuitem", "toolbar"):
                confidence = 0.9
                reasoning = f"Role '{role}' indicates action element"
                return ClassificationResult(
                    TextCategory.ACTION_ELEMENT, confidence,
                    self._extract_keywords(text_lower), reasoning
                )
            elif role in ("textfield", "textArea", "searchfield", "comboBox"):
                confidence = 0.85
                reasoning = f"Role '{role}' indicates value input"
                return ClassificationResult(
                    TextCategory.VALUE, confidence,
                    self._extract_keywords(text_lower), reasoning
                )
            elif role in ("heading", "title"):
                return self._classify_heading_level(text, role, bounds)
            elif role == "tableHeader":
                return ClassificationResult(
                    TextCategory.TABLE_HEADER, 0.9,
                    self._extract_keywords(text_lower), "Role is tableHeader"
                )
            elif role == "cell":
                return ClassificationResult(
                    TextCategory.TABLE_CELL, 0.7,
                    self._extract_keywords(text_lower), "Role is cell"
                )

        # Keyword-based classification
        if self._matches_keywords(text_lower, self._action_keywords):
            return ClassificationResult(
                TextCategory.ACTION_ELEMENT, 0.85,
                self._extract_keywords(text_lower), "Matched action keywords"
            )

        if self._matches_keywords(text_lower, self._error_keywords):
            return ClassificationResult(
                TextCategory.ERROR, 0.9,
                self._extract_keywords(text_lower), "Matched error keywords"
            )

        if self._matches_keywords(text_lower, self._status_keywords):
            return ClassificationResult(
                TextCategory.STATUS, 0.8,
                self._extract_keywords(text_lower), "Matched status keywords"
            )

        if self._matches_keywords(text_lower, self._navigation_keywords):
            return ClassificationResult(
                TextCategory.NAVIGATION, 0.75,
                self._extract_keywords(text_lower), "Matched navigation keywords"
            )

        # Position-based heuristics
        if bounds:
            x, y, w, h = bounds
            # Top of screen often = navigation/header
            if y < 100 and w > 300:
                confidence = 0.6
                reasoning = "Top position suggests navigation or header"
                return ClassificationResult(
                    TextCategory.NAVIGATION, confidence,
                    self._extract_keywords(text_lower), reasoning
                )
            # Short text on the right often = labels
            if len(text) < 30 and x > 400:
                confidence = 0.55
                reasoning = "Short text in right region may be label"

        # Default: paragraph
        return ClassificationResult(
            TextCategory.PARAGRAPH, 0.4,
            self._extract_keywords(text_lower), "Default classification"
        )

    def _classify_heading_level(
        self,
        text: str,
        role: str,
        bounds: Optional[tuple[int, int, int, int]],
    ) -> ClassificationResult:
        """Classify heading by level."""
        kw = self._extract_keywords(text.lower())

        if bounds:
            _, y, w, _ = bounds
            if y < 50 and w > 400:
                return ClassificationResult(
                    TextCategory.HEADING_1, 0.9, kw,
                    "Large text at top = H1"
                )
            elif y < 120:
                return ClassificationResult(
                    TextCategory.HEADING_2, 0.8, kw,
                    "Medium text near top = H2"
                )

        return ClassificationResult(
            TextCategory.HEADING, 0.7, kw,
            "Heading role"
        )

    def _matches_keywords(self, text: str, keywords: set[str]) -> bool:
        """Check if text contains any keyword."""
        words = set(re.findall(r"\b\w+\b", text))
        return bool(words & keywords)

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract significant words from text."""
        stop_words = {"the", "a", "an", "is", "are", "was", "were", "to", "of", "and", "or"}
        words = re.findall(r"\b\w{3,}\b", text.lower())
        return [w for w in words if w not in stop_words][:5]

    def batch_classify(
        self,
        elements: list[dict],
    ) -> list[ClassificationResult]:
        """
        Classify multiple text elements.

        Args:
            elements: List of dicts with keys: text, role, bounds.

        Returns:
            List of ClassificationResult in same order.
        """
        results: list[ClassificationResult] = []
        for el in elements:
            result = self.classify(
                text=el.get("text", ""),
                role=el.get("role"),
                bounds=el.get("bounds"),
                is_focused=el.get("focused", False),
            )
            results.append(result)
        return results
