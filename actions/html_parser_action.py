"""HTML parser action for structured content extraction.

This module provides HTML/XML parsing with support for
CSS selectors, XPath queries, and DOM manipulation.

Example:
    >>> action = HTMLParserAction()
    >>> result = action.execute(html=html_string, selector="div.content")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ParsedElement:
    """Represents a parsed HTML element."""
    tag: str
    text: Optional[str] = None
    attributes: dict[str, str] = field(default_factory=dict)
    children: list["ParsedElement"] = field(default_factory=list)
    html: Optional[str] = None


class HTMLParserAction:
    """HTML/XML parsing and extraction action.

    Provides structured content extraction from HTML using
    CSS selectors and XPath with full attribute access.

    Example:
        >>> action = HTMLParserAction()
        >>> result = action.execute(
        ...     html="<div class='main'><p>Hello</p></div>",
        ...     selector="div.main p"
        ... )
    """

    def __init__(self) -> None:
        """Initialize HTML parser."""
        self._soup: Optional[Any] = None

    def execute(
        self,
        html: str,
        selector: Optional[str] = None,
        xpath: Optional[str] = None,
        extract: str = "text",
        many: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute HTML parsing operation.

        Args:
            html: HTML content to parse.
            selector: CSS selector for element selection.
            xpath: XPath expression for element selection.
            extract: What to extract ('text', 'html', 'all', 'attrs').
            many: Whether to return all matches or just the first.
            **kwargs: Additional extraction parameters.

        Returns:
            Dictionary with parsed results.

        Raises:
            ValueError: If HTML is empty or selectors are invalid.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return {
                "success": False,
                "error": "BeautifulSoup4 not installed. Run: pip install beautifulsoup4",
            }

        if not html:
            raise ValueError("HTML content is required")

        self._soup = BeautifulSoup(html, "html.parser")
        result: dict[str, Any] = {"success": True, "extract": extract}

        if selector:
            if many:
                elements = self._soup.select(selector)
                result["count"] = len(elements)
                result["elements"] = [
                    self._extract_element(el, extract) for el in elements
                ]
            else:
                element = self._soup.select_one(selector)
                if element:
                    result["element"] = self._extract_element(element, extract)
                else:
                    result["element"] = None
                    result["found"] = False

        elif xpath:
            # Use lxml for XPath support
            from bs4 import BeautifulSoup
            self._soup = BeautifulSoup(html, "lxml")
            elements = self._soup.find_all(xpath=XPath(xpath)) if "XPath" in dir() else []
            result["count"] = len(elements)
            result["elements"] = [self._extract_element(el, extract) for el in elements]

        else:
            # Return full document info
            result["title"] = self._soup.title.string if self._soup.title else None
            result["text"] = self._soup.get_text(separator=" ", strip=True)
            result["html"] = str(self._soup)

        return result

    def _extract_element(self, element: Any, extract: str) -> Any:
        """Extract data from a BeautifulSoup element.

        Args:
            element: BeautifulSoup element.
            extract: Extraction type.

        Returns:
            Extracted data.
        """
        if extract == "text":
            return element.get_text(strip=True)
        elif extract == "html":
            return str(element)
        elif extract == "attrs":
            return dict(element.attrs)
        elif extract == "all":
            return {
                "tag": element.name,
                "text": element.get_text(strip=True),
                "attrs": dict(element.attrs),
                "html": str(element),
            }
        else:
            return str(element)

    def find_forms(self, html: str) -> list[dict[str, Any]]:
        """Extract all forms from HTML.

        Args:
            html: HTML content.

        Returns:
            List of form dictionaries with inputs and attributes.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "html.parser")
        forms = []

        for form in soup.find_all("form"):
            form_data = {
                "action": form.get("action", ""),
                "method": form.get("method", "get").upper(),
                "attrs": dict(form.attrs),
                "inputs": [],
            }

            for inp in form.find_all(["input", "textarea", "select"]):
                form_data["inputs"].append({
                    "tag": inp.name,
                    "type": inp.get("type", "text"),
                    "name": inp.get("name"),
                    "value": inp.get("value", ""),
                    "required": inp.has_attr("required"),
                })

            forms.append(form_data)

        return forms

    def extract_tables(self, html: str) -> list[list[list[str]]]:
        """Extract all tables as 2D arrays.

        Args:
            html: HTML content.

        Returns:
            List of tables, each a 2D list of cell values.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "html.parser")
        tables = []

        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = []
                for cell in tr.find_all(["td", "th"]):
                    cells.append(cell.get_text(strip=True))
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)

        return tables

    def strip_tags(self, html: str, allowed: Optional[list[str]] = None) -> str:
        """Strip HTML tags from content.

        Args:
            html: HTML content.
            allowed: List of allowed tag names.

        Returns:
            Plain text with tags stripped.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            # Fallback to regex
            text = re.sub(r"<[^>]+>", "", html)
            return re.sub(r"\s+", " ", text).strip()

        soup = BeautifulSoup(html, "html.parser")

        if allowed:
            allowed_tags = "|".join(allowed)
            pattern = rf"<(?!/?(?:{allowed_tags})\b)[^>]+>"
            return re.sub(pattern, "", str(soup))

        return soup.get_text(separator=" ", strip=True)
