"""
HTML parsing and automation module.

Provides HTML parsing, querying, manipulation, and
web scraping capabilities.

Author: Aito Auto Agent
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Callable,
    Optional,
)
import re


class SelectorType(Enum):
    """HTML selector types."""
    CSS = auto()
    XPATH = auto()
    ID = auto()
    CLASS = auto()
    TAG = auto()
    ATTRIBUTE = auto()


@dataclass
class HtmlElement:
    """Represents an HTML element."""
    tag: str
    attributes: dict[str, str] = field(default_factory=dict)
    text: str = ""
    html: str = ""
    children: list[HtmlElement] = field(default_factory=list)
    parent: Optional[HtmlElement] = None

    @property
    def id(self) -> Optional[str]:
        return self.attributes.get("id")

    @property
    def classes(self) -> list[str]:
        cls = self.attributes.get("class", "")
        return cls.split() if cls else []

    @property
    def inner_html(self) -> str:
        return "".join(child.html for child in self.children)

    @property
    def outer_html(self) -> str:
        attrs = "".join(f' {k}="{v}"' for k, v in self.attributes.items())
        if self.children:
            return f"<{self.tag}{attrs}>{self.inner_html}</{self.tag}>"
        return f"<{self.tag}{attrs}>{self.text}</{self.tag}>"

    def query(self, selector: str) -> list[HtmlElement]:
        """Query child elements using CSS selector."""
        return css_select(self, selector)

    def query_one(self, selector: str) -> Optional[HtmlElement]:
        """Query single child element."""
        results = self.query(selector)
        return results[0] if results else None


class HtmlParser:
    """
    HTML parser with CSS selector support.

    Example:
        parser = HtmlParser()

        with open("page.html") as f:
            root = parser.parse(f.read())

        # Find elements
        links = root.query("a.link")

        # Extract data
        for link in links:
            print(link.attributes["href"], link.text)
    """

    def __init__(self):
        self._html = ""

    def parse(self, html: str) -> HtmlElement:
        """
        Parse HTML string into element tree.

        Args:
            html: HTML content to parse

        Returns:
            Root HtmlElement
        """
        self._html = html

        html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)

        root = HtmlElement(tag="root")
        stack = [root]

        tag_pattern = re.compile(
            r'<(\w+)([^>]*)>([^<]*)|'
            r'</(\w+)>|'
            r'<(\w+)([^>]*)/>'
        )

        for match in tag_pattern.finditer(html):
            tag, attrs_str, text, close_tag, self_closing_tag, self_closing_attrs = match.groups()

            if close_tag:
                if stack:
                    stack.pop()
                continue

            effective_tag = tag or self_closing_tag
            effective_attrs = attrs_str or self_closing_attrs or ""

            if self_closing_tag:
                attrs = self._parse_attributes(self_closing_attrs or "")
                stack[-1].children.append(HtmlElement(
                    tag=effective_tag,
                    attributes=attrs
                ))
            else:
                attrs = self._parse_attributes(effective_attrs)
                element = HtmlElement(
                    tag=effective_tag,
                    attributes=attrs,
                    text=text.strip() if text else ""
                )
                stack[-1].children.append(element)
                stack.append(element)

        return root

    def _parse_attributes(self, attrs_str: str) -> dict[str, str]:
        """Parse HTML attributes string."""
        attrs = {}

        attr_pattern = re.compile(r'([\w-]+)="([^"]*)"')

        for match in attr_pattern.finditer(attrs_str):
            key, value = match.groups()
            attrs[key] = value

        return attrs


def css_select(root: HtmlElement, selector: str) -> list[HtmlElement]:
    """
    Select elements using CSS selector.

    Supports: tag, #id, .class, [attr], :first-child, etc.

    Args:
        root: Root element to search
        selector: CSS selector

    Returns:
        List of matching elements
    """
    selectors = _parse_css_selector(selector)
    return _match_selectors(root, selectors, 0)


def _parse_css_selector(selector: str) -> list[tuple[str, str, dict]]:
    """Parse CSS selector into component parts."""
    parts = []
    current_tag = None
    current_id = None
    current_classes = []
    current_attrs = {}

    tokens = re.findall(r'[#\.\[:]?[^\#\.\[:]+', selector)

    tag = selector.split()[0] if ' ' in selector else selector.split('#')[0].split('.')[0]

    if ' ' not in selector:
        if '#' in selector:
            tag, current_id = selector.split('#', 1)
            if '.' in current_id:
                current_id = current_id.split('.')[0]

        if '.' in selector:
            classes = selector.split('#')[0].split('.')[1:]
            current_classes = classes

    parts.append((tag or '*', current_id, current_classes, current_attrs))
    return parts


def _match_selectors(
    element: HtmlElement,
    selectors: list,
    index: int
) -> list[HtmlElement]:
    """Match selectors recursively."""
    if index >= len(selectors):
        return [element] if _matches_all(element, selectors[-1]) else []

    tag, id, classes, attrs = selectors[index]

    results = []

    if _matches_all(element, selectors[index]):
        if index == len(selectors) - 1:
            results.append(element)
        elif element.children:
            for child in element.children:
                results.extend(_match_selectors(child, selectors, index + 1))

    for child in element.children:
        results.extend(_match_selectors(child, selectors, index))

    return results


def _matches_all(element: HtmlElement, selector: tuple) -> bool:
    """Check if element matches selector criteria."""
    tag, id, classes, attrs = selector

    if tag != '*' and element.tag != tag:
        return False

    if id and element.id != id:
        return False

    if classes and not all(c in element.classes for c in classes):
        return False

    for attr, value in attrs.items():
        if attr.startswith(':'):
            continue
        if attr not in element.attributes:
            return False
        if value and element.attributes[attr] != value:
            return False

    return True


class HtmlAutomator:
    """
    HTML automation for web scraping and extraction.

    Example:
        automator = HtmlAutomator()

        # Extract links
        links = automator.extract_links(html, "a.article-link")

        # Extract tables
        rows = automator.extract_table(html, "table.data")

        # Clean HTML
        clean = automator.clean_html(html)
    """

    def __init__(self):
        self._parser = HtmlParser()

    def parse(self, html: str) -> HtmlElement:
        """Parse HTML into element tree."""
        return self._parser.parse(html)

    def extract_links(
        self,
        html: str,
        selector: str = "a"
    ) -> list[tuple[str, str]]:
        """
        Extract links from HTML.

        Args:
            html: HTML content
            selector: CSS selector for link elements

        Returns:
            List of (href, text) tuples
        """
        root = self.parse(html)
        links = []

        for element in root.query(selector):
            href = element.attributes.get("href", "")
            text = element.text.strip()
            links.append((href, text))

        return links

    def extract_images(
        self,
        html: str,
        selector: str = "img"
    ) -> list[tuple[str, str]]:
        """
        Extract images from HTML.

        Args:
            html: HTML content
            selector: CSS selector for image elements

        Returns:
            List of (src, alt) tuples
        """
        root = self.parse(html)
        images = []

        for element in root.query(selector):
            src = element.attributes.get("src", "")
            alt = element.attributes.get("alt", "")
            images.append((src, alt))

        return images

    def extract_table(
        self,
        html: str,
        selector: str = "table"
    ) -> list[dict[str, str]]:
        """
        Extract table data from HTML.

        Args:
            html: HTML content
            selector: CSS selector for table element

        Returns:
            List of row dictionaries
        """
        root = self.parse(html)
        table = root.query_one(selector)

        if not table:
            return []

        headers = []
        header_cells = table.query("thead th") or table.query("tr:first-child th")

        if header_cells:
            headers = [cell.text.strip() for cell in header_cells]

        rows = []
        data_rows = table.query("tbody tr") or table.query("tr")

        for row in data_rows:
            cells = row.query("td")
            if not cells:
                continue

            if not headers:
                headers = [f"col{i}" for i in range(len(cells))]

            row_data = {}
            for i, cell in enumerate(cells):
                key = headers[i] if i < len(headers) else f"col{i}"
                row_data[key] = cell.text.strip()

            rows.append(row_data)

        return rows

    def clean_html(self, html: str) -> str:
        """
        Clean HTML by removing scripts, styles, and comments.

        Args:
            html: HTML content

        Returns:
            Cleaned HTML
        """
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
        html = re.sub(r'\s+', ' ', html)

        return html.strip()

    def extract_text(self, html: str) -> str:
        """
        Extract plain text from HTML.

        Args:
            html: HTML content

        Returns:
            Plain text
        """
        root = self.parse(html)
        return self._extract_text_recursive(root)

    def _extract_text_recursive(self, element: HtmlElement) -> str:
        """Recursively extract text from elements."""
        text_parts = [element.text] if element.text else []

        for child in element.children:
            text_parts.append(self._extract_text_recursive(child))

        separator = " " if element.tag in ("p", "div", "tr") else ""
        return separator.join(t for t in text_parts if t).strip()


def create_html_automator() -> HtmlAutomator:
    """Factory to create HtmlAutomator."""
    return HtmlAutomator()
