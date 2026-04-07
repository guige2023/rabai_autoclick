"""
HTML Parsing and DOM Navigation Action Module.

Provides utilities for parsing HTML documents, navigating DOM trees,
extracting elements by CSS selectors and XPath, and handling malformed HTML.

Example:
    >>> from html_parser_action import HTMLParserAction
    >>> parser = HTMLParserAction()
    >>> doc = parser.parse_string("<html><body><div class='item'>Text</div></body></html>")
    >>> elements = parser.query_selector_all(doc, ".item")
    >>> text = parser.get_text(elements[0])
"""
from __future__ import annotations

import re
from html.parser import HTMLParser as StdHTMLParser
from typing import Any, Callable, Optional
from dataclasses import dataclass, field


@dataclass
class HTMLElement:
    """Represents a DOM element."""
    tag: str
    attrs: dict[str, str]
    text: str = ""
    children: list[HTMLElement] = field(default_factory=list)
    parent: Optional[HTMLElement] = None

    def query_selector(self, selector: str) -> Optional[HTMLElement]:
        """Return first matching element for CSS selector."""
        return CSSSelector(selector).match(self)

    def query_selector_all(self, selector: str) -> list[HTMLElement]:
        """Return all matching elements for CSS selector."""
        return CSSSelector(selector).match_all(self)

    def get_attribute(self, name: str) -> Optional[str]:
        """Get attribute value by name."""
        return self.attrs.get(name)

    def get_text(self, deep: bool = True) -> str:
        """Get text content, optionally including child text."""
        if deep:
            parts = [self.text] + [c.get_text(True) for c in self.children]
            return "".join(parts).strip()
        return self.text.strip()


@dataclass
class ParsedDocument:
    """Complete parsed HTML document."""
    root: HTMLElement
    errors: list[str] = field(default_factory=list)


class CSSSelector:
    """CSS selector matcher supporting tag, class, id, attribute selectors."""

    def __init__(self, selector: str):
        self.selector = selector.strip()
        self._parse()

    def _parse(self) -> None:
        self.tag = "*"
        self.cls: Optional[str] = None
        self.id: Optional[str] = None
        self.attr_conditions: list[tuple[str, str, str]] = []

        parts = self.selector.split()
        if not parts:
            return

        first = parts[0]
        if first.startswith("."):
            self.cls = first[1:]
        elif first.startswith("#"):
            self.id = first[1:]
        elif first != "*":
            self.tag = first

        for part in parts[1:]:
            if part.startswith("."):
                self.cls = part[1:]
            elif part.startswith("#"):
                self.id = part[1:]
            elif "=" in part:
                attr, _, value = part.partition("=")
                self.attr_conditions.append((attr, "!=", value.strip('"\'')))

    def _matches(self, elem: HTMLElement) -> bool:
        if self.tag != "*" and elem.tag.lower() != self.tag.lower():
            return False
        if self.id and elem.attrs.get("id") != self.id:
            return False
        if self.cls:
            classes = elem.attrs.get("class", "").split()
            if self.cls not in classes:
                return False
        for attr, op, val in self.attr_conditions:
            actual = elem.attrs.get(attr, "")
            if op == "!=" and actual == val:
                return False
        return True

    def match(self, root: HTMLElement) -> Optional[HTMLElement]:
        """Return first matching element in tree."""
        for elem in self._iter(root):
            if self._matches(elem):
                return elem
        return None

    def match_all(self, root: HTMLElement) -> list[HTMLElement]:
        """Return all matching elements in tree."""
        return [e for e in self._iter(root) if self._matches(e)]

    def _iter(self, elem: HTMLElement) -> Any:
        yield elem
        for child in elem.children:
            yield from self._iter(child)


class HTMLParserAction:
    """Action for parsing and querying HTML documents."""

    def __init__(self, strict: bool = False):
        self.strict = strict
        self._builder = _DOMBuilder(strict=strict)

    def parse_string(self, html: str) -> ParsedDocument:
        """
        Parse HTML string into document.

        Args:
            html: HTML content string

        Returns:
            ParsedDocument with root element and any parse errors
        """
        builder = _DOMBuilder(strict=False)
        errors: list[str] = []
        try:
            parser = StdHTMLParser()
            parser.handle_starttag = builder.handle_starttag
            parser.handle_endtag = builder.handle_endtag
            parser.handle_data = builder.handle_data
            parser.handle_comment = builder.handle_comment
            parser.error = lambda msg: errors.append(str(msg)) if not self.strict else None
            parser.feed(html)
            parser.close()
            root = builder.finalize()
            return ParsedDocument(root=root, errors=errors)
        except Exception as e:
            errors.append(f"Parse error: {e}")
            return ParsedDocument(root=HTMLElement(tag="html", attrs={}), errors=errors)

    def parse_file(self, path: str) -> ParsedDocument:
        """Parse HTML from file path."""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return self.parse_string(f.read())

    def query_selector(self, doc: ParsedDocument, selector: str) -> Optional[HTMLElement]:
        """Return first element matching CSS selector."""
        sel = CSSSelector(selector)
        return sel.match(doc.root)

    def query_selector_all(self, doc: ParsedDocument, selector: str) -> list[HTMLElement]:
        """Return all elements matching CSS selector."""
        sel = CSSSelector(selector)
        return sel.match_all(doc.root)

    def get_text(self, elem: HTMLElement, deep: bool = True) -> str:
        """Get text content of element."""
        return elem.get_text(deep=deep)

    def get_attribute(self, elem: HTMLElement, name: str) -> Optional[str]:
        """Get element attribute."""
        return elem.get_attribute(name)

    def outer_html(self, elem: HTMLElement) -> str:
        """Serialize element back to HTML string."""
        return _serialize(elem)

    def strip_tags(self, html: str) -> str:
        """Remove all HTML tags, leaving only text."""
        doc = self.parse_string(html)
        return self.get_text(doc.root)


class _DOMBuilder:
    def __init__(self, strict: bool):
        self.strict = strict
        self.root = HTMLElement(tag="root", attrs={})
        self.stack: list[HTMLElement] = [self.root]
        self.current: Optional[HTMLElement] = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]):
        attrs_dict = {k: (v or "") for k, v in attrs}
        elem = HTMLElement(tag=tag.lower(), attrs=attrs_dict)
        elem.parent = self.stack[-1]
        self.stack[-1].children.append(elem)
        if tag.lower() not in {"br", "hr", "img", "input", "meta", "link"}:
            self.stack.append(elem)
        self.current = elem

    def handle_endtag(self, tag: str):
        if self.stack[-1].tag == tag.lower() and len(self.stack) > 1:
            self.stack.pop()
        if self.stack:
            self.current = self.stack[-1]

    def handle_data(self, data: str):
        if self.current is not None:
            self.current.text += data

    def handle_comment(self, data: str):
        pass

    def finalize(self) -> HTMLElement:
        return self.root.children[0] if self.root.children else HTMLElement(tag="html", attrs={})


def _serialize(elem: HTMLElement) -> str:
    """Serialize element to HTML string."""
    attrs = "".join(f' {k}="{v}"' for k, v in elem.attrs.items())
    inner = elem.text + "".join(_serialize(c) for c in elem.children)
    return f"<{elem.tag}{attrs}>{inner}</{elem.tag}>"


class XPathSelector:
    """XPath-based element selector."""

    XPATH_RE = re.compile(r"//?(\w+)\[(\d+)\]|//?(\w+)|/@(\w+)")

    def __init__(self, xpath: str):
        self.xpath = xpath

    def match(self, root: HTMLElement) -> Optional[HTMLElement]:
        """Return first matching element."""
        results = self.match_all(root)
        return results[0] if results else None

    def match_all(self, root: HTMLElement) -> list[HTMLElement]:
        """Return all matching elements."""
        if self.xpath == "//*":
            return self._all_elements(root)
        parts = self.xpath.strip("/").split("/")

        current: list[HTMLElement] = [root]
        for part in parts:
            part = part.strip()
            if not part:
                continue
            next_elems: list[HTMLElement] = []
            tag_match = re.match(r"(\w+)\[(\d+)\]", part)
            if tag_match:
                tag, idx = tag_match.group(1), int(tag_match.group(2)) - 1
                for elem in current:
                    matches = [c for c in elem.children if c.tag.lower() == tag.lower()]
                    if 0 <= idx < len(matches):
                        next_elems.append(matches[idx])
            else:
                tag = part.replace("//", "").replace("*", "")
                for elem in current:
                    if tag == "*":
                        next_elems.extend(elem.children)
                    else:
                        next_elems.extend(c for c in elem.children if c.tag.lower() == tag.lower())
            current = next_elems
        return current

    def _all_elements(self, root: HTMLElement) -> list[HTMLElement]:
        results: list[HTMLElement] = []
        results.append(root)
        for child in root.children:
            results.extend(self._all_elements(child))
        return results


if __name__ == "__main__":
    parser = HTMLParserAction()
    doc = parser.parse_string("<html><body><div class='items'><span id='first'>Hello</span><span>World</span></div></body></html>")
    elem = parser.query_selector(doc, "#first")
    if elem:
        print(f"Found: {elem.get_text()}")
    all_spans = parser.query_selector_all(doc, "span")
    print(f"Found {len(all_spans)} spans")
