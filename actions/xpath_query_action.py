"""
XPath Query Action Module.

Provides XPath 1.0 query execution on HTML/XML documents
with support for predicates, axes, and functions.

Example:
    >>> from xpath_query_action import XPathQuery
    >>> xpath = XPathQuery()
    >>> results = xpath.query(doc, "//div[@class='item']//a/@href")
    >>> xpath.extract(doc, {"link": "string(//a/@href)", "text": "string(//a)"})
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class XPathResult:
    """Result of XPath evaluation."""
    nodes: list[Any]
    type: str  # nodeset, string, number, boolean


class XPathQuery:
    """Execute XPath 1.0 queries on DOM trees."""

    NS_MAP = {
        "html": "http://www.w3.org/1999/xhtml",
    }

    def __init__(self):
        self._lxml_available = self._check_lxml()

    def _check_lxml(self) -> bool:
        try:
            from lxml import etree
            return True
        except ImportError:
            return False

    def query(self, doc: Any, xpath_str: str, namespaces: Optional[dict[str, str]] = None) -> XPathResult:
        """
        Execute XPath query.

        Args:
            doc: lxml Element or HTML string
            xpath_str: XPath expression
            namespaces: Optional namespace prefix map

        Returns:
            XPathResult with nodes and type
        """
        if isinstance(doc, str):
            from html_parser_action import HTMLParserAction
            parser = HTMLParserAction()
            parsed = parser.parse_string(doc)
            if self._lxml_available:
                doc = self._elem_to_lxml(parsed)
            else:
                return XPathResult(nodes=[], type="nodeset")

        if self._lxml_available:
            from lxml import etree
            ns = dict(self.NS_MAP)
            if namespaces:
                ns.update(namespaces)
            try:
                result = doc.xpath(xpath_str, namespaces=ns if ns else None)
                if isinstance(result, str):
                    return XPathResult(nodes=[result], type="string")
                elif isinstance(result, (int, float)):
                    return XPathResult(nodes=[result], type="number")
                elif isinstance(result, bool):
                    return XPathResult(nodes=[result], type="boolean")
                else:
                    return XPathResult(nodes=list(result), type="nodeset")
            except Exception:
                return XPathResult(nodes=[], type="nodeset")
        return XPathResult(nodes=[], type="nodeset")

    def _elem_to_lxml(self, elem) -> Any:
        from lxml import etree
        tag = elem.tag if hasattr(elem, 'tag') else str(elem)
        attrs = dict(elem.attrs) if hasattr(elem, 'attrs') else {}
        children = elem.children if hasattr(elem, 'children') else []
        text = elem.text if hasattr(elem, 'text') else ""
        return etree.Element(tag, **attrs)

    def query_nodes(self, doc: Any, xpath_str: str) -> list[Any]:
        """Execute XPath and return nodes."""
        result = self.query(doc, xpath_str)
        return result.nodes

    def query_string(self, doc: Any, xpath_str: str) -> str:
        """Execute XPath and return string result."""
        result = self.query(doc, xpath_str)
        if result.nodes:
            return str(result.nodes[0])
        return ""

    def query_number(self, doc: Any, xpath_str: str) -> float:
        """Execute XPath and return number."""
        result = self.query(doc, xpath_str)
        if result.nodes and isinstance(result.nodes[0], (int, float)):
            return float(result.nodes[0])
        return 0.0

    def query_boolean(self, doc: Any, xpath_str: str) -> bool:
        """Execute XPath and return boolean."""
        result = self.query(doc, xpath_str)
        if result.nodes:
            return bool(result.nodes[0])
        return False

    def extract(self, doc: Any, selectors: dict[str, str]) -> list[dict[str, Any]]:
        """
        Extract multiple fields using XPath selectors.

        Args:
            doc: Parsed document or HTML string
            selectors: Map of field_name -> xpath expression

        Returns:
            List of result dictionaries (one per match group)
        """
        results: list[dict[str, Any]] = []

        if not selectors:
            return results

        first_field = list(selectors.values())[0]
        base_xpath = self._get_base_xpath(first_field)

        if base_xpath:
            nodes = self.query_nodes(doc, base_xpath)
        else:
            nodes = [doc]

        for node in nodes:
            record: dict[str, Any] = {}
            for field_name, xpath_expr in selectors.items():
                clean_xpath = self._strip_base(xpath_expr, base_xpath)
                if clean_xpath.startswith("@"):
                    attr_name = clean_xpath[1:]
                    record[field_name] = self._get_attr(node, attr_name)
                elif clean_xpath.startswith("text()"):
                    record[field_name] = self._get_text(node)
                elif clean_xpath.startswith("string()"):
                    inner = re.search(r"string\((.+)\)", clean_xpath)
                    if inner:
                        record[field_name] = self.query_string(node, inner.group(1))
                else:
                    record[field_name] = self.query_string(node, clean_xpath)
            results.append(record)

        return results

    def _get_base_xpath(self, xpath_str: str) -> str:
        """Extract common base path from XPath."""
        parts = xpath_str.split("/")
        base_parts: list[str] = []
        for part in parts[:-1]:
            if part and not part.startswith("@") and not part.startswith("text") and not part.startswith("string"):
                base_parts.append(part)
            elif part == "":
                base_parts.append("")
            else:
                break
        return "/".join(base_parts)

    def _strip_base(self, xpath_str: str, base: str) -> str:
        if not base:
            return xpath_str
        if xpath_str.startswith(base):
            return xpath_str[len(base):].lstrip("/")
        return xpath_str

    def _get_attr(self, node: Any, attr: str) -> Optional[str]:
        if hasattr(node, 'get'):
            return node.get(attr)
        if hasattr(node, 'attrs') and isinstance(node.attrs, dict):
            return node.attrs.get(attr)
        return None

    def _get_text(self, node: Any) -> str:
        if hasattr(node, 'text'):
            return node.text or ""
        if hasattr(node, 'get_text'):
            return node.get_text() or ""
        return ""

    def filter(self, doc: Any, xpath_str: str, predicate_fn: Callable[[Any], bool]) -> list[Any]:
        """Filter XPath results with predicate function."""
        nodes = self.query_nodes(doc, xpath_str)
        return [n for n in nodes if predicate_fn(n)]

    def find_links(self, html: str) -> list[dict[str, str]]:
        """Extract all links using XPath."""
        return self.extract(html, {"href": "//a/@href", "text": "string(//a)", "title": "string(//a/@title)"})

    def find_images(self, html: str) -> list[dict[str, str]]:
        """Extract all images using XPath."""
        return self.extract(html, {"src": "//img/@src", "alt": "string(//img/@alt)", "title": "string(//img/@title)"})

    def find_tables(self, html: str) -> list[list[list[str]]]:
        """Extract table data using XPath."""
        tables: list[list[list[str]]] = []
        table_nodes = self.query_nodes(html, "//table")
        for table in table_nodes:
            rows: list[list[str]] = []
            row_nodes = self.query_nodes(table, ".//tr")
            for row in row_nodes:
                cells: list[str] = []
                cell_nodes = self.query_nodes(row, ".//td|.//th")
                for cell in cell_nodes:
                    cells.append(self._get_text(cell).strip())
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables


if __name__ == "__main__":
    xpath = XPathQuery()
    html = """
    <html>
        <body>
            <div class='items'>
                <a href='/1' title='First'>Link 1</a>
                <a href='/2' title='Second'>Link 2</a>
            </div>
        </body>
    </html>
    """
    links = xpath.find_links(html)
    print(f"Found {len(links)} links:")
    for link in links:
        print(f"  {link}")
