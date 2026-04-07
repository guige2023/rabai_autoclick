"""XPath action for XML/HTML navigation and extraction.

This module provides XPath-based navigation for XML and HTML
documents with support for predicates and axes.

Example:
    >>> action = XPathAction()
    >>> result = action.execute(doc=html_string, query="//div[@class='content']")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class XPathResult:
    """XPath query result."""
    nodes: list[Any]
    count: int
    query: str


class XPathAction:
    """XPath navigation and extraction action.

    Provides XPath query support for XML/HTML documents
    with common predicates and axis navigation.

    Example:
        >>> action = XPathAction()
        >>> result = action.execute(
        ...     doc="<html><body><div id='main'>Hello</div></body></html>",
        ...     query="//div[@id='main']/text()"
        ... )
    """

    def __init__(self) -> None:
        """Initialize XPath action."""
        self._doc: Optional[Any] = None

    def execute(
        self,
        doc: str,
        query: str,
        namespaces: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute XPath query.

        Args:
            doc: XML/HTML document string.
            query: XPath query expression.
            namespaces: Optional namespace prefix mappings.
            **kwargs: Additional parameters.

        Returns:
            Query result dictionary.

        Raises:
            ValueError: If document or query is invalid.
        """
        try:
            from lxml import etree
        except ImportError:
            return {
                "success": False,
                "error": "lxml not installed. Run: pip install lxml",
            }

        if not doc:
            raise ValueError("Document is required")
        if not query:
            raise ValueError("XPath query is required")

        result: dict[str, Any] = {"success": True, "query": query}

        try:
            parser = etree.HTMLParser() if not doc.strip().startswith("<?xml") else None
            if parser:
                tree = etree.HTML(doc)
            else:
                tree = etree.fromstring(doc.encode())

            self._doc = tree

            # Register namespaces if provided
            if namespaces:
                for prefix, uri in namespaces.items():
                    etree.register_namespace(prefix, uri)

            # Execute query
            nodes = tree.xpath(query, namespaces=namespaces)

            result["count"] = len(nodes)
            result["nodes"] = [self._serialize_node(n) for n in nodes]

            if len(nodes) == 1:
                result["value"] = result["nodes"][0]
            elif len(nodes) == 0:
                result["value"] = None

        except etree.XPathEvalError as e:
            result["success"] = False
            result["error"] = f"XPath error: {str(e)}"
        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _serialize_node(self, node: Any) -> Any:
        """Serialize XPath node to string.

        Args:
            node: XPath node.

        Returns:
            Serialized node value.
        """
        from lxml import etree

        if isinstance(node, etree._Element):
            return etree.tostring(node, encoding="unicode")
        elif isinstance(node, etree._ElementStringResult):
            return str(node)
        elif isinstance(node, etree._ElementUnicodeResult):
            return str(node)
        else:
            return node

    def find_elements(
        self,
        doc: str,
        tag: str,
        attrs: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Find elements by tag and attributes.

        Args:
            doc: HTML document.
            tag: Element tag name.
            attrs: Required attributes.

        Returns:
            Find result dictionary.
        """
        if attrs:
            attr_conditions = " and ".join(f"@{k}='{v}'" for k, v in attrs.items())
            query = f"//{tag}[{attr_conditions}]"
        else:
            query = f"//{tag}"

        return self.execute(doc=doc, query=query)

    def get_text(self, doc: str, query: str) -> list[str]:
        """Get text content from XPath results.

        Args:
            doc: Document.
            query: XPath query.

        Returns:
            List of text values.
        """
        text_query = f"{query}/text()"
        result = self.execute(doc=doc, query=text_query)
        return result.get("nodes", [])

    def get_attribute(
        self,
        doc: str,
        query: str,
        attr: str,
    ) -> list[str]:
        """Get attribute values from XPath results.

        Args:
            doc: Document.
            query: XPath query.
            attr: Attribute name.

        Returns:
            List of attribute values.
        """
        attr_query = f"{query}/@{attr}"
        result = self.execute(doc=doc, query=attr_query)
        return result.get("nodes", [])

    def get_parent(self, doc: str, query: str) -> dict[str, Any]:
        """Get parent elements.

        Args:
            doc: Document.
            query: Child element query.

        Returns:
            Parent result.
        """
        parent_query = f"{query}/parent::*"
        return self.execute(doc=doc, query=parent_query)

    def get_children(
        self,
        doc: str,
        query: str,
        tag: Optional[str] = None,
    ) -> dict[str, Any]:
        """Get child elements.

        Args:
            doc: Document.
            query: Parent element query.
            tag: Optional child tag filter.

        Returns:
            Children result.
        """
        if tag:
            child_query = f"{query}/{tag}"
        else:
            child_query = f"{query}/*"
        return self.execute(doc=doc, query=child_query)
