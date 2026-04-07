"""XML parser action for XML document processing.

This module provides XML parsing with namespace support,
XPATH queries, and element manipulation.

Example:
    >>> action = XMLParserAction()
    >>> result = action.execute(operation="parse", data='<root><item>Value</item></root>')
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class XMLNode:
    """Represents an XML node."""
    tag: str
    text: Optional[str]
    attributes: dict[str, str]
    children: list["XMLNode"]


class XMLParserAction:
    """XML parsing and manipulation action.

    Provides XML parsing with namespace handling,
    element finding, and structure navigation.

    Example:
        >>> action = XMLParserAction()
        >>> result = action.execute(
        ...     operation="find",
        ...     xml="<root><item id='1'>Value</item></root>",
        ...     tag="item"
        ... )
    """

    def __init__(self) -> None:
        """Initialize XML parser."""
        self._tree: Optional[Any] = None

    def execute(
        self,
        operation: str,
        data: Optional[str] = None,
        tag: Optional[str] = None,
        path: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute XML operation.

        Args:
            operation: Operation (parse, find, to_dict, etc.).
            data: XML data string.
            tag: Element tag name.
            path: Element path.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        try:
            from lxml import etree
        except ImportError:
            return {
                "success": False,
                "error": "lxml not installed. Run: pip install lxml",
            }

        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "parse":
            if not data:
                raise ValueError("data required for 'parse'")
            result.update(self._parse_xml(data))

        elif op == "find":
            if not tag:
                raise ValueError("tag required for 'find'")
            result.update(self._find_elements(tag))

        elif op == "find_one":
            if not tag:
                raise ValueError("tag required for 'find_one'")
            result.update(self._find_element(tag))

        elif op == "to_dict":
            result.update(self._xml_to_dict())

        elif op == "to_json":
            result.update(self._xml_to_json())

        elif op == "get_text":
            if not tag:
                raise ValueError("tag required")
            result["text"] = self._get_text(tag)

        elif op == "get_attr":
            if not tag or not path:
                raise ValueError("tag and path required")
            attr = kwargs.get("attr")
            if not attr:
                raise ValueError("attr required")
            result["value"] = self._get_attribute(tag, attr)

        elif op == "validate":
            if not data:
                raise ValueError("data required")
            result.update(self._validate_xml(data))

        elif op == "beautify":
            if not data:
                raise ValueError("data required")
            result["xml"] = self._beautify_xml(data)

        elif op == "minify":
            if not data:
                raise ValueError("data required")
            result["xml"] = self._minify_xml(data)

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _parse_xml(self, data: str) -> dict[str, Any]:
        """Parse XML data.

        Args:
            data: XML string.

        Returns:
            Result dictionary.
        """
        from lxml import etree

        try:
            self._tree = etree.fromstring(data.encode())
            return {
                "parsed": True,
                "root": self._tree.tag,
            }
        except etree.XMLSyntaxError as e:
            return {"success": False, "error": f"XML syntax error: {e}"}

    def _find_elements(self, tag: str) -> dict[str, Any]:
        """Find all elements by tag.

        Args:
            tag: Element tag name.

        Returns:
            Result dictionary.
        """
        if not self._tree:
            return {"success": False, "error": "No parsed XML. Run 'parse' first."}

        elements = self._tree.findall(f".//{tag}")
        return {
            "count": len(elements),
            "elements": [self._element_to_dict(el) for el in elements],
        }

    def _find_element(self, tag: str) -> dict[str, Any]:
        """Find first element by tag.

        Args:
            tag: Element tag name.

        Returns:
            Result dictionary.
        """
        if not self._tree:
            return {"success": False, "error": "No parsed XML. Run 'parse' first."}

        element = self._tree.find(f".//{tag}")
        if element is not None:
            return {"found": True, "element": self._element_to_dict(element)}
        return {"found": False}

    def _element_to_dict(self, element: Any) -> dict[str, Any]:
        """Convert element to dictionary.

        Args:
            element: lxml element.

        Returns:
            Element dictionary.
        """
        return {
            "tag": element.tag,
            "text": (element.text or "").strip(),
            "attributes": dict(element.attrib),
            "children": [self._element_to_dict(child) for child in element],
        }

    def _xml_to_dict(self) -> dict[str, Any]:
        """Convert XML to dictionary.

        Returns:
            Result dictionary.
        """
        if not self._tree:
            return {"success": False, "error": "No parsed XML"}

        def etree_to_dict(t):
            return {
                t.tag: (
                    t.text.strip() if (len(t) == 0 and t.text) else
                    [etree_to_dict(child) for child in t]
                ) or (
                    dict(t.attrib) if t.attrib else None
                )
            }

        return {"dict": etree_to_dict(self._tree)}

    def _xml_to_json(self) -> dict[str, Any]:
        """Convert XML to JSON.

        Returns:
            Result dictionary.
        """
        import json

        if not self._tree:
            return {"success": False, "error": "No parsed XML"}

        def etree_to_dict(t):
            d = {"#tag": t.tag}
            if t.attrib:
                d["@attributes"] = dict(t.attrib)
            if t.text and t.text.strip():
                d["#text"] = t.text.strip()
            children = [etree_to_dict(child) for child in t]
            if children:
                d["children"] = children
            return d

        import json
        return {"json": json.dumps(etree_to_dict(self._tree), indent=2)}

    def _get_text(self, tag: str) -> Optional[str]:
        """Get text content of element.

        Args:
            tag: Element tag.

        Returns:
            Text content or None.
        """
        if not self._tree:
            return None
        element = self._tree.find(f".//{tag}")
        return element.text.strip() if element is not None and element.text else None

    def _get_attribute(self, tag: str, attr: str) -> Optional[str]:
        """Get attribute value.

        Args:
            tag: Element tag.
            attr: Attribute name.

        Returns:
            Attribute value or None.
        """
        if not self._tree:
            return None
        element = self._tree.find(f".//{tag}")
        return element.get(attr) if element is not None else None

    def _validate_xml(self, data: str) -> dict[str, Any]:
        """Validate XML syntax.

        Args:
            data: XML string.

        Returns:
            Validation result.
        """
        from lxml import etree

        try:
            etree.fromstring(data.encode())
            return {"valid": True}
        except etree.XMLSyntaxError as e:
            return {"valid": False, "error": str(e)}

    def _beautify_xml(self, data: str) -> str:
        """Beautify XML with indentation.

        Args:
            data: XML string.

        Returns:
            Beautified XML.
        """
        from lxml import etree
        import io

        tree = etree.parse(io.StringIO(data))
        return etree.tostring(tree, pretty_print=True, encoding="unicode")

    def _minify_xml(self, data: str) -> str:
        """Minify XML by removing whitespace.

        Args:
            data: XML string.

        Returns:
            Minified XML.
        """
        import re
        # Remove whitespace between tags
        return re.sub(r">\s+<", "><", data.strip())
